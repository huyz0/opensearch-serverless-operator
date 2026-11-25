package controller

import (
	"context"
	"reflect"
	"time"

	coreapi "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	corev1 "k8s.io/client-go/kubernetes/typed/core/v1"
	discoveryv1 "k8s.io/client-go/kubernetes/typed/discovery/v1"
	policyv1 "k8s.io/client-go/kubernetes/typed/policy/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	"github.com/huyz0/opensearch-serverless-operator/api/v1alpha1"
)

// OperationReconciler validates operation intent and runs the enabled
// development operation handlers.
type OperationReconciler struct {
	client.Client
	Core                         corev1.CoreV1Interface
	Discovery                    discoveryv1.DiscoveryV1Interface
	Policy                       policyv1.PolicyV1Interface
	DevelopmentSettingsImage     string
	accountBackend               securityAccountBackend
	SettingsBackend              SettingsBackend
	uncachedOperationReader      client.Reader
	DevelopmentRetryProvisioning bool
	RolloutReader                client.Reader
	RolloutInstanceName          string
	RolloutEvidence              rolloutEvidenceReader
	RolloutPodDomain             func(*v1alpha1.OpenSearchServerless) (string, error)
	weightedRoutes               map[string]string
	rolloutAuthSecrets           map[string]string
	rolloutRouteURLs             map[string]string
	weightedRuntimeSupported     func(*v1alpha1.OpenSearchServerless) bool
	weightedEvidence             weightStepEvidence
	rolloutSourceProbe           func(context.Context, *v1alpha1.OpenSearchServerless, *coreapi.Pod, string) error
	writerRetirement             writerRetirementBackend
	Now                          func() time.Time
}

func (r *OperationReconciler) updateOperationFinalizer(ctx context.Context, operation *v1alpha1.OpenSearchServerlessOperation, name string, add bool) (ctrl.Result, error) {
	before := operation.DeepCopyObject().(*v1alpha1.OpenSearchServerlessOperation)
	if add {
		controllerutil.AddFinalizer(operation, name)
	} else {
		controllerutil.RemoveFinalizer(operation, name)
	}
	if err := r.Patch(ctx, operation, client.MergeFromWithOptions(before, client.MergeFromWithOptimisticLock{})); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	return ctrl.Result{Requeue: true}, nil
}

// Reconcile dispatches to the enabled development handler or records that
// the requested operation has no available handler.
func (r *OperationReconciler) Reconcile(ctx context.Context, request ctrl.Request) (ctrl.Result, error) {
	var operation v1alpha1.OpenSearchServerlessOperation
	if err := r.Get(ctx, request.NamespacedName, &operation); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	if operation.Spec.Action == "ApplyClusterSettings" && (r.DevelopmentSettingsImage != "" ||
		controllerutil.ContainsFinalizer(&operation, settingsFinalizer) || settingsOutcomeRecorded(operation.Status.Phase)) {
		return r.reconcileSettingsOperation(ctx, &operation)
	}
	if operation.Spec.Action == "ApplySecurityConfiguration" && operation.Spec.SecurityConfiguration != nil && operation.Spec.SecurityConfiguration.Account != nil &&
		(r.accountBackend != nil || controllerutil.ContainsFinalizer(&operation, securityAccountFinalizer)) {
		return r.reconcileSecurityAccount(ctx, &operation)
	}
	if operation.Spec.Action == "RetryProvisioning" && r.DevelopmentRetryProvisioning {
		return r.reconcileProvisioningRetry(ctx, &operation)
	}
	if operation.Spec.Action == "RetireWriter" && (r.writerRetirement != nil || controllerutil.ContainsFinalizer(&operation, writerRetirementFinalizer)) {
		return r.reconcileWriterRetirement(ctx, request.NamespacedName)
	}
	if (operation.Spec.Action == "Promote" || operation.Spec.Action == "Rollback") &&
		(r.RolloutInstanceName == "" || operation.Spec.InstanceRef.Name == r.RolloutInstanceName) &&
		r.RolloutReader != nil && r.RolloutEvidence != nil && r.RolloutPodDomain != nil {
		return r.reconcileFleetOperation(ctx, request.NamespacedName)
	}
	if operation.Status.Phase == "Succeeded" {
		return ctrl.Result{}, nil
	}
	before := operation.DeepCopyObject().(*v1alpha1.OpenSearchServerlessOperation)
	condition := metav1.Condition{Type: "Ready", Status: metav1.ConditionFalse, ObservedGeneration: operation.Generation,
		Reason: "NotImplemented", Message: "Operation execution is not implemented"}
	if err := operation.Spec.Validate(); err != nil {
		condition.Reason = "InvalidSpec"
		condition.Message = err.Error()
	} else {
		var instance v1alpha1.OpenSearchServerless
		err := r.Get(ctx, types.NamespacedName{Namespace: operation.Namespace, Name: operation.Spec.InstanceRef.Name}, &instance)
		if client.IgnoreNotFound(err) != nil {
			return ctrl.Result{}, err
		}
		if err != nil || string(instance.UID) != operation.Spec.InstanceRef.UID {
			condition.Reason = "InstanceMismatch"
			condition.Message = "Referenced instance is missing or has a different UID"
		}
	}
	operation.Status.ObservedGeneration = operation.Generation
	if operation.Status.Phase == "" {
		operation.Status.Phase = "Pending"
	}
	meta.SetStatusCondition(&operation.Status.Conditions, condition)
	if reflect.DeepEqual(before.Status, operation.Status) {
		return ctrl.Result{RequeueAfter: time.Minute}, nil
	}
	if err := r.Status().Patch(ctx, &operation, client.MergeFromWithOptions(before, client.MergeFromWithOptimisticLock{})); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	return ctrl.Result{RequeueAfter: time.Minute}, nil
}

func (r *OperationReconciler) operationReader() client.Reader {
	if r.uncachedOperationReader != nil {
		return r.uncachedOperationReader
	}
	return r.Client
}

// SetupWithManager registers the operation watch.
func (r *OperationReconciler) SetupWithManager(manager ctrl.Manager) error {
	r.uncachedOperationReader = manager.GetAPIReader()
	if r.RolloutEvidence != nil && r.RolloutReader == nil {
		r.RolloutReader = manager.GetAPIReader()
	}
	return ctrl.NewControllerManagedBy(manager).For(&v1alpha1.OpenSearchServerlessOperation{}).Complete(r)
}
