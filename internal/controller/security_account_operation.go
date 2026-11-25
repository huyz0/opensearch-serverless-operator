package controller

import (
	"context"
	"fmt"
	"maps"
	"reflect"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	"github.com/huyz0/opensearch-serverless-operator/api/v1alpha1"
	"github.com/huyz0/opensearch-serverless-operator/internal/runtimeconfig"
	"github.com/huyz0/opensearch-serverless-operator/internal/security"
)

const (
	securityAccountFinalizer = "serverless.opensearch.org/security-account"
	accountPrepared          = "AccountPrepared"
	accountWriting           = "AccountWriting"
)

func validateSecurityAccountChange(change *v1alpha1.SecurityConfigurationChange) bool {
	if change == nil {
		return false
	}
	account := change.Account
	if account == nil || !security.ValidAccountName(account.Username) || account.Username == runtimeconfig.BasicAuthUser ||
		change.CandidateRevision == change.ExpectedActiveRevision || len(change.CandidateRevision) > 96 || len(change.ExpectedActiveRevision) > 160 ||
		(account.PreviousPasswordRef == nil && account.DesiredPasswordRef == nil) {
		return false
	}
	for _, ref := range []*v1alpha1.SecretKeyReference{account.PreviousPasswordRef, account.DesiredPasswordRef} {
		if ref != nil && (ref.Name == "" || ref.Key == "") {
			return false
		}
	}
	return true
}

func (r *OperationReconciler) reconcileSecurityAccount(ctx context.Context, event *v1alpha1.OpenSearchServerlessOperation) (ctrl.Result, error) {
	reader := r.operationReader()
	var op v1alpha1.OpenSearchServerlessOperation
	if err := reader.Get(ctx, client.ObjectKeyFromObject(event), &op); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	if op.UID != event.UID {
		return ctrl.Result{Requeue: true}, nil
	}
	before := op.DeepCopyObject().(*v1alpha1.OpenSearchServerlessOperation)
	status := func(phase, reason, message string) (ctrl.Result, error) {
		op.Status.Phase, op.Status.ObservedGeneration = phase, op.Generation
		condition := metav1.ConditionFalse
		if phase == "Succeeded" {
			condition = metav1.ConditionTrue
		}
		meta.SetStatusCondition(&op.Status.Conditions, metav1.Condition{Type: "Ready", Status: condition, Reason: reason, Message: message, ObservedGeneration: op.Generation})
		if reflect.DeepEqual(op.Status, before.Status) {
			return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
		}
		if err := r.Status().Patch(ctx, &op, client.MergeFromWithOptions(before, client.MergeFromWithOptimisticLock{})); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{Requeue: true}, nil
	}
	change := op.Spec.SecurityConfiguration
	if err := op.Spec.Validate(); err != nil || !validateSecurityAccountChange(change) {
		return status(op.Status.Phase, "InvalidAccountChange", "Expected a revision transition and one unreserved account with approved password references")
	}
	var instance v1alpha1.OpenSearchServerless
	instanceKey := client.ObjectKey{Namespace: op.Namespace, Name: op.Spec.InstanceRef.Name}
	err := reader.Get(ctx, instanceKey, &instance)
	if err != nil && !apierrors.IsNotFound(err) {
		return ctrl.Result{}, err
	}
	finalized := controllerutil.ContainsFinalizer(&op, securityAccountFinalizer)
	if err != nil || string(instance.UID) != op.Spec.InstanceRef.UID || instance.DeletionTimestamp != nil {
		if op.Status.Phase == accountWriting && finalized {
			return status(accountWriting, "InstanceUnavailable", "The original instance is needed to verify a possible account write")
		}
		if finalized {
			return r.updateOperationFinalizer(ctx, &op, securityAccountFinalizer, false)
		}
		return status(op.Status.Phase, "InstanceMismatch", "The original live instance is unavailable")
	}
	claim := instance.Status.ActiveOperation
	deleting := op.DeletionTimestamp != nil
	owned := claim != nil && claim.UID == string(op.UID) && claim.Name == op.Name && claim.Action == op.Spec.Action &&
		(claim.Generation == op.Generation || (deleting && claim.Generation > 0 && claim.Generation <= op.Generation))
	patchInstance := func(previous *v1alpha1.OpenSearchServerless) (ctrl.Result, error) {
		if err := r.Status().Patch(ctx, &instance, client.MergeFromWithOptions(previous, client.MergeFromWithOptimisticLock{})); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{Requeue: true}, nil
	}
	targetRevision := change.CandidateRevision + "/" + string(op.UID)
	if op.Status.Phase == "Succeeded" || op.Status.Phase == "Cancelled" || op.Status.Phase == "Conflict" || (deleting && !owned && op.Status.Phase != accountWriting) {
		if owned {
			previous := instance.DeepCopyObject().(*v1alpha1.OpenSearchServerless)
			instance.Status.ActiveOperation = nil
			return patchInstance(previous)
		}
		if finalized {
			if deleting && claim == nil && op.Status.Phase != "Succeeded" {
				fence := string(op.UID)
				if last := meta.FindStatusCondition(instance.Status.Conditions, "SecurityClaimFence"); last == nil || last.Message != fence {
					previous := instance.DeepCopyObject().(*v1alpha1.OpenSearchServerless)
					meta.SetStatusCondition(&instance.Status.Conditions, metav1.Condition{Type: "SecurityClaimFence", Status: metav1.ConditionTrue, Reason: "OperationDeleting", Message: fence, ObservedGeneration: instance.Generation})
					return patchInstance(previous)
				}
			}
			return r.updateOperationFinalizer(ctx, &op, securityAccountFinalizer, false)
		}
		return ctrl.Result{}, nil
	}
	if owned && instance.Status.ActiveSecurityRevision == targetRevision {
		return status("Succeeded", "AccountObserved", "The account change was verified on the observed Java cohort")
	}
	if deleting && owned && op.Status.Phase != accountWriting {
		return status("Cancelled", "CancelledBeforeDispatch", "Deletion was observed before a write was recorded")
	}
	activeRevision := instance.Status.ActiveSecurityRevision
	if activeRevision == "" {
		activeRevision = "initial"
	}
	if activeRevision != change.ExpectedActiveRevision {
		if op.Status.Phase == accountWriting {
			return status(accountWriting, "WriteOutcomeUnknown", "The expected security revision changed after write reservation")
		}
		return status("Conflict", "RevisionMismatch", "Expected active security revision differs from the live instance")
	}
	backend := r.accountBackend

	if backend == nil {
		return status(op.Status.Phase, "UnqualifiedRuntime", "The approved account operation backend is unavailable")
	}
	if claim == nil && (op.Status.Phase == "" || op.Status.Phase == "Pending") {
		if instance.Status.FleetRetirement != nil {
			return status("Pending", "FleetRetirementReserved", "The instance is reserved for fleet retirement")
		}
		if instance.Status.PendingPodCreation != nil || instance.Status.PendingGateRelease != nil || instance.Status.ProvisioningAttempt != nil || instance.Status.SearchWarmupSlot != nil {
			return status("Pending", "InstanceBusy", "Pod provisioning and search warm-up must settle before claiming")
		}
		if _, err := backend.resolve(ctx, &instance, change.Account); err != nil {
			return status("Pending", "InputsUnavailable", "The managed Basic Auth runtime and immutable account credentials must qualify")
		}
		if !finalized {
			return r.updateOperationFinalizer(ctx, &op, securityAccountFinalizer, true)
		}
		var latest v1alpha1.OpenSearchServerlessOperation
		if err := reader.Get(ctx, client.ObjectKeyFromObject(&op), &latest); err != nil {
			return ctrl.Result{}, client.IgnoreNotFound(err)
		}
		if latest.UID != op.UID || latest.ResourceVersion != op.ResourceVersion {
			return ctrl.Result{Requeue: true}, nil
		}
		if err := ClaimOperation(ctx, r.Client, &instance, &op); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{Requeue: true}, nil
	}
	if !owned || !finalized {
		return status(op.Status.Phase, "ClaimUnavailable", "An owned instance claim and operation finalizer are required")
	}
	return r.advanceClaimedSecurityAccount(ctx, &op, &instance, targetRevision, status)
}

// advanceClaimedSecurityAccount prepares or verifies a write for an operation
// that already owns the instance claim and its finalizer.
func (r *OperationReconciler) advanceClaimedSecurityAccount(ctx context.Context, op *v1alpha1.OpenSearchServerlessOperation, instance *v1alpha1.OpenSearchServerless, targetRevision string, status func(string, string, string) (ctrl.Result, error)) (ctrl.Result, error) {
	account := op.Spec.SecurityConfiguration.Account
	backend := r.accountBackend
	input, err := backend.resolve(ctx, instance, account)
	if err != nil {
		return status(op.Status.Phase, "InputsUnavailable", "Reconciliation requires the qualified runtime and original immutable password Secrets")
	}
	known := op.Status.SecurityAccountSources
	if op.Status.Phase == accountWriting || op.Status.Phase == accountPrepared {
		if known == nil || known.PreviousSecretUID != input.sources.PreviousSecretUID || known.DesiredSecretUID != input.sources.DesiredSecretUID || len(known.Consumers) == 0 {
			return status(op.Status.Phase, "InputIdentityChanged", "The original credential Secrets and Java cohort are required")
		}
	}
	cohort, err := backend.verify(ctx, instance, input, account.Username, op.Status.Phase == accountWriting)
	if err != nil || len(cohort) == 0 {
		if op.Status.Phase == accountWriting {
			return status(accountWriting, "WriteOutcomeUnknown", "Account record and cohort have not converged; the write will not be replayed")
		}
		return status(op.Status.Phase, "PreviousStateUnverified", "The previous account state must be observed in the store and across the Java cohort")
	}
	switch op.Status.Phase {
	case "", "Pending":
		input.sources.Consumers = cohort
		op.Status.SecurityAccountSources = &input.sources
		return status(accountPrepared, "PreviousStateObserved", "Previous account state and credential input identities are recorded")
	case accountWriting:
		for uid, process := range known.Consumers {
			if cohort[uid] != process {
				return status(accountWriting, "ConsumerMissing", "A prior Java process cannot be verified or shown to be fenced")
			}
		}
		previous := instance.DeepCopy()
		instance.Status.ActiveSecurityRevision = targetRevision
		if err := r.Status().Patch(ctx, instance, client.MergeFromWithOptions(previous, client.MergeFromWithOptimisticLock{})); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{Requeue: true}, nil
	case accountPrepared:
		if !maps.Equal(known.Consumers, cohort) {
			return status("Conflict", "CohortChangedBeforeDispatch", "The Java cohort changed before write reservation")
		}
	default:
		return ctrl.Result{}, fmt.Errorf("unknown account operation phase")
	}
	var latest v1alpha1.OpenSearchServerless
	if err := r.operationReader().Get(ctx, client.ObjectKeyFromObject(instance), &latest); err != nil {
		return ctrl.Result{}, err
	}
	if latest.UID != instance.UID || latest.ResourceVersion != instance.ResourceVersion {
		return ctrl.Result{Requeue: true}, nil
	}
	// The optimistic operation patch is the only dispatch permit. A
	// reconciler that reads Writing may verify but never write again.
	if _, err := status(accountWriting, "WriteRecorded", "One Java account write was reserved; read-back is required"); err != nil {
		return ctrl.Result{}, err
	}
	if err := backend.apply(ctx, instance, input, account.Username); err != nil {
		return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
	}
	return ctrl.Result{Requeue: true}, nil
}
