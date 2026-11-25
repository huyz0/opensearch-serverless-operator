// Package controller contains operator reconciliation.
package controller

import (
	"context"
	"crypto/ed25519"
	"net/http"
	"reflect"
	"time"

	coreapi "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/dynamic"
	corev1 "k8s.io/client-go/kubernetes/typed/core/v1"
	discoveryv1 "k8s.io/client-go/kubernetes/typed/discovery/v1"
	policyv1 "k8s.io/client-go/kubernetes/typed/policy/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/huyz0/opensearch-serverless-operator/api/v1alpha1"
	"github.com/huyz0/opensearch-serverless-operator/internal/java"
	"github.com/huyz0/opensearch-serverless-operator/internal/storage"
)

// InstanceReconciler evaluates the currently supported instance API subset.
type InstanceReconciler struct {
	client.Client
	APIReader              client.Reader
	Core                   corev1.CoreV1Interface
	Policy                 policyv1.PolicyV1Interface
	Discovery              discoveryv1.DiscoveryV1Interface
	Certificates           dynamic.Interface
	PeerCertificateIssuers map[string]bool
	HTTPCertificateIssuers map[string]bool
	// DevelopmentGatedPods permits creation of held Pods; it never releases them.
	DevelopmentGatedPods bool
	// DevelopmentReleaseGates allows the development-only, bounded release path.
	DevelopmentReleaseGates         bool
	DevelopmentObserveEndpoints     bool
	DevelopmentEmergencyFailback    bool
	DevelopmentSearchScaleDown      bool
	DevelopmentServiceProbeImage    string
	ServiceProbeClient              *http.Client
	DevelopmentPendingTimeout       time.Duration
	DevelopmentReleaseWindow        time.Duration
	DevelopmentReleaseLimit         int
	DevelopmentWarmupImage          string
	DevelopmentRuntimeSettingsImage string
	DevelopmentS3WebIdentityImage   string
	DevelopmentPeerDNSImage         string
	DevelopmentPeerTLSImage         string
	DevelopmentHTTPTLSImage         string
	DevelopmentBasicAuthImage       string
	DevelopmentPluginInstallerImage string
	DevelopmentPluginRuntimeImage   string
	DevelopmentPluginPolicy         string
	DevelopmentClusterDomain        string
	retirementEvidenceKey           ed25519.PublicKey
	retirementInstanceName          string
	ObserveWarmup                   func(context.Context, *coreapi.Pod) (java.Warmup, error)
	ObserveSearchQualification      func(context.Context, *coreapi.Pod) (java.SearchQualification, error)
	ObserveIngestQualification      func(context.Context, *coreapi.Pod) (java.IngestQualification, error)
	Now                             func() time.Time
}

// Reconcile the instance's managed resources and observed endpoint coverage.
func (r *InstanceReconciler) Reconcile(ctx context.Context, request ctrl.Request) (ctrl.Result, error) {
	var instance v1alpha1.OpenSearchServerless
	if err := r.Get(ctx, request.NamespacedName, &instance); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	if result, handled, err := r.recoverInstanceActions(ctx, &instance); handled || err != nil {
		return result, err
	}
	progress, result, err := r.observeProvisioningProgress(ctx, &instance)
	if progress == nil {
		return result, err
	}
	eligible, result, changed, err := r.reconcileInstanceStatus(ctx, &instance)
	if changed || err != nil {
		return result, err
	}
	if eligible && instance.Status.ActiveOperation == nil {
		return r.reconcileServingInstance(ctx, &instance, *progress)
	}
	return ctrl.Result{RequeueAfter: progress.trackingDelay}, nil
}

type provisioningProgress struct {
	isolatedDelay time.Duration
	trackingDelay time.Duration
	tracking      bool
}

// Recover persisted effects before validating a possibly changed spec.
func (r *InstanceReconciler) recoverInstanceActions(ctx context.Context, instance *v1alpha1.OpenSearchServerless) (ctrl.Result, bool, error) {
	if current := meta.FindStatusCondition(instance.Status.Conditions, "Available"); current != nil &&
		current.Status == metav1.ConditionTrue && current.Reason == "ReadyRoleEndpointsObserved" {
		last := instance.Status.LastEndpointObservation
		now := r.now()
		if !r.DevelopmentObserveEndpoints || instance.Status.ActiveOperation != nil || instance.DeletionTimestamp != nil ||
			!endpointAvailabilitySampled(instance) || now.Before(last.ObservedAt.Time) || now.Sub(last.ObservedAt.Time) >= 2*time.Minute {
			before := instance.DeepCopyObject().(*v1alpha1.OpenSearchServerless)
			meta.SetStatusCondition(&instance.Status.Conditions, metav1.Condition{Type: "Available", Status: metav1.ConditionUnknown,
				Reason: "EndpointObservationPending", Message: "Waiting to verify Kubernetes endpoints for the configured roles", ObservedGeneration: instance.Generation})
			if err := r.Status().Patch(ctx, instance, client.MergeFromWithOptions(before, client.MergeFromWithOptimisticLock{})); err != nil {
				return ctrl.Result{}, true, client.IgnoreNotFound(err)
			}
			return ctrl.Result{Requeue: true}, true, nil
		}
	}
	if instance.Status.EmergencyRetirement != nil {
		if result, handled, err := r.reconcileEmergencyRetirement(ctx, instance); handled || err != nil {
			return result, true, err
		}
	}
	if instance.Status.ScaleDownRetirement != nil {
		if result, handled, err := r.reconcileSearchScaleDown(ctx, instance); handled || err != nil {
			return result, true, err
		}
	}
	// Settle old gate work before current inputs can block its recovery.
	if instance.Status.PendingGateRelease != nil && r.Core != nil {
		pending := instance.Status.PendingGateRelease
		if pending.WithdrawalRequested || (instance.Status.ActiveOperation == nil && r.gateReleaseExpired(pending)) {
			result, err := r.reconcileGateRelease(ctx, instance, "")
			return result, true, err
		}
		pod, err := r.Core.Pods(instance.Namespace).Get(ctx, pending.Pod.Name, metav1.GetOptions{})
		if err == nil {
			owner := metav1.GetControllerOf(pod)
			if owner != nil && owner.UID == instance.UID && string(pod.UID) == pending.PodUID && pod.DeletionTimestamp == nil && !hasPlacementGate(pod) {
				result, err := r.reconcileGateRelease(ctx, instance, "")
				return result, true, err
			}
		} else if !apierrors.IsNotFound(err) {
			return ctrl.Result{}, true, err
		}
	}
	if instance.Status.ActiveOperation == nil && r.DevelopmentGatedPods && r.Core != nil {
		changed, err := r.recordLegacyTemplateRevision(ctx, instance)
		if err != nil || changed {
			return ctrl.Result{Requeue: changed}, true, err
		}
		result, changed, err := r.recoverGatedPreflight(ctx, instance)
		if err != nil || changed {
			return result, true, err
		}
	}
	if r.DevelopmentGatedPods && r.Core != nil && len(instance.Status.RetiredPodCreations) > 0 {
		result, changed, err := r.reconcileRetiredPodCreations(ctx, instance)
		if err != nil || changed {
			return result, true, err
		}
	}
	if r.Core != nil && len(instance.Status.RetiredCandidateFleets) > 0 && instance.Status.ActiveOperation == nil {
		result, changed, err := r.reconcileRetiredCandidateFleets(ctx, instance)
		if err != nil || changed {
			return result, true, err
		}
	}
	if instance.Status.ActiveOperation == nil && r.DevelopmentGatedPods {
		result, changed, err := r.archiveBlockedAttempt(ctx, instance)
		if err != nil || changed {
			return result, true, err
		}
	}
	return ctrl.Result{}, false, nil
}

func (r *InstanceReconciler) observeProvisioningProgress(ctx context.Context, instance *v1alpha1.OpenSearchServerless) (*provisioningProgress, ctrl.Result, error) {
	isolatedDelay := time.Minute
	if instance.Status.ActiveOperation == nil && r.DevelopmentGatedPods && r.Core != nil && len(instance.Status.BlockedProvisioningAttempts) > 0 {
		result, changed, err := r.observeIsolatedAttempts(ctx, instance)
		if err != nil || changed {
			return nil, result, err
		}
		if result.RequeueAfter > 0 && result.RequeueAfter < isolatedDelay {
			isolatedDelay = result.RequeueAfter
		}
	}
	tracking := instance.Status.ProvisioningAttempt != nil
	trackingDelay := isolatedDelay
	if tracking && r.Core != nil {
		result, changed, err := r.reconcileProvisioningAttempt(ctx, instance)
		if err != nil || changed {
			return nil, result, err
		}
		if result.RequeueAfter > 0 && result.RequeueAfter < trackingDelay {
			trackingDelay = result.RequeueAfter
		}
	}
	if instance.Status.SearchWarmupSlot != nil && r.Core != nil {
		result, changed, err := r.reconcileWarmupSlot(ctx, instance)
		if err != nil || changed {
			return nil, result, err
		}
		if result.RequeueAfter > 0 && result.RequeueAfter < trackingDelay {
			trackingDelay = result.RequeueAfter
		}
	}
	if r.DevelopmentGatedPods && r.Core != nil && instance.Status.ActiveOperation == nil {
		changed, err := r.retireCancelledCandidateGated(ctx, instance)
		if err != nil {
			return nil, ctrl.Result{RequeueAfter: time.Minute}, err
		}
		if changed {
			return nil, ctrl.Result{Requeue: true}, nil
		}
	}
	return &provisioningProgress{isolatedDelay: isolatedDelay, trackingDelay: trackingDelay, tracking: tracking}, ctrl.Result{}, nil
}

func (r *InstanceReconciler) reconcileInstanceStatus(ctx context.Context, instance *v1alpha1.OpenSearchServerless) (bool, ctrl.Result, bool, error) {
	before := instance.DeepCopyObject().(*v1alpha1.OpenSearchServerless)
	condition := metav1.Condition{
		Type: "Available", Status: metav1.ConditionFalse, ObservedGeneration: instance.Generation,
		Reason: "InstanceIdentityUnavailable", Message: "The instance is terminating or its identity is unavailable",
	}
	eligible := false
	if err := instance.Spec.Validate(); err != nil {
		condition.Reason = "InvalidSpec"
		condition.Message = err.Error()
	} else if r.Core == nil {
		condition.Reason = "StorageUnavailable"
		condition.Message = "Storage configuration reader is unavailable"
	} else {
		resolved, err := storage.ResolveS3(ctx, r.Core, instance.Namespace, instance.Spec.Storage)
		if err != nil {
			condition.Reason = "StorageUnavailable"
			condition.Message = err.Error()
		} else {
			current := resolved.PrimaryStoreStatus()
			if instance.Status.PrimaryStore != nil && *instance.Status.PrimaryStore != current {
				condition.Reason = "StorageIdentityChanged"
				condition.Message = "Resolved primary store differs from the pinned configuration"
			} else if instance.Status.PrimaryStore == nil {
				instance.Status.PrimaryStore = &current
			}
			if instance.Status.PrimaryStore != nil && *instance.Status.PrimaryStore == current && instance.DeletionTimestamp == nil && instance.UID != "" {
				eligible = true
				if instance.Status.ServingFleetIntentByRole == nil {
					instance.Status.ServingFleetIntentByRole = map[string]string{"search": "initial", "ingest": "initial"}
				}
			}
		}
	}
	if eligible {
		condition.Status = metav1.ConditionUnknown
		previous := meta.FindStatusCondition(instance.Status.Conditions, "Available")
		switch {
		case instance.Status.ActiveOperation != nil:
			condition.Reason, condition.Message = "OperationInProgress", "Endpoint coverage is held while an instance operation is active"
		case !r.DevelopmentObserveEndpoints:
			condition.Reason, condition.Message = "EndpointObservationDisabled", "Kubernetes endpoint observation is disabled"
		case len(endpointServingFleets(instance)) == 0:
			condition.Status = metav1.ConditionFalse
			condition.Reason, condition.Message = "NoServingCapacityRequested", "No role requests serving Pods"
		case endpointAvailabilitySampled(instance):
			condition = *previous
		case previous != nil && previous.ObservedGeneration == instance.Generation && previous.Reason == "ServiceUnavailable":
			condition = *previous
		default:
			condition.Reason, condition.Message = "EndpointObservationPending", "Waiting to verify Kubernetes endpoints for the configured roles"
		}
	}
	instance.Status.ObservedGeneration = instance.Generation
	meta.SetStatusCondition(&instance.Status.Conditions, condition)
	if (!eligible || instance.Status.ActiveOperation != nil) && meta.FindStatusCondition(instance.Status.Conditions, "ServicesConfigured") != nil {
		meta.SetStatusCondition(&instance.Status.Conditions, metav1.Condition{Type: "ServicesConfigured", Status: metav1.ConditionUnknown, Reason: "ReconciliationHeld", Message: "Service state is not being verified while prerequisites or operation state are unresolved", ObservedGeneration: instance.Generation})
	}
	if (!eligible || instance.Status.ActiveOperation != nil) && meta.FindStatusCondition(instance.Status.Conditions, gatewayBackendsCondition) != nil {
		meta.SetStatusCondition(&instance.Status.Conditions, metav1.Condition{Type: gatewayBackendsCondition, Status: metav1.ConditionUnknown, Reason: "ReconciliationHeld", Message: "Gateway backend reconciliation is held while prerequisites or operation state are unresolved", ObservedGeneration: instance.Generation})
	}
	if !reflect.DeepEqual(before.Status, instance.Status) {
		if err := r.Status().Patch(ctx, instance, client.MergeFromWithOptions(before, client.MergeFromWithOptimisticLock{})); err != nil {
			if apierrors.IsNotFound(err) {
				return false, ctrl.Result{}, true, nil
			}
			return false, ctrl.Result{}, true, err
		}
		if eligible {
			return false, ctrl.Result{Requeue: true}, true, nil
		}
		return false, ctrl.Result{RequeueAfter: time.Minute}, true, nil
	}
	return eligible, ctrl.Result{}, false, nil
}

func (r *InstanceReconciler) reconcileServingInstance(ctx context.Context, instance *v1alpha1.OpenSearchServerless, progress provisioningProgress) (ctrl.Result, error) {
	isolatedDelay, trackingDelay, tracking := progress.isolatedDelay, progress.trackingDelay, progress.tracking
	before := instance.DeepCopy()
	serviceCondition := metav1.Condition{Type: "ServicesConfigured", Status: metav1.ConditionTrue, Reason: "ServicesObserved", Message: "Stable role Services match recorded fleet intent", ObservedGeneration: instance.Generation}
	if err := ensureRoleServices(ctx, r.Core, instance); err != nil {
		serviceCondition.Status = metav1.ConditionFalse
		serviceCondition.Reason = "ServiceUnavailable"
		serviceCondition.Message = err.Error()
		if r.DevelopmentObserveEndpoints {
			meta.SetStatusCondition(&instance.Status.Conditions, metav1.Condition{Type: "Available", Status: metav1.ConditionUnknown,
				Reason: "ServiceUnavailable", Message: "Kubernetes endpoint coverage needs verified role Services", ObservedGeneration: instance.Generation})
		}
	}
	meta.SetStatusCondition(&instance.Status.Conditions, serviceCondition)
	if !reflect.DeepEqual(before.Status, instance.Status) {
		if err := r.Status().Patch(ctx, instance, client.MergeFromWithOptions(before, client.MergeFromWithOptimisticLock{})); err != nil {
			return ctrl.Result{}, client.IgnoreNotFound(err)
		}
		return ctrl.Result{Requeue: true}, nil
	}
	if serviceCondition.Status == metav1.ConditionTrue && r.DevelopmentObserveEndpoints {
		changed, err := r.sampleEndpointObservation(ctx, instance)
		if err != nil {
			return ctrl.Result{}, err
		}
		if changed {
			return ctrl.Result{Requeue: true}, nil
		}
		if delay := endpointObservationDelay(instance, r.now()); delay > 0 {
			isolatedDelay = min(isolatedDelay, delay)
			trackingDelay = min(trackingDelay, delay)
		}
	}
	if serviceCondition.Status == metav1.ConditionTrue {
		changed, err := r.reconcileFleetRetirement(ctx, instance)
		if err != nil || changed {
			return ctrl.Result{Requeue: changed}, err
		}
		changed, err = r.reconcileCandidatePreview(ctx, instance)
		if err != nil {
			return ctrl.Result{}, err
		}
		if changed {
			return ctrl.Result{Requeue: true}, nil
		}
		changed, err = r.reconcileGatewayBackends(ctx, instance)
		if err != nil || changed {
			return ctrl.Result{Requeue: changed}, err
		}
	}
	if serviceCondition.Status == metav1.ConditionTrue && r.DevelopmentServiceProbeImage != "" {
		changed, err := r.sampleRoleServiceProbe(ctx, instance)
		if err != nil {
			return ctrl.Result{}, err
		}
		if changed {
			return ctrl.Result{Requeue: true}, nil
		}
	}
	if r.DevelopmentGatedPods {
		changed, err := r.sampleEmergencyCapacity(ctx, instance)
		if err != nil {
			return ctrl.Result{}, err
		}
		if changed {
			return ctrl.Result{Requeue: true}, nil
		}
		changed, err = r.refreshZoneCapacity(ctx, instance)
		if err != nil {
			return ctrl.Result{}, err
		}
		if changed {
			return ctrl.Result{Requeue: true}, nil
		}
		changed, err = r.sampleSearchQualification(ctx, instance)
		if err != nil {
			return ctrl.Result{}, err
		}
		if changed {
			return ctrl.Result{Requeue: true}, nil
		}
		changed, err = r.sampleServingSearchProtection(ctx, instance)
		if err != nil {
			return ctrl.Result{}, err
		}
		if changed {
			return ctrl.Result{Requeue: true}, nil
		}
		changed, err = r.sampleServingIngestProtection(ctx, instance)
		if err != nil {
			return ctrl.Result{}, err
		}
		if changed {
			return ctrl.Result{Requeue: true}, nil
		}
		changed, err = r.sampleCandidateIngest(ctx, instance)
		if err != nil {
			return ctrl.Result{}, err
		}
		if changed {
			return ctrl.Result{Requeue: true}, nil
		}
		changed, err = r.sampleCandidateSearch(ctx, instance)
		if err != nil {
			return ctrl.Result{}, err
		}
		if changed {
			return ctrl.Result{Requeue: true}, nil
		}
	}
	if serviceCondition.Status == metav1.ConditionTrue && r.DevelopmentGatedPods {
		result, ready, err := r.reconcileDisruptionBudgets(ctx, instance)
		if err != nil || !ready {
			return result, err
		}
		if !tracking {
			if r.DevelopmentEmergencyFailback {
				if result, handled, err := r.reconcileEmergencyRetirement(ctx, instance); handled || err != nil {
					return result, err
				}
			}
			if r.DevelopmentSearchScaleDown && instance.Status.ScaleDownRetirement == nil {
				if result, handled, err := r.reconcileSearchScaleDown(ctx, instance); handled || err != nil {
					return result, err
				}
			}
			result, err := r.advanceGatedPod(ctx, instance)
			if !result.Requeue && (result.RequeueAfter == 0 || result.RequeueAfter > isolatedDelay) {
				result.RequeueAfter = isolatedDelay
			}
			return result, err
		}
	}
	return ctrl.Result{RequeueAfter: trackingDelay}, nil
}

func (r *InstanceReconciler) now() time.Time {
	if r.Now != nil {
		return r.Now()
	}
	return time.Now()
}
