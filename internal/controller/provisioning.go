package controller

import (
	"context"
	"fmt"
	"reflect"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/huyz0/opensearch-serverless-operator/api/v1alpha1"
	"github.com/huyz0/opensearch-serverless-operator/internal/workload"
	"github.com/huyz0/opensearch-serverless-operator/pkg/podlifecycle"
	podkube "github.com/huyz0/opensearch-serverless-operator/pkg/podlifecycle/kubernetes"
)

const previewRecoveryReason = "ServingRecoveryPriority"

func peerDNSPreflightComplete(pod *corev1.Pod) bool {
	count := 0
	complete := false
	for _, status := range pod.Status.InitContainerStatuses {
		if status.Name == "peer-dns-preflight" {
			count++
			complete = status.State.Terminated != nil && status.State.Terminated.ExitCode == 0
		}
	}
	return count == 1 && complete
}

func pluginInstallerComplete(pod *corev1.Pod) bool {
	count := 0
	complete := false
	for _, status := range pod.Status.InitContainerStatuses {
		if status.Name == "plugin-installer" {
			count++
			complete = status.State.Terminated != nil && status.State.Terminated.ExitCode == 0
		}
	}
	return count == 1 && complete
}

// podStartupReady is a kubelet observation, not evidence of cache warmth or
// continuing application health. Exactly one Ready condition must be true.
func podStartupReady(pod *corev1.Pod) bool {
	if pod.Status.Phase != corev1.PodRunning {
		return false
	}
	count := 0
	ready := false
	for _, condition := range pod.Status.Conditions {
		if condition.Type == corev1.PodReady {
			count++
			ready = condition.Status == corev1.ConditionTrue
		}
	}
	return count == 1 && ready
}

// podReadyForServing is the Kubernetes precondition for using a Pod as
// serving evidence. Application warmth and leases are checked separately.
func podReadyForServing(pod *corev1.Pod) bool {
	return pod.DeletionTimestamp == nil && pod.Spec.NodeName != "" && !hasPlacementGate(pod) &&
		podStartupReady(pod) && !workload.PodDisruptionTarget(pod)
}

func (r *InstanceReconciler) pendingTimeout() time.Duration {
	if r.DevelopmentPendingTimeout > 0 {
		return r.DevelopmentPendingTimeout
	}
	return 10 * time.Minute
}

// reconcileProvisioningAttempt tracks one released Pod independently of the
// current spec, so changing an input cannot leave stale autoscaler demand.
// changed tells callers to re-read after a transition or withdrawal.
func (r *InstanceReconciler) reconcileProvisioningAttempt(ctx context.Context, instance *v1alpha1.OpenSearchServerless) (ctrl.Result, bool, error) {
	attempt := instance.Status.ProvisioningAttempt
	if attempt == nil {
		return ctrl.Result{}, false, nil
	}
	if attempt.Phase == "Blocked" && !recoverableBoundReason(attempt.Reason) && attempt.Reason != "PodTerminal" {
		return ctrl.Result{RequeueAfter: time.Minute}, false, nil
	}
	block := func(reason, message string) (ctrl.Result, bool, error) {
		return r.blockProvisioningAttempt(ctx, instance, reason, message)
	}
	switch attempt.Phase {
	case "Watching", "Withdrawing", "CheckingPeerDNS", "CheckingPluginInstall", "CheckingStartup", "Blocked":
	default:
		return block("InvalidAttempt", "Provisioning attempt identity or phase is invalid; recovery is required")
	}
	if attempt.Name == "" || attempt.PodUID == "" || attempt.StartedAt.IsZero() {
		return block("InvalidAttempt", "Provisioning attempt identity or phase is invalid; recovery is required")
	}
	withdrawCandidate := attempt.Phase == "Withdrawing" && (attempt.Reason == "CandidateCancelled" || attempt.Reason == previewRecoveryReason)
	pod, err := r.Core.Pods(instance.Namespace).Get(ctx, attempt.Name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		if attempt.Phase == "Withdrawing" && attempt.Reason == "OutageConfirmed" {
			before := instance.DeepCopyObject().(*v1alpha1.OpenSearchServerless)
			instance.Status.ProvisioningAttempt = nil
			meta.RemoveStatusCondition(&instance.Status.Conditions, "ProvisioningBlocked")
			if err := r.Status().Patch(ctx, instance, client.MergeFromWithOptions(before, client.MergeFromWithOptimisticLock{})); err != nil {
				return ctrl.Result{}, false, err
			}
			return ctrl.Result{Requeue: true}, true, nil
		}
		if withdrawCandidate && attempt.Cancellation != nil {
			tombstone := attempt.Cancellation
			if tombstone.Name != attempt.Name || tombstone.Role != attempt.Role || tombstone.Zone != attempt.Zone || instance.Status.CandidateFleet == nil || tombstone.Fleet != instance.Status.CandidateFleet.Fleet || tombstone.Revision != instance.Status.CandidateFleet.Revision || tombstone.Fleet == instance.Status.ServingFleetIntentByRole["search"] || tombstone.Fleet == instance.Status.ServingFleetIntentByRole["ingest"] {
				return block("CancellationIdentityChanged", "Candidate withdrawal identity or serving state changed")
			}
			before := instance.DeepCopyObject().(*v1alpha1.OpenSearchServerless)
			index, err := releasedRetirementIndex(instance, tombstone, attempt.PodUID)
			if err != nil {
				return ctrl.Result{RequeueAfter: time.Minute}, false, err
			}
			if index >= 0 {
				record := &instance.Status.RetiredPodCreations[index]
				record.PodUID, record.Released = "", false
			} else {
				retired := &v1alpha1.PodCreationIntent{Name: tombstone.Name, Role: tombstone.Role, Zone: tombstone.Zone, Fleet: tombstone.Fleet, Revision: tombstone.Revision, Sequence: tombstone.Sequence}
				if err := appendRetiredCreation(instance, retired); err != nil {
					return ctrl.Result{RequeueAfter: time.Minute}, false, err
				}
			}
			if slot := instance.Status.SearchWarmupSlot; slot != nil && slot.Name == attempt.Name && slot.PodUID == attempt.PodUID {
				instance.Status.SearchWarmupSlot = nil
			}
			instance.Status.ProvisioningAttempt = nil
			meta.RemoveStatusCondition(&instance.Status.Conditions, "ProvisioningBlocked")
			if err := r.Status().Patch(ctx, instance, client.MergeFromWithOptions(before, client.MergeFromWithOptimisticLock{})); err != nil {
				return ctrl.Result{}, false, err
			}
			return ctrl.Result{Requeue: true}, true, nil
		}
		return block("PodGone", "The provisioning Pod is gone; further automatic attempts are held")
	}
	if err != nil {
		return ctrl.Result{}, false, err
	}
	if !provisioningPodMatches(instance, attempt, pod) {
		return block("IdentityChanged", "The provisioning Pod identity, placement labels or gate changed; further attempts are held")
	}
	if withdrawCandidate {
		identity, err := cancelledCandidatePodIdentity(instance, pod)
		if err != nil || attempt.Cancellation == nil || !reflect.DeepEqual(identity, attempt.Cancellation) {
			return ctrl.Result{RequeueAfter: time.Minute}, false, fmt.Errorf("candidate withdrawal identity changed")
		}
	}
	if pod.DeletionTimestamp != nil {
		if withdrawCandidate || (attempt.Phase == "Withdrawing" && attempt.Reason == "OutageConfirmed") {
			return ctrl.Result{RequeueAfter: time.Second}, false, nil
		}
		return block("PodTerminating", "The provisioning Pod is terminating; further attempts are held")
	}
	previewMayYield := attempt.Phase == "Watching" || (attempt.Phase == "Withdrawing" && attempt.Reason == "PendingTimeout")
	if previewMayYield && instance.Status.ActiveOperation == nil && instance.Status.CandidateFleet != nil &&
		pod.Labels[workload.FleetLabel] == instance.Status.CandidateFleet.Fleet && pod.Spec.NodeName == "" && !hasPlacementGate(pod) {
		reason := "CandidateCancelled"
		if !instance.Status.CandidateFleet.PreviewRetiring && instance.Spec.Rollout != nil && instance.Spec.Rollout.Candidate != nil {
			pods, err := r.previewPodInventory(ctx, instance)
			if err != nil {
				return ctrl.Result{}, false, err
			}
			reason = ""
			if !previewServingCapacityReady(instance, pods) {
				reason = previewRecoveryReason
			}
		}
		if reason != "" {
			return r.stageReleasedPreviewWithdrawal(ctx, instance, attempt, pod, reason)
		}
	}
	if withdrawCandidate && (pod.Spec.NodeName == "" || attempt.Reason == previewRecoveryReason) {
		resume := pod.Spec.NodeName != ""
		if !resume && attempt.Reason == previewRecoveryReason {
			pods, err := r.previewPodInventory(ctx, instance)
			if err != nil {
				return ctrl.Result{}, false, err
			}
			resume = instance.Status.ActiveOperation != nil || previewServingCapacityReady(instance, pods)
		}
		if resume {
			before := instance.DeepCopyObject().(*v1alpha1.OpenSearchServerless)
			attempt.Phase, attempt.Reason, attempt.Cancellation = "Watching", "", nil
			if err := r.Status().Patch(ctx, instance, client.MergeFromWithOptions(before, client.MergeFromWithOptimisticLock{})); err != nil {
				return ctrl.Result{}, false, err
			}
			return ctrl.Result{Requeue: true}, true, nil
		}
		identity := podlifecycle.Identity{Namespace: pod.Namespace, Name: pod.Name, UID: string(pod.UID), OwnerUID: string(instance.UID)}
		if err := podkube.WithdrawUnboundAtVersion(ctx, r.Core, identity, pod.ResourceVersion); err != nil {
			return ctrl.Result{}, false, fmt.Errorf("withdraw unbound preview Pod: %w", err)
		}
		return ctrl.Result{Requeue: true}, true, nil
	}
	if pod.Status.Phase == corev1.PodFailed || pod.Status.Phase == corev1.PodSucceeded {
		if attempt.Phase == "Blocked" && attempt.Reason == "PodTerminal" {
			return ctrl.Result{RequeueAfter: time.Minute}, false, nil
		}
		return block("PodTerminal", "The provisioning Pod became terminal; further automatic attempts are held")
	}
	if pod.Spec.NodeName != "" {
		return r.reconcileBoundProvisioning(ctx, instance, pod)
	}
	if attempt.Phase == "CheckingPeerDNS" || attempt.Phase == "CheckingPluginInstall" || attempt.Phase == "CheckingStartup" || attempt.Phase == "Blocked" {
		return block("BindingChanged", "The Pod binding disappeared during initialization")
	}
	if attempt.Phase == "Watching" {
		remaining := attempt.StartedAt.Add(r.pendingTimeout()).Sub(r.now())
		outage := r.confirmedOutageZones(ctx, instance)[attempt.Zone]
		if remaining > 0 && !outage {
			return ctrl.Result{RequeueAfter: remaining}, false, nil
		}
		before := instance.DeepCopyObject().(*v1alpha1.OpenSearchServerless)
		attempt.Phase = "Withdrawing"
		attempt.Reason = "PendingTimeout"
		message := "Pending Pod exceeded its scheduling deadline; withdrawal is in progress"
		if outage {
			attempt.Reason = "OutageConfirmed"
			message = "Pending Pod is in a zone with a confirmed outage; withdrawal is in progress"
		}
		meta.SetStatusCondition(&instance.Status.Conditions, metav1.Condition{Type: "ProvisioningBlocked", Status: metav1.ConditionTrue, Reason: attempt.Reason, Message: message, ObservedGeneration: instance.Generation})
		if err := r.Status().Patch(ctx, instance, client.MergeFromWithOptions(before, client.MergeFromWithOptimisticLock{})); err != nil {
			return ctrl.Result{}, false, err
		}
		return ctrl.Result{Requeue: true}, true, nil
	}
	if attempt.Reason == "OutageConfirmed" && !r.confirmedOutageZones(ctx, instance)[attempt.Zone] {
		before := instance.DeepCopyObject().(*v1alpha1.OpenSearchServerless)
		attempt.Phase, attempt.Reason = "Watching", ""
		meta.RemoveStatusCondition(&instance.Status.Conditions, "ProvisioningBlocked")
		if err := r.Status().Patch(ctx, instance, client.MergeFromWithOptions(before, client.MergeFromWithOptimisticLock{})); err != nil {
			return ctrl.Result{}, false, err
		}
		return ctrl.Result{Requeue: true}, true, nil
	}
	if attempt.Reason != "PendingTimeout" && attempt.Reason != "OutageConfirmed" {
		return block("InvalidAttempt", "Unbound Pod withdrawal has no recognized reason")
	}
	identity := podlifecycle.Identity{Namespace: pod.Namespace, Name: pod.Name, UID: string(pod.UID), OwnerUID: string(instance.UID)}
	if err := podkube.WithdrawUnboundAtVersion(ctx, r.Core, identity, pod.ResourceVersion); err != nil {
		return ctrl.Result{}, false, fmt.Errorf("withdraw timed-out Pod: %w", err)
	}
	return ctrl.Result{Requeue: true}, true, nil
}

func (r *InstanceReconciler) reconcileBoundProvisioning(ctx context.Context, instance *v1alpha1.OpenSearchServerless, pod *corev1.Pod) (ctrl.Result, bool, error) {
	attempt := instance.Status.ProvisioningAttempt
	if attempt.PeerDNS {
		if pod.Annotations[peerDNSPreflightAnnotation] != "v1" || pod.Annotations[peerDomainAnnotation] == "" {
			return r.blockProvisioningAttempt(ctx, instance, "PeerDNSIdentityChanged", "The bound Pod no longer has the expected peer DNS preflight identity")
		}
		if !peerDNSPreflightComplete(pod) {
			if attempt.Phase == "Blocked" {
				return ctrl.Result{RequeueAfter: 5 * time.Second}, false, nil
			}
			return r.waitForProvisioningCheck(ctx, instance, "CheckingPeerDNS", "PeerDNSPreflightTimeout", "Bound Pod did not complete peer DNS preflight")
		}
	}
	if attempt.PluginInstall != nil {
		pluginIntent, err := pluginIntentFromPod(pod)
		if err != nil || pluginIntent == nil || pluginIntent.RequestUID == "" || pluginIntent.PolicyUID == "" || !reflect.DeepEqual(pluginIntent, attempt.PluginInstall) {
			return r.blockProvisioningAttempt(ctx, instance, "PluginInstallIdentityChanged", "Bound Pod no longer has the recorded plugin installer identity")
		}
		if !pluginInstallerComplete(pod) {
			return r.waitForProvisioningCheck(ctx, instance, "CheckingPluginInstall", "PluginInstallTimeout", "Bound Pod did not complete plugin installation")
		}
	}
	if !podStartupReady(pod) {
		return r.waitForProvisioningCheck(ctx, instance, "CheckingStartup", "StartupTimeout", "Bound Pod did not become Ready")
	}
	before := instance.DeepCopy()
	instance.Status.ProvisioningAttempt = nil
	meta.RemoveStatusCondition(&instance.Status.Conditions, "ProvisioningBlocked")
	if err := r.Status().Patch(ctx, instance, client.MergeFromWithOptions(before, client.MergeFromWithOptimisticLock{})); err != nil {
		return ctrl.Result{}, false, err
	}
	return ctrl.Result{Requeue: true}, true, nil
}

func (r *InstanceReconciler) waitForProvisioningCheck(ctx context.Context, instance *v1alpha1.OpenSearchServerless, phase, timeoutReason, timeoutMessage string) (ctrl.Result, bool, error) {
	attempt := instance.Status.ProvisioningAttempt
	if attempt.Phase == "Blocked" && attempt.Reason == timeoutReason {
		return ctrl.Result{RequeueAfter: 5 * time.Second}, false, nil
	}
	now := r.now()
	if attempt.Phase != phase {
		before := instance.DeepCopy()
		attempt.Phase, attempt.Reason, attempt.StartedAt = phase, "", metav1.NewTime(now)
		meta.RemoveStatusCondition(&instance.Status.Conditions, "ProvisioningBlocked")
		if err := r.Status().Patch(ctx, instance, client.MergeFromWithOptions(before, client.MergeFromWithOptimisticLock{})); err != nil {
			return ctrl.Result{}, false, err
		}
		return ctrl.Result{Requeue: true}, true, nil
	}
	if !now.Before(attempt.StartedAt.Add(r.pendingTimeout())) {
		return r.blockProvisioningAttempt(ctx, instance, timeoutReason, timeoutMessage+"; further automatic provisioning is held")
	}
	return ctrl.Result{RequeueAfter: 5 * time.Second}, false, nil
}

// Older attempt records can omit the role or zone. Compare each recorded
// placement label and always require the exact Pod and controller reference.
func provisioningPodMatches(instance *v1alpha1.OpenSearchServerless, attempt *v1alpha1.ProvisioningAttempt, pod *corev1.Pod) bool {
	owner := metav1.GetControllerOf(pod)
	return pod.Name == attempt.Name && pod.Namespace == instance.Namespace && string(pod.UID) == attempt.PodUID && pod.ResourceVersion != "" &&
		owner != nil && owner.UID == instance.UID && owner.Name == instance.Name && owner.Kind == "OpenSearchServerless" && owner.APIVersion == v1alpha1.GroupVersion.String() &&
		(attempt.Role == "" || pod.Labels[workload.RoleLabel] == attempt.Role) && (attempt.Zone == "" || pod.Labels[workload.ZoneLabel] == attempt.Zone) && !hasPlacementGate(pod)
}

func (r *InstanceReconciler) blockProvisioningAttempt(ctx context.Context, instance *v1alpha1.OpenSearchServerless, reason, message string) (ctrl.Result, bool, error) {
	before := instance.DeepCopy()
	attempt := instance.Status.ProvisioningAttempt
	attempt.Phase, attempt.Reason = "Blocked", reason
	meta.SetStatusCondition(&instance.Status.Conditions, metav1.Condition{Type: "ProvisioningBlocked", Status: metav1.ConditionTrue, Reason: reason, Message: message, ObservedGeneration: instance.Generation})
	if err := r.Status().Patch(ctx, instance, client.MergeFromWithOptions(before, client.MergeFromWithOptimisticLock{})); err != nil {
		return ctrl.Result{}, false, err
	}
	return ctrl.Result{RequeueAfter: time.Minute}, true, nil
}
