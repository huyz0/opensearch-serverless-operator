package controller

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/huyz0/opensearch-serverless-operator/api/v1alpha1"
	"github.com/huyz0/opensearch-serverless-operator/internal/workload"
)

const maxRetiredPodCreations = 64

// stageGatedWithdrawal records a gated Pod's identity for withdrawal once its
// creation intent has cleared. No profile lookup is needed for cleanup.
func (r *InstanceReconciler) stageGatedWithdrawal(ctx context.Context, instance *v1alpha1.OpenSearchServerless, pod *corev1.Pod) (ctrl.Result, error) {
	owner, rawSequence, ok := strings.Cut(pod.Annotations[creationIntentKey], "/")
	sequence, err := strconv.ParseInt(rawSequence, 10, 64)
	if !ok || err != nil || owner != string(instance.UID) || instance.Status.ActiveOperation != nil ||
		instance.Status.PendingPodCreation != nil || instance.Status.PendingGateRelease != nil || instance.Status.ProvisioningAttempt != nil {
		return ctrl.Result{RequeueAfter: time.Minute}, fmt.Errorf("gated Pod does not have an isolated creation identity for withdrawal")
	}
	retired := v1alpha1.RetiredPodCreation{
		Name: pod.Name, Sequence: sequence, Role: pod.Labels[workload.RoleLabel], Zone: pod.Labels[workload.ZoneLabel],
		Fleet: pod.Labels[workload.FleetLabel], Revision: pod.Labels["serverless.opensearch.org/revision"],
	}
	if err := validateRetiredPod(instance, retired, pod); err != nil {
		return ctrl.Result{RequeueAfter: time.Minute}, err
	}
	plugins, err := pluginIntentFromPod(pod)
	if err != nil {
		return ctrl.Result{RequeueAfter: time.Minute}, err
	}
	intent := v1alpha1.PodCreationIntent{
		Name: retired.Name, Sequence: retired.Sequence, Role: retired.Role, Zone: retired.Zone,
		Fleet: retired.Fleet, Revision: retired.Revision, Generation: instance.Generation,
		PeerDNS: pod.Annotations[peerDomainAnnotation] != "", PluginInstall: plugins,
	}
	before := instance.DeepCopyObject().(*v1alpha1.OpenSearchServerless)
	if err := appendRetiredCreation(instance, &intent); err != nil {
		return ctrl.Result{RequeueAfter: time.Minute}, err
	}
	instance.Status.PendingGateRelease = &v1alpha1.GateReleaseIntent{
		PodUID: string(pod.UID), Pod: intent, StartedAt: metav1.NewTime(r.now()), WithdrawalRequested: true,
	}
	if err := r.Status().Patch(ctx, instance, client.MergeFromWithOptions(before, client.MergeFromWithOptimisticLock{})); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{Requeue: true}, nil
}

// recoverStaleGatedCreation moves an already-created, still-gated stale Pod
// into the persisted withdrawal state machine. Missing names are retained as
// durable tombstones because a delayed create may still arrive.
func (r *InstanceReconciler) recoverStaleGatedCreation(ctx context.Context, instance *v1alpha1.OpenSearchServerless) (ctrl.Result, error) {
	intent := instance.Status.PendingPodCreation
	if intent == nil || instance.Status.PendingGateRelease != nil || intent.Sequence <= 0 || intent.Sequence > instance.Status.PodCreationSequence || intent.Name != podNameForSequence(instance, intent.Sequence) {
		return ctrl.Result{RequeueAfter: time.Minute}, fmt.Errorf("stale creation intent has inconsistent identity")
	}
	pod, err := r.Core.Pods(instance.Namespace).Get(ctx, intent.Name, metav1.GetOptions{})
	if err != nil {
		if apierrors.IsNotFound(err) {
			if bootstrap := instance.Status.ColdBootstrap; bootstrap != nil && bootstrap.Name == intent.Name && bootstrap.PodUID != "" {
				return ctrl.Result{RequeueAfter: time.Minute}, fmt.Errorf("missing stale Pod has a pinned bootstrap UID")
			}
			before := instance.DeepCopyObject().(*v1alpha1.OpenSearchServerless)
			if err := appendRetiredCreation(instance, intent); err != nil {
				return ctrl.Result{RequeueAfter: time.Minute}, err
			}
			if bootstrap := instance.Status.ColdBootstrap; bootstrap != nil && bootstrap.Name == intent.Name {
				instance.Status.ColdBootstrap = nil
			}
			instance.Status.PendingPodCreation = nil
			if err := r.Status().Patch(ctx, instance, client.MergeFromWithOptions(before, client.MergeFromWithOptimisticLock{})); err != nil {
				return ctrl.Result{}, err
			}
			return ctrl.Result{Requeue: true}, nil
		}
		return ctrl.Result{RequeueAfter: time.Minute}, fmt.Errorf("stale creation target is unavailable; recovery remains held: %w", err)
	}
	if err := validateRetiredPod(instance, creationRecord(intent), pod); err != nil {
		return ctrl.Result{RequeueAfter: time.Minute}, err
	}
	if bootstrap := instance.Status.ColdBootstrap; bootstrap != nil && bootstrap.Name == intent.Name && bootstrap.PodUID != "" && bootstrap.PodUID != string(pod.UID) {
		return ctrl.Result{RequeueAfter: time.Minute}, fmt.Errorf("cold bootstrap Pod identity changed")
	}
	before := instance.DeepCopyObject().(*v1alpha1.OpenSearchServerless)
	if err := appendRetiredCreation(instance, intent); err != nil {
		return ctrl.Result{RequeueAfter: time.Minute}, err
	}
	if bootstrap := instance.Status.ColdBootstrap; bootstrap != nil && bootstrap.Name == intent.Name {
		bootstrap.PodUID = string(pod.UID)
	}
	instance.Status.PendingGateRelease = &v1alpha1.GateReleaseIntent{PodUID: string(pod.UID), Pod: *intent, StartedAt: metav1.NewTime(r.now()), WithdrawalRequested: true}
	instance.Status.PendingPodCreation = nil
	if err := r.Status().Patch(ctx, instance, client.MergeFromWithOptions(before, client.MergeFromWithOptimisticLock{})); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{Requeue: true}, nil
}

func appendRetiredCreation(instance *v1alpha1.OpenSearchServerless, intent *v1alpha1.PodCreationIntent) error {
	if intent == nil || intent.Sequence <= 0 || intent.Sequence > instance.Status.PodCreationSequence || intent.Name != podNameForSequence(instance, intent.Sequence) || intent.Role == "" || intent.Zone == "" || intent.Fleet == "" || intent.Revision == "" {
		return fmt.Errorf("retired creation identity is incomplete")
	}
	if len(instance.Status.RetiredPodCreations) >= maxRetiredPodCreations {
		return fmt.Errorf("retired creation history is full")
	}
	for _, retired := range instance.Status.RetiredPodCreations {
		if retired.Name == intent.Name || retired.Sequence == intent.Sequence {
			return fmt.Errorf("duplicate retired creation identity")
		}
	}
	instance.Status.RetiredPodCreations = append(instance.Status.RetiredPodCreations, creationRecord(intent))
	return nil
}

func creationRecord(intent *v1alpha1.PodCreationIntent) v1alpha1.RetiredPodCreation {
	return v1alpha1.RetiredPodCreation{
		Name: intent.Name, Sequence: intent.Sequence, Role: intent.Role, Zone: intent.Zone,
		Fleet: intent.Fleet, Revision: intent.Revision,
	}
}

func validateRetiredPod(instance *v1alpha1.OpenSearchServerless, intent v1alpha1.RetiredPodCreation, pod *corev1.Pod) error {
	if pod.DeletionTimestamp != nil || pod.Spec.NodeName != "" || !hasPlacementGate(pod) {
		return fmt.Errorf("stale creation target is no longer unbound and gated")
	}
	return validateCreationPodIdentity(instance, intent, pod)
}

func validateCreationPodIdentity(instance *v1alpha1.OpenSearchServerless, intent v1alpha1.RetiredPodCreation, pod *corev1.Pod) error {
	owner := metav1.GetControllerOf(pod)
	if pod.Name != intent.Name || pod.Namespace != instance.Namespace || pod.UID == "" || pod.ResourceVersion == "" ||
		owner == nil || owner.UID != instance.UID || owner.Name != instance.Name || owner.Kind != "OpenSearchServerless" || owner.APIVersion != v1alpha1.GroupVersion.String() {
		return fmt.Errorf("Pod identity or creation owner changed")
	}
	if pod.Annotations[creationIntentKey] != fmt.Sprintf("%s/%d", instance.UID, intent.Sequence) ||
		pod.Labels[workload.InstanceLabel] != string(instance.UID) || pod.Labels[workload.RoleLabel] != intent.Role ||
		pod.Labels[workload.ZoneLabel] != intent.Zone || pod.Labels[workload.FleetLabel] != intent.Fleet || pod.Labels[podRevisionLabel] != intent.Revision {
		return fmt.Errorf("Pod creation annotation or workload labels changed")
	}
	return nil
}
