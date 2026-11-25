package controller

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/huyz0/opensearch-serverless-operator/api/v1alpha1"
	"github.com/huyz0/opensearch-serverless-operator/internal/workload"
)

func cancelledCandidatePodIdentity(instance *v1alpha1.OpenSearchServerless, pod *corev1.Pod) (*v1alpha1.RetiredPodCreation, error) {
	if instance == nil || instance.Status.CandidateFleet == nil || pod == nil {
		return nil, fmt.Errorf("candidate and Pod identity are required")
	}
	return retiredFleetPodIdentity(instance, pod, instance.Status.CandidateFleet.Fleet, instance.Status.CandidateFleet.Revision)
}

func (r *InstanceReconciler) stageReleasedPreviewWithdrawal(ctx context.Context, instance *v1alpha1.OpenSearchServerless, attempt *v1alpha1.ProvisioningAttempt, pod *corev1.Pod, reason string) (ctrl.Result, bool, error) {
	identity, err := cancelledCandidatePodIdentity(instance, pod)
	if err != nil {
		return ctrl.Result{RequeueAfter: time.Minute}, false, err
	}
	index, err := releasedRetirementIndex(instance, identity, attempt.PodUID)
	if err != nil {
		return ctrl.Result{RequeueAfter: time.Minute}, false, err
	}
	if identity.Role != attempt.Role || identity.Zone != attempt.Zone || (index < 0 && len(instance.Status.RetiredPodCreations) >= maxRetiredPodCreations) {
		return ctrl.Result{RequeueAfter: time.Minute}, false, fmt.Errorf("candidate withdrawal cannot reserve its retired identity")
	}
	before := instance.DeepCopyObject().(*v1alpha1.OpenSearchServerless)
	attempt.Phase, attempt.Reason, attempt.Cancellation = "Withdrawing", reason, identity
	meta.RemoveStatusCondition(&instance.Status.Conditions, "ProvisioningBlocked")
	if err := r.Status().Patch(ctx, instance, client.MergeFromWithOptions(before, client.MergeFromWithOptimisticLock{})); err != nil {
		return ctrl.Result{}, false, err
	}
	return ctrl.Result{Requeue: true}, true, nil
}

// releasedRetirementIndex identifies an earlier gated withdrawal that lost
// the release race. Its tombstone remains reserved for that exact Pod UID.
func releasedRetirementIndex(instance *v1alpha1.OpenSearchServerless, identity *v1alpha1.RetiredPodCreation, uid string) (int, error) {
	expected := *identity
	expected.PodUID, expected.Released = uid, true
	found := -1
	for i, record := range instance.Status.RetiredPodCreations {
		if record.Name != identity.Name && record.Sequence != identity.Sequence {
			continue
		}
		if found != -1 || record != expected {
			return -1, fmt.Errorf("candidate withdrawal conflicts with the retired Pod identity")
		}
		found = i
	}
	return found, nil
}

func retiredFleetPodIdentity(instance *v1alpha1.OpenSearchServerless, pod *corev1.Pod, fleet, revision string) (*v1alpha1.RetiredPodCreation, error) {
	owner := metav1.GetControllerOf(pod)
	ownerUID, sequenceText, ok := strings.Cut(pod.Annotations[creationIntentKey], "/")
	sequence, err := strconv.ParseInt(sequenceText, 10, 64)
	role, zone := pod.Labels[workload.RoleLabel], pod.Labels[workload.ZoneLabel]
	if !ok || err != nil || ownerUID != string(instance.UID) || sequence <= 0 || sequence > instance.Status.PodCreationSequence || pod.Name != podNameForSequence(instance, sequence) || pod.Namespace != instance.Namespace || pod.UID == "" || pod.ResourceVersion == "" || owner == nil || owner.UID != instance.UID || owner.Name != instance.Name || owner.Kind != "OpenSearchServerless" || owner.APIVersion != v1alpha1.GroupVersion.String() || pod.Labels[workload.InstanceLabel] != string(instance.UID) || fleet == "" || pod.Labels[workload.FleetLabel] != fleet || (role != "search" && role != "ingest") || zone == "" || pod.Labels["serverless.opensearch.org/revision"] != revision || revision == "" || fleet == instance.Status.ServingFleetIntentByRole["search"] || fleet == instance.Status.ServingFleetIntentByRole["ingest"] {
		return nil, fmt.Errorf("candidate Pod identity or serving state conflicts with retirement")
	}
	return &v1alpha1.RetiredPodCreation{Name: pod.Name, Role: role, Zone: zone, Fleet: fleet, Revision: revision, Sequence: sequence}, nil
}

// retireCancelledCandidateGated records one gated Pod for the existing
// retired-creation recovery loop. It does not delete running or released Pods,
// remove preview Services, or clear the candidate record.
func (r *InstanceReconciler) retireCancelledCandidateGated(ctx context.Context, instance *v1alpha1.OpenSearchServerless) (bool, error) {
	if r.Core == nil || instance == nil || instance.UID == "" || instance.DeletionTimestamp != nil || instance.Status.CandidateFleet == nil || instance.Status.ActiveOperation != nil || instance.Status.PendingPodCreation != nil || instance.Status.PendingGateRelease != nil || instance.Status.ProvisioningAttempt != nil || instance.Status.SearchWarmupSlot != nil || (instance.Spec.Rollout != nil && instance.Spec.Rollout.Candidate != nil && !instance.Status.CandidateFleet.PreviewRetiring) {
		return false, nil
	}
	fleet := instance.Status.CandidateFleet.Fleet
	if fleet == "" {
		return false, fmt.Errorf("cancelled candidate fleet identity is missing")
	}
	if instance.Status.CandidateFleet.WasServing || fleet == instance.Status.ServingFleetIntentByRole["search"] || fleet == instance.Status.ServingFleetIntentByRole["ingest"] {
		// The ordinary preview reconciler handles alias-only detach for a serving fleet.
		return false, nil
	}
	selector := labels.SelectorFromSet(labels.Set{workload.InstanceLabel: string(instance.UID), workload.FleetLabel: fleet}).String()
	pods, err := r.Core.Pods(instance.Namespace).List(ctx, metav1.ListOptions{LabelSelector: selector, Limit: 513})
	if err != nil || pods.Continue != "" || len(pods.Items) > 512 {
		return false, fmt.Errorf("cannot obtain complete candidate Pod observation")
	}
	sort.Slice(pods.Items, func(i, j int) bool { return pods.Items[i].Name < pods.Items[j].Name })
	for _, pod := range pods.Items {
		// A bound, released or already terminating Pod needs a separate
		// retirement protocol, including application and availability checks.
		if pod.Spec.NodeName != "" || !hasPlacementGate(&pod) || pod.DeletionTimestamp != nil {
			continue
		}
		identity, err := cancelledCandidatePodIdentity(instance, &pod)
		if err != nil {
			return false, err
		}
		intent := &v1alpha1.PodCreationIntent{Name: identity.Name, Role: identity.Role, Zone: identity.Zone, Fleet: identity.Fleet, Revision: identity.Revision, Sequence: identity.Sequence}
		if err := validateRetiredPod(instance, *identity, &pod); err != nil {
			return false, err
		}
		before := instance.DeepCopyObject().(*v1alpha1.OpenSearchServerless)
		if err := appendRetiredCreation(instance, intent); err != nil {
			return false, err
		}
		if err := r.Status().Patch(ctx, instance, client.MergeFromWithOptions(before, client.MergeFromWithOptimisticLock{})); err != nil {
			return false, err
		}
		return true, nil
	}
	return false, nil
}
