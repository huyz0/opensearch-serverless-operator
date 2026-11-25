package controller

import (
	"context"
	"errors"
	"fmt"
	"sort"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"

	"github.com/huyz0/opensearch-serverless-operator/api/v1alpha1"
	"github.com/huyz0/opensearch-serverless-operator/internal/workload"
	"github.com/huyz0/opensearch-serverless-operator/pkg/podlifecycle"
)

var errPreviewServingCapacity = errors.New("serving role and zone lack Ready capacity for preview")

func readyForPreview(pod *corev1.Pod) bool {
	return podReadyForServing(pod) && pod.Annotations[emergencyCapacityAnnotation] != "true"
}

// developmentCandidatePolicies permits only a same-image preview with pinned
// Services. A different image needs compatibility evidence not yet available.
// Allocation is deterministic and bounded across all roles and zones.
func developmentCandidatePolicies(instance *v1alpha1.OpenSearchServerless) []podlifecycle.GroupPolicy {
	if instance == nil || instance.Spec.Rollout == nil || instance.Spec.Rollout.Strategy != "BlueGreen" || instance.Spec.Rollout.Candidate == nil || instance.Status.CandidateFleet == nil || instance.Spec.Rollout.MaxPreviewReplicas <= 0 {
		return nil
	}
	want, state := instance.Spec.Rollout.Candidate, instance.Status.CandidateFleet
	condition := meta.FindStatusCondition(instance.Status.Conditions, "PreviewServicesConfigured")
	if want.Fleet == "" || want.Fleet != state.Fleet || want.Image != state.Image || want.Image != instance.Spec.Image || !candidateInputsMatch(state, want) || state.PreviewRetiring || state.Revision == "" || state.Generation != instance.Generation || state.SearchServiceUID == "" || state.IngestServiceUID == "" || condition == nil || condition.Status != metav1.ConditionTrue || condition.ObservedGeneration != instance.Generation {
		return nil
	}
	type group struct {
		role, zone string
		desired    int64
	}
	var groups []group
	for role, pool := range instance.Spec.Pools {
		if want.Fleet == instance.Status.ServingFleetIntentByRole[role] {
			return nil
		}
		for zone, desired := range pool.Zones {
			if total := int64(desired.Replicas) + int64(desired.WarmSpare); total > 0 {
				groups = append(groups, group{role: role, zone: zone, desired: total})
			}
		}
	}
	sort.Slice(groups, func(i, j int) bool {
		if groups[i].role != groups[j].role {
			return groups[i].role < groups[j].role
		}
		return groups[i].zone < groups[j].zone
	})
	// Round-robin arithmetic avoids a loop proportional to a user-supplied
	// replica count, while spreading a partial preview over all groups.
	remaining := int64(instance.Spec.Rollout.MaxPreviewReplicas)
	counts := make([]int64, len(groups))
	for remaining > 0 {
		active := int64(0)
		var next int64 = -1
		for i, g := range groups {
			if counts[i] < g.desired {
				active++
				if gap := g.desired - counts[i]; next < 0 || gap < next {
					next = gap
				}
			}
		}
		if active == 0 {
			break
		}
		step := remaining / active
		if step == 0 {
			for i, g := range groups {
				if remaining > 0 && counts[i] < g.desired {
					counts[i]++
					remaining--
				}
			}
			break
		}
		if step > next {
			step = next
		}
		for i, g := range groups {
			if counts[i] < g.desired {
				counts[i] += step
				remaining -= step
			}
		}
	}
	policies := make([]podlifecycle.GroupPolicy, 0, len(groups))
	for i, g := range groups {
		if counts[i] > 0 {
			policies = append(policies, podlifecycle.GroupPolicy{Group: podlifecycle.Group{Workload: string(instance.UID), Fleet: want.Fleet, Role: g.role, Zone: g.zone}, Desired: int(counts[i])})
		}
	}
	return policies
}

// checkCandidateAdmission rechecks the preview Service identities and counts
// all observed candidate Pods, including terminating ones, immediately before
// creating or releasing one. Kubernetes readiness here is a development gate,
// not proof of Java warmth, peer compatibility or client traffic health.
func (r *InstanceReconciler) checkCandidateAdmission(ctx context.Context, instance *v1alpha1.OpenSearchServerless, intent *v1alpha1.PodCreationIntent) error {
	if intent.Fleet == instance.Status.ServingFleetIntentByRole[intent.Role] {
		return nil
	}
	if !fleetAllowedForPod(instance, intent.Role, intent.Zone, intent.Fleet) || instance.Status.CandidateFleet == nil || instance.Status.CandidateFleet.Revision != intent.Revision {
		return fmt.Errorf("candidate Pod intent is no longer eligible")
	}
	if _, rotatesPeerTLS := instance.Status.PeerTLSByFleet[intent.Fleet]; rotatesPeerTLS {
		if err := r.qualifyPeerRotation(ctx, instance, intent.Fleet, instance.Status.ServingFleetIntentByRole[intent.Role], true); err != nil {
			return err
		}
	}
	pods, err := r.previewPodInventory(ctx, instance)
	if err != nil {
		return err
	}
	if !previewServingCapacityReady(instance, pods) {
		return errPreviewServingCapacity
	}
	candidateCount, targetObserved := 0, false
	for _, pod := range pods {
		if pod.Labels[workload.FleetLabel] == intent.Fleet {
			candidateCount++
			if pod.Name == intent.Name {
				targetObserved = true
			}
		}
	}
	if candidateCount > int(instance.Spec.Rollout.MaxPreviewReplicas) || (!targetObserved && candidateCount >= int(instance.Spec.Rollout.MaxPreviewReplicas)) {
		return fmt.Errorf("candidate preview Pod limit is exhausted")
	}
	pinned := map[string]types.UID{"search": types.UID(instance.Status.CandidateFleet.SearchServiceUID), "ingest": types.UID(instance.Status.CandidateFleet.IngestServiceUID)}
	_, err = ensurePreviewServices(ctx, nil, r.Core, instance, intent.Fleet, pinned)
	return err
}

func (r *InstanceReconciler) previewPodInventory(ctx context.Context, instance *v1alpha1.OpenSearchServerless) ([]corev1.Pod, error) {
	selector := labels.SelectorFromSet(labels.Set{workload.InstanceLabel: string(instance.UID)}).String()
	pods, err := r.Core.Pods(instance.Namespace).List(ctx, metav1.ListOptions{LabelSelector: selector, Limit: 513})
	if err != nil || pods.Continue != "" || len(pods.Items) > 512 {
		return nil, fmt.Errorf("cannot obtain complete bounded Pod observation")
	}
	for _, pod := range pods.Items {
		owner := metav1.GetControllerOf(&pod)
		if owner == nil || owner.UID != instance.UID || pod.UID == "" {
			return nil, fmt.Errorf("Pod identity conflicts with instance")
		}
	}
	return pods.Items, nil
}

func previewServingCapacityReady(instance *v1alpha1.OpenSearchServerless, pods []corev1.Pod) bool {
	ready := make(map[provisioningGroup]int)
	for _, pod := range pods {
		if pod.Annotations[emergencyCapacityAnnotation] == "true" {
			return false
		}
		role := pod.Labels[workload.RoleLabel]
		if pod.Labels[workload.FleetLabel] == instance.Status.ServingFleetIntentByRole[role] && readyForPreview(&pod) {
			ready[provisioningGroup{role, pod.Labels[workload.ZoneLabel]}]++
		}
	}
	for role, pool := range instance.Spec.Pools {
		for zone, desired := range pool.Zones {
			if int64(ready[provisioningGroup{role, zone}]) < int64(desired.Replicas)+int64(desired.WarmSpare) {
				return false
			}
		}
	}
	return true
}

func fleetAllowedForPod(instance *v1alpha1.OpenSearchServerless, role, zone, fleet string) bool {
	if fleet != "" && fleet == instance.Status.ServingFleetIntentByRole[role] {
		return true
	}
	for _, policy := range developmentCandidatePolicies(instance) {
		if policy.Group.Role == role && policy.Group.Zone == zone && policy.Group.Fleet == fleet {
			return true
		}
	}
	return false
}
