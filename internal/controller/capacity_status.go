package controller

import (
	"context"
	"fmt"
	"reflect"
	"sort"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/huyz0/opensearch-serverless-operator/api/v1alpha1"
	"github.com/huyz0/opensearch-serverless-operator/internal/workload"
)

// sampleSearchQualification records one historical development observation per
// minute, rotating by UID. It never feeds a serving or disruption decision.
func (r *InstanceReconciler) sampleSearchQualification(ctx context.Context, instance *v1alpha1.OpenSearchServerless) (bool, error) {
	pool, ok := instance.Spec.Pools["search"]
	if !ok || !pool.WarmOnJoin || !r.fleetQualification().supportsDevelopmentSearchProbe(instance) {
		return false, nil
	}
	now := r.now()
	if last := instance.Status.LastSearchQualification; last != nil && last.ObservedGeneration == instance.Generation && !now.Before(last.ObservedAt.Time) && now.Sub(last.ObservedAt.Time) < time.Minute {
		return false, nil
	}
	selector := labels.SelectorFromSet(labels.Set{workload.InstanceLabel: string(instance.UID), workload.RoleLabel: "search"}).String()
	pods, err := r.Core.Pods(instance.Namespace).List(ctx, metav1.ListOptions{LabelSelector: selector, Limit: 513})
	if err != nil {
		return false, fmt.Errorf("list search Pods for qualification: %w", err)
	}
	if pods.Continue != "" || len(pods.Items) > 512 {
		return false, fmt.Errorf("too many search Pods for a complete qualification sample")
	}
	eligible := make([]corev1.Pod, 0, len(pods.Items))
	for _, pod := range pods.Items {
		if pod.UID != "" && pod.Name != "" && pod.Labels[workload.ZoneLabel] != "" && fleetAllowedForPod(instance, "search", pod.Labels[workload.ZoneLabel], pod.Labels[workload.FleetLabel]) && pod.DeletionTimestamp == nil && pod.Status.Phase == corev1.PodRunning {
			eligible = append(eligible, pod)
		}
	}
	if len(eligible) == 0 {
		return false, nil
	}
	sort.Slice(eligible, func(i, j int) bool { return string(eligible[i].UID) < string(eligible[j].UID) })
	index := 0
	if last := instance.Status.LastSearchQualification; last != nil {
		for i := range eligible {
			if string(eligible[i].UID) > last.PodUID {
				index = i
				break
			}
		}
	}
	pod := &eligible[index]
	observation := &v1alpha1.SearchQualificationObservation{ObservedGeneration: instance.Generation, PodName: pod.Name, PodUID: string(pod.UID), Fleet: pod.Labels[workload.FleetLabel], Zone: pod.Labels[workload.ZoneLabel], Result: "Unavailable", ObservedAt: metav1.NewTime(now)}
	probeCtx, cancel := context.WithTimeout(ctx, 8*time.Second)
	defer cancel()
	qualified, err := r.fleetQualification().qualifyDevelopmentSearchPod(probeCtx, instance, pod.Name, string(pod.UID), observation.Fleet, observation.Zone, 5*time.Second)
	if err == nil {
		observation.Result = "WarmupCompleted"
		observation.ContainerID = qualified.ContainerID
		observation.ProcessIncarnation = qualified.ProcessIncarnation
		if activity := qualified.LocalSearchActivity; qualified.StatsProcessVerified && activity != nil {
			observation.LocalSearchActivity = &v1alpha1.LocalSearchActivity{
				ShardReaders: activity.ShardReaders, BlockCacheHits: activity.BlockCacheHits,
				BlockCacheMisses: activity.BlockCacheMisses, BlockBytesFetched: activity.BlockBytesFetched,
			}
		}
	}
	before := instance.DeepCopyObject().(*v1alpha1.OpenSearchServerless)
	instance.Status.LastSearchQualification = observation
	if err := r.Status().Patch(ctx, instance, client.MergeFromWithOptions(before, client.MergeFromWithOptimisticLock{})); err != nil {
		return false, err
	}
	return true, nil
}

func (r *InstanceReconciler) refreshZoneCapacity(ctx context.Context, instance *v1alpha1.OpenSearchServerless) (bool, error) {
	selector := labels.SelectorFromSet(labels.Set{workload.InstanceLabel: string(instance.UID)}).String()
	pods, err := r.Core.Pods(instance.Namespace).List(ctx, metav1.ListOptions{LabelSelector: selector, Limit: 513})
	if err != nil {
		return false, fmt.Errorf("list Pods for capacity status: %w", err)
	}
	if pods.Continue != "" || len(pods.Items) > 512 {
		return false, fmt.Errorf("too many Pods for a complete capacity observation")
	}
	for _, pod := range pods.Items {
		owner := metav1.GetControllerOf(&pod)
		if owner == nil || owner.UID != instance.UID || pod.UID == "" {
			return false, fmt.Errorf("Pod with instance label has a conflicting owner or identity")
		}
	}
	capacity := summarizeZoneCapacity(instance, pods.Items)
	candidate := summarizeCandidateCapacity(instance, pods.Items)
	now := r.now()
	observed := instance.Status.ZoneCapacityObservedAt
	candidateUnchanged := instance.Status.CandidateFleet == nil || reflect.DeepEqual(instance.Status.CandidateFleet.Capacity, candidate)
	if reflect.DeepEqual(instance.Status.ZoneCapacity, capacity) && candidateUnchanged && instance.Status.ZoneCapacityObservedGeneration == instance.Generation && observed != nil && !now.Before(observed.Time) && now.Sub(observed.Time) < time.Minute {
		return false, nil
	}
	before := instance.DeepCopyObject().(*v1alpha1.OpenSearchServerless)
	instance.Status.ZoneCapacity = capacity
	if instance.Status.CandidateFleet != nil {
		instance.Status.CandidateFleet.Capacity = candidate
	}
	stamp := metav1.NewTime(now)
	instance.Status.ZoneCapacityObservedAt = &stamp
	instance.Status.ZoneCapacityObservedGeneration = instance.Generation
	if err := r.Status().Patch(ctx, instance, client.MergeFromWithOptions(before, client.MergeFromWithOptimisticLock{})); err != nil {
		return false, err
	}
	return true, nil
}

// summarizeZoneCapacity reports Kubernetes observations, including when
// even the maximum possible reserve is below the requested capacity.
func summarizeZoneCapacity(instance *v1alpha1.OpenSearchServerless, pods []corev1.Pod) []v1alpha1.ZoneCapacityStatus {
	groups := make([]v1alpha1.ZoneCapacityStatus, 0)
	for role, pool := range instance.Spec.Pools {
		for zone, desired := range pool.Zones {
			group := v1alpha1.ZoneCapacityStatus{Role: role, Zone: zone, Fleet: instance.Status.ServingFleetIntentByRole[role], Replicas: desired.Replicas, WarmSpare: desired.WarmSpare, MinServing: desired.MinServing, WarmReserveStatus: "NotRequested"}
			var regularReadyAfterDisruption int32
			for _, pod := range pods {
				if pod.Labels[workload.RoleLabel] != role || pod.Labels[workload.ZoneLabel] != zone || pod.Labels[workload.FleetLabel] != group.Fleet {
					continue
				}
				emergency := pod.Annotations[emergencyCapacityAnnotation] == "true"
				if emergency {
					group.EmergencyPods++
				}
				if pod.DeletionTimestamp != nil {
					group.TerminatingPods++
					continue
				}
				if pod.Status.Phase == corev1.PodFailed || pod.Status.Phase == corev1.PodSucceeded {
					group.TerminalPods++
					continue
				}
				group.ObservedPods++
				disrupted := workload.PodDisruptionTarget(&pod)
				if disrupted {
					group.DisruptionTargetPods++
				}
				if pod.Spec.NodeName == "" {
					group.UnboundPods++
					continue
				}
				if podStartupReady(&pod) {
					group.KubernetesReady++
					if disrupted {
						group.ReadyDisruptionTargetPods++
					}
					if emergency {
						group.EmergencyReady++
					}
					if !emergency && !disrupted {
						regularReadyAfterDisruption++
					}
				}
			}
			group.ReadyShortfall = max(0, desired.Replicas+desired.WarmSpare-(group.KubernetesReady-group.EmergencyReady))
			group.BelowMinServing = group.KubernetesReady < desired.MinServing
			group.BelowMinServingAfterDisruption = group.KubernetesReady-group.ReadyDisruptionTargetPods < desired.MinServing
			if desired.WarmSpare > 0 {
				possibleReserve := max(0, regularReadyAfterDisruption-desired.Replicas)
				group.WarmReserveReadyShortfall = max(0, desired.WarmSpare-possibleReserve)
				group.WarmReserveStatus = "Unknown"
				if group.WarmReserveReadyShortfall > 0 {
					group.WarmReserveStatus = "InsufficientReadyPods"
				}
			}

			groups = append(groups, group)
		}
	}
	sort.Slice(groups, func(i, j int) bool {
		if groups[i].Role != groups[j].Role {
			return groups[i].Role < groups[j].Role
		}
		return groups[i].Zone < groups[j].Zone
	})
	return groups
}

// summarizeCandidateCapacity reports even candidate Pods outside the current
// allocation, so a changed or unavailable candidate policy cannot hide them.
func summarizeCandidateCapacity(instance *v1alpha1.OpenSearchServerless, pods []corev1.Pod) []v1alpha1.CandidateZoneCapacity {
	if instance.Status.CandidateFleet == nil || instance.Status.CandidateFleet.Fleet == "" {
		return nil
	}
	type key struct{ role, zone string }
	groups := map[key]v1alpha1.CandidateZoneCapacity{}
	for _, policy := range developmentCandidatePolicies(instance) {
		k := key{policy.Group.Role, policy.Group.Zone}
		groups[k] = v1alpha1.CandidateZoneCapacity{Role: k.role, Zone: k.zone, DesiredKnown: true, DesiredPods: int32(policy.Desired), Warmth: "Unknown"}
	}
	for _, pod := range pods {
		if pod.Labels[workload.FleetLabel] != instance.Status.CandidateFleet.Fleet {
			continue
		}
		k := key{pod.Labels[workload.RoleLabel], pod.Labels[workload.ZoneLabel]}
		group, ok := groups[k]
		if !ok {
			group = v1alpha1.CandidateZoneCapacity{Role: k.role, Zone: k.zone, Warmth: "Unknown"}
		}
		switch {
		case pod.DeletionTimestamp != nil:
			group.TerminatingPods++
		case pod.Status.Phase == corev1.PodFailed || pod.Status.Phase == corev1.PodSucceeded:
			group.TerminalPods++
		default:
			group.ObservedPods++
			if pod.Spec.NodeName == "" {
				group.UnboundPods++
			} else if podStartupReady(&pod) {
				group.KubernetesReady++
			}
		}
		groups[k] = group
	}
	var result []v1alpha1.CandidateZoneCapacity
	for _, group := range groups {
		result = append(result, group)
	}
	sort.Slice(result, func(i, j int) bool {
		if result[i].Role != result[j].Role {
			return result[i].Role < result[j].Role
		}
		return result[i].Zone < result[j].Zone
	})
	return result
}
