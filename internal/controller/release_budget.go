package controller

import (
	"fmt"
	"github.com/huyz0/opensearch-serverless-operator/api/v1alpha1"
	"github.com/huyz0/opensearch-serverless-operator/pkg/podlifecycle"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"time"
)

const maxReleaseRecords = 256
const releaseRetention = time.Hour

// reservePodRelease counts gate-release intents, including attempts whose
// eventual API outcome is unknown. The caller must persist the returned record
// with its gate-release intent in the same optimistic status update.
func reservePodRelease(records []v1alpha1.PodReleaseRecord, role, zone, uid string, now time.Time, window time.Duration, limit int) ([]v1alpha1.PodReleaseRecord, time.Duration, error) {
	if uid == "" {
		return nil, 0, fmt.Errorf("Pod UID is required")
	}
	kept, wait, err := inspectReleaseBudget(records, role, zone, uid, now, window, limit)
	if err != nil || wait > 0 {
		return kept, wait, err
	}
	// Round up because metav1.Time serializes at second precision. This can
	// conservatively extend a window by one second, never shorten it.
	kept = append(kept, v1alpha1.PodReleaseRecord{PodUID: uid, Role: role, Zone: zone, StartedAt: metav1.NewTime(now.Truncate(time.Second).Add(time.Second))})
	return kept, 0, nil
}

// inspectReleaseBudget can check a candidate before it has a Pod UID.
func inspectReleaseBudget(records []v1alpha1.PodReleaseRecord, role, zone, uid string, now time.Time, window time.Duration, limit int) ([]v1alpha1.PodReleaseRecord, time.Duration, error) {
	if role == "" || zone == "" || now.IsZero() || window < time.Second || window > releaseRetention || limit < 1 || limit > 64 || len(records) > maxReleaseRecords {
		return nil, 0, fmt.Errorf("invalid Pod release budget or identity")
	}
	kept := make([]v1alpha1.PodReleaseRecord, 0, len(records)+1)
	seen := make(map[string]bool, len(records))
	count := 0
	var firstWindow, firstRetained time.Time
	for _, record := range records {
		when := record.StartedAt.Time
		if record.PodUID == "" || record.Role == "" || record.Zone == "" || when.IsZero() || when.After(now.Add(time.Second)) || seen[record.PodUID] {
			return nil, 0, fmt.Errorf("invalid or future-dated Pod release history")
		}
		seen[record.PodUID] = true
		if !when.After(now.Add(-releaseRetention)) {
			continue
		}
		kept = append(kept, record)
		if firstRetained.IsZero() || when.Before(firstRetained) {
			firstRetained = when
		}
		if record.Role == role && record.Zone == zone && when.After(now.Add(-window)) {
			count++
			if firstWindow.IsZero() || when.Before(firstWindow) {
				firstWindow = when
			}
		}
	}
	if uid != "" && seen[uid] {
		return nil, 0, fmt.Errorf("Pod already has a release reservation")
	}
	var wait time.Duration
	if count >= limit {
		wait = firstWindow.Add(window).Sub(now)
	}
	if len(kept) >= maxReleaseRecords {
		if next := firstRetained.Add(releaseRetention).Sub(now); next > wait {
			wait = next
		}
	}
	return kept, wait, nil
}

func (r *InstanceReconciler) releaseBudget() (time.Duration, int) {
	window, limit := r.DevelopmentReleaseWindow, r.DevelopmentReleaseLimit
	if window == 0 {
		window = 10 * time.Minute
	}
	if limit == 0 {
		limit = 32
	}
	return window, limit
}

// selectReleaseEligibleCapacity keeps the lifecycle planner's ordering while
// skipping deficient groups that cannot yet release a new Pod. It does not
// reserve a release; the later status update remains authoritative.
func selectReleaseEligibleCapacity(snapshot podlifecycle.Snapshot, ready map[podlifecycle.Group]int, regionalShortfalls map[string]string, records []v1alpha1.PodReleaseRecord, now time.Time, window time.Duration, limit int) (podlifecycle.Action, time.Duration, error) {
	budget := podlifecycle.CreationBudget{Maximum: 1}
	first, err := podlifecycle.PlanCapacity(snapshot, budget)
	if err != nil || first.Kind != podlifecycle.CreatePod {
		return first, 0, err
	}
	filtered := snapshot
	filtered.Policies = nil
	var earliest time.Duration
	for _, policy := range snapshot.Policies {
		allowed, err := podlifecycle.CheckCreation(snapshot, policy, budget)
		if err != nil {
			return podlifecycle.Action{}, 0, err
		}
		if !allowed {
			continue
		}
		_, wait, err := inspectReleaseBudget(records, policy.Group.Role, policy.Group.Zone, "", now, window, limit)
		if err != nil {
			return podlifecycle.Action{}, 0, err
		}
		if wait > 0 {
			if earliest == 0 || wait < earliest {
				earliest = wait
			}
			continue
		}
		filtered.Policies = append(filtered.Policies, policy)
	}
	action, err := selectFloorPriorityCapacity(filtered, ready, regionalShortfalls)
	if err != nil {
		return podlifecycle.Action{}, 0, err
	}
	if action.Kind == podlifecycle.CreatePod {
		return action, 0, nil
	}
	return action, earliest, nil
}

// selectFloorPriorityCapacity uses Ready counts excluding disruption targets
// to order creation proposals. The counts do not qualify Java or retirement.
func selectFloorPriorityCapacity(snapshot podlifecycle.Snapshot, ready map[podlifecycle.Group]int, regionalShortfalls map[string]string) (podlifecycle.Action, error) {
	budget := podlifecycle.CreationBudget{Maximum: 1}
	first, err := podlifecycle.PlanCapacity(snapshot, budget)
	if err != nil || first.Kind != podlifecycle.CreatePod {
		return first, err
	}
	both, regional, zonal := snapshot, snapshot, snapshot
	both.Policies, regional.Policies, zonal.Policies = nil, nil, nil
	for _, policy := range snapshot.Policies {
		if ready[policy.Group] < 0 {
			return podlifecycle.Action{}, fmt.Errorf("negative Ready count")
		}
		allowed, err := podlifecycle.CheckCreation(snapshot, policy, budget)
		if err != nil {
			return podlifecycle.Action{}, err
		}
		if !allowed {
			continue
		}
		fleet := regionalShortfalls[policy.Group.Role]
		regionalDeficit := fleet != "" && fleet == policy.Group.Fleet
		zonalDeficit := ready[policy.Group] < policy.MinServing
		if regionalDeficit && zonalDeficit {
			both.Policies = append(both.Policies, policy)
		}
		if regionalDeficit {
			regional.Policies = append(regional.Policies, policy)
		}
		if zonalDeficit {
			zonal.Policies = append(zonal.Policies, policy)
		}
	}
	if len(both.Policies) != 0 {
		return podlifecycle.PlanCapacity(both, budget)
	}
	if len(regional.Policies) != 0 {
		return podlifecycle.PlanCapacity(regional, budget)
	}
	if len(zonal.Policies) != 0 {
		return podlifecycle.PlanCapacity(zonal, budget)
	}
	return first, nil
}

// regionalReadyShortfalls identifies serving fleets below an explicit role
// floor. Ready Pods in a preview or a retired fleet do not restore that floor.
func regionalReadyShortfalls(instance *v1alpha1.OpenSearchServerless, ready map[podlifecycle.Group]int) map[string]string {
	if instance.Spec.Availability == nil {
		return nil
	}
	shortfalls := make(map[string]string)
	for role, minimum := range instance.Spec.Availability.MinServingTotal {
		fleet := instance.Status.ServingFleetIntentByRole[role]
		if fleet == "" || minimum <= 0 {
			continue
		}
		total := 0
		for zone, desired := range instance.Spec.Pools[role].Zones {
			if int64(desired.Replicas)+int64(desired.WarmSpare) > 0 {
				total += ready[podlifecycle.Group{Workload: string(instance.UID), Fleet: fleet, Role: role, Zone: zone}]
			}
		}
		if total < int(minimum) {
			shortfalls[role] = fleet
		}
	}
	return shortfalls
}
