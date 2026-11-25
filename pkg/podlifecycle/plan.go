package podlifecycle

import (
	"fmt"
	"sort"
)

// PlanCapacity proposes one creation for a deficient group or a wait. It does
// not release scheduling gates or retire Pods. The caller persists a unique
// creation intent and shared budget reservation before executing the action.
func PlanCapacity(snapshot Snapshot, budget CreationBudget) (Action, error) {
	if budget.PendingOrCommitted < 0 || budget.Maximum < 0 || budget.PendingOrCommitted > budget.Maximum {
		return Action{}, fmt.Errorf("invalid creation budget")
	}
	wait := Action{Kind: Wait, Reason: "NoCreationAdmitted"}
	if len(snapshot.Policies) == 0 {
		return wait, nil
	}
	policies := append([]GroupPolicy{}, snapshot.Policies...)
	sort.Slice(policies, func(i, j int) bool {
		a, b := policies[i].Group, policies[j].Group
		if a.Workload != b.Workload {
			return a.Workload < b.Workload
		}
		if a.Fleet != b.Fleet {
			return a.Fleet < b.Fleet
		}
		if a.Role != b.Role {
			return a.Role < b.Role
		}
		return a.Zone < b.Zone
	})
	seen := make(map[Group]bool, len(policies))
	for _, policy := range policies {
		if policy.Group.Workload == "" || policy.Group.Fleet == "" || policy.Group.Role == "" || policy.Group.Zone == "" || seen[policy.Group] {
			return Action{}, fmt.Errorf("placement groups must be complete and unique")
		}
		seen[policy.Group] = true
		if err := validateGroupCapacity(policy); err != nil {
			return Action{}, err
		}
	}
	active := make(map[Group]int, len(policies))
	podUIDs := make(map[string]struct{}, len(snapshot.Pods))
	for _, pod := range snapshot.Pods {
		if err := recordPodIdentity(podUIDs, pod.Identity); err != nil {
			return Action{}, err
		}
		if seen[pod.Group] && !pod.Terminating && !pod.Terminal {
			active[pod.Group]++
		}
	}
	if budget.PendingOrCommitted == budget.Maximum {
		return wait, nil
	}
	for _, policy := range policies {
		if active[policy.Group] < policy.Desired {
			return Action{Kind: CreatePod, Group: policy.Group, Reason: "DesiredCapacityMissing"}, nil
		}
	}
	return wait, nil
}
