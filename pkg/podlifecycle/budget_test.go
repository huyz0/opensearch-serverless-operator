package podlifecycle_test

import (
	"testing"

	"github.com/huyz0/opensearch-serverless-operator/pkg/podlifecycle"
)

func TestRetirementBudgetCountsOtherZonesAndCommittedPods(t *testing.T) {
	group := podlifecycle.Group{Workload: "w", Fleet: "a", Role: "search", Zone: "one"}
	other := group
	other.Zone = "two"
	pod := func(uid string, group podlifecycle.Group) podlifecycle.Pod {
		return podlifecycle.Pod{Identity: podlifecycle.Identity{Namespace: "n", Name: uid, UID: uid, OwnerUID: "owner"}, Group: group, Bound: true, Eligible: true}
	}
	pods := []podlifecycle.Pod{pod("a", group), pod("b", group), pod("c", other), pod("d", other)}
	pods[3].RetirementHeld = true
	snapshot := podlifecycle.Snapshot{Pods: pods}
	policy := podlifecycle.GroupPolicy{Group: group, Desired: 2, MinServing: 1}
	budget := podlifecycle.RetirementBudget{MinServingRole: 3, MaxConcurrentRetirements: 2}
	allowed, err := podlifecycle.CheckRetirementBudget(snapshot, policy, pods[0].Identity, budget)
	if err != nil || allowed {
		t.Fatalf("committed retirement leaves fewer than role floor: allowed=%v err=%v", allowed, err)
	}
	budget.MinServingRole = 2
	allowed, err = podlifecycle.CheckRetirementBudget(snapshot, policy, pods[0].Identity, budget)
	if err != nil || !allowed {
		t.Fatalf("expected capacity in both zones to permit retirement: allowed=%v err=%v", allowed, err)
	}
	budget.MaxConcurrentRetirements = 1
	allowed, err = podlifecycle.CheckRetirementBudget(snapshot, policy, pods[0].Identity, budget)
	if err != nil || allowed {
		t.Fatalf("existing retirement must consume concurrency: allowed=%v err=%v", allowed, err)
	}
}
