package podlifecycle_test

import (
	"testing"

	"github.com/huyz0/opensearch-serverless-operator/pkg/podlifecycle"
)

func TestCheckCreationRespectsExistingPendingAndReservedSlots(t *testing.T) {
	group := podlifecycle.Group{Workload: "workers", Fleet: "a", Role: "reader", Zone: "z1"}
	snapshot := podlifecycle.Snapshot{Pods: []podlifecycle.Pod{
		{Identity: podlifecycle.Identity{UID: "pending", OwnerUID: "owner"}, Group: group},
	}}
	policy := podlifecycle.GroupPolicy{Group: group, Desired: 2, MinServing: 1}
	allowed, err := podlifecycle.CheckCreation(snapshot, policy, podlifecycle.CreationBudget{PendingOrCommitted: 1, Maximum: 1})
	if err != nil || allowed {
		t.Fatalf("full pending budget must hold creation: allowed=%v err=%v", allowed, err)
	}
	allowed, err = podlifecycle.CheckCreation(snapshot, policy, podlifecycle.CreationBudget{PendingOrCommitted: 1, Maximum: 2})
	if err != nil || !allowed {
		t.Fatalf("remaining slot should allow one missing Pod: allowed=%v err=%v", allowed, err)
	}
	policy.Desired = 1
	allowed, err = podlifecycle.CheckCreation(snapshot, policy, podlifecycle.CreationBudget{PendingOrCommitted: 1, Maximum: 2})
	if err != nil || allowed {
		t.Fatalf("pending Pod already satisfies target: allowed=%v err=%v", allowed, err)
	}
}
