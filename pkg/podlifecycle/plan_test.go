package podlifecycle_test

import (
	"reflect"
	"testing"

	"github.com/huyz0/opensearch-serverless-operator/pkg/podlifecycle"
)

func TestPlanCapacityIsDeterministicAndBounded(t *testing.T) {
	a := podlifecycle.Group{Workload: "w", Fleet: "f", Role: "search", Zone: "a"}
	b := a
	b.Zone = "b"
	policies := []podlifecycle.GroupPolicy{{Group: b, Desired: 1}, {Group: a, Desired: 1}}
	snapshot := podlifecycle.Snapshot{Policies: policies}
	budget := podlifecycle.CreationBudget{Maximum: 1}
	first, err := podlifecycle.PlanCapacity(snapshot, budget)
	if err != nil || first.Kind != podlifecycle.CreatePod || first.Group != a {
		t.Fatalf("unexpected first action %+v: %v", first, err)
	}
	snapshot.Policies[0], snapshot.Policies[1] = snapshot.Policies[1], snapshot.Policies[0]
	second, err := podlifecycle.PlanCapacity(snapshot, budget)
	if err != nil || !reflect.DeepEqual(first, second) {
		t.Fatalf("plan must not depend on input ordering: %+v, %v", second, err)
	}
	budget.PendingOrCommitted = 1
	wait, err := podlifecycle.PlanCapacity(snapshot, budget)
	if err != nil || wait.Kind != podlifecycle.Wait {
		t.Fatalf("full budget must wait: %+v, %v", wait, err)
	}
}

func TestPlanCapacityRejectsInvalidSnapshotsBeforeProposingCreation(t *testing.T) {
	group := podlifecycle.Group{Workload: "w", Fleet: "f", Role: "search", Zone: "a"}
	other := group
	other.Zone = "b"
	pod := podlifecycle.Pod{Group: other, Identity: podlifecycle.Identity{UID: "pod", OwnerUID: "owner"}}
	duplicate := pod
	duplicate.Group = group
	for _, tc := range []struct {
		name     string
		snapshot podlifecycle.Snapshot
	}{
		{name: "duplicate pod across groups", snapshot: podlifecycle.Snapshot{
			Policies: []podlifecycle.GroupPolicy{{Group: group, Desired: 2}},
			Pods:     []podlifecycle.Pod{pod, duplicate},
		}},
		{name: "invalid later policy", snapshot: podlifecycle.Snapshot{
			Policies: []podlifecycle.GroupPolicy{{Group: group, Desired: 1}, {Group: other, Desired: -1}},
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if action, err := podlifecycle.PlanCapacity(tc.snapshot, podlifecycle.CreationBudget{Maximum: 1}); err == nil {
				t.Fatalf("invalid snapshot produced action %+v", action)
			}
		})
	}
}
