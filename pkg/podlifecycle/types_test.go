package podlifecycle_test

import (
	"testing"
	"time"

	"github.com/huyz0/opensearch-serverless-operator/pkg/podlifecycle"
)

func TestWorkerConsumerPlansCapacity(t *testing.T) {
	group := podlifecycle.Group{Workload: "worker", Fleet: "stable", Role: "consumer", Zone: "zone-a"}
	observation := podlifecycle.Snapshot{
		Now:      time.Unix(100, 0),
		Policies: []podlifecycle.GroupPolicy{{Group: group, Desired: 2, MinServing: 1}},
		Pods: []podlifecycle.Pod{{
			Identity: podlifecycle.Identity{Namespace: "work", Name: "worker-1", UID: "uid-1", OwnerUID: "owner-1"},
			Group:    group, Eligible: true, Bound: true,
		}},
	}
	budget := podlifecycle.CreationBudget{Maximum: 1}
	action, err := podlifecycle.PlanCapacity(observation, budget)
	if err != nil || action.Kind != podlifecycle.CreatePod || action.Group != group {
		t.Fatalf("expected one worker create in the required zone, got %+v, %v", action, err)
	}
	observation.Pods = append(observation.Pods, podlifecycle.Pod{
		Identity: podlifecycle.Identity{Namespace: "work", Name: "worker-2", UID: "uid-2", OwnerUID: "owner-1"},
		Group:    group, Eligible: true, Bound: true,
	})
	action, err = podlifecycle.PlanCapacity(observation, budget)
	if err != nil || action.Kind != podlifecycle.Wait {
		t.Fatalf("expected no further create after observing the new worker, got %+v, %v", action, err)
	}
}
