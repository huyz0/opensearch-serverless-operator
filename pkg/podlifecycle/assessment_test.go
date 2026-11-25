package podlifecycle_test

import (
	"testing"

	"github.com/huyz0/opensearch-serverless-operator/pkg/podlifecycle"
)

func TestAssessCountsCommittedRetirementOnce(t *testing.T) {
	group := podlifecycle.Group{Workload: "worker", Fleet: "a", Role: "reader", Zone: "z1"}
	pods := []podlifecycle.Pod{
		{Identity: podlifecycle.Identity{UID: "1", OwnerUID: "owner"}, Group: group, Bound: true, Eligible: true},
		{Identity: podlifecycle.Identity{UID: "2", OwnerUID: "owner"}, Group: group, Bound: true, Eligible: true, RetirementHeld: true},
		{Identity: podlifecycle.Identity{UID: "3", OwnerUID: "owner"}, Group: group, Bound: true, Eligible: true, Terminating: true, RetirementHeld: true},
		{Identity: podlifecycle.Identity{UID: "4", OwnerUID: "owner"}, Group: group},
	}
	got, err := podlifecycle.Assess(podlifecycle.Snapshot{Pods: pods}, podlifecycle.GroupPolicy{Group: group, Desired: 4, MinServing: 2})
	if err != nil {
		t.Fatal(err)
	}
	if got.Active != 3 || got.Pending != 1 || got.Terminating != 1 || got.Serving != 2 || got.ProjectedServing != 1 || got.Deficit != 1 {
		t.Fatalf("unexpected assessment: %+v", got)
	}
}

func TestAssessRejectsDuplicatePodIdentity(t *testing.T) {
	pod := podlifecycle.Pod{Identity: podlifecycle.Identity{UID: "same", OwnerUID: "owner"}}
	_, err := podlifecycle.Assess(podlifecycle.Snapshot{Pods: []podlifecycle.Pod{pod, pod}}, podlifecycle.GroupPolicy{})
	if err == nil {
		t.Fatal("expected duplicate Pod UID to be rejected")
	}
}

func TestTerminalPodDoesNotSatisfyDesiredCapacity(t *testing.T) {
	group := podlifecycle.Group{Workload: "w", Fleet: "f", Role: "search", Zone: "z"}
	pod := podlifecycle.Pod{Identity: podlifecycle.Identity{UID: "old", OwnerUID: "owner"}, Group: group, Bound: true, Eligible: true, Terminal: true}
	assessment, err := podlifecycle.Assess(podlifecycle.Snapshot{Pods: []podlifecycle.Pod{pod}}, podlifecycle.GroupPolicy{Group: group, Desired: 1})
	if err != nil || assessment.Active != 0 || assessment.Serving != 0 || assessment.Deficit != 1 {
		t.Fatalf("terminal Pod counted toward capacity: %+v, %v", assessment, err)
	}
}
