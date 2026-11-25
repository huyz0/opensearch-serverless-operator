package controller

import (
	"fmt"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"github.com/huyz0/opensearch-serverless-operator/api/v1alpha1"
	"github.com/huyz0/opensearch-serverless-operator/pkg/podlifecycle"
	"testing"
	"time"
)

func TestReleaseBudgetIsPerRoleZoneAndSurvivesWindowChanges(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	records, wait, err := reservePodRelease(nil, "search", "a", "one", now, 10*time.Minute, 1)
	if err != nil || wait != 0 || len(records) != 1 {
		t.Fatalf("first reservation: %v %v", wait, err)
	}
	if _, wait, err = reservePodRelease(records, "search", "a", "two", now.Add(time.Minute), 10*time.Minute, 1); err != nil || wait != 9*time.Minute+time.Second {
		t.Fatalf("same group must wait: %v %v", wait, err)
	}
	if _, wait, err = reservePodRelease(records, "ingest", "a", "two", now.Add(time.Minute), 10*time.Minute, 1); err != nil || wait != 0 {
		t.Fatal("other role should not be throttled")
	}
	if _, wait, err = reservePodRelease(records, "search", "b", "two", now.Add(time.Minute), 10*time.Minute, 1); err != nil || wait != 0 {
		t.Fatal("other zone should not be throttled")
	}
	records, wait, err = reservePodRelease(records, "search", "a", "two", now.Add(10*time.Minute+time.Second), 10*time.Minute, 1)
	if err != nil || wait != 0 || len(records) != 2 {
		t.Fatal("expired release must not throttle")
	}
	if _, wait, err = reservePodRelease(records, "search", "a", "three", now.Add(11*time.Minute), time.Hour, 2); err != nil || wait != 49*time.Minute+time.Second {
		t.Fatalf("longer window must count retained history: %v %v", wait, err)
	}
	if _, _, err = reservePodRelease(records, "search", "a", "two", now.Add(11*time.Minute), 10*time.Minute, 1); err == nil {
		t.Fatal("duplicate Pod reservation accepted")
	}
	if _, _, err = reservePodRelease(records, "search", "a", "three", now.Add(-2*time.Second), 10*time.Minute, 1); err == nil {
		t.Fatal("future history accepted")
	}
}

func TestReleaseBudgetBoundsHistory(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	records := make([]v1alpha1.PodReleaseRecord, 0, maxReleaseRecords)
	for i := 0; i < maxReleaseRecords; i++ {
		records = append(records, v1alpha1.PodReleaseRecord{PodUID: fmt.Sprint(i), Role: "search", Zone: fmt.Sprint(i), StartedAt: metav1.NewTime(now.Add(-time.Minute))})
	}
	if _, wait, err := reservePodRelease(records, "ingest", "a", "new", now, 10*time.Minute, 1); err != nil || wait != 59*time.Minute {
		t.Fatalf("full history must hold: %v %v", wait, err)
	}
	if next, wait, err := reservePodRelease(records, "ingest", "a", "new", now.Add(59*time.Minute), 10*time.Minute, 1); err != nil || wait != 0 || len(next) != 1 {
		t.Fatalf("expired history must be pruned: %v %v %v", len(next), wait, err)
	}
}

func TestReleaseBudgetDoesNotShortenWindowOnSerialization(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 900000000, time.UTC)
	records, _, err := reservePodRelease(nil, "search", "a", "one", now, time.Second, 1)
	if err != nil {
		t.Fatal(err)
	}
	if _, wait, err := reservePodRelease(records, "search", "a", "two", now.Add(200*time.Millisecond), time.Second, 1); err != nil || wait == 0 {
		t.Fatalf("rounded release allowed too early: %v %v", wait, err)
	}
}

func TestReleaseEligibleCapacitySkipsThrottledGroup(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	group := func(role, zone string) podlifecycle.Group {
		return podlifecycle.Group{Workload: "owner", Fleet: "blue", Role: role, Zone: zone}
	}
	snapshot := podlifecycle.Snapshot{Policies: []podlifecycle.GroupPolicy{{Group: group("search", "a"), Desired: 1}, {Group: group("search", "b"), Desired: 1}}}
	records, _, err := reservePodRelease(nil, "search", "a", "pod-a", now, 10*time.Minute, 1)
	if err != nil {
		t.Fatal(err)
	}
	action, wait, err := selectReleaseEligibleCapacity(snapshot, nil, nil, records, now, 10*time.Minute, 1)
	if err != nil || wait != 0 || action.Kind != podlifecycle.CreatePod || action.Group.Zone != "b" {
		t.Fatalf("healthy zone was not selected: %#v %v %v", action, wait, err)
	}
	records, _, err = reservePodRelease(records, "search", "b", "pod-b", now, 10*time.Minute, 1)
	if err != nil {
		t.Fatal(err)
	}
	action, wait, err = selectReleaseEligibleCapacity(snapshot, nil, nil, records, now, 10*time.Minute, 1)
	if err != nil || action.Kind != podlifecycle.Wait || wait != 10*time.Minute+time.Second {
		t.Fatalf("all groups must wait: %#v %v %v", action, wait, err)
	}
	action, wait, err = selectReleaseEligibleCapacity(snapshot, nil, nil, records, now.Add(10*time.Minute+time.Second), 10*time.Minute, 1)
	if err != nil || wait != 0 || action.Group.Zone != "a" {
		t.Fatalf("expired budget did not restore planner ordering: %#v %v %v", action, wait, err)
	}
}

func TestCapacitySelectionPrioritizesBelowKubernetesReadyFloor(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	a := podlifecycle.Group{Workload: "owner", Fleet: "blue", Role: "ingest", Zone: "a"}
	b := podlifecycle.Group{Workload: "owner", Fleet: "blue", Role: "search", Zone: "b"}
	snapshot := podlifecycle.Snapshot{Policies: []podlifecycle.GroupPolicy{{Group: a, Desired: 2, MinServing: 1}, {Group: b, Desired: 1, MinServing: 1}}, Pods: []podlifecycle.Pod{{Identity: podlifecycle.Identity{UID: "a-ready", OwnerUID: "owner"}, Group: a, Bound: true}}}
	ready := map[podlifecycle.Group]int{a: 1, b: 0}
	action, wait, err := selectReleaseEligibleCapacity(snapshot, ready, nil, nil, now, 10*time.Minute, 1)
	if err != nil || wait != 0 || action.Group != b {
		t.Fatalf("below-floor group was not prioritized: %#v %v", action, err)
	}
	records, _, err := reservePodRelease(nil, "search", "b", "earlier", now, 10*time.Minute, 1)
	if err != nil {
		t.Fatal(err)
	}
	action, wait, err = selectReleaseEligibleCapacity(snapshot, ready, nil, records, now, 10*time.Minute, 1)
	if err != nil || wait != 0 || action.Group != a {
		t.Fatalf("throttled group blocked eligible fallback: %#v %v", action, err)
	}
}

func TestFloorPriorityDoesNotCreateBeyondDesired(t *testing.T) {
	a := podlifecycle.Group{Workload: "owner", Fleet: "blue", Role: "ingest", Zone: "a"}
	b := podlifecycle.Group{Workload: "owner", Fleet: "blue", Role: "search", Zone: "b"}
	snapshot := podlifecycle.Snapshot{Policies: []podlifecycle.GroupPolicy{{Group: a, Desired: 1, MinServing: 1}, {Group: b, Desired: 1}}, Pods: []podlifecycle.Pod{{Identity: podlifecycle.Identity{UID: "unready", OwnerUID: "owner"}, Group: a, Bound: true}}}
	action, err := selectFloorPriorityCapacity(snapshot, nil, nil)
	if err != nil || action.Kind != podlifecycle.CreatePod || action.Group != b {
		t.Fatalf("unready Pod incorrectly caused surge: %#v %v", action, err)
	}
}

func TestRegionalShortfallPrioritizesZoneBelowItsOwnFloor(t *testing.T) {
	searchA := podlifecycle.Group{Workload: "owner", Fleet: "blue", Role: "search", Zone: "a"}
	searchB := podlifecycle.Group{Workload: "owner", Fleet: "blue", Role: "search", Zone: "b"}
	snapshot := podlifecycle.Snapshot{
		Policies: []podlifecycle.GroupPolicy{
			{Group: searchA, Desired: 2, MinServing: 1},
			{Group: searchB, Desired: 2, MinServing: 1},
		},
		Pods: []podlifecycle.Pod{
			{Identity: podlifecycle.Identity{UID: "a", OwnerUID: "owner"}, Group: searchA, Bound: true},
			{Identity: podlifecycle.Identity{UID: "b", OwnerUID: "owner"}, Group: searchB, Bound: true},
		},
	}
	ready := map[podlifecycle.Group]int{searchA: 1}
	regional := map[string]string{"search": "blue"}
	action, err := selectFloorPriorityCapacity(snapshot, ready, regional)
	if err != nil || action.Kind != podlifecycle.CreatePod || action.Group != searchB {
		t.Fatalf("eligible zone below both floors did not get priority: %#v %v", action, err)
	}
}

func TestRegionalShortfallPrioritizesServingFleetInSurvivingZone(t *testing.T) {
	now := time.Now()
	instance := &v1alpha1.OpenSearchServerless{
		ObjectMeta: metav1.ObjectMeta{UID: "owner"},
		Spec: v1alpha1.OpenSearchServerlessSpec{
			Availability: &v1alpha1.Availability{MinServingTotal: map[string]int32{"search": 2}},
			Pools: map[string]v1alpha1.Pool{"search": {Zones: map[string]v1alpha1.Zone{
				"a": {Replicas: 1, MinServing: 1}, "b": {Replicas: 1, WarmSpare: 1, MinServing: 1},
			}}},
		},
		Status: v1alpha1.OpenSearchServerlessStatus{ServingFleetIntentByRole: map[string]string{"search": "blue", "ingest": "blue"}},
	}
	searchA := podlifecycle.Group{Workload: "owner", Fleet: "blue", Role: "search", Zone: "a"}
	searchB := podlifecycle.Group{Workload: "owner", Fleet: "blue", Role: "search", Zone: "b"}
	ingest := podlifecycle.Group{Workload: "owner", Fleet: "blue", Role: "ingest", Zone: "a"}
	preview := podlifecycle.Group{Workload: "owner", Fleet: "green", Role: "search", Zone: "a"}
	snapshot := podlifecycle.Snapshot{
		Policies: []podlifecycle.GroupPolicy{
			{Group: ingest, Desired: 1, MinServing: 1}, {Group: searchB, Desired: 2, MinServing: 1}, {Group: preview, Desired: 3},
		},
		Pods: []podlifecycle.Pod{{Identity: podlifecycle.Identity{UID: "search", OwnerUID: "owner"}, Group: searchB, Bound: true}},
	}
	ready := map[podlifecycle.Group]int{searchB: 1, preview: 2}
	shortfalls := regionalReadyShortfalls(instance, ready)
	action, err := selectFloorPriorityCapacity(snapshot, ready, shortfalls)
	if err != nil || action.Kind != podlifecycle.CreatePod || action.Group != searchB {
		t.Fatalf("regional shortfall did not prioritize the surviving serving group: %#v %v", action, err)
	}
	records, _, err := reservePodRelease(nil, "search", "b", "earlier", now, time.Minute, 1)
	if err != nil {
		t.Fatal(err)
	}
	action, wait, err := selectReleaseEligibleCapacity(snapshot, ready, shortfalls, records, now, time.Minute, 1)
	if err != nil || wait != 0 || action.Group != ingest {
		t.Fatalf("release-limited regional recovery blocked other eligible work: %#v %v", action, err)
	}
	ready[searchA] = 1
	action, err = selectFloorPriorityCapacity(snapshot, ready, regionalReadyShortfalls(instance, ready))
	if err != nil || action.Group != ingest {
		t.Fatalf("recovered role still had regional priority: %#v %v", action, err)
	}
}
