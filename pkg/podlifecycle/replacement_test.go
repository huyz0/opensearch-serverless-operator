package podlifecycle_test

import (
	"testing"
	"time"

	"github.com/huyz0/opensearch-serverless-operator/pkg/podlifecycle"
)

func TestReplacementRequiresQualifiedSameGroupSurgeAndHonorsHolds(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	group := podlifecycle.Group{Workload: "w", Fleet: "blue", Role: "search", Zone: "a"}
	makePod := func(uid, revision string, eligible bool) podlifecycle.Pod {
		return podlifecycle.Pod{Identity: podlifecycle.Identity{Namespace: "n", Name: uid, UID: uid, OwnerUID: "owner"}, Group: group, Revision: revision, Bound: true, Eligible: eligible, ObservedAt: now}
	}
	policy := podlifecycle.ReplacementPolicy{GroupPolicy: podlifecycle.GroupPolicy{Group: group, Desired: 2, MinServing: 2}, OwnerUID: "owner", TargetRevision: "new", MaxSurge: 1, MaxObservationAge: time.Minute, Creation: podlifecycle.CreationBudget{Maximum: 1}, Retirement: podlifecycle.RetirementBudget{MinServingRole: 2, MaxConcurrentRetirements: 1}}
	snapshot := podlifecycle.Snapshot{Now: now, Pods: []podlifecycle.Pod{makePod("old-a", "old", true), makePod("old-b", "old", true)}}
	check := func(kind podlifecycle.ActionKind, reason string) podlifecycle.Action {
		t.Helper()
		action, err := podlifecycle.PlanReplacement(snapshot, policy)
		if err != nil || action.Kind != kind || action.Reason != reason {
			t.Fatalf("got %#v, %v", action, err)
		}
		return action
	}
	check(podlifecycle.CreatePod, "TargetRevisionCapacityNeeded")
	snapshot.Pods = append(snapshot.Pods, makePod("new-a", "new", false))
	check(podlifecycle.Wait, "ReplacementBlocked")
	snapshot.Pods[2].Eligible = true
	policy.Retirement.MinServingRole = 3
	check(podlifecycle.Wait, "ReplacementBlocked")
	policy.Retirement.MinServingRole = 2
	action := check(podlifecycle.EvictPod, "QualifiedReplacementAvailable")
	if action.Pod.UID != "old-a" {
		t.Fatalf("victim was not deterministic: %s", action.Pod.UID)
	}
	snapshot.Pods[0].RetirementHeld = true
	policy.Retirement.MaxConcurrentRetirements = 2
	check(podlifecycle.Wait, "ReplacementBlocked")
	snapshot.Pods[0].RetirementHeld = false
	snapshot.Pods[0].Terminating = true
	check(podlifecycle.Wait, "ReplacementBlocked")
	snapshot.Pods = snapshot.Pods[1:]
	check(podlifecycle.CreatePod, "TargetRevisionCapacityNeeded")
}

func TestReplacementRejectsStaleEvidenceAndCountsReservations(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	g := podlifecycle.Group{Workload: "w", Fleet: "blue", Role: "index", Zone: "a"}
	pod := podlifecycle.Pod{Identity: podlifecycle.Identity{Namespace: "n", Name: "p", UID: "p", OwnerUID: "o"}, Group: g, Revision: "old", Bound: true, Eligible: true, ObservedAt: now}
	snapshot := podlifecycle.Snapshot{Now: now, Pods: []podlifecycle.Pod{pod}}
	policy := podlifecycle.ReplacementPolicy{GroupPolicy: podlifecycle.GroupPolicy{Group: g, Desired: 1, MinServing: 1}, OwnerUID: "o", TargetRevision: "new", MaxSurge: 1, MaxObservationAge: time.Minute, Creation: podlifecycle.CreationBudget{Maximum: 1}, Retirement: podlifecycle.RetirementBudget{MinServingRole: 1, MaxConcurrentRetirements: 1}}
	policy.UnobservedGroupCreations = 1
	policy.Creation.PendingOrCommitted = 1
	action, err := podlifecycle.PlanReplacement(snapshot, policy)
	if err != nil || action.Kind != podlifecycle.Wait {
		t.Fatalf("committed creation was not counted: %#v %v", action, err)
	}
	policy.UnobservedGroupCreations = 0
	policy.Creation.PendingOrCommitted = 0
	pending := pod
	pending.Identity = podlifecycle.Identity{Namespace: "n", Name: "pending", UID: "pending", OwnerUID: "o"}
	pending.Bound, pending.Eligible = false, false
	snapshot.Pods = append(snapshot.Pods, pending)
	if _, err := podlifecycle.PlanReplacement(snapshot, policy); err == nil {
		t.Fatal("omitted pending demand accepted")
	}
	snapshot.Pods = snapshot.Pods[:1]
	snapshot.Pods[0].ObservedAt = now.Add(-2 * time.Minute)
	if _, err := podlifecycle.PlanReplacement(snapshot, policy); err == nil {
		t.Fatal("stale eligibility accepted")
	}
	snapshot.Pods[0].ObservedAt = now.Add(time.Second)
	if _, err := podlifecycle.PlanReplacement(snapshot, policy); err == nil {
		t.Fatal("future observation accepted")
	}
	snapshot.Pods[0].ObservedAt = now
	snapshot.Pods[0].Identity.OwnerUID = "foreign"
	if _, err := podlifecycle.PlanReplacement(snapshot, policy); err == nil {
		t.Fatal("foreign owner accepted")
	}
	snapshot.Pods[0].Identity.OwnerUID = "o"
	snapshot.Pods[0].Revision = ""
	if _, err := podlifecycle.PlanReplacement(snapshot, policy); err == nil {
		t.Fatal("unknown revision accepted")
	}
}

func TestReplacementDoesNotDuplicateCommittedCapacity(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	g := podlifecycle.Group{Workload: "w", Fleet: "blue", Role: "search", Zone: "a"}
	snapshot := podlifecycle.Snapshot{Now: now, Pods: []podlifecycle.Pod{{Identity: podlifecycle.Identity{Namespace: "n", Name: "p", UID: "p", OwnerUID: "o"}, Group: g, Revision: "new", Bound: true, Eligible: true, ObservedAt: now}}}
	policy := podlifecycle.ReplacementPolicy{GroupPolicy: podlifecycle.GroupPolicy{Group: g, Desired: 2, MinServing: 1}, OwnerUID: "o", TargetRevision: "new", MaxSurge: 1, MaxObservationAge: time.Minute, UnobservedGroupCreations: 1, Creation: podlifecycle.CreationBudget{Maximum: 2, PendingOrCommitted: 1}, Retirement: podlifecycle.RetirementBudget{MaxConcurrentRetirements: 1}}
	action, err := podlifecycle.PlanReplacement(snapshot, policy)
	if err != nil || action.Kind != podlifecycle.Wait {
		t.Fatalf("committed target capacity was duplicated: %#v %v", action, err)
	}
}

func TestReplacementDoesNotReduceAlreadyDegradedServingCapacity(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	g := podlifecycle.Group{Workload: "w", Fleet: "blue", Role: "search", Zone: "a"}
	makePod := func(name, revision string, eligible bool) podlifecycle.Pod {
		return podlifecycle.Pod{Identity: podlifecycle.Identity{Namespace: "n", Name: name, UID: name, OwnerUID: "o"}, Group: g, Revision: revision, Bound: true, Eligible: eligible, ObservedAt: now}
	}
	snapshot := podlifecycle.Snapshot{Now: now, Pods: []podlifecycle.Pod{makePod("old", "old", true), makePod("new", "new", true), makePod("warming", "new", false)}}
	policy := podlifecycle.ReplacementPolicy{GroupPolicy: podlifecycle.GroupPolicy{Group: g, Desired: 2, MinServing: 1}, OwnerUID: "o", TargetRevision: "new", MaxSurge: 1, MaxObservationAge: time.Minute, Creation: podlifecycle.CreationBudget{Maximum: 1}, Retirement: podlifecycle.RetirementBudget{MinServingRole: 1, MaxConcurrentRetirements: 1}}
	action, err := podlifecycle.PlanReplacement(snapshot, policy)
	if err != nil || action.Kind != podlifecycle.Wait {
		t.Fatalf("degraded serving capacity was reduced: %#v %v", action, err)
	}
}

func TestReplacementDoesNotUseAnotherZoneOrEvictUnboundPod(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	g := podlifecycle.Group{Workload: "w", Fleet: "blue", Role: "search", Zone: "a"}
	pod := func(name, revision string, group podlifecycle.Group, bound, eligible bool) podlifecycle.Pod {
		return podlifecycle.Pod{Identity: podlifecycle.Identity{Namespace: "n", Name: name, UID: name, OwnerUID: "o"}, Group: group, Revision: revision, Bound: bound, Eligible: eligible, ObservedAt: now}
	}
	other := g
	other.Zone = "b"
	snapshot := podlifecycle.Snapshot{Now: now, Pods: []podlifecycle.Pod{pod("old", "old", g, true, true), pod("other-new", "new", other, true, true)}}
	policy := podlifecycle.ReplacementPolicy{GroupPolicy: podlifecycle.GroupPolicy{Group: g, Desired: 1, MinServing: 1}, OwnerUID: "o", TargetRevision: "new", MaxSurge: 0, MaxObservationAge: time.Minute, Creation: podlifecycle.CreationBudget{Maximum: 1}, Retirement: podlifecycle.RetirementBudget{MinServingRole: 1, MaxConcurrentRetirements: 1}}
	action, err := podlifecycle.PlanReplacement(snapshot, policy)
	if err != nil || action.Kind != podlifecycle.Wait {
		t.Fatalf("another zone authorized retirement: %#v %v", action, err)
	}
	policy.MaxSurge = 1
	snapshot.Pods[0].Bound = false
	snapshot.Pods[0].Eligible = false
	snapshot.Pods = append(snapshot.Pods, pod("new", "new", g, true, true))
	policy.Creation.PendingOrCommitted = 1
	action, err = podlifecycle.PlanReplacement(snapshot, policy)
	if err != nil || action.Kind == podlifecycle.EvictPod {
		t.Fatalf("unbound Pod was selected for eviction: %#v %v", action, err)
	}
}

func TestClaimedReplacementRechecksFreshCapacityAndOtherClaims(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	g := podlifecycle.Group{Workload: "w", Fleet: "blue", Role: "search", Zone: "a"}
	makePod := func(name, revision string) podlifecycle.Pod {
		return podlifecycle.Pod{Identity: podlifecycle.Identity{Namespace: "n", Name: name, UID: name, OwnerUID: "o"}, Group: g, Revision: revision, Bound: true, Eligible: true, ObservedAt: now}
	}
	snapshot := podlifecycle.Snapshot{Now: now, Pods: []podlifecycle.Pod{makePod("old-b", "old"), makePod("old-a", "old"), makePod("new", "new")}}
	policy := podlifecycle.ReplacementPolicy{GroupPolicy: podlifecycle.GroupPolicy{Group: g, Desired: 2, MinServing: 2}, OwnerUID: "o", TargetRevision: "new", MaxSurge: 1, MaxObservationAge: time.Minute, Creation: podlifecycle.CreationBudget{Maximum: 1}, Retirement: podlifecycle.RetirementBudget{MinServingRole: 2, MaxConcurrentRetirements: 1}}
	action, err := podlifecycle.PlanReplacement(snapshot, policy)
	if err != nil || action.Kind != podlifecycle.EvictPod || action.Pod.UID != "old-a" {
		t.Fatalf("expected unclaimed planning to prefer old-a, got %+v, %v", action, err)
	}
	victim := snapshot.Pods[0].Identity
	snapshot.Pods[0].RetirementHeld = true
	check := func(want bool) {
		t.Helper()
		got, err := podlifecycle.RecheckClaimedReplacement(snapshot, policy, victim)
		if err != nil || got != want {
			t.Fatalf("recheck got %v, %v; want %v", got, err, want)
		}
	}
	check(true)
	if !snapshot.Pods[0].RetirementHeld {
		t.Fatal("recheck modified the original snapshot")
	}
	snapshot.Pods[1].RetirementHeld = true
	check(false)
	snapshot.Pods[1].RetirementHeld = false
	snapshot.Pods[2].Eligible = false
	check(false)
	snapshot.Pods[2].Eligible = true
	snapshot.Pods[0].Terminating = true
	check(false)
	snapshot.Pods[0].Terminating = false
	snapshot.Pods[0].ObservedAt = now.Add(-2 * time.Minute)
	if _, err := podlifecycle.RecheckClaimedReplacement(snapshot, policy, victim); err == nil {
		t.Fatal("stale claimed victim accepted")
	}
	snapshot.Pods[0].ObservedAt = now
	snapshot.Pods[0].Identity.Name = "reused"
	if _, err := podlifecycle.RecheckClaimedReplacement(snapshot, policy, victim); err == nil {
		t.Fatal("changed identity accepted")
	}
}
