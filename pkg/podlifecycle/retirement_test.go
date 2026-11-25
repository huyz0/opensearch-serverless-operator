package podlifecycle_test

import (
	"testing"

	"github.com/huyz0/opensearch-serverless-operator/pkg/podlifecycle"
)

func TestPlanScaleDown(t *testing.T) {
	group := func(zone string) podlifecycle.Group {
		return podlifecycle.Group{Workload: "workers", Fleet: "active", Role: "reader", Zone: zone}
	}
	pod := func(name string, group podlifecycle.Group) podlifecycle.Pod {
		return podlifecycle.Pod{
			Identity: podlifecycle.Identity{Namespace: "work", Name: name, UID: name, OwnerUID: "owner"},
			Group:    group, Bound: true, Eligible: true,
		}
	}
	for _, test := range []struct {
		name   string
		change func(*podlifecycle.Snapshot, *podlifecycle.RetirementBudget)
		victim string
		reason string
		err    bool
	}{
		{name: "remove abandoned zone after destination is ready", victim: "removed-a", reason: "ServingSurplus"},
		{name: "reduce existing zone while keeping the remaining replica and reserve", change: func(s *podlifecycle.Snapshot, b *podlifecycle.RetirementBudget) {
			s.Pods[0].Group = group("new")
			s.Pods = append(s.Pods[:1], s.Pods[2:]...)
			b.MinServingRole = 2
		}, victim: "removed-b", reason: "ServingSurplus"},
		{name: "retain reserve while destination warms", change: func(s *podlifecycle.Snapshot, _ *podlifecycle.RetirementBudget) {
			s.Pods[3].Eligible = false
		}, reason: "SteadyCapacityMissing"},
		{name: "terminating Pod cannot cover desired", change: func(s *podlifecycle.Snapshot, _ *podlifecycle.RetirementBudget) {
			s.Pods[3].Terminating = true
		}, reason: "SteadyCapacityMissing"},
		{name: "candidate cannot satisfy regional floor", change: func(_ *podlifecycle.Snapshot, b *podlifecycle.RetirementBudget) {
			b.MinServingRole = 4
		}, reason: "RetirementBudgetExhausted"},
		{name: "index retirement consumes shared budget", change: func(s *podlifecycle.Snapshot, _ *podlifecycle.RetirementBudget) {
			s.Pods[4].RetirementHeld = true
			s.Pods[4].Group.Role = "writer"
		}, reason: "RetirementBudgetExhausted"},
		{name: "do not evict the same Pod on retry", change: func(s *podlifecycle.Snapshot, b *podlifecycle.RetirementBudget) {
			s.Pods[1].RetirementHeld = true
			b.MaxConcurrentRetirements = 2
			b.MinServingRole = 2
		}, victim: "removed-b", reason: "ServingSurplus"},
		{name: "stop at the desired count", change: func(s *podlifecycle.Snapshot, b *podlifecycle.RetirementBudget) {
			s.Pods = s.Pods[2:]
			b.MinServingRole = 1
		}, reason: "NoServingSurplus"},
		{name: "require a zero policy to remove a zone", change: func(s *podlifecycle.Snapshot, _ *podlifecycle.RetirementBudget) {
			s.Policies = s.Policies[1:]
		}, err: true},
		{name: "choose the first surplus zone regardless of input order", change: func(s *podlifecycle.Snapshot, _ *podlifecycle.RetirementBudget) {
			s.Pods[4].Group.Fleet = "active"
			s.Policies[0], s.Policies[1] = s.Policies[1], s.Policies[0]
		}, victim: "preview", reason: "ServingSurplus"},
		{name: "reject duplicate UID outside the serving fleet", change: func(s *podlifecycle.Snapshot, _ *podlifecycle.RetirementBudget) {
			s.Pods[4].Identity.UID = s.Pods[0].Identity.UID
		}, err: true},
		{name: "reject incomplete surplus victim even if another is valid", change: func(s *podlifecycle.Snapshot, _ *podlifecycle.RetirementBudget) {
			s.Pods[0].Identity.Namespace = ""
		}, err: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			snapshot := podlifecycle.Snapshot{
				Policies: []podlifecycle.GroupPolicy{
					{Group: group("old")},
					{Group: group("new"), Desired: 2, MinServing: 1},
				},
				Pods: []podlifecycle.Pod{
					pod("removed-b", group("old")), pod("removed-a", group("old")),
					pod("replica", group("new")), pod("reserve", group("new")), pod("preview", group("new")),
				},
			}
			snapshot.Pods[4].Group.Fleet = "preview"
			budget := podlifecycle.RetirementBudget{MinServingRole: 3, MaxConcurrentRetirements: 1}
			if test.change != nil {
				test.change(&snapshot, &budget)
			}
			action, err := podlifecycle.PlanScaleDown(snapshot, budget)
			if test.err {
				if err == nil {
					t.Fatal("expected an invalid snapshot error")
				}
				return
			}
			kind := podlifecycle.Wait
			if test.victim != "" {
				kind = podlifecycle.EvictPod
			}
			if err != nil || action.Kind != kind || action.Pod.UID != test.victim || action.Reason != test.reason {
				t.Fatalf("unexpected plan %+v, err=%v", action, err)
			}
		})
	}
}

func TestCheckRetirement(t *testing.T) {
	group := podlifecycle.Group{Workload: "workers", Fleet: "a", Role: "reader", Zone: "z1"}
	identity := func(uid string) podlifecycle.Identity {
		return podlifecycle.Identity{Namespace: "work", Name: uid, UID: uid, OwnerUID: "owner"}
	}
	pods := []podlifecycle.Pod{
		{Identity: identity("one"), Group: group, Bound: true, Eligible: true},
		{Identity: identity("two"), Group: group, Bound: true, Eligible: true},
		{Identity: identity("three"), Group: group, Bound: true, Eligible: true, RetirementHeld: true},
		{Identity: identity("pending"), Group: group},
	}
	policy := podlifecycle.GroupPolicy{Group: group, Desired: 4, MinServing: 2}
	snapshot := podlifecycle.Snapshot{Pods: pods}
	allowed, err := podlifecycle.CheckRetirement(snapshot, policy, identity("one"))
	if err != nil || allowed {
		t.Fatalf("eligible victim would violate projected floor: allowed=%v err=%v", allowed, err)
	}
	allowed, err = podlifecycle.CheckRetirement(snapshot, policy, identity("pending"))
	if err != nil || !allowed {
		t.Fatalf("unbound victim should preserve the floor: allowed=%v err=%v", allowed, err)
	}
	stale := identity("one")
	stale.OwnerUID = "old-owner"
	if _, err = podlifecycle.CheckRetirement(snapshot, policy, stale); err == nil {
		t.Fatal("stale owner identity must fail")
	}
	allowed, err = podlifecycle.CheckRetirement(snapshot, policy, identity("three"))
	if err != nil || allowed {
		t.Fatalf("already committed victim must not be selected again: allowed=%v err=%v", allowed, err)
	}
}
