package rollout

import (
	"maps"
	"testing"
	"time"
)

func weightStepFixture() (WeightIntent, WeightSnapshot) {
	now := time.Date(2026, 11, 24, 3, 0, 0, 0, time.UTC)
	intent := WeightIntent{
		InstanceUID: "instance", OperationUID: "operation", Role: "search", RouteUID: "route",
		From:            map[string]int{"blue": 100, "green": 0, "red": 0},
		To:              map[string]int{"blue": 99, "green": 1, "red": 0},
		Revisions:       map[string]string{"blue": "v1", "green": "v2"},
		AdmissionWeight: 1, Phase: Prepared,
	}
	snapshot := WeightSnapshot{
		Now: now, MaxAge: 10 * time.Second, ClaimInstanceUID: "instance", ClaimOperationUID: "operation",
		Route: WeightRouteObservation{
			InstanceUID: "instance", Role: "search", RouteUID: "route", ResourceVersion: "11",
			Weights: maps.Clone(intent.From), ScopeVerified: true, ObservedAt: now,
		},
		Qualifications:    make(map[string]FleetWeightQualification),
		ExpectedProxyUIDs: []string{"proxy-a", "proxy-b"}, MinimumSamples: 100, TolerancePoints: 3,
	}
	for fleet, version := range intent.Revisions {
		snapshot.Qualifications[fleet] = FleetWeightQualification{
			CandidateEvidence: CandidateEvidence{
				InstanceUID: intent.InstanceUID, OperationUID: intent.OperationUID, Role: intent.Role,
				Fleet: fleet, Revision: version, ObservedAt: now, ValidUntil: now.Add(time.Second),
				CapacityQualified: true, PeerQualified: true, SecurityQualified: true, RollbackCompatible: true,
			},
			Processes: map[string]string{fleet + "-pod": version + "-process"},
		}
	}
	for _, uid := range snapshot.ExpectedProxyUIDs {
		snapshot.Proxies = append(snapshot.Proxies, ProxyWeightObservation{
			ProxyUID: uid, InstanceUID: "instance", OperationUID: "operation", Role: "search", RouteUID: "route",
			RouteVersion: "11", ObservedAt: now, ValidUntil: now.Add(time.Second),
			Healthy: map[string]map[string]string{"blue": maps.Clone(snapshot.Qualifications["blue"].Processes)},
		})
	}
	return intent, snapshot
}

func appliedWeights(intent WeightIntent, snapshot WeightSnapshot) (WeightIntent, WeightSnapshot) {
	intent.Phase, intent.AppliedAt = Applied, snapshot.Now.Add(-time.Second)
	snapshot.Route.Weights = maps.Clone(intent.To)
	for i := range snapshot.Proxies {
		proxy := &snapshot.Proxies[i]
		proxy.Requests = maps.Clone(intent.To)
		for fleet, weight := range intent.To {
			if weight > 0 {
				proxy.Healthy[fleet] = maps.Clone(snapshot.Qualifications[fleet].Processes)
			}
		}
	}
	return intent, snapshot
}

func TestPlanWeightStep(t *testing.T) {
	tests := []struct {
		name   string
		change func(*WeightIntent, *WeightSnapshot)
		kind   ActionKind
		reason string
	}{
		{name: "bounded admission while incumbent is healthy", kind: PatchRouteWeights, reason: "WeightStepQualified"},
		{name: "acknowledge an ambiguous patch before continuing", change: func(intent *WeightIntent, snapshot *WeightSnapshot) {
			snapshot.Route.Weights = maps.Clone(intent.To)
			snapshot.Proxies = nil
		}, kind: RecordApplied, reason: "TargetWeightsObserved"},
		{name: "unexpected live weights", change: func(_ *WeightIntent, snapshot *WeightSnapshot) {
			snapshot.Route.Weights = map[string]int{"blue": 97, "green": 3, "red": 0}
		}, kind: Hold, reason: "WeightConflict"},
		{name: "claim changed", change: func(_ *WeightIntent, snapshot *WeightSnapshot) {
			snapshot.ClaimOperationUID = "other-operation"
		}, kind: Hold, reason: "ClaimMismatch"},
		{name: "route replaced at the same name", change: func(_ *WeightIntent, snapshot *WeightSnapshot) {
			snapshot.Route.RouteUID = "other"
		}, kind: Hold, reason: "RouteObservationInvalid"},
		{name: "one serving proxy unobserved", change: func(_ *WeightIntent, snapshot *WeightSnapshot) {
			snapshot.Proxies = snapshot.Proxies[:1]
		}, kind: Hold, reason: "ProxyEvidenceIncomplete"},
		{name: "one proxy still on the previous route version", change: func(_ *WeightIntent, snapshot *WeightSnapshot) {
			snapshot.Proxies[1].RouteVersion = "10"
		}, kind: Hold, reason: "ProxyEvidenceIncomplete"},
		{name: "restarted JVM does not match the qualified endpoint", change: func(_ *WeightIntent, snapshot *WeightSnapshot) {
			snapshot.Proxies[1].Healthy["blue"]["blue-pod"] = "new-jvm"
		}, kind: Hold, reason: "ProxyEvidenceIncomplete"},
		{name: "fleet qualification became stale", change: func(_ *WeightIntent, snapshot *WeightSnapshot) {
			qual := snapshot.Qualifications["green"]
			qual.ValidUntil = snapshot.Now
			snapshot.Qualifications["green"] = qual
		}, kind: Hold, reason: "FleetNotQualified"},
		{name: "fleet revision changed", change: func(intent *WeightIntent, _ *WeightSnapshot) {
			intent.Revisions["green"] = "v3"
		}, kind: Hold, reason: "FleetNotQualified"},
		{name: "all proxies verify the new distribution", change: func(intent *WeightIntent, snapshot *WeightSnapshot) {
			*intent, *snapshot = appliedWeights(*intent, *snapshot)
		}, kind: RecordConverged, reason: "WeightsObserved"},
		{name: "evidence predates the applied write", change: func(intent *WeightIntent, snapshot *WeightSnapshot) {
			*intent, *snapshot = appliedWeights(*intent, *snapshot)
			snapshot.Proxies[0].ObservedAt = intent.AppliedAt.Add(-time.Second)
		}, kind: Hold, reason: "ProxyEvidenceIncomplete"},
		{name: "traffic going to inactive fleet", change: func(intent *WeightIntent, snapshot *WeightSnapshot) {
			*intent, *snapshot = appliedWeights(*intent, *snapshot)
			snapshot.Proxies[0].Requests["red"] = 1
		}, kind: Hold, reason: "ProxyEvidenceIncomplete"},
		{name: "a failed production probe", change: func(intent *WeightIntent, snapshot *WeightSnapshot) {
			*intent, *snapshot = appliedWeights(*intent, *snapshot)
			snapshot.Proxies[1].Failures = 1
		}, kind: Hold, reason: "ProxyEvidenceIncomplete"},
		{name: "one proxy above the tolerance", change: func(intent *WeightIntent, snapshot *WeightSnapshot) {
			*intent, *snapshot = appliedWeights(*intent, *snapshot)
			snapshot.Proxies[1].Requests = map[string]int{"blue": 95, "green": 5}
		}, kind: Hold, reason: "ProxyEvidenceIncomplete"},
		{name: "rollback sheds unhealthy canary while verified incumbent stays", change: func(intent *WeightIntent, snapshot *WeightSnapshot) {
			intent.From, intent.To = intent.To, intent.From
			snapshot.Route.Weights = maps.Clone(intent.From)
			delete(snapshot.Qualifications, "green")
		}, kind: PatchRouteWeights, reason: "WeightStepQualified"},
		{name: "a second cohort enters an existing split", change: func(intent *WeightIntent, snapshot *WeightSnapshot) {
			*intent, *snapshot = appliedWeights(*intent, *snapshot)
			intent.From = intent.To
			intent.To = map[string]int{"blue": 98, "green": 1, "red": 1}
			intent.Phase, intent.AppliedAt = Prepared, time.Time{}
			intent.Revisions["red"] = "v3"
			red := snapshot.Qualifications["green"]
			red.Fleet, red.Revision = "red", "v3"
			red.Processes = map[string]string{"red-pod": "red-process"}
			snapshot.Qualifications["red"] = red
		}, kind: PatchRouteWeights, reason: "WeightStepQualified"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			intent, snapshot := weightStepFixture()
			if tc.change != nil {
				tc.change(&intent, &snapshot)
			}
			action, err := PlanWeightStep(intent, snapshot)
			if err != nil || action.Kind != tc.kind || action.Reason != tc.reason {
				t.Fatalf("PlanWeightStep = %#v, %v, want %s %s", action, err, tc.kind, tc.reason)
			}
		})
	}
}

func TestPlanWeightStepRejectsUnsafeOpening(t *testing.T) {
	for _, weights := range []map[string]int{
		{"blue": 0, "green": 100, "red": 0},
		{"blue": 98, "green": 1, "red": 1},
		{"blue": 100, "green": 1, "red": -1},
	} {
		intent, snapshot := weightStepFixture()
		intent.To = weights
		intent.Revisions["red"] = "v3"
		if _, err := PlanWeightStep(intent, snapshot); err == nil {
			t.Errorf("unsafe target accepted: %v", weights)
		}
	}
}

func TestPreparedAuthenticatedWeightStepChecksEveryProxy(t *testing.T) {
	tests := []struct {
		name     string
		removal  bool
		requests map[string]int
		failures int
		want     ActionKind
	}{
		{name: "admit a candidate while the incumbent serves", requests: map[string]int{"blue": 100}, want: PatchRouteWeights},
		{name: "one proxy has no authenticated successes", failures: 100, want: Hold},
		{name: "one proxy has too few samples", requests: map[string]int{"blue": 99}, want: Hold},
		{name: "admission has failed requests", requests: map[string]int{"blue": 99}, failures: 1, want: Hold},
		{name: "rollback removes an unhealthy candidate", removal: true, requests: map[string]int{"blue": 99}, failures: 1, want: PatchRouteWeights},
		{name: "rollback has no samples from the retained fleet", removal: true, requests: map[string]int{"green": 1}, failures: 99, want: Hold},
		{name: "served an inactive fleet", requests: map[string]int{"blue": 99, "green": 1}, want: Hold},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			intent, snapshot := weightStepFixture()
			snapshot.RouteAuthIdentity = RouteAuthIdentity{AuthSecretName: "probe", AuthSecretUID: "secret-uid", AuthSecretVersion: "1"}
			if tc.removal {
				intent.From, intent.To = intent.To, intent.From
				snapshot.Route.Weights = maps.Clone(intent.From)
				delete(snapshot.Qualifications, "green")
			}
			snapshot.Proxies[0].Requests = map[string]int{"blue": 100}
			snapshot.Proxies[1].Requests, snapshot.Proxies[1].Failures = tc.requests, tc.failures
			action, err := PlanWeightStep(intent, snapshot)
			if err != nil || action.Kind != tc.want {
				t.Fatalf("PlanWeightStep = %#v, %v, want %s", action, err, tc.want)
			}
		})
	}
}
