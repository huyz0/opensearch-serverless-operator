package rollout

import (
	"testing"
	"time"
)

func TestRoleSwitchPlansAndRecoversAmbiguousEffect(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	intent := SwitchIntent{InstanceUID: "instance", OperationUID: "operation", Role: "search", ServiceUID: "service", SourceFleet: "blue", TargetFleet: "green", CandidateRevision: "revision", Phase: Prepared}
	snapshot := SwitchSnapshot{Now: now, MaxAge: time.Minute, ClaimInstanceUID: "instance", ClaimOperationUID: "operation", Service: ServiceObservation{InstanceUID: "instance", ServiceUID: "service", ResourceVersion: "10", Role: "search", Fleet: "blue", Owned: true, RoutingShapeValid: true, ObservedAt: now}, Candidate: &CandidateEvidence{InstanceUID: "instance", OperationUID: "operation", Role: "search", Fleet: "green", Revision: "revision", CapacityQualified: true, PeerQualified: true, SecurityQualified: true, RollbackCompatible: true, ObservedAt: now}}
	check := func(want ActionKind) {
		t.Helper()
		action, err := PlanSwitch(intent, snapshot)
		if err != nil || action.Kind != want {
			t.Fatalf("got %#v %v, want %s", action, err, want)
		}
	}
	check(PatchService)
	snapshot.Candidate = nil
	check(Hold)
	// A matching target after a lost patch response is recorded even if the
	// candidate's present qualification is unknown.
	snapshot.Service.Fleet = "green"
	snapshot.Service.ResourceVersion = "11"
	check(RecordApplied)
	intent.Phase = Applied
	intent.AppliedAt = now
	check(Hold)
	snapshot.Candidate = &CandidateEvidence{InstanceUID: "instance", OperationUID: "operation", Role: "search", Fleet: "green", Revision: "revision", CapacityQualified: true, PeerQualified: true, SecurityQualified: true, RollbackCompatible: true, ObservedAt: now}
	check(Hold)
	snapshot.Route = &RouteEvidence{InstanceUID: "instance", OperationUID: "operation", Role: "search", Fleet: "green", ServiceUID: "service", ServiceVersion: "11", EndpointsConverged: true, ClientProbePassed: true, ObservedAt: now}
	check(RecordConverged)
	snapshot.Service.Fleet = "blue"
	check(Hold)
}

func TestRoleSwitchRejectsStaleOrMismatchedEvidence(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	intent := SwitchIntent{InstanceUID: "i", OperationUID: "o", Role: "ingest", ServiceUID: "s", SourceFleet: "blue", TargetFleet: "green", CandidateRevision: "r", Phase: Applied, AppliedAt: now.Add(-time.Second)}
	good := func() SwitchSnapshot {
		return SwitchSnapshot{Now: now, MaxAge: time.Minute, ClaimInstanceUID: "i", ClaimOperationUID: "o", Service: ServiceObservation{InstanceUID: "i", ServiceUID: "s", ResourceVersion: "10", Role: "ingest", Fleet: "green", Owned: true, RoutingShapeValid: true, ObservedAt: now}, Candidate: &CandidateEvidence{InstanceUID: "i", OperationUID: "o", Role: "ingest", Fleet: "green", Revision: "r", CapacityQualified: true, PeerQualified: true, SecurityQualified: true, RollbackCompatible: true, ObservedAt: now}, Route: &RouteEvidence{InstanceUID: "i", OperationUID: "o", Role: "ingest", Fleet: "green", ServiceUID: "s", ServiceVersion: "10", EndpointsConverged: true, ClientProbePassed: true, ObservedAt: now}}
	}
	cases := []struct {
		name   string
		mutate func(*SwitchSnapshot)
	}{
		{"claim", func(s *SwitchSnapshot) { s.ClaimOperationUID = "other" }},
		{"service uid", func(s *SwitchSnapshot) { s.Service.ServiceUID = "new" }},
		{"unsafe shape", func(s *SwitchSnapshot) { s.Service.RoutingShapeValid = false }},
		{"service stale", func(s *SwitchSnapshot) { s.Service.ObservedAt = now.Add(-2 * time.Minute) }},
		{"service future", func(s *SwitchSnapshot) { s.Service.ObservedAt = now.Add(time.Second) }},
		{"selector conflict", func(s *SwitchSnapshot) { s.Service.Fleet = "other" }},
		{"candidate revision", func(s *SwitchSnapshot) { s.Candidate.Revision = "old" }},
		{"candidate peer", func(s *SwitchSnapshot) { s.Candidate.PeerQualified = false }},
		{"candidate rollback", func(s *SwitchSnapshot) { s.Candidate.RollbackCompatible = false }},
		{"candidate stale", func(s *SwitchSnapshot) { s.Candidate.ObservedAt = now.Add(-2 * time.Minute) }},
		{"candidate lease deadline", func(s *SwitchSnapshot) { s.Candidate.ValidUntil = now }},
		{"route version", func(s *SwitchSnapshot) { s.Route.ServiceVersion = "old" }},
		{"route lease deadline", func(s *SwitchSnapshot) { s.Route.ValidUntil = now }},
		{"route before apply", func(s *SwitchSnapshot) { s.Route.ObservedAt = now.Add(-2 * time.Second) }},
		{"missing endpoints", func(s *SwitchSnapshot) { s.Route.EndpointsConverged = false }},
		{"missing client probe", func(s *SwitchSnapshot) { s.Route.ClientProbePassed = false }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			snapshot := good()
			tc.mutate(&snapshot)
			action, err := PlanSwitch(intent, snapshot)
			if err != nil || action.Kind != Hold {
				t.Fatalf("unsafe transition accepted: %#v %v", action, err)
			}
		})
	}
	action, err := PlanSwitch(intent, good())
	if err != nil || action.Kind != RecordConverged {
		t.Fatalf("valid observation rejected: %#v %v", action, err)
	}
}
