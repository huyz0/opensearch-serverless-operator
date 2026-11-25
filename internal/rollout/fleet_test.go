package rollout

import (
	"testing"
	"time"
)

func TestFleetSwitchSequencesAndRechecksRoles(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	roles := []RoleProgress{
		{Intent: SwitchIntent{InstanceUID: "i", OperationUID: "o", Role: "search", ServiceUID: "s", SourceFleet: "blue", TargetFleet: "green", CandidateRevision: "rs", Phase: Prepared}},
		{Intent: SwitchIntent{InstanceUID: "i", OperationUID: "o", Role: "ingest", ServiceUID: "w", SourceFleet: "blue", TargetFleet: "green", CandidateRevision: "rw", Phase: Prepared}},
	}
	snapshots := map[string]SwitchSnapshot{}
	for _, role := range roles {
		i := role.Intent
		snapshots[i.Role] = SwitchSnapshot{Now: now, MaxAge: time.Minute, ClaimInstanceUID: "i", ClaimOperationUID: "o", Service: ServiceObservation{InstanceUID: "i", ServiceUID: i.ServiceUID, ResourceVersion: "1", Role: i.Role, Fleet: "blue", Owned: true, RoutingShapeValid: true, ObservedAt: now}, Candidate: &CandidateEvidence{InstanceUID: "i", OperationUID: "o", Role: i.Role, Fleet: "green", Revision: i.CandidateRevision, CapacityQualified: true, PeerQualified: true, SecurityQualified: true, RollbackCompatible: true, ObservedAt: now}}
	}
	check := func(role string, kind ActionKind, complete bool) {
		t.Helper()
		a, err := PlanFleetSwitch(roles, snapshots)
		if err != nil || a.Role != role || a.Switch.Kind != kind || a.Complete != complete {
			t.Fatalf("got %#v, %v; want role %s, action %s, complete %v", a, err, role, kind, complete)
		}
	}
	check("search", PatchService, false)
	s := snapshots["search"]
	s.Service.Fleet, s.Service.ResourceVersion = "green", "2"
	snapshots["search"] = s
	check("search", RecordApplied, false)
	roles[0].Intent.Phase, roles[0].Intent.AppliedAt = Applied, now
	check("search", Hold, false)
	s.Route = &RouteEvidence{InstanceUID: "i", OperationUID: "o", Role: "search", Fleet: "green", ServiceUID: "s", ServiceVersion: "2", EndpointsConverged: true, ClientProbePassed: true, ObservedAt: now}
	snapshots["search"] = s
	check("search", RecordConverged, false)
	roles[0].Converged = true
	check("ingest", PatchService, false)
	// An earlier role losing qualification stops later work, even after its
	// convergence was durably recorded.
	s.Candidate.CapacityQualified = false
	check("search", Hold, false)
	s.Candidate.CapacityQualified = true
	w := snapshots["ingest"]
	w.Service.Fleet, w.Service.ResourceVersion = "green", "3"
	snapshots["ingest"] = w
	check("ingest", RecordApplied, false)
	roles[1].Intent.Phase, roles[1].Intent.AppliedAt = Applied, now
	w.Route = &RouteEvidence{InstanceUID: "i", OperationUID: "o", Role: "ingest", Fleet: "green", ServiceUID: "w", ServiceVersion: "3", EndpointsConverged: true, ClientProbePassed: true, ObservedAt: now}
	snapshots["ingest"] = w
	check("ingest", RecordConverged, false)
	roles[1].Converged = true
	check("", "", true)
	w.Service.ResourceVersion = "4"
	snapshots["ingest"] = w
	check("ingest", Hold, false)
}

func TestFleetSwitchRejectsInconsistentProgress(t *testing.T) {
	base := []RoleProgress{
		{Intent: SwitchIntent{InstanceUID: "i", OperationUID: "o", Role: "search", ServiceUID: "s", SourceFleet: "blue", TargetFleet: "green", CandidateRevision: "rs", Phase: Prepared}},
		{Intent: SwitchIntent{InstanceUID: "i", OperationUID: "o", Role: "ingest", ServiceUID: "w", SourceFleet: "blue", TargetFleet: "green", CandidateRevision: "rw", Phase: Prepared}},
	}
	cases := []struct {
		name string
		edit func([]RoleProgress)
	}{
		{"different operation", func(p []RoleProgress) { p[1].Intent.OperationUID = "other" }},
		{"different target", func(p []RoleProgress) { p[1].Intent.TargetFleet = "other" }},
		{"same service", func(p []RoleProgress) { p[1].Intent.ServiceUID = "s" }},
		{"already on target", func(p []RoleProgress) { p[1].Intent.SourceFleet = "green" }},
		{"adoption with different source", func(p []RoleProgress) { p[1].Intent.AdoptExisting = true }},
		{"early ingest", func(p []RoleProgress) { p[1].Intent.Phase = Applied }},
		{"converged before applied", func(p []RoleProgress) { p[0].Converged = true }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			p := append([]RoleProgress(nil), base...)
			tc.edit(p)
			if _, err := PlanFleetSwitch(p, nil); err == nil {
				t.Fatal("accepted inconsistent progress")
			}
		})
	}
}

func TestRollbackAdoptsAlreadyServingRole(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	for _, adoptedRole := range []string{"search", "ingest"} {
		t.Run(adoptedRole, func(t *testing.T) {
			roles := []RoleProgress{
				{Intent: SwitchIntent{InstanceUID: "i", OperationUID: "o", Role: "search", ServiceUID: "s", SourceFleet: "green", TargetFleet: "blue", CandidateRevision: "rs", Phase: Prepared}},
				{Intent: SwitchIntent{InstanceUID: "i", OperationUID: "o", Role: "ingest", ServiceUID: "w", SourceFleet: "green", TargetFleet: "blue", CandidateRevision: "rw", Phase: Prepared}},
			}
			snapshots := map[string]SwitchSnapshot{}
			for n := range roles {
				i := &roles[n].Intent
				if i.Role == adoptedRole {
					i.SourceFleet, i.AdoptExisting = "blue", true
				}
				snapshots[i.Role] = SwitchSnapshot{Now: now, MaxAge: time.Minute, ClaimInstanceUID: "i", ClaimOperationUID: "o", Service: ServiceObservation{InstanceUID: "i", ServiceUID: i.ServiceUID, ResourceVersion: "1", Role: i.Role, Fleet: i.SourceFleet, Owned: true, RoutingShapeValid: true, ObservedAt: now}, Candidate: &CandidateEvidence{InstanceUID: "i", OperationUID: "o", Role: i.Role, Fleet: "blue", Revision: i.CandidateRevision, CapacityQualified: true, PeerQualified: true, SecurityQualified: true, RollbackCompatible: true, ObservedAt: now}}
			}
			for n := range roles {
				i := &roles[n].Intent
				a, err := PlanFleetSwitch(roles, snapshots)
				want := PatchService
				if i.AdoptExisting {
					want = RecordApplied
				}
				if err != nil || a.Role != i.Role || a.Switch.Kind != want {
					t.Fatalf("initial decision: %#v %v", a, err)
				}
				s := snapshots[i.Role]
				if !i.AdoptExisting {
					s.Service.Fleet, s.Service.ResourceVersion = "blue", "2"
					snapshots[i.Role] = s
					a, err = PlanFleetSwitch(roles, snapshots)
					if err != nil || a.Switch.Kind != RecordApplied {
						t.Fatalf("recover applied switch: %#v %v", a, err)
					}
				}
				i.Phase, i.AppliedAt = Applied, now
				// Evidence from before adoption or mutation cannot prove convergence.
				s.Route = &RouteEvidence{InstanceUID: "i", OperationUID: "o", Role: i.Role, Fleet: "blue", ServiceUID: i.ServiceUID, ServiceVersion: s.Service.ResourceVersion, EndpointsConverged: true, ClientProbePassed: true, ObservedAt: now.Add(-time.Second)}
				snapshots[i.Role] = s
				a, err = PlanFleetSwitch(roles, snapshots)
				if err != nil || a.Switch.Kind != Hold || a.Switch.Reason != "RouteNotConverged" {
					t.Fatalf("accepted old route evidence: %#v %v", a, err)
				}
				s.Route.ObservedAt = now
				a, err = PlanFleetSwitch(roles, snapshots)
				if err != nil || a.Switch.Kind != RecordConverged {
					t.Fatalf("qualified route rejected: %#v %v", a, err)
				}
				roles[n].Converged = true
			}
			a, err := PlanFleetSwitch(roles, snapshots)
			if err != nil || !a.Complete {
				t.Fatalf("rollback did not converge: %#v %v", a, err)
			}
		})
	}
}
