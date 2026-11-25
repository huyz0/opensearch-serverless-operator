package rollout

import (
	"fmt"
	"maps"
	"time"
)

const PatchRouteWeights ActionKind = "PatchRouteWeights"

// WeightIntent is one persisted step for one role. A new cohort may join only
// at the configured admission weight while existing cohorts retain weight.
type WeightIntent struct {
	InstanceUID, OperationUID, Role, RouteUID string
	From, To                                  map[string]int
	Revisions                                 map[string]string
	AdmissionWeight                           int
	Phase                                     SwitchPhase
	AppliedAt                                 time.Time
	// AdoptExisting verifies a distribution that already serves the rollback
	// target. It can only be observed in Applied and can never authorize a patch.
	AdoptExisting bool
}

type WeightRouteObservation struct {
	InstanceUID, Role, RouteUID, ResourceVersion string
	Weights                                      map[string]int
	ScopeVerified                                bool
	ObservedAt                                   time.Time
}

type FleetWeightQualification struct {
	CandidateEvidence
	// Processes map qualified backend Pod UIDs to their JVM incarnations.
	Processes map[string]string
}

type ProxyWeightObservation struct {
	ProxyUID, InstanceUID, OperationUID, Role, RouteUID, RouteVersion string
	ObservedAt, ValidUntil                                            time.Time
	// Healthy lists the processes admitted to this production route per fleet.
	Healthy map[string]map[string]string
	// ConfiguredWeights comes from the proxy's active configuration.
	ConfiguredWeights map[string]int
	// Requests counts successful probes by the fleet identity returned by the
	// application. Failed or unidentified probes must count toward Failures.
	Requests map[string]int
	Failures int
}

type WeightSnapshot struct {
	Now                                 time.Time
	MaxAge                              time.Duration
	ClaimInstanceUID, ClaimOperationUID string
	Route                               WeightRouteObservation
	Qualifications                      map[string]FleetWeightQualification
	RouteAuthIdentity                   RouteAuthIdentity
	// ExpectedProxyUIDs comes from an independent live inventory of replicas
	// that can receive production requests on this route.
	ExpectedProxyUIDs []string
	Proxies           []ProxyWeightObservation
	MinimumSamples    int
	TolerancePoints   int
}

type WeightAction struct {
	Kind                              ActionKind
	Reason, RouteUID, ResourceVersion string
	From, To                          map[string]int
}

func validWeights(weights map[string]int) bool {
	total := 0
	for fleet, weight := range weights {
		if fleet == "" || weight < 0 || weight > 100 {
			return false
		}
		total += weight
		if total > 100 {
			return false
		}
	}
	return total == 100
}

func ValidateWeightIntent(intent WeightIntent) error {
	if intent.InstanceUID == "" || intent.OperationUID == "" || intent.RouteUID == "" ||
		(intent.Role != "search" && intent.Role != "ingest") ||
		(intent.Phase != Prepared && intent.Phase != Applied) ||
		(intent.Phase == Prepared) != intent.AppliedAt.IsZero() ||
		intent.AdmissionWeight <= 0 || intent.AdmissionWeight >= 100 ||
		!validWeights(intent.From) || !validWeights(intent.To) ||
		len(intent.From) != len(intent.To) || maps.Equal(intent.From, intent.To) != intent.AdoptExisting ||
		(intent.AdoptExisting && intent.Phase != Applied) {
		return fmt.Errorf("complete weight step and distinct distributions are required")
	}
	joining, removing := 0, 0
	for fleet, oldWeight := range intent.From {
		newWeight, found := intent.To[fleet]
		if !found {
			return fmt.Errorf("a weight step must retain the same backend identities")
		}
		if newWeight > 0 && intent.Revisions[fleet] == "" {
			return fmt.Errorf("missing pinned revision for fleet %q", fleet)
		}
		if oldWeight == 0 && newWeight > 0 {
			if newWeight > intent.AdmissionWeight {
				return fmt.Errorf("initial weight exceeds the admission limit for fleet %q", fleet)
			}
			joining++
		}
		if oldWeight > 0 && newWeight == 0 {
			removing++
		}
	}
	if joining > 1 || (joining > 0 && removing > 0) {
		return fmt.Errorf("admit one fleet per step while retaining the serving cohorts")
	}
	return nil
}

// PlanWeightStep requires both application qualification and observation from
// every serving proxy. Its caller must recheck the claim and use a conditional
// write tied to the returned route identity and resource version.
func PlanWeightStep(intent WeightIntent, snapshot WeightSnapshot) (WeightAction, error) {
	if err := ValidateWeightIntent(intent); err != nil {
		return WeightAction{}, err
	}
	if snapshot.Now.IsZero() || snapshot.MaxAge <= 0 || intent.AppliedAt.After(snapshot.Now) ||
		snapshot.MinimumSamples <= 0 || snapshot.MinimumSamples > 1_000_000 ||
		snapshot.TolerancePoints < 0 || snapshot.TolerancePoints >= 100 {
		return WeightAction{}, fmt.Errorf("valid observation window and sampling bounds are required")
	}
	action := WeightAction{Kind: Hold, RouteUID: intent.RouteUID, From: maps.Clone(intent.From), To: maps.Clone(intent.To)}
	if snapshot.ClaimInstanceUID != intent.InstanceUID || snapshot.ClaimOperationUID != intent.OperationUID {
		action.Reason = "ClaimMismatch"
		return action, nil
	}
	route := snapshot.Route
	if route.InstanceUID != intent.InstanceUID || route.Role != intent.Role || route.RouteUID != intent.RouteUID ||
		route.ResourceVersion == "" || !route.ScopeVerified || !fresh(snapshot.Now, route.ObservedAt, snapshot.MaxAge) {
		action.Reason = "RouteObservationInvalid"
		return action, nil
	}
	action.ResourceVersion = route.ResourceVersion
	atTarget := maps.Equal(route.Weights, intent.To)
	if intent.Phase == Prepared && atTarget {
		action.Kind, action.Reason = RecordApplied, "TargetWeightsObserved"
		return action, nil
	}
	if (intent.Phase == Applied && !atTarget) || (intent.Phase == Prepared && !maps.Equal(route.Weights, intent.From)) {
		action.Reason = "WeightConflict"
		return action, nil
	}
	for fleet, weight := range intent.To {
		if weight > 0 && !qualifiedWeightFleet(intent, snapshot, fleet) {
			action.Reason = "FleetNotQualified"
			return action, nil
		}
	}
	if !qualifiedProxyWeights(intent, snapshot) {
		action.Reason = "ProxyEvidenceIncomplete"
		return action, nil
	}
	if intent.Phase == Prepared {
		action.Kind, action.Reason = PatchRouteWeights, "WeightStepQualified"
		return action, nil
	}
	action.Kind, action.Reason = RecordConverged, "WeightsObserved"
	return action, nil
}

func qualifiedWeightFleet(intent WeightIntent, snapshot WeightSnapshot, fleet string) bool {
	qualification := snapshot.Qualifications[fleet]
	evidence := qualification.CandidateEvidence
	if evidence.InstanceUID != intent.InstanceUID || evidence.OperationUID != intent.OperationUID ||
		evidence.Role != intent.Role || evidence.Fleet != fleet || evidence.Revision != intent.Revisions[fleet] ||
		!evidence.CapacityQualified || !evidence.PeerQualified || !evidence.SecurityQualified ||
		!evidence.RollbackCompatible || !currentEvidence(snapshot.Now, evidence.ObservedAt, evidence.ValidUntil, snapshot.MaxAge) ||
		len(qualification.Processes) == 0 {
		return false
	}
	for pod, incarnation := range qualification.Processes {
		if pod == "" || incarnation == "" {
			return false
		}
	}
	return true
}

func qualifiedProxyWeights(intent WeightIntent, snapshot WeightSnapshot) bool {
	expected := make(map[string]bool, len(snapshot.ExpectedProxyUIDs))
	for _, uid := range snapshot.ExpectedProxyUIDs {
		if uid == "" || expected[uid] {
			return false
		}
		expected[uid] = true
	}
	if len(expected) == 0 || len(expected) != len(snapshot.Proxies) {
		return false
	}
	requireConfiguredWeights := intent.Phase == Applied && !intent.AdoptExisting && sameWeightedFleets(intent.From, intent.To)
	for _, proxy := range snapshot.Proxies {
		if !expected[proxy.ProxyUID] || proxy.InstanceUID != intent.InstanceUID ||
			proxy.OperationUID != intent.OperationUID || proxy.Role != intent.Role || proxy.RouteUID != intent.RouteUID ||
			proxy.RouteVersion != snapshot.Route.ResourceVersion ||
			!currentEvidence(snapshot.Now, proxy.ObservedAt, proxy.ValidUntil, snapshot.MaxAge) ||
			proxy.ObservedAt.Before(intent.AppliedAt) {
			return false
		}
		if (requireConfiguredWeights || proxy.ConfiguredWeights != nil) && !maps.Equal(proxy.ConfiguredWeights, snapshot.Route.Weights) {
			return false
		}
		delete(expected, proxy.ProxyUID)
		for fleet, weight := range intent.To {
			// An opening step leaves incumbents in service while the joining
			// backend enters the proxy's health-check pool.
			if weight > 0 && (intent.Phase == Applied || intent.From[fleet] > 0) &&
				!maps.Equal(proxy.Healthy[fleet], snapshot.Qualifications[fleet].Processes) {
				return false
			}
		}
		if intent.Phase == Prepared && snapshot.RouteAuthIdentity != (RouteAuthIdentity{}) &&
			!qualifiedAuthenticatedIncumbents(intent, proxy, snapshot.MinimumSamples) {
			return false
		}
		if intent.Phase == Applied && !weightsWithinTolerance(intent.To, proxy, snapshot.MinimumSamples, snapshot.TolerancePoints) {
			return false
		}
	}
	return true
}

func qualifiedAuthenticatedIncumbents(intent WeightIntent, proxy ProxyWeightObservation, minimum int) bool {
	// Permit a removal even if requests to the outgoing fleet failed. Its
	// failures must not stand in for a successful probe of any retained fleet.
	if proxy.Failures < 0 || proxy.Failures > 1_000_000 ||
		(proxy.Failures != 0 && !removesWeightedFleet(intent)) {
		return false
	}
	total := proxy.Failures
	for fleet, count := range proxy.Requests {
		if count < 0 || count > 1_000_000-total || (count > 0 && intent.From[fleet] == 0) {
			return false
		}
		total += count
	}
	for fleet, weight := range intent.From {
		if weight > 0 && intent.To[fleet] > 0 && proxy.Requests[fleet] == 0 {
			return false
		}
	}
	return total >= minimum
}

func removesWeightedFleet(intent WeightIntent) bool {
	for fleet, weight := range intent.From {
		if weight > 0 && intent.To[fleet] == 0 {
			return true
		}
	}
	return false
}

func sameWeightedFleets(from, to map[string]int) bool {
	for fleet, weight := range from {
		if (weight > 0) != (to[fleet] > 0) {
			return false
		}
	}
	return true
}

func weightsWithinTolerance(weights map[string]int, proxy ProxyWeightObservation, minimum, tolerance int) bool {
	if proxy.Failures != 0 {
		return false
	}
	total := 0
	for fleet, count := range proxy.Requests {
		if count < 0 || count > 1_000_000 || (count > 0 && weights[fleet] == 0) {
			return false
		}
		total += count
		if total > 1_000_000 {
			return false
		}
	}
	if total < minimum {
		return false
	}
	for fleet, weight := range weights {
		if weight > 0 && proxy.Requests[fleet] == 0 {
			return false
		}
		difference := proxy.Requests[fleet]*100 - weight*total
		if difference > tolerance*total || difference < -tolerance*total {
			return false
		}
	}
	return true
}
