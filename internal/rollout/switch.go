// Package rollout plans fleet transitions without performing Kubernetes effects.
package rollout

import (
	"fmt"
	"time"
)

type SwitchPhase string

const (
	Prepared SwitchPhase = "Prepared"
	Applied  SwitchPhase = "Applied"
)

type ActionKind string

const (
	Hold            ActionKind = "Hold"
	PatchService    ActionKind = "PatchService"
	RecordApplied   ActionKind = "RecordApplied"
	RecordConverged ActionKind = "RecordConverged"
)

// SwitchIntent must be durably persisted before a Service selector mutation.
// CandidateRevision is an immutable qualification identity, not a fleet name.
type SwitchIntent struct {
	InstanceUID, OperationUID                   string
	Role, ServiceUID                            string
	SourceFleet, TargetFleet, CandidateRevision string
	// AdoptExisting is only valid for rollback when the expected source is
	// already the target. It never authorizes a Service patch.
	AdoptExisting bool
	Phase         SwitchPhase
	AppliedAt     time.Time
}

type ServiceObservation struct {
	InstanceUID, ServiceUID, ResourceVersion string
	Role, Fleet                              string
	Owned, RoutingShapeValid                 bool
	ObservedAt                               time.Time
}

// CandidateEvidence is supplied by qualified application and platform
// adapters. Kubernetes readiness or a configuration declaration alone does
// not establish these properties.
type CandidateEvidence struct {
	InstanceUID, OperationUID, Role, Fleet, Revision                        string
	CapacityQualified, PeerQualified, SecurityQualified, RollbackCompatible bool
	ObservedAt                                                              time.Time
	ValidUntil                                                              time.Time
}

// RouteEvidence requires both EndpointSlice convergence and an independent
// production-route probe. It says nothing about future traffic behavior.
type RouteEvidence struct {
	InstanceUID, OperationUID, Role, Fleet, ServiceUID, ServiceVersion string
	EndpointsConverged, ClientProbePassed                              bool
	ObservedAt                                                         time.Time
	ValidUntil                                                         time.Time
}

type SwitchSnapshot struct {
	Now                                 time.Time
	MaxAge                              time.Duration
	ClaimInstanceUID, ClaimOperationUID string
	Service                             ServiceObservation
	Candidate                           *CandidateEvidence
	Route                               *RouteEvidence
}

type SwitchAction struct {
	Kind   ActionKind
	Reason string
	// The executor still has to re-read and conditionally patch this identity.
	ServiceUID, ResourceVersion, Role, SourceFleet, TargetFleet string
}

func fresh(now, observed time.Time, maxAge time.Duration) bool {
	return !observed.IsZero() && !observed.After(now) && now.Sub(observed) <= maxAge
}

func currentEvidence(now, observed, validUntil time.Time, maxAge time.Duration) bool {
	return fresh(now, observed, maxAge) && (validUntil.IsZero() || now.Before(validUntil))
}

func validateSwitchIntent(intent SwitchIntent) error {
	if intent.InstanceUID == "" || intent.OperationUID == "" || (intent.Role != "search" && intent.Role != "ingest") || intent.ServiceUID == "" || intent.SourceFleet == "" || intent.TargetFleet == "" || (intent.SourceFleet == intent.TargetFleet) != intent.AdoptExisting || intent.CandidateRevision == "" || (intent.Phase != Prepared && intent.Phase != Applied) || (intent.Phase == Prepared && !intent.AppliedAt.IsZero()) || (intent.Phase == Applied && intent.AppliedAt.IsZero()) {
		return fmt.Errorf("complete switch intent and phase are required")
	}
	return nil
}

// PlanSwitch proposes one action for an already claimed, persisted role
// transition. It does not patch a Service, establish candidate qualification,
// prove route convergence, or authorize promotion of the other role.
func PlanSwitch(intent SwitchIntent, snapshot SwitchSnapshot) (SwitchAction, error) {
	if err := validateSwitchIntent(intent); err != nil {
		return SwitchAction{}, err
	}
	if snapshot.Now.IsZero() || snapshot.MaxAge <= 0 || (intent.Phase == Applied && intent.AppliedAt.After(snapshot.Now)) {
		return SwitchAction{}, fmt.Errorf("complete switch intent, phase and observation window are required")
	}
	action := SwitchAction{Kind: Hold, Role: intent.Role, ServiceUID: intent.ServiceUID, SourceFleet: intent.SourceFleet, TargetFleet: intent.TargetFleet}
	if snapshot.ClaimInstanceUID != intent.InstanceUID || snapshot.ClaimOperationUID != intent.OperationUID {
		action.Reason = "ClaimMismatch"
		return action, nil
	}
	service := snapshot.Service
	if service.InstanceUID != intent.InstanceUID || service.ServiceUID != intent.ServiceUID || service.Role != intent.Role || service.ResourceVersion == "" || !service.Owned || !service.RoutingShapeValid || !fresh(snapshot.Now, service.ObservedAt, snapshot.MaxAge) {
		action.Reason = "ServiceObservationInvalid"
		return action, nil
	}
	action.ResourceVersion = service.ResourceVersion
	if service.Fleet != intent.SourceFleet && service.Fleet != intent.TargetFleet {
		action.Reason = "SelectorConflict"
		return action, nil
	}
	if intent.Phase == Prepared && service.Fleet == intent.TargetFleet {
		if intent.AdoptExisting {
			// Persist the observation time before accepting route evidence. The
			// matching Service is not itself proof that traffic has converged.
			action.Kind, action.Reason = RecordApplied, "ExistingTargetObserved"
			return action, nil
		}
		// A successful patch may have returned an error. Record the effect
		// before any further transition, even if qualification since changed.
		action.Kind, action.Reason = RecordApplied, "TargetObserved"
		return action, nil
	}
	if intent.Phase == Applied && service.Fleet != intent.TargetFleet {
		action.Reason = "SelectorChangedAfterApply"
		return action, nil
	}
	evidence := snapshot.Candidate
	if evidence == nil || evidence.InstanceUID != intent.InstanceUID || evidence.OperationUID != intent.OperationUID || evidence.Role != intent.Role || evidence.Fleet != intent.TargetFleet || evidence.Revision != intent.CandidateRevision || !currentEvidence(snapshot.Now, evidence.ObservedAt, evidence.ValidUntil, snapshot.MaxAge) || !evidence.CapacityQualified || !evidence.PeerQualified || !evidence.SecurityQualified || !evidence.RollbackCompatible {
		action.Reason = "CandidateNotQualified"
		return action, nil
	}
	if intent.Phase == Prepared {
		action.Kind, action.Reason = PatchService, "CandidateQualified"
		return action, nil
	}
	route := snapshot.Route
	if route == nil || route.InstanceUID != intent.InstanceUID || route.OperationUID != intent.OperationUID || route.Role != intent.Role || route.Fleet != intent.TargetFleet || route.ServiceUID != intent.ServiceUID || route.ServiceVersion != service.ResourceVersion || !currentEvidence(snapshot.Now, route.ObservedAt, route.ValidUntil, snapshot.MaxAge) || route.ObservedAt.Before(intent.AppliedAt) || !route.EndpointsConverged || !route.ClientProbePassed {
		action.Reason = "RouteNotConverged"
		return action, nil
	}
	action.Kind, action.Reason = RecordConverged, "RouteObserved"
	return action, nil
}
