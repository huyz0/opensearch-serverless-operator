package podlifecycle

import (
	"fmt"
	"math"
	"sort"
	"time"
)

// ReplacementPolicy describes one role/fleet/zone revision change. Eligibility
// is supplied by the application adapter and must include routing, process,
// lease and any required warmness checks.
type ReplacementPolicy struct {
	GroupPolicy
	OwnerUID                 string
	TargetRevision           string
	MaxSurge                 int
	MaxObservationAge        time.Duration
	UnobservedGroupCreations int
	Creation                 CreationBudget
	Retirement               RetirementBudget
}

// PlanReplacement proposes one bounded action for a group. It does not reserve
// a creation slot, persist a victim, check a PDB or perform a Kubernetes call.
// The caller must serialize competing operations, supply a complete workload
// snapshot, persist the selected action, and re-observe immediately before an
// effect. UnobservedGroupCreations counts committed creates in this group that
// are not already represented by a Pod; Creation counts workload-wide slots.
func PlanReplacement(snapshot Snapshot, policy ReplacementPolicy) (Action, error) {
	return planReplacement(snapshot, policy, "")
}

func planReplacement(snapshot Snapshot, policy ReplacementPolicy, claimedUID string) (Action, error) {
	g := policy.Group
	if g.Workload == "" || g.Fleet == "" || g.Role == "" || g.Zone == "" || policy.OwnerUID == "" || policy.TargetRevision == "" || snapshot.Now.IsZero() || policy.MaxObservationAge <= 0 || policy.MaxSurge < 0 || policy.UnobservedGroupCreations < 0 || policy.Creation.PendingOrCommitted < 0 || policy.Creation.Maximum < 0 || policy.Creation.PendingOrCommitted > policy.Creation.Maximum || policy.Retirement.MinServingRole < 0 || policy.Retirement.MaxConcurrentRetirements < 0 {
		return Action{}, fmt.Errorf("complete group, revision, fresh observations and valid budgets are required")
	}
	if policy.Desired < 0 || policy.Desired > math.MaxInt-policy.MaxSurge || policy.UnobservedGroupCreations > policy.Creation.PendingOrCommitted {
		return Action{}, fmt.Errorf("invalid desired capacity or surge")
	}
	assessment, err := Assess(snapshot, policy.GroupPolicy)
	if err != nil {
		return Action{}, err
	}
	resident, targetEligible, oldActive, heldActive, observedPending := 0, 0, 0, 0, 0
	var candidates []Pod
	for _, pod := range snapshot.Pods {
		if pod.Group.Workload != g.Workload {
			continue
		}
		if pod.Identity.Namespace == "" || pod.Identity.Name == "" || pod.Identity.UID == "" || pod.Identity.OwnerUID != policy.OwnerUID || pod.Group.Fleet == "" || pod.Group.Role == "" || pod.Group.Zone == "" || pod.ObservedAt.IsZero() || pod.ObservedAt.After(snapshot.Now) || snapshot.Now.Sub(pod.ObservedAt) > policy.MaxObservationAge {
			return Action{}, fmt.Errorf("complete, fresh workload Pod observations are required")
		}
		if !pod.Bound && !pod.Terminating && !pod.Terminal {
			observedPending++
		}
		if pod.Group != g {
			continue
		}
		if pod.Revision == "" {
			return Action{}, fmt.Errorf("group Pod revision is unknown")
		}
		resident++ // Terminating and terminal objects still consume a Pod slot.
		if pod.Terminating || pod.Terminal {
			continue
		}
		if pod.RetirementHeld {
			heldActive++
		}
		if pod.Revision == policy.TargetRevision {
			if pod.Bound && pod.Eligible && !pod.RetirementHeld {
				targetEligible++
			}
		} else {
			oldActive++
			if pod.Bound && !pod.RetirementHeld && (claimedUID == "" || pod.Identity.UID == claimedUID) {
				candidates = append(candidates, pod)
			}
		}
	}
	if observedPending > policy.Creation.PendingOrCommitted-policy.UnobservedGroupCreations {
		return Action{}, fmt.Errorf("creation budget omits observed pending Pods or committed group creations")
	}
	if policy.UnobservedGroupCreations > math.MaxInt-resident {
		return Action{}, fmt.Errorf("creation reservation count overflow")
	}
	resident += policy.UnobservedGroupCreations
	capacity := policy.Desired + policy.MaxSurge
	if oldActive == 0 {
		if resident == assessment.Active && assessment.Active == policy.Desired && assessment.ProjectedServing >= policy.Desired {
			return Action{Kind: Wait, Group: g, Reason: "RevisionConverged"}, nil
		}
		if assessment.Active >= policy.Desired {
			return Action{Kind: Wait, Group: g, Reason: "RevisionQualifying"}, nil
		}
	}
	if assessment.Active-heldActive > policy.Desired && targetEligible > 0 {
		sort.Slice(candidates, func(i, j int) bool { return candidates[i].Identity.UID < candidates[j].Identity.UID })
		for _, candidate := range candidates {
			if candidate.Eligible && assessment.ProjectedServing <= policy.Desired {
				continue
			}
			allowed, err := CheckRetirementBudget(snapshot, policy.GroupPolicy, candidate.Identity, policy.Retirement)
			if err != nil {
				return Action{}, err
			}
			if allowed {
				return Action{Kind: EvictPod, Group: g, Pod: candidate.Identity, Reason: "QualifiedReplacementAvailable"}, nil
			}
		}
	}
	if (assessment.Active+policy.UnobservedGroupCreations < policy.Desired || oldActive > 0) && resident < capacity && policy.Creation.PendingOrCommitted < policy.Creation.Maximum {
		return Action{Kind: CreatePod, Group: g, Reason: "TargetRevisionCapacityNeeded"}, nil
	}
	return Action{Kind: Wait, Group: g, Reason: "ReplacementBlocked"}, nil
}

// RecheckClaimedReplacement re-evaluates a previously persisted retirement
// claim using a fresh complete snapshot. The caller must prove that its durable
// operation owns this exact claim and serialize competing decisions. A true
// result is only a planning decision: the caller must still re-read the Pod,
// use UID preconditions and obtain Kubernetes eviction admission. A terminating
// or absent Pod requires separate recovery rather than a new eviction.
//
// The planner considers only the claimed victim and temporarily discounts its
// hold in a private copy. Other holds remain in force.
func RecheckClaimedReplacement(snapshot Snapshot, policy ReplacementPolicy, victim Identity) (bool, error) {
	if victim.Namespace == "" || victim.Name == "" || victim.UID == "" || victim.OwnerUID == "" || victim.OwnerUID != policy.OwnerUID {
		return false, fmt.Errorf("complete matching claimed victim identity is required")
	}
	copySnapshot := snapshot
	copySnapshot.Pods = append([]Pod(nil), snapshot.Pods...)
	found := false
	for i := range copySnapshot.Pods {
		pod := &copySnapshot.Pods[i]
		if pod.Identity.UID != victim.UID {
			continue
		}
		if found || pod.Identity != victim || pod.Group != policy.Group {
			return false, fmt.Errorf("claimed victim identity or group changed")
		}
		found = true
		if !pod.RetirementHeld || pod.Terminating || pod.Terminal {
			return false, nil
		}
		pod.RetirementHeld = false
	}
	if !found {
		return false, fmt.Errorf("claimed victim not present in snapshot")
	}
	action, err := planReplacement(copySnapshot, policy, victim.UID)
	if err != nil {
		return false, err
	}
	return action.Kind == EvictPod && action.Pod == victim, nil
}
