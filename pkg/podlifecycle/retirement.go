package podlifecycle

import "fmt"

type scaleDownGroup struct {
	desired       int
	serving       int
	firstVictim   Identity
	invalidVictim bool
}

// PlanScaleDown proposes one retirement after every group in the serving
// role and fleet can meet its steady target. Policies must cover every zone
// in that role and fleet, including removed zones with a desired count of zero.
// Pods in other roles or fleets may consume the shared retirement budget.
// The caller supplies qualified serving observations, persists the proposal,
// and revalidates it before submitting an eviction.
func PlanScaleDown(snapshot Snapshot, budget RetirementBudget) (Action, error) {
	if budget.MinServingRole < 0 || budget.MaxConcurrentRetirements < 0 {
		return Action{}, fmt.Errorf("retirement budget must be nonnegative")
	}
	wait := Action{Kind: Wait, Reason: "NoServingSurplus"}
	if len(snapshot.Policies) == 0 {
		return wait, nil
	}
	scope := snapshot.Policies[0].Group
	groups := make(map[Group]*scaleDownGroup, len(snapshot.Policies))
	for _, policy := range snapshot.Policies {
		group := policy.Group
		if group.Workload == "" || group.Fleet == "" || group.Role == "" || group.Zone == "" ||
			group.Workload != scope.Workload || group.Fleet != scope.Fleet || group.Role != scope.Role {
			return Action{}, fmt.Errorf("scale-down policies must cover one workload, serving fleet and role")
		}
		if _, exists := groups[group]; exists {
			return Action{}, fmt.Errorf("duplicate scale-down group %v", group)
		}
		if err := validateGroupCapacity(policy); err != nil {
			return Action{}, err
		}
		groups[group] = &scaleDownGroup{desired: policy.Desired}
	}

	seen := make(map[string]struct{}, len(snapshot.Pods))
	regional := 0
	retiring := 0
	for _, pod := range snapshot.Pods {
		if err := recordPodIdentity(seen, pod.Identity); err != nil {
			return Action{}, err
		}
		group := pod.Group
		if group.Workload != scope.Workload {
			continue
		}
		if pod.Terminating || pod.RetirementHeld {
			retiring++
		}
		if group.Fleet != scope.Fleet || group.Role != scope.Role {
			continue
		}
		capacity, exists := groups[group]
		if !exists {
			return Action{}, fmt.Errorf("scale-down policy missing for zone %q", group.Zone)
		}
		if !pod.isProjectedServing() {
			continue
		}
		capacity.serving++
		regional++
		if pod.Identity.Namespace == "" || pod.Identity.Name == "" {
			capacity.invalidVictim = true
		}
		first := capacity.firstVictim
		if first.UID == "" || pod.Identity.Name < first.Name || (pod.Identity.Name == first.Name && pod.Identity.UID < first.UID) {
			capacity.firstVictim = pod.Identity
		}
	}

	underTarget := false
	var victim Action
	for group, capacity := range groups {
		underTarget = underTarget || capacity.serving < capacity.desired
		if capacity.serving <= capacity.desired {
			continue
		}
		if capacity.invalidVictim {
			return Action{}, fmt.Errorf("complete victim identity is required")
		}
		if victim.Pod.UID == "" || group.Zone < victim.Group.Zone {
			victim = Action{Kind: EvictPod, Group: group, Pod: capacity.firstVictim, Reason: "ServingSurplus"}
		}
	}
	if underTarget {
		return Action{Kind: Wait, Reason: "SteadyCapacityMissing"}, nil
	}
	if victim.Pod.UID == "" {
		return wait, nil
	}
	if retiring >= budget.MaxConcurrentRetirements || regional <= budget.MinServingRole {
		return Action{Kind: Wait, Reason: "RetirementBudgetExhausted"}, nil
	}
	return victim, nil
}

// CheckRetirement checks a specific victim against its group floor. The caller
// must separately check role-wide floors, concurrency and observation freshness.
// A successful result does not authorize a mutation or indicate PDB admission.
func CheckRetirement(snapshot Snapshot, policy GroupPolicy, victim Identity) (bool, error) {
	assessment, err := Assess(snapshot, policy)
	if err != nil {
		return false, err
	}
	if victim.UID == "" || victim.OwnerUID == "" || victim.Name == "" || victim.Namespace == "" {
		return false, fmt.Errorf("complete victim identity is required")
	}
	for _, pod := range snapshot.Pods {
		if pod.Identity.UID != victim.UID {
			continue
		}
		if pod.Identity != victim || pod.Group != policy.Group {
			return false, fmt.Errorf("victim identity or group changed")
		}
		if pod.Terminating || pod.Terminal || pod.RetirementHeld {
			return false, nil
		}
		remaining := assessment.ProjectedServing
		if pod.Bound && pod.Eligible {
			remaining--
		}
		return remaining >= policy.MinServing, nil
	}
	return false, fmt.Errorf("victim not present in snapshot")
}

// RetirementBudget sets workload-wide limits for a voluntary retirement.
// Eligible Pods must already be filtered by the caller's actual serving routes.
type RetirementBudget struct {
	MinServingRole           int
	MaxConcurrentRetirements int
}

// CheckRetirementBudget checks group, role and workload-wide limits for a
// proposed victim. The caller persists the claim and serializes competing
// decisions, then rechecks against current observations before eviction.
func CheckRetirementBudget(snapshot Snapshot, policy GroupPolicy, victim Identity, budget RetirementBudget) (bool, error) {
	if budget.MinServingRole < 0 || budget.MaxConcurrentRetirements < 0 {
		return false, fmt.Errorf("retirement budget must be nonnegative")
	}
	allowed, err := CheckRetirement(snapshot, policy, victim)
	if err != nil || !allowed {
		return allowed, err
	}
	roleServing := 0
	retiring := 0
	for _, pod := range snapshot.Pods {
		if pod.Group.Workload != policy.Group.Workload {
			continue
		}
		if pod.Terminating || pod.RetirementHeld {
			retiring++
			continue
		}
		if pod.Terminal {
			continue
		}
		if pod.Group.Role == policy.Group.Role && pod.Bound && pod.Eligible && pod.Identity.UID != victim.UID {
			roleServing++
		}
	}
	return retiring < budget.MaxConcurrentRetirements && roleServing >= budget.MinServingRole, nil
}
