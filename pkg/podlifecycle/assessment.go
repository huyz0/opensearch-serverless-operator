package podlifecycle

import "fmt"

// Assessment reports capacity for one group. A Pod already committed to
// retirement is excluded from projected serving capacity once.
type Assessment struct {
	Active           int
	Pending          int
	Terminating      int
	Serving          int
	ProjectedServing int
	Deficit          int
}

// Assess validates the supplied identities and counts capacity for a group.
// Callers must supply a complete, fresh observation and enforce wider budgets.
func Assess(snapshot Snapshot, policy GroupPolicy) (Assessment, error) {
	var result Assessment
	if err := validateGroupCapacity(policy); err != nil {
		return result, err
	}
	seen := make(map[string]struct{})
	for _, pod := range snapshot.Pods {
		if err := recordPodIdentity(seen, pod.Identity); err != nil {
			return Assessment{}, err
		}
		if pod.Group != policy.Group {
			continue
		}
		if pod.Terminating {
			result.Terminating++
			continue
		}
		if pod.Terminal {
			continue
		}
		result.Active++
		if !pod.Bound {
			result.Pending++
		}
		if pod.Eligible && pod.Bound {
			result.Serving++
		}
		if pod.isProjectedServing() {
			result.ProjectedServing++
		}
	}
	if result.Active < policy.Desired {
		result.Deficit = policy.Desired - result.Active
	}
	return result, nil
}

func validateGroupCapacity(policy GroupPolicy) error {
	if policy.Desired < 0 || policy.MinServing < 0 || policy.MinServing > policy.Desired {
		return fmt.Errorf("invalid desired count or serving floor")
	}
	return nil
}

func recordPodIdentity(seen map[string]struct{}, identity Identity) error {
	if identity.UID == "" || identity.OwnerUID == "" {
		return fmt.Errorf("Pod and owner UIDs are required")
	}
	if _, exists := seen[identity.UID]; exists {
		return fmt.Errorf("duplicate Pod UID %q", identity.UID)
	}
	seen[identity.UID] = struct{}{}
	return nil
}
