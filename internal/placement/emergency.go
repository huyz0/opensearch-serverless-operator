package placement

import (
	"fmt"
	"math"
	"sort"
)

// EmergencyZone is one role's observation for a configured AZ. Fit is the
// additional capacity independently qualified for the role's actual Pod
// template. Committed includes pending and terminating temporary Pods.
type EmergencyZone struct {
	Zone         string
	Unavailable  bool
	Fit          int
	Ready        int
	Pending      int
	Committed    int
	MaxTemporary int
}

// PlanEmergencyCapacity proposes one additional temporary Pod for a role.
// The caller owns outage confirmation, observation freshness, and the
// shared pending and warm-up budgets. Existing temporary Pods are retained
// here even after recovery. Failback is a separate retirement decision.
func PlanEmergencyCapacity(zones []EmergencyZone, regionalFloor, maximumTemporary, freeSlots int) (string, error) {
	if regionalFloor < 0 || maximumTemporary < 0 || freeSlots < 0 {
		return "", fmt.Errorf("emergency floors and limits cannot be negative")
	}
	var totalReady, totalPending, totalCommitted int64
	unavailable := false
	seen := make(map[string]struct{}, len(zones))
	candidates := make([]EmergencyZone, 0, len(zones))
	for _, zone := range zones {
		if _, exists := seen[zone.Zone]; exists || zone.Zone == "" || zone.Fit < 0 || zone.Ready < 0 ||
			zone.Pending < 0 || zone.Committed < 0 || zone.MaxTemporary < 0 {
			return "", fmt.Errorf("emergency zone observations must be valid and unique")
		}
		if totalCommitted > math.MaxInt64-int64(zone.Committed) {
			return "", fmt.Errorf("temporary Pod count exceeds the planner limit")
		}
		seen[zone.Zone] = struct{}{}
		totalCommitted += int64(zone.Committed)
		if zone.Unavailable {
			unavailable = true
			continue
		}
		if totalReady > math.MaxInt64-int64(zone.Ready) || totalPending > math.MaxInt64-int64(zone.Pending) {
			return "", fmt.Errorf("survivor Pod count exceeds the planner limit")
		}
		totalReady += int64(zone.Ready)
		totalPending += int64(zone.Pending)
		if zone.Ready > 0 && zone.Fit > 0 && zone.Committed < zone.MaxTemporary {
			candidates = append(candidates, zone)
		}
	}
	if !unavailable || freeSlots == 0 || totalReady >= int64(regionalFloor)-totalPending ||
		totalCommitted >= int64(maximumTemporary) || len(candidates) == 0 {
		return "", nil
	}
	sort.Slice(candidates, func(i, j int) bool {
		a, b := candidates[i], candidates[j]
		if a.Committed != b.Committed {
			return a.Committed < b.Committed
		}
		return a.Zone < b.Zone
	})
	return candidates[0].Zone, nil
}
