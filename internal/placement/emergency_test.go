package placement

import "testing"

func TestEmergencyCapacityOnlyAddsQualifiedSurvivorCapacity(t *testing.T) {
	zones := []EmergencyZone{
		{Zone: "a", Unavailable: true, Ready: 3, Pending: 2, Committed: 1, Fit: 2, MaxTemporary: 2},
		{Zone: "b", Ready: 2, Fit: 2, Committed: 1, MaxTemporary: 2},
		{Zone: "c", Ready: 2, Fit: 1, MaxTemporary: 1},
	}
	assertZone := func(want string, floor, max, slots int) {
		t.Helper()
		got, err := PlanEmergencyCapacity(zones, floor, max, slots)
		if err != nil || got != want {
			t.Fatalf("emergency zone = %q, want %q: %v", got, want, err)
		}
	}
	assertZone("c", 5, 3, 1)
	assertZone("", 5, 2, 1) // The temporary Pod in the lost zone still counts.
	assertZone("", 5, 3, 0)
	zones[2].Fit = 0
	assertZone("b", 5, 3, 1)
	zones[1].Pending = 1
	assertZone("", 5, 3, 1) // Ordinary pending recovery can cover the shortfall.
	zones[1].Pending = 0
	zones[0].Unavailable = false
	assertZone("", 8, 3, 1) // A shortfall without an outage cannot trigger surge.
}
