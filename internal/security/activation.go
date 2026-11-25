// Package security contains security configuration activation decisions.
package security

import "fmt"

// Consumer identifies one currently reachable enforcement process. ProcessID
// must change on every application restart, even within the same Pod UID.
type Consumer struct {
	PodUID    string
	ProcessID string
}

// Acknowledgement is observed from a protected provider management path.
type Acknowledgement struct {
	PodUID    string
	ProcessID string
	Revision  string
}

// Applied reports whether all current consumers have acknowledged revision.
// The caller must provide the complete eligible production, preview, rollback
// and peer cohort, and must fence or exclude processes that cannot be observed.
// A stale acknowledgement from an earlier process cannot satisfy the check.
func Applied(revision string, consumers []Consumer, acknowledgements []Acknowledgement) (bool, error) {
	if revision == "" || len(consumers) == 0 {
		return false, fmt.Errorf("revision and nonempty consumer cohort are required")
	}
	observed := make(map[Consumer]string, len(acknowledgements))
	for _, ack := range acknowledgements {
		identity := Consumer{PodUID: ack.PodUID, ProcessID: ack.ProcessID}
		if identity.PodUID == "" || identity.ProcessID == "" || ack.Revision == "" {
			return false, fmt.Errorf("acknowledgement identity and revision are required")
		}
		if _, exists := observed[identity]; exists {
			return false, fmt.Errorf("duplicate acknowledgement for process")
		}
		observed[identity] = ack.Revision
	}
	seen := make(map[string]bool, len(consumers))
	for _, consumer := range consumers {
		if consumer.PodUID == "" || consumer.ProcessID == "" || seen[consumer.PodUID] {
			return false, fmt.Errorf("consumer identities must be complete and unique")
		}
		seen[consumer.PodUID] = true
		if observed[consumer] != revision {
			return false, nil
		}
	}
	return true, nil
}
