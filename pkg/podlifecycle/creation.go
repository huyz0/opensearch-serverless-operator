package podlifecycle

import "fmt"

// CreationBudget describes currently reserved creation slots. The caller must
// count pending and already committed creation intents across the workload.
type CreationBudget struct {
	PendingOrCommitted int
	Maximum            int
}

// CheckCreation reports whether a single additional Pod can be created for a
// group. Persist the intent and reserve its slot before creating the Pod.
// Callers must serialize competing decisions and re-observe after restarts.
func CheckCreation(snapshot Snapshot, policy GroupPolicy, budget CreationBudget) (bool, error) {
	if budget.PendingOrCommitted < 0 || budget.Maximum < 0 || budget.PendingOrCommitted > budget.Maximum {
		return false, fmt.Errorf("invalid creation budget")
	}
	assessment, err := Assess(snapshot, policy)
	if err != nil {
		return false, err
	}
	return assessment.Deficit > 0 && budget.PendingOrCommitted < budget.Maximum, nil
}
