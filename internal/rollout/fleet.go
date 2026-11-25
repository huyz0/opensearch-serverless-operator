package rollout

import "fmt"

// RoleProgress is durable operation state. Converged must only be recorded
// after PlanSwitch returns RecordConverged for this exact intent.
type RoleProgress struct {
	Intent    SwitchIntent
	Converged bool
}

type FleetAction struct {
	Role     string
	Switch   SwitchAction
	Complete bool
}

// PlanFleetSwitch sequences search before ingest. A previously converged role
// is checked again against fresh evidence before another role may advance or
// the operation may complete. The caller persists each transition separately.
// A rollback role already on its target must explicitly adopt the observed
// Service and qualify it before that role can be marked converged.
func PlanFleetSwitch(roles []RoleProgress, snapshots map[string]SwitchSnapshot) (FleetAction, error) {
	if len(roles) != 2 || roles[0].Intent.Role != "search" || roles[1].Intent.Role != "ingest" {
		return FleetAction{}, fmt.Errorf("search and ingest progress in that order are required")
	}
	first, second := roles[0].Intent, roles[1].Intent
	for _, role := range roles {
		if err := validateSwitchIntent(role.Intent); err != nil {
			return FleetAction{}, fmt.Errorf("invalid %s intent: %w", role.Intent.Role, err)
		}
		if role.Converged && role.Intent.Phase != Applied {
			return FleetAction{}, fmt.Errorf("a converged role must have an applied intent")
		}
	}
	if first.InstanceUID != second.InstanceUID || first.OperationUID != second.OperationUID || first.TargetFleet != second.TargetFleet || first.ServiceUID == second.ServiceUID {
		return FleetAction{}, fmt.Errorf("role intents must share operation and target identities and use distinct Services")
	}
	if !roles[0].Converged && (roles[1].Converged || roles[1].Intent.Phase != Prepared) {
		return FleetAction{}, fmt.Errorf("ingest cannot advance before search converges")
	}
	for _, role := range roles {
		snapshot, ok := snapshots[role.Intent.Role]
		if !ok {
			return FleetAction{Role: role.Intent.Role, Switch: SwitchAction{Kind: Hold, Reason: "ObservationMissing"}}, nil
		}
		action, err := PlanSwitch(role.Intent, snapshot)
		if err != nil {
			return FleetAction{}, err
		}
		if !role.Converged || action.Kind != RecordConverged {
			return FleetAction{Role: role.Intent.Role, Switch: action}, nil
		}
	}
	return FleetAction{Complete: true}, nil
}
