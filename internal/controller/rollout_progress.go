package controller

import (
	"context"
	"errors"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/huyz0/opensearch-serverless-operator/api/v1alpha1"
	"github.com/huyz0/opensearch-serverless-operator/internal/rollout"
)

var errRolloutPlanChanged = errors.New("rollout plan changed before traffic patch")

// rolloutProgress converts persisted status back into an ordered planner input
// and binds it to the immutable operation and current instance claim.
func rolloutProgress(instance *v1alpha1.OpenSearchServerless, operation *v1alpha1.OpenSearchServerlessOperation) ([]rollout.RoleProgress, error) {
	if instance == nil || operation == nil || instance.UID == "" || operation.UID == "" || instance.ResourceVersion == "" || operation.ResourceVersion == "" || instance.DeletionTimestamp != nil || operation.DeletionTimestamp != nil || instance.Namespace == "" || instance.Namespace != operation.Namespace || instance.Name != operation.Spec.InstanceRef.Name || string(instance.UID) != operation.Spec.InstanceRef.UID {
		return nil, fmt.Errorf("live instance and operation identities are required")
	}
	if err := operation.Spec.Validate(); err != nil {
		return nil, err
	}
	if operation.Spec.Action != "Promote" && operation.Spec.Action != "Rollback" {
		return nil, fmt.Errorf("fleet operation is required")
	}
	want := operationClaimFor(operation)
	if want.Name == "" || want.Generation <= 0 || instance.Status.ActiveOperation == nil || *instance.Status.ActiveOperation != want {
		return nil, fmt.Errorf("operation does not own the instance claim")
	}
	if len(operation.Status.RolloutRoles) != 2 {
		return nil, fmt.Errorf("both role intents are required")
	}
	roles := make([]rollout.RoleProgress, 0, 2)
	committed := rolloutAlreadyCommitted(instance)
	for _, role := range []string{"search", "ingest"} {
		state, ok := operation.Status.RolloutRoles[role]
		expectedFleet := state.SourceFleet
		if committed {
			expectedFleet = state.TargetFleet
		}
		if !ok || state.SourceFleet != operation.Spec.ExpectedServingFleetByRole[role] || state.TargetFleet != operation.Spec.TargetFleet || instance.Status.ServingFleetIntentByRole[role] != expectedFleet || (committed && !state.Converged) || (state.AdoptExisting && operation.Spec.Action != "Rollback") {
			return nil, fmt.Errorf("%s intent conflicts with the operation or serving intent", role)
		}
		intent := rollout.SwitchIntent{InstanceUID: string(instance.UID), OperationUID: string(operation.UID), Role: role, ServiceUID: state.ServiceUID, SourceFleet: state.SourceFleet, TargetFleet: state.TargetFleet, CandidateRevision: state.CandidateRevision, AdoptExisting: state.AdoptExisting, Phase: rollout.SwitchPhase(state.Phase)}
		if state.AppliedAt != nil {
			intent.AppliedAt = state.AppliedAt.Time
		}
		roles = append(roles, rollout.RoleProgress{Intent: intent, Converged: state.Converged})
	}
	if _, err := rollout.PlanFleetSwitch(roles, nil); err != nil {
		return nil, err
	}
	return roles, nil
}

func rolloutAlreadyCommitted(instance *v1alpha1.OpenSearchServerless) bool {
	return instance.Status.LastRolloutCommit != nil && instance.Status.ActiveOperation != nil &&
		*instance.Status.LastRolloutCommit == *instance.Status.ActiveOperation
}

// prepareRolloutProgress pins both intents in a single status update. The
// caller must obtain Service identities and candidate revisions from qualified
// observations; this function does not establish their eligibility.
func prepareRolloutProgress(ctx context.Context, c client.Client, instance *v1alpha1.OpenSearchServerless, operation *v1alpha1.OpenSearchServerlessOperation, roles map[string]v1alpha1.RolloutRoleStatus) error {
	if c == nil || operation == nil || operation.Status.RolloutRoles != nil {
		return fmt.Errorf("empty rollout status and status client are required")
	}
	before := operation.DeepCopyObject().(*v1alpha1.OpenSearchServerlessOperation)
	updated := operation.DeepCopyObject().(*v1alpha1.OpenSearchServerlessOperation)
	updated.Status.RolloutRoles = make(map[string]v1alpha1.RolloutRoleStatus, len(roles))
	for role, state := range roles {
		if state.Phase != string(rollout.Prepared) || state.AppliedAt != nil || state.Converged {
			return fmt.Errorf("initial role intents must be prepared and unconverged")
		}
		updated.Status.RolloutRoles[role] = state
	}
	if _, err := rolloutProgress(instance, updated); err != nil {
		return err
	}
	if err := c.Status().Patch(ctx, updated, client.MergeFromWithOptions(before, client.MergeFromWithOptimisticLock{})); err != nil {
		return fmt.Errorf("persist rollout intents: %w", err)
	}
	*operation = *updated
	return nil
}

func planRolloutProgress(instance *v1alpha1.OpenSearchServerless, operation *v1alpha1.OpenSearchServerlessOperation, snapshots map[string]rollout.SwitchSnapshot) (rollout.FleetAction, error) {
	roles, err := rolloutProgress(instance, operation)
	if err != nil {
		return rollout.FleetAction{}, err
	}
	action, err := rollout.PlanFleetSwitch(roles, snapshots)
	if err == nil && (action.Switch.Kind == rollout.PatchService || action.Switch.Kind == rollout.RecordConverged || action.Complete) {
		// Before advancing traffic, verify that neither role's Service has
		// disappeared, been replaced, or changed outside the operation.
		for _, role := range roles {
			snapshot, ok := snapshots[role.Intent.Role]
			service := snapshot.Service
			if !ok || snapshot.Now.IsZero() || snapshot.MaxAge <= 0 || snapshot.ClaimInstanceUID != role.Intent.InstanceUID || snapshot.ClaimOperationUID != role.Intent.OperationUID || service.InstanceUID != role.Intent.InstanceUID || service.ServiceUID != role.Intent.ServiceUID || service.Role != role.Intent.Role || service.ResourceVersion == "" || !service.Owned || !service.RoutingShapeValid || service.ObservedAt.IsZero() || service.ObservedAt.After(snapshot.Now) || snapshot.Now.Sub(service.ObservedAt) > snapshot.MaxAge || (service.Fleet != role.Intent.SourceFleet && service.Fleet != role.Intent.TargetFleet) || (role.Intent.Phase == rollout.Applied && service.Fleet != role.Intent.TargetFleet) || (role.Intent.Role != action.Role && role.Intent.Phase == rollout.Prepared && !role.Intent.AdoptExisting && service.Fleet != role.Intent.SourceFleet) {
				return rollout.FleetAction{Role: role.Intent.Role, Switch: rollout.SwitchAction{Kind: rollout.Hold, Reason: "OtherServiceObservationInvalid"}}, nil
			}
		}
	}
	if err == nil && operation.Spec.Action == "Promote" && action.Switch.Kind == rollout.PatchService && action.Role == "search" {
		// Preparation qualified both roles. If ingest loses qualification
		// before the first switch, hold rather than enter mixed routing.
		ingest, err := rollout.PlanSwitch(roles[1].Intent, snapshots["ingest"])
		if err != nil {
			return rollout.FleetAction{}, err
		}
		if ingest.Kind != rollout.PatchService {
			ingest.Kind = rollout.Hold
			return rollout.FleetAction{Role: "ingest", Switch: ingest}, nil
		}
	}
	return action, err
}

// advanceRolloutProgress records observations with an optimistic status patch.
// The caller rechecks the returned plan before changing Service selectors.
func advanceRolloutProgress(ctx context.Context, c client.Client, instance *v1alpha1.OpenSearchServerless, operation *v1alpha1.OpenSearchServerlessOperation, snapshots map[string]rollout.SwitchSnapshot) (rollout.FleetAction, error) {
	if c == nil {
		return rollout.FleetAction{}, fmt.Errorf("status client is required")
	}
	action, err := planRolloutProgress(instance, operation, snapshots)
	if err != nil || (action.Switch.Kind != rollout.RecordApplied && action.Switch.Kind != rollout.RecordConverged) {
		return action, err
	}
	before := operation.DeepCopyObject().(*v1alpha1.OpenSearchServerlessOperation)
	updated := operation.DeepCopyObject().(*v1alpha1.OpenSearchServerlessOperation)
	state := updated.Status.RolloutRoles[action.Role]
	if action.Switch.Kind == rollout.RecordApplied {
		state.Phase = string(rollout.Applied)
		observedAt := snapshots[action.Role].Now
		// Kubernetes serializes MicroTime to microseconds. Round upward so
		// evidence from before this observation cannot become valid on reload.
		persistedAt := observedAt.Truncate(time.Microsecond)
		if persistedAt.Before(observedAt) {
			persistedAt = persistedAt.Add(time.Microsecond)
		}
		applied := metav1.NewMicroTime(persistedAt)
		state.AppliedAt = &applied
	} else {
		state.Converged = true
	}
	updated.Status.RolloutRoles[action.Role] = state
	if err := c.Status().Patch(ctx, updated, client.MergeFromWithOptions(before, client.MergeFromWithOptimisticLock{})); err != nil {
		return rollout.FleetAction{}, fmt.Errorf("persist rollout observation: %w", err)
	}
	*operation = *updated
	return action, nil
}

// observeAndAdvanceRollout reads both stable Services before persisting one
// planner transition. Candidate and route evidence must come from separately
// qualified adapters. This function never changes a Service selector.
func (r *OperationReconciler) observeAndAdvanceRollout(ctx context.Context, instance *v1alpha1.OpenSearchServerless, operation *v1alpha1.OpenSearchServerlessOperation, candidates map[string]*rollout.CandidateEvidence, routes map[string]*rollout.RouteEvidence, maxAge time.Duration) (rollout.FleetAction, error) {
	snapshots, err := r.observeRolloutServices(ctx, instance, operation, candidates, routes, maxAge)
	if err != nil {
		return rollout.FleetAction{}, err
	}
	return advanceRolloutProgress(ctx, r.Client, instance, operation, snapshots)
}

func (r *OperationReconciler) observeRolloutServices(ctx context.Context, instance *v1alpha1.OpenSearchServerless, operation *v1alpha1.OpenSearchServerlessOperation, candidates map[string]*rollout.CandidateEvidence, routes map[string]*rollout.RouteEvidence, maxAge time.Duration) (map[string]rollout.SwitchSnapshot, error) {
	roles, err := rolloutProgress(instance, operation)
	if err != nil {
		return nil, err
	}
	snapshots := make(map[string]rollout.SwitchSnapshot, len(roles))
	for _, role := range roles {
		snapshot, err := r.observeRoleSwitch(ctx, instance, operation, role.Intent, candidates[role.Intent.Role], routes[role.Intent.Role], maxAge)
		if err != nil {
			return nil, err
		}
		snapshots[role.Intent.Role] = snapshot
	}
	return snapshots, nil
}

type rolloutEvidence struct {
	Candidates map[string]*rollout.CandidateEvidence
	Routes     map[string]*rollout.RouteEvidence
	revalidate func(context.Context, *rolloutEvidence)
}

type rolloutEvidenceReader func(context.Context, *v1alpha1.OpenSearchServerless, *v1alpha1.OpenSearchServerlessOperation) (rolloutEvidence, error)

// executePreparedRolloutStep advances at most one transition. The caller must
// serialize rollout writers and supply an uncached reader and qualified
// evidence. Instance, operation and Service writes are separate transactions.
func (r *OperationReconciler) executePreparedRolloutStep(ctx context.Context, reader client.Reader, key types.NamespacedName, operationUID types.UID, readEvidence rolloutEvidenceReader, maxAge time.Duration) (rollout.FleetAction, error) {
	if reader == nil || r.Client == nil || operationUID == "" || readEvidence == nil || maxAge <= 0 {
		return rollout.FleetAction{}, fmt.Errorf("live reader, status client, operation UID and fresh evidence are required")
	}
	var operation v1alpha1.OpenSearchServerlessOperation
	if err := reader.Get(ctx, key, &operation); err != nil {
		return rollout.FleetAction{}, err
	}
	if operation.UID != operationUID {
		return rollout.FleetAction{Switch: rollout.SwitchAction{Kind: rollout.Hold, Reason: "OperationReplaced"}}, nil
	}
	instanceKey := types.NamespacedName{Namespace: key.Namespace, Name: operation.Spec.InstanceRef.Name}
	var instance v1alpha1.OpenSearchServerless
	if err := reader.Get(ctx, instanceKey, &instance); err != nil {
		return rollout.FleetAction{}, err
	}
	if _, err := rolloutProgress(&instance, &operation); err != nil {
		return rollout.FleetAction{}, err
	}
	if rolloutAlreadyCommitted(&instance) {
		return rollout.FleetAction{Complete: true}, nil
	}
	evidence, err := readEvidence(ctx, instance.DeepCopyObject().(*v1alpha1.OpenSearchServerless), operation.DeepCopyObject().(*v1alpha1.OpenSearchServerlessOperation))
	if err != nil {
		return rollout.FleetAction{}, err
	}
	snapshots, err := r.observeRolloutServices(ctx, &instance, &operation, evidence.Candidates, evidence.Routes, maxAge)
	if err != nil {
		return rollout.FleetAction{}, err
	}
	// Verify target protection before the final state and evidence age checks.
	// A budget failure does not prevent recording an already applied switch.
	protected, budgetErr := r.observeRolloutBudgets(ctx, &instance, operation.Spec.TargetFleet)
	var unqualifiedSource, sourceHoldReason string
	var sourcePods map[string][]corev1.Pod
	if operation.Spec.Action == "Promote" {
		for role, state := range operation.Status.RolloutRoles {
			if state.Phase == string(rollout.Prepared) && snapshots[role].Service.Fleet == state.SourceFleet {
				sourcePods, unqualifiedSource, sourceHoldReason = r.qualifyPromotionSource(ctx, &instance, &operation)
				break
			}
		}
	}
	if evidence.revalidate != nil {
		evidence.revalidate(ctx, &evidence)
		latest, err := r.observeRolloutServices(ctx, &instance, &operation, evidence.Candidates, evidence.Routes, maxAge)
		if err != nil {
			return rollout.FleetAction{}, err
		}
		for _, role := range []string{"search", "ingest"} {
			previous, current := snapshots[role].Service, latest[role].Service
			if previous.ServiceUID != current.ServiceUID || previous.ResourceVersion != current.ResourceVersion {
				return rollout.FleetAction{Role: role, Switch: rollout.SwitchAction{Kind: rollout.Hold, Reason: "RolloutStateChanged"}}, nil
			}
		}
		snapshots = latest
	}
	if len(sourcePods) != 0 && unqualifiedSource == "" {
		for _, role := range []string{"search", "ingest"} {
			fleet := operation.Status.RolloutRoles[role].SourceFleet
			if len(sourcePods[role]) == 0 || !fleetPodsUnchanged(ctx, r.Core.Pods(instance.Namespace), string(instance.UID), role, fleet, sourcePods[role]) {
				unqualifiedSource, sourceHoldReason = role, "SourceRevisionUnqualified"
				break
			}
		}
	}
	var currentInstance v1alpha1.OpenSearchServerless
	if err := reader.Get(ctx, instanceKey, &currentInstance); err != nil {
		return rollout.FleetAction{}, err
	}
	var currentOperation v1alpha1.OpenSearchServerlessOperation
	if err := reader.Get(ctx, key, &currentOperation); err != nil {
		return rollout.FleetAction{}, err
	}
	if instance.UID != currentInstance.UID || instance.ResourceVersion != currentInstance.ResourceVersion || operation.UID != currentOperation.UID || operation.ResourceVersion != currentOperation.ResourceVersion {
		return rollout.FleetAction{Switch: rollout.SwitchAction{Kind: rollout.Hold, Reason: "RolloutStateChanged"}}, nil
	}
	stampSnapshots := func() time.Time {
		now := time.Now()
		if r.Now != nil {
			now = r.Now()
		}
		for role, snapshot := range snapshots {
			snapshot.Now = now
			snapshots[role] = snapshot
		}
		return now
	}
	now := stampSnapshots()
	action, err := advanceRolloutProgress(ctx, r.Client, &currentInstance, &currentOperation, snapshots)
	if err != nil {
		return action, err
	}
	if currentOperation.Spec.Action == "Promote" && (action.Complete || action.Switch.Kind == rollout.PatchService) {
		state := currentPromotionCandidate(&currentInstance, currentOperation.Spec.TargetFleet)
		if state == nil || state.Revision != currentOperation.Status.RolloutRoles["search"].CandidateRevision ||
			state.Revision != currentOperation.Status.RolloutRoles["ingest"].CandidateRevision {
			return rollout.FleetAction{Switch: rollout.SwitchAction{Kind: rollout.Hold, Reason: "PromotionCandidateChanged"}}, nil
		}
	}
	if action.Switch.Kind == rollout.PatchService && unqualifiedSource != "" {
		return rollout.FleetAction{Role: unqualifiedSource, Switch: rollout.SwitchAction{Kind: rollout.Hold, Reason: sourceHoldReason}}, nil
	}
	if action.Complete || action.Switch.Kind == rollout.PatchService {
		if budgetErr != nil {
			return rollout.FleetAction{}, budgetErr
		}
		if !protected {
			return rollout.FleetAction{Role: action.Role, Switch: rollout.SwitchAction{Kind: rollout.Hold, Reason: "TargetDisruptionBudgetsUnqualified"}}, nil
		}
	}
	switch {
	case action.Complete:
		// The claim stays in place until operation finalization and retained
		// fleet handling can take over. Commit the role mapping and marker in
		// one status write so a restart can distinguish an applied commit.
		updated := currentInstance.DeepCopyObject().(*v1alpha1.OpenSearchServerless)
		markRolloutCandidateServed(updated, &currentOperation)
		switch currentOperation.Spec.Action {
		case "Promote":
			target, err := committedRollbackTarget(&currentInstance, &currentOperation, now)
			if err != nil {
				return rollout.FleetAction{}, err
			}
			updated.Status.RollbackTarget = target
		case "Rollback":
			if target := updated.Status.RollbackTarget; target != nil && target.PromotionUID == currentOperation.Spec.SupersedesOperationUID {
				updated.Status.RollbackTarget = nil
			}
		}
		for _, role := range []string{"search", "ingest"} {
			updated.Status.ServingFleetIntentByRole[role] = currentOperation.Spec.TargetFleet
			pinServingRevision(updated, role, currentOperation.Spec.TargetFleet, currentOperation.Status.RolloutRoles[role].CandidateRevision)
		}
		claim := *updated.Status.ActiveOperation
		updated.Status.LastRolloutCommit = &claim
		if err := r.Status().Patch(ctx, updated, client.MergeFromWithOptions(&currentInstance, client.MergeFromWithOptimisticLock{})); err != nil {
			return rollout.FleetAction{}, fmt.Errorf("commit serving fleet mapping: %w", err)
		}
	case action.Switch.Kind == rollout.PatchService:
		if currentOperation.Spec.Action == "Promote" && len(sourcePods) == 0 {
			return rollout.FleetAction{Role: action.Role, Switch: rollout.SwitchAction{Kind: rollout.Hold, Reason: "SourceRevisionUnqualified"}}, nil
		}
		var latest rollout.FleetAction
		guard := func() error {
			stampSnapshots()
			var err error
			latest, err = planRolloutProgress(&currentInstance, &currentOperation, snapshots)
			if err == nil && latest != action {
				return errRolloutPlanChanged
			}
			return err
		}
		if err := r.applyPlannedRoleSwitch(ctx, &currentInstance, action.Switch, guard); errors.Is(err, errRolloutPlanChanged) {
			if latest.Switch.Kind == rollout.Hold {
				return latest, nil
			}
			return rollout.FleetAction{Role: action.Role, Switch: rollout.SwitchAction{Kind: rollout.Hold, Reason: "RolloutStateChanged"}}, nil
		} else if err != nil {
			return rollout.FleetAction{}, err
		}
	}
	return action, nil
}
