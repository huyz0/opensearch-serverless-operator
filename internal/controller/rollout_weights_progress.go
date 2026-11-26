package controller

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	"github.com/huyz0/opensearch-serverless-operator/api/v1alpha1"
	"github.com/huyz0/opensearch-serverless-operator/internal/rollout"
)

var weightedRoleOrder = [...]string{"search", "ingest"}

type weightStepEvidence func(context.Context, *v1alpha1.OpenSearchServerless, *v1alpha1.OpenSearchServerlessOperation, rollout.WeightIntent, observedWeightRoute) (weightedRolloutEvidence, error)

type weightedRolloutEvidence struct {
	rollout.WeightSnapshot
	proxyEvidenceMissing bool
	unchanged            func(context.Context) bool
}

type weightedProgress struct {
	active   rollout.WeightIntent
	bindings map[string]weightRouteBinding
	routes   map[string]observedWeightRoute
}

func weightedClaimMatches(instance *v1alpha1.OpenSearchServerless, operation *v1alpha1.OpenSearchServerlessOperation) bool {
	claim := operationClaimFor(operation)
	return instance.UID != "" && operation.UID != "" && instance.ResourceVersion != "" && operation.ResourceVersion != "" &&
		instance.Namespace != "" && instance.Namespace == operation.Namespace && instance.DeletionTimestamp == nil &&
		operation.DeletionTimestamp == nil && instance.Name == operation.Spec.InstanceRef.Name &&
		string(instance.UID) == operation.Spec.InstanceRef.UID && claim.Generation > 0 &&
		controllerutil.ContainsFinalizer(operation, rolloutOperationFinalizer) &&
		instance.Spec.Rollout != nil && instance.Spec.Rollout.GatewayBackends &&
		instance.Status.ActiveOperation != nil && *instance.Status.ActiveOperation == claim
}

func (r *OperationReconciler) observeWeightedProgress(ctx context.Context, reader client.Reader, instance *v1alpha1.OpenSearchServerless, op *v1alpha1.OpenSearchServerlessOperation, state *v1alpha1.WeightedRolloutStatus) (weightedProgress, error) {
	if r.Core == nil || !weightedClaimMatches(instance, op) || state == nil ||
		state.InstanceGeneration != instance.Generation || len(state.Roles) != len(weightedRoleOrder) ||
		op.Status.RolloutRoles != nil || (op.Spec.Action != "Promote" && op.Spec.Action != "Rollback") {
		return weightedProgress{}, fmt.Errorf("weighted journal requires its original instance generation and claim")
	}
	if err := op.Spec.Validate(); err != nil {
		return weightedProgress{}, err
	}
	progress := weightedProgress{
		bindings: make(map[string]weightRouteBinding, len(weightedRoleOrder)),
		routes:   make(map[string]observedWeightRoute, len(weightedRoleOrder)),
	}
	names := make(map[string]bool, len(weightedRoleOrder))
	routeUIDs := make(map[string]bool, len(weightedRoleOrder))
	steps := 0
	for _, role := range weightedRoleOrder {
		part, found := state.Roles[role]
		if !found || names[part.RouteName] || routeUIDs[part.RouteUID] || part.NextStep < 0 || part.NextStep > len(part.Targets) ||
			len(part.Targets) > 32 || (part.NextStep == len(part.Targets) && part.AppliedAt != nil) ||
			(progress.active.Role != "" && (part.NextStep != 0 || part.AppliedAt != nil)) ||
			len(part.BackendUIDs) != len(part.InitialWeights) {
			return weightedProgress{}, fmt.Errorf("invalid weighted rollout journal for %s", role)
		}
		if op.Spec.Action == "Promote" && op.Spec.RoutingMode == "WeightedGateway" {
			source, target := op.Spec.ExpectedServingFleetByRole[role], op.Spec.TargetFleet
			expected := weightedPromotionTargets(source, target, op.Spec.WeightedTraffic.ForRole(role))
			if source == target || state.AdmissionWeight != weightedAdmission ||
				!maps.Equal(part.InitialWeights, map[string]int{source: 100, target: 0}) ||
				!slices.EqualFunc(part.Targets, expected, func(a, b v1alpha1.WeightedTarget) bool { return maps.Equal(a.Weights, b.Weights) }) {
				return weightedProgress{}, fmt.Errorf("%s weighted journal differs from the immutable promotion schedule", role)
			}
		}
		names[part.RouteName], routeUIDs[part.RouteUID] = true, true
		binding := weightRouteBinding{
			Key: client.ObjectKey{Namespace: instance.Namespace, Name: part.RouteName}, UID: types.UID(part.RouteUID),
			InstanceUID: string(instance.UID), Role: role, SpecWithoutWeights: part.RouteSpecDigest,
			ServiceNames: make(map[string]string, len(part.BackendUIDs)), ServicePort: 9200,
		}
		for fleet, uid := range part.BackendUIDs {
			pin := instance.Status.GatewayBackends[role+"/"+fleet]
			if uid == "" || pin.ServiceUID != uid || pin.Role != role || pin.Fleet != fleet {
				return weightedProgress{}, fmt.Errorf("%s backend identity for fleet %q changed", role, fleet)
			}
			if _, exists := part.InitialWeights[fleet]; !exists {
				return weightedProgress{}, fmt.Errorf("initial weights omit fleet %q", fleet)
			}
			if _, err := readGatewayBackend(ctx, r.Core, instance, pin); err != nil {
				return weightedProgress{}, err
			}
			binding.ServiceNames[fleet] = pin.Name
		}
		live, err := readWeightRoute(ctx, reader, binding, r.weightNow)
		if err != nil {
			return weightedProgress{}, err
		}
		progress.bindings[role], progress.routes[role] = binding, live
		steps += len(part.Targets)
		expected := part.InitialWeights
		if part.NextStep > 0 {
			expected = part.Targets[part.NextStep-1].Weights
		}
		previous := part.InitialWeights
		for i, target := range part.Targets {
			intent := rollout.WeightIntent{
				InstanceUID: string(instance.UID), OperationUID: string(op.UID), Role: role, RouteUID: part.RouteUID,
				From: previous, To: target.Weights, Revisions: part.Revisions, AdmissionWeight: state.AdmissionWeight, Phase: rollout.Prepared,
			}
			if err := rollout.ValidateWeightIntent(intent); err != nil {
				return weightedProgress{}, fmt.Errorf("invalid %s weight step %d: %w", role, i, err)
			}
			if i == part.NextStep && progress.active.Role == "" {
				if part.AppliedAt != nil {
					intent.Phase, intent.AppliedAt = rollout.Applied, part.AppliedAt.Time
					expected = target.Weights
				}
				progress.active = intent
			}
			previous = target.Weights
		}
		awaitingReadBack := progress.active.Role == role && progress.active.Phase == rollout.Prepared &&
			maps.Equal(live.Weights, progress.active.To)
		if !maps.Equal(live.Weights, expected) && !awaitingReadBack {
			return weightedProgress{}, fmt.Errorf("%s HTTPRoute weights differ from the journal", role)
		}
	}
	if steps == 0 && op.Spec.Action != "Rollback" {
		return weightedProgress{}, fmt.Errorf("at least one weighted step is required")
	}
	return progress, nil
}

func (r *OperationReconciler) weightNow() time.Time {
	if r.Now != nil {
		return r.Now()
	}
	return time.Now()
}

// prepareWeightedRollout records both role schedules before a route can be
// modified. The caller supplies platform-approved route and revision pins.
func (r *OperationReconciler) prepareWeightedRollout(ctx context.Context, reader client.Reader, instance *v1alpha1.OpenSearchServerless, operation *v1alpha1.OpenSearchServerlessOperation, state *v1alpha1.WeightedRolloutStatus) error {
	if operation.Status.WeightedRollout != nil {
		return fmt.Errorf("weighted rollout journal already exists")
	}
	progress, err := r.observeWeightedProgress(ctx, reader, instance, operation, state)
	if err != nil {
		return err
	}
	for _, role := range weightedRoleOrder {
		part := state.Roles[role]
		if part.NextStep != 0 || part.AppliedAt != nil || part.LastAppliedAt != nil || !maps.Equal(part.InitialWeights, progress.routes[role].Weights) {
			return fmt.Errorf("weighted rollout must start at the recorded baseline for %s", role)
		}
	}
	if err := verifyWeightedClaim(ctx, reader, instance, operation); err != nil {
		return err
	}
	next := operation.DeepCopyObject().(*v1alpha1.OpenSearchServerlessOperation)
	next.Status.WeightedRollout = state
	return r.Status().Patch(ctx, next, client.MergeFromWithOptions(operation, client.MergeFromWithOptimisticLock{}))
}

func verifyWeightedClaim(ctx context.Context, reader client.Reader, instance *v1alpha1.OpenSearchServerless, operation *v1alpha1.OpenSearchServerlessOperation) error {
	var liveInstance v1alpha1.OpenSearchServerless
	if err := reader.Get(ctx, client.ObjectKeyFromObject(instance), &liveInstance); err != nil {
		return err
	}
	var liveOperation v1alpha1.OpenSearchServerlessOperation
	if err := reader.Get(ctx, client.ObjectKeyFromObject(operation), &liveOperation); err != nil {
		return err
	}
	if liveInstance.UID != instance.UID || liveOperation.UID != operation.UID ||
		liveInstance.ResourceVersion != instance.ResourceVersion || liveOperation.ResourceVersion != operation.ResourceVersion ||
		!weightedClaimMatches(&liveInstance, weightedCommittedOperation(&liveInstance, &liveOperation)) {
		return fmt.Errorf("weighted rollout claim or journal changed before advancing")
	}
	return nil
}

// recheckWeightedProgress validates both routes and their pinned backend
// Services after evidence reads. Only the active route has a patch precondition.
func (r *OperationReconciler) recheckWeightedProgress(ctx context.Context, reader client.Reader, instance *v1alpha1.OpenSearchServerless, op *v1alpha1.OpenSearchServerlessOperation, observed weightedProgress) (weightedProgress, error) {
	current, err := r.observeWeightedProgress(ctx, reader, instance, weightedCommittedOperation(instance, op), op.Status.WeightedRollout)
	if err != nil {
		return weightedProgress{}, err
	}
	for _, role := range weightedRoleOrder {
		before, after := observed.routes[role], current.routes[role]
		if before.RouteUID != after.RouteUID || before.ResourceVersion != after.ResourceVersion {
			return weightedProgress{}, fmt.Errorf("%s route changed while qualifying the weighted step", role)
		}
	}
	return current, nil
}

// A deleting operation may finish an already recorded commit. A deleting
// uncommitted operation may not continue changing traffic.
func weightedCommittedOperation(instance *v1alpha1.OpenSearchServerless, operation *v1alpha1.OpenSearchServerlessOperation) *v1alpha1.OpenSearchServerlessOperation {
	if operation.DeletionTimestamp == nil || !rolloutAlreadyCommitted(instance) {
		return operation
	}
	claim := instance.Status.ActiveOperation
	if claim.UID != string(operation.UID) || claim.Name != operation.Name || claim.Action != operation.Spec.Action || operation.Generation < claim.Generation {
		return operation
	}
	pinned := operation.DeepCopyObject().(*v1alpha1.OpenSearchServerlessOperation)
	pinned.DeletionTimestamp, pinned.Generation = nil, claim.Generation
	return pinned
}

// executeWeightedRolloutStep advances one prepared operation effect. An
// ambiguous write is recovered by observing the target weights and recording
// the read-back time before accepting any proxy convergence report.
func (r *OperationReconciler) executeWeightedRolloutStep(ctx context.Context, reader client.Reader, key types.NamespacedName, uid types.UID, evidence weightStepEvidence) (rollout.WeightAction, bool, error) {
	var operation v1alpha1.OpenSearchServerlessOperation
	if err := reader.Get(ctx, key, &operation); err != nil {
		return rollout.WeightAction{}, false, err
	}
	if uid == "" || uid != operation.UID {
		return rollout.WeightAction{}, false, fmt.Errorf("weighted rollout operation was replaced")
	}
	var instance v1alpha1.OpenSearchServerless
	if err := reader.Get(ctx, client.ObjectKey{Namespace: key.Namespace, Name: operation.Spec.InstanceRef.Name}, &instance); err != nil {
		return rollout.WeightAction{}, false, err
	}
	state := operation.Status.WeightedRollout
	progress, err := r.observeWeightedProgress(ctx, reader, &instance, &operation, state)
	if err != nil {
		return rollout.WeightAction{}, false, err
	}
	intent := progress.active
	if intent.Role == "" {
		err := verifyWeightedClaim(ctx, reader, &instance, &operation)
		return rollout.WeightAction{}, err == nil, err
	}
	live := progress.routes[intent.Role]
	var action rollout.WeightAction
	var snapshot weightedRolloutEvidence
	if intent.Phase == rollout.Prepared && maps.Equal(live.Weights, intent.To) {
		action = rollout.WeightAction{Kind: rollout.RecordApplied, Reason: "TargetWeightsObserved", RouteUID: live.RouteUID, ResourceVersion: live.ResourceVersion}
	} else {
		if evidence == nil {
			return rollout.WeightAction{Kind: rollout.Hold, Reason: "WeightedEvidenceUnavailable"}, false, nil
		}
		snapshot, err = evidence(ctx, &instance, &operation, intent, live)
		if err != nil {
			return rollout.WeightAction{}, false, err
		}
		if snapshot.proxyEvidenceMissing {
			return rollout.WeightAction{Kind: rollout.Hold, Reason: "ProxyEvidenceIncomplete"}, false, nil
		}
		snapshot.Now, snapshot.Route = r.weightNow(), live.WeightRouteObservation
		snapshot.ClaimInstanceUID, snapshot.ClaimOperationUID = intent.InstanceUID, intent.OperationUID
		action, err = rollout.PlanWeightStep(intent, snapshot.WeightSnapshot)
		if err != nil || action.Kind == rollout.Hold {
			return action, false, err
		}
	}
	checkRoutesAndClaim := func() error {
		current, err := r.recheckWeightedProgress(ctx, reader, &instance, &operation, progress)
		if err != nil {
			return err
		}
		snapshot.Route = current.routes[intent.Role].WeightRouteObservation
		return verifyWeightedClaim(ctx, reader, &instance, &operation)
	}
	revalidate := func() (rollout.WeightAction, error) {
		if !snapshot.RouteAuthIdentity.Unchanged(ctx, r.Core, instance.Namespace) {
			return rollout.WeightAction{Kind: rollout.Hold, Reason: "RouteCredentialChanged"}, nil
		}
		if snapshot.unchanged != nil && !snapshot.unchanged(ctx) {
			return rollout.WeightAction{Kind: rollout.Hold, Reason: "WeightedEvidenceChanged"}, nil
		}
		if err := checkRoutesAndClaim(); err != nil {
			return rollout.WeightAction{}, err
		}
		snapshot.Now = r.weightNow()
		return rollout.PlanWeightStep(intent, snapshot.WeightSnapshot)
	}
	if action.Kind == rollout.PatchRouteWeights {
		var latest rollout.WeightAction
		authorize := func() error {
			var err error
			latest, err = revalidate()
			if err == nil && latest.Kind != rollout.PatchRouteWeights {
				return errRolloutPlanChanged
			}
			return err
		}
		err := applyWeightRoute(ctx, reader, r.Client, progress.bindings[intent.Role], action, r.weightNow, authorize)
		if errors.Is(err, errRolloutPlanChanged) {
			return latest, false, nil
		}
		return action, false, err
	}
	if action.Kind == rollout.RecordConverged {
		action, err = revalidate()
		if err != nil || action.Kind != rollout.RecordConverged {
			return action, false, err
		}
	}
	next := operation.DeepCopyObject().(*v1alpha1.OpenSearchServerlessOperation)
	part := next.Status.WeightedRollout.Roles[intent.Role]
	switch action.Kind {
	case rollout.RecordApplied:
		if err := checkRoutesAndClaim(); err != nil {
			return rollout.WeightAction{}, false, err
		}
		readBackAt := r.weightNow()
		at := metav1.NewMicroTime(readBackAt.Add(time.Microsecond - 1).Truncate(time.Microsecond))
		part.AppliedAt = &at
	case rollout.RecordConverged:
		part.LastAppliedAt = part.AppliedAt
		part.AppliedAt = nil
		part.NextStep++
	default:
		return rollout.WeightAction{}, false, fmt.Errorf("unsupported weighted rollout action %q", action.Kind)
	}
	next.Status.WeightedRollout.Roles[intent.Role] = part
	return action, false, r.Status().Patch(ctx, next, client.MergeFromWithOptions(&operation, client.MergeFromWithOptimisticLock{}))
}
