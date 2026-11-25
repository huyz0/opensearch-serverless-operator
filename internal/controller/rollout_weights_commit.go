package controller

import (
	"context"
	"fmt"
	"maps"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/huyz0/opensearch-serverless-operator/api/v1alpha1"
	"github.com/huyz0/opensearch-serverless-operator/internal/rollout"
)

// finalizeWeightedRollout synchronizes stable Services after both Gateway
// routes converge. The commit marker and operation success precede claim release.
func (r *OperationReconciler) finalizeWeightedRollout(ctx context.Context, reader client.Reader, key types.NamespacedName, uid types.UID, domain string, evidence weightStepEvidence) (bool, error) {
	var op v1alpha1.OpenSearchServerlessOperation
	if err := reader.Get(ctx, key, &op); err != nil {
		return false, err
	}
	if uid == "" || uid != op.UID || (op.Spec.Action != "Promote" && op.Spec.Action != "Rollback") {
		return false, fmt.Errorf("weighted rollout identity changed")
	}
	var instance v1alpha1.OpenSearchServerless
	if err := reader.Get(ctx, client.ObjectKey{Namespace: key.Namespace, Name: op.Spec.InstanceRef.Name}, &instance); err != nil {
		return false, err
	}
	pinned := weightedCommittedOperation(&instance, &op)
	claim := operationClaimFor(pinned)
	if instance.Status.ActiveOperation == nil && instance.Status.LastRolloutCommit != nil &&
		*instance.Status.LastRolloutCommit == claim && string(instance.UID) == op.Spec.InstanceRef.UID && op.Status.Phase == "Succeeded" {
		return true, nil
	}
	state := op.Status.WeightedRollout
	progress, err := r.observeWeightedProgress(ctx, reader, &instance, pinned, state)
	if err != nil {
		return false, err
	}
	if progress.active.Role != "" || instance.Status.PendingPodCreation != nil || instance.Status.PendingGateRelease != nil ||
		instance.Status.ProvisioningAttempt != nil || instance.Status.SearchWarmupSlot != nil {
		return false, fmt.Errorf("completed route journal and settled Pod lifecycle are required")
	}
	committed := rolloutAlreadyCommitted(&instance)
	promoting := op.Spec.Action == "Promote"
	if !committed && (evidence == nil || (promoting && currentPromotionCandidate(&instance, op.Spec.TargetFleet) == nil)) {
		return false, fmt.Errorf("weighted evidence and any promotion candidate must be current before committing")
	}
	if !promoting && committed && instance.Status.RollbackTarget != nil {
		return false, fmt.Errorf("committed weighted rollback still has a rollback target")
	}
	if !promoting && !committed && instance.Status.RollbackTarget != nil &&
		(!instance.Status.RollbackTarget.Weighted || instance.Status.RollbackTarget.PromotionUID != op.Spec.SupersedesOperationUID) {
		return false, fmt.Errorf("weighted rollback target changed")
	}
	revisions, err := podRevisions(&instance, domain)
	if err != nil {
		return false, err
	}
	target := &v1alpha1.RollbackTarget{
		Weighted: true, PromotionUID: string(op.UID), Roles: make(map[string]v1alpha1.RollbackRoleTarget, len(weightedRoleOrder)),
	}
	services := make(map[string]rollout.ServiceObservation, len(weightedRoleOrder))
	lastSteps := make(map[string]rollout.WeightIntent, len(weightedRoleOrder))
	snapshots := make(map[string]weightedRolloutEvidence, len(weightedRoleOrder))
	var changeRole string
	for _, role := range weightedRoleOrder {
		part := state.Roles[role]
		source, destination := op.Spec.ExpectedServingFleetByRole[role], op.Spec.TargetFleet
		expected := source
		if committed {
			expected = destination
		}
		weights := progress.routes[role].Weights
		if instance.Status.ServingFleetIntentByRole[role] != expected || !revisions.forFleetTemplate(destination).accepts(part.Revisions[destination]) ||
			len(weights) != 2 || weights[destination] != 100 || part.Revisions[source] == "" ||
			(promoting && (source == destination || len(part.Targets) < 2 ||
				!maps.Equal(part.InitialWeights, map[string]int{source: 100, destination: 0}))) ||
			(len(part.Targets) != 0 && (part.LastAppliedAt == nil || part.LastAppliedAt.IsZero())) {
			return false, fmt.Errorf("%s weighted journal does not match the serving fleet transition", role)
		}
		target.Roles[role] = v1alpha1.RollbackRoleTarget{
			Fleet: source, Revision: part.Revisions[source], ServiceUID: part.StableServiceUID,
			RouteName: part.RouteName, RouteUID: part.RouteUID, RouteSpecDigest: part.RouteSpecDigest,
			RetainedBackendUID: part.BackendUIDs[source], ServingBackendUID: part.BackendUIDs[destination],
			ServingRevision: part.Revisions[destination],
		}
		probe, err := r.observeRoleSwitch(ctx, &instance, &op, rollout.SwitchIntent{Role: role, SourceFleet: source, TargetFleet: destination}, nil, nil, rolloutObservationAge)
		if err != nil {
			return false, err
		}
		service := probe.Service
		if !service.Owned || !service.RoutingShapeValid || service.ServiceUID == "" || service.ServiceUID != part.StableServiceUID ||
			service.ResourceVersion == "" || (service.Fleet != source && service.Fleet != destination) || (committed && service.Fleet != destination) {
			return false, fmt.Errorf("%s stable Service changed outside weighted promotion", role)
		}
		services[role] = service
		if changeRole == "" && service.Fleet != destination {
			changeRole = role
		}
		if committed && promoting {
			if retained := instance.Status.RollbackTarget; retained == nil || !retained.Weighted ||
				retained.PromotionUID != target.PromotionUID || retained.Roles[role] != target.Roles[role] {
				return false, fmt.Errorf("%s committed weighted rollback target changed", role)
			}
			continue
		}
		if committed {
			continue
		}
		intent := rollout.WeightIntent{
			InstanceUID: string(instance.UID), OperationUID: string(op.UID), Role: role, RouteUID: part.RouteUID,
			From: part.InitialWeights, To: weights, Revisions: part.Revisions, AdmissionWeight: state.AdmissionWeight, Phase: rollout.Applied,
		}
		if n := len(part.Targets); n > 0 {
			intent.AppliedAt = part.LastAppliedAt.Time
			if n > 1 {
				intent.From = part.Targets[n-2].Weights
			}
		} else {
			// The rollback already owns this role's desired route. Require
			// an observation scoped to this rollback before committing it.
			intent.AdoptExisting, intent.AppliedAt = true, op.CreationTimestamp.Time
		}
		snapshot, err := evidence(ctx, &instance, &op, intent, progress.routes[role])
		if err != nil {
			return false, err
		}
		lastSteps[role], snapshots[role] = intent, snapshot
	}
	// The stable Service selectors can move one at a time. Once both have
	// moved, also require their EndpointSlices to match the qualified Pods.
	endpoints := make(map[string]roleEndpointObservation, len(weightedRoleOrder))
	if !committed && changeRole == "" {
		for _, role := range weightedRoleOrder {
			allowed := make(map[types.UID]struct{})
			for podUID := range snapshots[role].Qualifications[op.Spec.TargetFleet].Processes {
				allowed[types.UID(podUID)] = struct{}{}
			}
			ready, err := observeRoleEndpoints(ctx, r.Core, r.Discovery, &instance, role, op.Spec.TargetFleet,
				types.UID(services[role].ServiceUID), servingZoneMinima(&instance, role), allowed)
			if err != nil {
				return false, err
			}
			if len(ready.ReadyPodVersions) < servingRegionalFloor(&instance, role) {
				return false, fmt.Errorf("%s stable Service is below its regional floor", role)
			}
			endpoints[role] = ready
		}
	}
	authorize := func() error {
		if !committed {
			for _, role := range weightedRoleOrder {
				snapshot := snapshots[role]
				if !snapshot.RouteAuthIdentity.Unchanged(ctx, r.Core, instance.Namespace) {
					return fmt.Errorf("%s route credential changed while finalizing weighted rollout", role)
				}
				if snapshot.unchanged != nil && !snapshot.unchanged(ctx) {
					return fmt.Errorf("%s weighted observations changed while finalizing rollout", role)
				}
			}
		}
		for _, role := range weightedRoleOrder {
			observed, err := r.observeRoleSwitch(ctx, &instance, &op, rollout.SwitchIntent{
				Role: role, SourceFleet: op.Spec.ExpectedServingFleetByRole[role], TargetFleet: op.Spec.TargetFleet,
			}, nil, nil, rolloutObservationAge)
			if err != nil {
				return err
			}
			if observed.Service.ServiceUID != services[role].ServiceUID ||
				observed.Service.ResourceVersion != services[role].ResourceVersion || observed.Service.Fleet != services[role].Fleet {
				return fmt.Errorf("%s stable Service changed while finalizing weighted rollout", role)
			}
			if ready, ok := endpoints[role]; ok {
				if ready.ServiceVersion != services[role].ResourceVersion {
					return fmt.Errorf("%s stable Service changed during endpoint observation", role)
				}
				if err := recheckEndpointSlices(ctx, r.Discovery.EndpointSlices(instance.Namespace), ready.ServiceName, ready.SliceVersions); err != nil {
					return err
				}
			}
		}
		current, err := r.recheckWeightedProgress(ctx, reader, &instance, &op, progress)
		if err != nil {
			return err
		}
		if err := verifyWeightedClaim(ctx, reader, &instance, &op); err != nil {
			return err
		}
		if committed {
			return nil
		}
		now := r.weightNow()
		for _, role := range weightedRoleOrder {
			intent := lastSteps[role]
			snapshot := snapshots[role]
			snapshot.Now, snapshot.Route = now, current.routes[role].WeightRouteObservation
			snapshot.ClaimInstanceUID, snapshot.ClaimOperationUID = intent.InstanceUID, intent.OperationUID
			action, err := rollout.PlanWeightStep(intent, snapshot.WeightSnapshot)
			if err != nil {
				return err
			}
			if action.Kind != rollout.RecordConverged {
				return fmt.Errorf("%s weighted convergence evidence changed: %s", role, action.Reason)
			}
		}
		return nil
	}
	if committed {
		if err := authorize(); err != nil {
			return false, err
		}
	}
	switch {
	case changeRole != "":
		service := services[changeRole]
		return false, switchRoleServiceConditional(ctx, r.Core, &instance, changeRole, types.UID(service.ServiceUID),
			service.ResourceVersion, service.Fleet, op.Spec.TargetFleet, authorize)
	case !committed:
		if err := authorize(); err != nil {
			return false, err
		}
		if promoting {
			if err := setRollbackDeadline(&instance, target, r.weightNow()); err != nil {
				return false, err
			}
		} else {
			target = nil
		}
		before := instance.DeepCopyObject().(*v1alpha1.OpenSearchServerless)
		markRolloutCandidateServed(&instance, &op)
		for _, role := range weightedRoleOrder {
			instance.Status.ServingFleetIntentByRole[role] = op.Spec.TargetFleet
			pinServingRevision(&instance, role, op.Spec.TargetFleet, state.Roles[role].Revisions[op.Spec.TargetFleet])
		}
		instance.Status.LastRolloutCommit, instance.Status.RollbackTarget = &claim, target
		return false, r.Status().Patch(ctx, &instance, client.MergeFromWithOptions(before, client.MergeFromWithOptimisticLock{}))
	case op.Status.Phase != "Succeeded":
		before := op.DeepCopyObject().(*v1alpha1.OpenSearchServerlessOperation)
		op.Status.Phase, op.Status.ObservedGeneration = "Succeeded", op.Generation
		meta.SetStatusCondition(&op.Status.Conditions, metav1.Condition{
			Type: "Ready", Status: metav1.ConditionTrue, Reason: "ServingFleetCommitted",
			Message: "Both weighted routes converged and the serving fleet was committed", ObservedGeneration: op.Generation,
		})
		return false, r.Status().Patch(ctx, &op, client.MergeFromWithOptions(before, client.MergeFromWithOptimisticLock{}))
	default:
		before := instance.DeepCopyObject().(*v1alpha1.OpenSearchServerless)
		instance.Status.ActiveOperation = nil
		err := r.Status().Patch(ctx, &instance, client.MergeFromWithOptions(before, client.MergeFromWithOptimisticLock{}))
		return err == nil, err
	}
}
