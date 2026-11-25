package controller

import (
	"context"
	"fmt"
	"maps"
	"reflect"

	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/huyz0/opensearch-serverless-operator/api/v1alpha1"
	"github.com/huyz0/opensearch-serverless-operator/internal/rollout"
)

// prepareWeightedRollbackHandoff observes the predecessor before persisting
// recovery intent. Only a later pass can move the instance claim.
func (r *OperationReconciler) prepareWeightedRollbackHandoff(ctx context.Context, reader client.Reader, key types.NamespacedName, uid types.UID, approvedRoutes map[string]string) (bool, error) {
	var op v1alpha1.OpenSearchServerlessOperation
	if err := reader.Get(ctx, key, &op); err != nil {
		return false, err
	}
	if uid == "" || op.UID != uid || op.Spec.Action != "Rollback" || op.Spec.SupersedesOperationUID == "" || op.Status.RolloutRoles != nil {
		return false, fmt.Errorf("weighted rollback requires an explicit predecessor")
	}
	if err := op.Spec.Validate(); err != nil {
		return false, err
	}
	var instance v1alpha1.OpenSearchServerless
	if err := reader.Get(ctx, client.ObjectKey{Namespace: key.Namespace, Name: op.Spec.InstanceRef.Name}, &instance); err != nil {
		return false, err
	}
	if instance.Status.FleetRetirement != nil {
		return false, fmt.Errorf("fleet retirement is reserved")
	}
	claim := operationClaimFor(&op)
	active := instance.Status.ActiveOperation
	if active != nil && *active == claim {
		_, err := r.observeWeightedProgress(ctx, reader, &instance, &op, op.Status.WeightedRollout)
		return err == nil, err
	}
	if instance.Status.PendingPodCreation != nil || instance.Status.PendingGateRelease != nil ||
		instance.Status.ProvisioningAttempt != nil || instance.Status.SearchWarmupSlot != nil {
		return false, fmt.Errorf("Pod lifecycle must settle before weighted rollback handoff")
	}
	projected := instance.DeepCopyObject().(*v1alpha1.OpenSearchServerless)
	projected.Status.ActiveOperation = &claim
	if !weightedClaimMatches(projected, &op) {
		return false, fmt.Errorf("weighted rollback does not match the live instance")
	}
	var predecessor *v1alpha1.OpenSearchServerlessOperation
	var predecessorRoutes map[string]observedWeightRoute
	pins := make(map[string]weightedRollbackRole, len(weightedRoleOrder))
	if active != nil {
		if active.Action != "Promote" || active.UID != op.Spec.SupersedesOperationUID {
			return false, fmt.Errorf("weighted rollback does not supersede the active promotion")
		}
		predecessor = &v1alpha1.OpenSearchServerlessOperation{}
		if err := reader.Get(ctx, client.ObjectKey{Namespace: key.Namespace, Name: active.Name}, predecessor); err != nil {
			return false, err
		}
		pinned := predecessor.DeepCopyObject().(*v1alpha1.OpenSearchServerlessOperation)
		if pinned.DeletionTimestamp != nil && pinned.Generation >= active.Generation {
			pinned.DeletionTimestamp, pinned.Generation = nil, active.Generation
		}
		progress, err := r.observeWeightedProgress(ctx, reader, &instance, pinned, pinned.Status.WeightedRollout)
		if err != nil {
			return false, err
		}
		predecessorRoutes = progress.routes
		for _, role := range weightedRoleOrder {
			part := pinned.Status.WeightedRollout.Roles[role]
			oldFleet, newFleet := pinned.Spec.ExpectedServingFleetByRole[role], pinned.Spec.TargetFleet
			pins[role] = weightedRollbackRole{incumbent: newFleet, target: v1alpha1.RollbackRoleTarget{
				Fleet: oldFleet, Revision: part.Revisions[oldFleet], ServingRevision: part.Revisions[newFleet],
				ServiceUID: part.StableServiceUID, RouteName: part.RouteName, RouteUID: part.RouteUID, RouteSpecDigest: part.RouteSpecDigest,
				RetainedBackendUID: part.BackendUIDs[oldFleet], ServingBackendUID: part.BackendUIDs[newFleet],
			}}
		}
	}
	last := instance.Status.LastRolloutCommit
	if active == nil || rolloutAlreadyCommitted(&instance) {
		target := instance.Status.RollbackTarget
		if last == nil || last.Action != "Promote" || last.UID != op.Spec.SupersedesOperationUID ||
			!r.rollbackTargetEligible(target, last.UID) || !target.Weighted {
			return false, fmt.Errorf("no eligible committed weighted rollback target")
		}
		for _, role := range weightedRoleOrder {
			retained := weightedRollbackRole{incumbent: instance.Status.ServingFleetIntentByRole[role], target: target.Roles[role]}
			if active != nil && pins[role] != retained {
				return false, fmt.Errorf("%s retained weighted route differs from the active journal", role)
			}
			pins[role] = retained
		}
	}
	journal := &v1alpha1.WeightedRolloutStatus{
		InstanceGeneration: instance.Generation, AdmissionWeight: weightedAdmission,
		Roles: make(map[string]v1alpha1.WeightedRoleStatus, len(weightedRoleOrder)),
	}
	routeVersions := make(map[string]string, len(weightedRoleOrder))
	stableVersions := make(map[string]string, len(weightedRoleOrder))
	for _, role := range weightedRoleOrder {
		pin := pins[role]
		old, next := pin.incumbent, pin.target.Fleet
		if old == "" || next != op.Spec.TargetFleet || old == next || pin.target.Revision == "" || pin.target.ServingRevision == "" {
			return false, fmt.Errorf("%s rollback target does not match the retained fleet", role)
		}
		part := v1alpha1.WeightedRoleStatus{
			StableServiceUID: pin.target.ServiceUID, RouteName: pin.target.RouteName, RouteUID: pin.target.RouteUID,
			RouteSpecDigest: pin.target.RouteSpecDigest,
			BackendUIDs:     map[string]string{old: pin.target.ServingBackendUID, next: pin.target.RetainedBackendUID},
			Revisions:       map[string]string{old: pin.target.ServingRevision, next: pin.target.Revision},
		}
		if part.RouteName == "" || part.RouteName != approvedRoutes[role] {
			return false, fmt.Errorf("%s rollback route is outside the configured scope", role)
		}
		stable, err := r.observeRoleSwitch(ctx, &instance, &op, rollout.SwitchIntent{Role: role, SourceFleet: old, TargetFleet: next}, nil, nil, rolloutObservationAge)
		if err != nil {
			return false, err
		}
		if service := stable.Service; !service.Owned || !service.RoutingShapeValid || service.ServiceUID == "" ||
			service.ServiceUID != part.StableServiceUID || service.ResourceVersion == "" || part.Revisions[service.Fleet] == "" ||
			service.Fleet != op.Spec.ExpectedServingFleetByRole[role] || (active == nil && service.Fleet != old) {
			return false, fmt.Errorf("%s stable Service differs from the authorized rollback baseline", role)
		}
		projected.Status.ServingFleetIntentByRole[role] = stable.Service.Fleet
		pinServingRevision(projected, role, stable.Service.Fleet, part.Revisions[stable.Service.Fleet])
		stableVersions[role] = stable.Service.ResourceVersion
		binding := weightRouteBinding{
			Key: client.ObjectKey{Namespace: instance.Namespace, Name: part.RouteName}, UID: types.UID(part.RouteUID),
			InstanceUID: string(instance.UID), Role: role, SpecWithoutWeights: part.RouteSpecDigest,
			ServiceNames: make(map[string]string, 2), ServicePort: 9200,
		}
		for fleet, uid := range part.BackendUIDs {
			backend := instance.Status.GatewayBackends[role+"/"+fleet]
			if uid == "" || backend.ServiceUID != uid {
				return false, fmt.Errorf("%s rollback backend for %q changed", role, fleet)
			}
			binding.ServiceNames[fleet] = backend.Name
		}
		route, err := readWeightRoute(ctx, reader, binding, r.weightNow)
		if err != nil {
			return false, err
		}
		if predecessor != nil && predecessorRoutes[role].ResourceVersion != route.ResourceVersion {
			return false, fmt.Errorf("%s route changed during weighted rollback observation", role)
		}
		if active == nil && !maps.Equal(route.Weights, map[string]int{old: 100, next: 0}) {
			return false, fmt.Errorf("%s route differs from the committed weighted baseline", role)
		}
		routeVersions[role] = route.ResourceVersion
		part.InitialWeights = route.Weights
		if route.Weights[next] == 0 {
			part.Targets = append(part.Targets, v1alpha1.WeightedTarget{Weights: map[string]int{old: 100 - weightedAdmission, next: weightedAdmission}})
		}
		if route.Weights[next] < 100 {
			part.Targets = append(part.Targets, v1alpha1.WeightedTarget{Weights: map[string]int{old: 0, next: 100}})
		}
		journal.Roles[role] = part
	}
	progress, err := r.observeWeightedProgress(ctx, reader, projected, &op, journal)
	if err != nil {
		return false, err
	}
	for _, role := range weightedRoleOrder {
		if progress.routes[role].ResourceVersion != routeVersions[role] {
			return false, fmt.Errorf("%s route changed before weighted rollback handoff", role)
		}
		stable, err := r.observeRoleSwitch(ctx, &instance, &op, rollout.SwitchIntent{
			Role: role, SourceFleet: pins[role].incumbent, TargetFleet: op.Spec.TargetFleet,
		}, nil, nil, rolloutObservationAge)
		if err != nil {
			return false, err
		}
		if stable.Service.ResourceVersion != stableVersions[role] ||
			stable.Service.ServiceUID != journal.Roles[role].StableServiceUID {
			return false, fmt.Errorf("%s stable Service changed before weighted rollback handoff", role)
		}
	}
	if before := op.Status.WeightedRollout; before != nil {
		for _, part := range before.Roles {
			if part.NextStep != 0 || part.AppliedAt != nil || part.LastAppliedAt != nil {
				return false, fmt.Errorf("weighted rollback journal already advanced without this claim")
			}
		}
	}
	if !reflect.DeepEqual(op.Status.WeightedRollout, journal) {
		before := op.DeepCopyObject().(*v1alpha1.OpenSearchServerlessOperation)
		op.Status.WeightedRollout = journal
		return false, r.Status().Patch(ctx, &op, client.MergeFromWithOptions(before, client.MergeFromWithOptimisticLock{}))
	}
	if err := r.verifyRollbackHandoff(ctx, reader, &instance, &op, predecessor); err != nil {
		return false, err
	}
	if err := r.Status().Patch(ctx, projected, client.MergeFromWithOptions(&instance, client.MergeFromWithOptimisticLock{})); err != nil {
		return false, fmt.Errorf("transfer weighted rollout claim: %w", err)
	}
	return true, nil
}

type weightedRollbackRole struct {
	incumbent string
	target    v1alpha1.RollbackRoleTarget
}
