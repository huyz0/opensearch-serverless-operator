package controller

import (
	"context"
	"fmt"

	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	"github.com/huyz0/opensearch-serverless-operator/api/v1alpha1"
	"github.com/huyz0/opensearch-serverless-operator/internal/rollout"
)

func (r *OperationReconciler) usesWeightedRouting(ctx context.Context, reader client.Reader, instance *v1alpha1.OpenSearchServerless, op *v1alpha1.OpenSearchServerlessOperation) (bool, error) {
	if op.Spec.RoutingMode == "WeightedGateway" || op.Status.WeightedRollout != nil {
		return true, nil
	}
	if op.Spec.Action != "Rollback" || op.Spec.SupersedesOperationUID == "" {
		return false, nil
	}
	if retained := instance.Status.RollbackTarget; retained != nil && retained.PromotionUID == op.Spec.SupersedesOperationUID {
		return retained.Weighted, nil
	}
	active := instance.Status.ActiveOperation
	if active == nil || active.Action != "Promote" || active.UID != op.Spec.SupersedesOperationUID {
		return false, nil
	}
	var predecessor v1alpha1.OpenSearchServerlessOperation
	if err := reader.Get(ctx, client.ObjectKey{Namespace: instance.Namespace, Name: active.Name}, &predecessor); err != nil {
		return false, err
	}
	if string(predecessor.UID) != active.UID {
		return false, fmt.Errorf("rollback predecessor identity changed")
	}
	return predecessor.Spec.RoutingMode == "WeightedGateway" || predecessor.Status.WeightedRollout != nil, nil
}

func (r *OperationReconciler) reconcileWeightedOperation(ctx context.Context, reader client.Reader, instance *v1alpha1.OpenSearchServerless,
	op *v1alpha1.OpenSearchServerlessOperation, owned bool, retry ctrl.Result, hold func(string, string) (ctrl.Result, error),
) (ctrl.Result, error) {
	active, state := instance.Status.ActiveOperation, op.Status.WeightedRollout
	if op.DeletionTimestamp != nil {
		if active == nil || active.UID != string(op.UID) {
			return r.setRolloutFinalizer(ctx, op, false)
		}
		if state == nil && op.Status.RolloutRoles == nil {
			return r.clearObservedOperationClaim(ctx, instance, string(op.UID))
		}
		if !rolloutAlreadyCommitted(instance) {
			return hold("WeightedRecoveryRequired", "Recover the weighted routes with a rollback before deleting the claimed operation")
		}
	}
	if r.weightedEvidence == nil || instance.Spec.Rollout == nil || !instance.Spec.Rollout.GatewayBackends {
		return hold("WeightedRoutingUnavailable", "WeightedGateway requires configured role HTTPRoutes and fleet backends")
	}
	if state == nil && !r.supportsWeightedRuntime(instance) {
		return hold("WeightedRuntimeUnqualified", "WeightedGateway requires the approved runtime and authentication mode")
	}
	if state == nil && instance.Status.BasicAuthBaseline != nil {
		for _, role := range weightedRoleOrder {
			if _, err := rollout.ReadRouteCredential(ctx, r.Core, instance, r.rolloutAuthSecrets[role]); err != nil {
				return hold("WeightedCredentialUnavailable", "Both role credentials must qualify before preparing the weighted rollout")
			}
		}
	}
	if op.Status.RolloutRoles != nil {
		return hold("RoutingModeConflict", "A selector journal already owns this operation")
	}
	if !controllerutil.ContainsFinalizer(op, rolloutOperationFinalizer) {
		return r.setRolloutFinalizer(ctx, op, true)
	}
	key := client.ObjectKeyFromObject(op)
	if state != nil {
		for _, role := range weightedRoleOrder {
			if state.Roles[role].RouteName == "" || state.Roles[role].RouteName != r.weightedRoutes[role] {
				return hold("WeightedRouteScopeChanged", "The weighted journal refers to routes outside the configured scope")
			}
		}
		if owned || (op.DeletionTimestamp != nil && rolloutAlreadyCommitted(instance)) {
			if !rolloutAlreadyCommitted(instance) {
				action, complete, err := r.executeWeightedRolloutStep(ctx, reader, key, op.UID, r.weightedEvidence)
				if err != nil {
					return ctrl.Result{}, err
				}
				if action.Kind == rollout.Hold {
					return hold(action.Reason, "Waiting for qualified weighted route evidence")
				}
				if !complete {
					return retry, nil
				}
			}
			domain, err := r.RolloutPodDomain(instance)
			if err != nil {
				return ctrl.Result{}, err
			}
			_, err = r.finalizeWeightedRollout(ctx, reader, key, op.UID, domain, r.weightedEvidence)
			return retry, err
		}
	}
	if op.Spec.Action == "Rollback" {
		_, err := r.prepareWeightedRollbackHandoff(ctx, reader, key, op.UID, r.weightedRoutes)
		return retry, err
	}
	if !owned && (active != nil || state != nil) {
		return hold("ClaimUnavailable", "Another operation owns the instance or this promotion was superseded")
	}
	if currentPromotionCandidate(instance, op.Spec.TargetFleet) == nil || instance.Status.PendingPodCreation != nil ||
		instance.Status.PendingGateRelease != nil || instance.Status.ProvisioningAttempt != nil || instance.Status.SearchWarmupSlot != nil {
		return hold("CandidateUnavailable", "Waiting for the candidate and Pod lifecycle to settle")
	}
	projected := instance.DeepCopyObject().(*v1alpha1.OpenSearchServerless)
	claim := operationClaimFor(op)
	projected.Status.ActiveOperation = &claim
	journal, err := r.buildWeightedPromotionJournal(ctx, reader, projected, op, r.weightedRoutes)
	if err != nil {
		return hold("WeightedBaselineUnavailable", err.Error())
	}
	if owned {
		return retry, r.prepareWeightedRollout(ctx, reader, instance, op, journal)
	}
	if _, err := r.observeWeightedProgress(ctx, reader, projected, op, journal); err != nil {
		return hold("WeightedBaselineUnavailable", err.Error())
	}
	evidence, err := r.RolloutEvidence(ctx, instance.DeepCopyObject().(*v1alpha1.OpenSearchServerless),
		op.DeepCopyObject().(*v1alpha1.OpenSearchServerlessOperation))
	if err != nil {
		return ctrl.Result{}, err
	}
	if evidence.revalidate != nil {
		evidence.revalidate(ctx, &evidence)
	}
	if evidence.Candidates["search"] == nil || evidence.Candidates["ingest"] == nil {
		return hold("CandidateUnavailable", "Waiting for both candidate roles to qualify")
	}
	return retry, r.qualifyObservedPromotion(ctx, reader, instance, op, evidence.Candidates, rolloutObservationAge, true)
}
