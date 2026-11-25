package controller

import (
	"context"
	"fmt"
	"reflect"
	"time"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	"github.com/huyz0/opensearch-serverless-operator/api/v1alpha1"
	"github.com/huyz0/opensearch-serverless-operator/internal/rollout"
)

const rolloutOperationFinalizer = "serverless.opensearch.org/fleet-rollout"
const rolloutObservationAge = 30 * time.Second

// reconcileFleetOperation runs only when the manager supplies qualification
// and matching Pod template configuration. Every transition uses API reads.
func (r *OperationReconciler) reconcileFleetOperation(ctx context.Context, key types.NamespacedName) (ctrl.Result, error) {
	reader := r.RolloutReader
	var op v1alpha1.OpenSearchServerlessOperation
	if err := reader.Get(ctx, key, &op); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	retry := ctrl.Result{RequeueAfter: 2 * time.Second}
	hold := func(reason, message string) (ctrl.Result, error) {
		before := op.DeepCopyObject().(*v1alpha1.OpenSearchServerlessOperation)
		op.Status.ObservedGeneration = op.Generation
		meta.SetStatusCondition(&op.Status.Conditions, metav1.Condition{Type: "Ready", Status: metav1.ConditionFalse, Reason: reason, Message: message, ObservedGeneration: op.Generation})
		if reflect.DeepEqual(before.Status, op.Status) {
			return retry, nil
		}
		return retry, r.Status().Patch(ctx, &op, client.MergeFromWithOptions(before, client.MergeFromWithOptimisticLock{}))
	}
	if err := op.Spec.Validate(); err != nil {
		return hold("InvalidSpec", err.Error())
	}
	var instance v1alpha1.OpenSearchServerless
	instanceKey := types.NamespacedName{Namespace: key.Namespace, Name: op.Spec.InstanceRef.Name}
	if err := reader.Get(ctx, instanceKey, &instance); err != nil {
		if client.IgnoreNotFound(err) != nil {
			return ctrl.Result{}, err
		}
		if op.DeletionTimestamp != nil {
			return r.setRolloutFinalizer(ctx, &op, false)
		}
		return hold("InstanceMismatch", "Referenced instance was not found")
	}
	if string(instance.UID) != op.Spec.InstanceRef.UID || instance.DeletionTimestamp != nil {
		if op.DeletionTimestamp != nil {
			return r.setRolloutFinalizer(ctx, &op, false)
		}
		return hold("InstanceMismatch", "Referenced instance was replaced or is terminating")
	}
	claim := operationClaimFor(&op)
	owned := instance.Status.ActiveOperation != nil && *instance.Status.ActiveOperation == claim
	if op.Status.Phase == "Succeeded" && (instance.Status.ActiveOperation == nil || instance.Status.ActiveOperation.UID != claim.UID) {
		return r.setRolloutFinalizer(ctx, &op, false)
	}
	if instance.Status.FleetRetirement != nil && op.DeletionTimestamp == nil {
		return hold("FleetRetirementReserved", "A displaced fleet is reserved for platform drain and fencing")
	}
	weighted, err := r.usesWeightedRouting(ctx, reader, &instance, &op)
	if err != nil {
		return ctrl.Result{}, err
	}
	if weighted {
		return r.reconcileWeightedOperation(ctx, reader, &instance, &op, owned, retry, hold)
	}
	// Deletion may advance the object's generation while its spec stays
	// immutable. An old generation still owns the claim by UID.
	if op.DeletionTimestamp != nil {
		active := instance.Status.ActiveOperation
		if active == nil || active.UID != claim.UID {
			return r.setRolloutFinalizer(ctx, &op, false)
		}
		if rolloutAlreadyCommitted(&instance) {
			return r.reconcileFleetFinalization(ctx, key, &op, &instance, retry)
		}
		if op.Status.RolloutRoles != nil {
			if op.Spec.Action == "Promote" && !rolloutAlreadyCommitted(&instance) {
				clear, err := r.promotionHasSourceSelectors(ctx, &instance, &op)
				if err != nil {
					return ctrl.Result{}, err
				}
				if clear {
					return r.clearObservedOperationClaim(ctx, &instance, string(op.UID))
				}
			}
			return hold("RecoveryRequired", "This rollout still owns the claim. Recover the live mapping before deleting it")
		}
		return r.clearObservedOperationClaim(ctx, &instance, string(op.UID))
	}
	if !controllerutil.ContainsFinalizer(&op, rolloutOperationFinalizer) {
		return r.setRolloutFinalizer(ctx, &op, true)
	}
	if owned && rolloutAlreadyCommitted(&instance) {
		return r.reconcileFleetFinalization(ctx, key, &op, &instance, retry)
	}
	if owned && op.Status.RolloutRoles != nil {
		action, err := r.executePreparedRolloutStep(ctx, reader, key, op.UID, r.RolloutEvidence, rolloutObservationAge)
		if err != nil {
			return ctrl.Result{}, err
		}
		if action.Switch.Kind == rollout.Hold {
			return hold(action.Switch.Reason, fmt.Sprintf("Waiting for rollout qualification for role %q", action.Role))
		}
		return retry, nil
	}
	if op.Spec.Action == "Rollback" {
		_, err := r.prepareRollbackHandoff(ctx, reader, key, op.UID)
		return retry, err
	}
	if !owned && (instance.Status.ActiveOperation != nil || op.Status.RolloutRoles != nil) {
		return hold("ClaimUnavailable", "Another operation owns the claim or this promotion was superseded")
	}
	if currentPromotionCandidate(&instance, op.Spec.TargetFleet) == nil {
		return hold("CandidateUnavailable", "Waiting for the staged candidate and its preview Services")
	}
	evidence, err := r.RolloutEvidence(ctx, instance.DeepCopyObject().(*v1alpha1.OpenSearchServerless), op.DeepCopyObject().(*v1alpha1.OpenSearchServerlessOperation))
	if err != nil {
		return ctrl.Result{}, err
	}
	if evidence.revalidate != nil {
		evidence.revalidate(ctx, &evidence)
	}
	candidates := evidence.Candidates
	if !owned {
		// Leave provisioning free to finish staging until evidence for both
		// roles is available. Preparation validates freshness and identity.
		if candidates["search"] == nil || candidates["ingest"] == nil || instance.Status.PendingPodCreation != nil ||
			instance.Status.PendingGateRelease != nil || instance.Status.ProvisioningAttempt != nil || instance.Status.SearchWarmupSlot != nil {
			return hold("CandidateUnavailable", "Waiting for both roles to qualify and Pod lifecycle work to settle")
		}
		return retry, r.qualifyObservedPromotion(ctx, reader, &instance, &op, candidates, rolloutObservationAge, true)
	}
	return retry, r.prepareObservedPromotion(ctx, reader, &instance, &op, candidates, rolloutObservationAge)
}

func (r *OperationReconciler) reconcileFleetFinalization(ctx context.Context, key types.NamespacedName, op *v1alpha1.OpenSearchServerlessOperation, instance *v1alpha1.OpenSearchServerless, retry ctrl.Result) (ctrl.Result, error) {
	domain, err := r.RolloutPodDomain(instance)
	if err != nil {
		return ctrl.Result{}, err
	}
	_, err = r.finalizeCommittedRollout(ctx, r.RolloutReader, key, op.UID, domain)
	return retry, err
}

func (r *OperationReconciler) promotionHasSourceSelectors(ctx context.Context, instance *v1alpha1.OpenSearchServerless, op *v1alpha1.OpenSearchServerlessOperation) (bool, error) {
	progress, err := recoverablePromotionProgress(instance, op)
	if err != nil {
		return false, err
	}
	for _, role := range progress {
		if role.Intent.Phase != rollout.Prepared {
			return false, nil
		}
		snapshot, err := r.observeRoleSwitch(ctx, instance, op, role.Intent, nil, nil, rolloutObservationAge)
		if err != nil {
			return false, err
		}
		service := snapshot.Service
		if !service.Owned || !service.RoutingShapeValid || service.ResourceVersion == "" ||
			service.ServiceUID != role.Intent.ServiceUID || service.Fleet != role.Intent.SourceFleet {
			return false, nil
		}
	}
	return true, nil
}

func (r *OperationReconciler) setRolloutFinalizer(ctx context.Context, op *v1alpha1.OpenSearchServerlessOperation, add bool) (ctrl.Result, error) {
	before := op.DeepCopyObject().(*v1alpha1.OpenSearchServerlessOperation)
	if add {
		controllerutil.AddFinalizer(op, rolloutOperationFinalizer)
	} else {
		controllerutil.RemoveFinalizer(op, rolloutOperationFinalizer)
	}
	if reflect.DeepEqual(before.Finalizers, op.Finalizers) {
		return ctrl.Result{}, nil
	}
	return ctrl.Result{Requeue: true}, client.IgnoreNotFound(r.Patch(ctx, op, client.MergeFromWithOptions(before, client.MergeFromWithOptimisticLock{})))
}
