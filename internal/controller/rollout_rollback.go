package controller

import (
	"context"
	"fmt"
	"reflect"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/huyz0/opensearch-serverless-operator/api/v1alpha1"
	"github.com/huyz0/opensearch-serverless-operator/internal/rollout"
)

// prepareRollbackHandoff stages recovery on the first pass and transfers the
// claim on the next. The caller serializes rollout writers and uses an
// uncached reader. The promotion record supplies the original revisions.
// Neither pass changes traffic. Normal execution still qualifies the target.
func (r *OperationReconciler) prepareRollbackHandoff(ctx context.Context, reader client.Reader, key types.NamespacedName, uid types.UID) (bool, error) {
	if reader == nil || r.Client == nil || uid == "" {
		return false, fmt.Errorf("live clients and rollback UID are required")
	}
	var rollback v1alpha1.OpenSearchServerlessOperation
	if err := reader.Get(ctx, key, &rollback); err != nil {
		return false, err
	}
	if rollback.UID != uid || rollback.DeletionTimestamp != nil || rollback.Spec.Action != "Rollback" || rollback.Spec.SupersedesOperationUID == "" {
		return false, fmt.Errorf("live rollback must identify the superseded promotion")
	}
	if err := rollback.Spec.Validate(); err != nil {
		return false, err
	}
	instanceKey := types.NamespacedName{Namespace: key.Namespace, Name: rollback.Spec.InstanceRef.Name}
	var instance v1alpha1.OpenSearchServerless
	if err := reader.Get(ctx, instanceKey, &instance); err != nil {
		return false, err
	}
	claim := operationClaimFor(&rollback)
	if instance.Status.ActiveOperation != nil && *instance.Status.ActiveOperation == claim {
		_, err := rolloutProgress(&instance, &rollback)
		if err != nil {
			return false, err
		}
		return true, nil
	}
	active := instance.Status.ActiveOperation
	if active == nil {
		return r.prepareReleasedPromotionRollback(ctx, reader, &instance, &rollback)
	}
	if active.Action != "Promote" || active.UID != rollback.Spec.SupersedesOperationUID {
		return false, fmt.Errorf("rollback does not supersede the claimed promotion")
	}
	committed := rolloutAlreadyCommitted(&instance)
	target := instance.Status.RollbackTarget
	if committed {
		if !r.rollbackTargetEligible(target, active.UID) {
			return false, fmt.Errorf("committed promotion has no eligible rollback target")
		}
	}
	var promotion v1alpha1.OpenSearchServerlessOperation
	if err := reader.Get(ctx, types.NamespacedName{Namespace: key.Namespace, Name: active.Name}, &promotion); err != nil {
		return false, err
	}
	progress, err := recoverablePromotionProgress(&instance, &promotion)
	if err != nil {
		return false, err
	}
	projected := instance.DeepCopyObject().(*v1alpha1.OpenSearchServerless)
	projected.Status.ActiveOperation = &claim
	roles := make(map[string]v1alpha1.RolloutRoleStatus, len(progress))
	switched := false
	for _, role := range progress {
		intent := role.Intent
		if intent.SourceFleet != rollback.Spec.TargetFleet {
			return false, fmt.Errorf("rollback target differs from the original %s fleet", intent.Role)
		}
		revision := promotion.Status.RolloutRoles[intent.Role].SourceRevision
		if revision == "" {
			return false, fmt.Errorf("promotion did not record the original %s revision", intent.Role)
		}
		if committed && target.Roles[intent.Role] != (v1alpha1.RollbackRoleTarget{Fleet: intent.SourceFleet, Revision: revision, ServiceUID: intent.ServiceUID}) {
			return false, fmt.Errorf("recorded rollback target differs from the original %s identity", intent.Role)
		}
		snapshot, err := r.observeRoleSwitch(ctx, &instance, &promotion, intent, nil, nil, time.Second)
		if err != nil {
			return false, err
		}
		service := snapshot.Service
		if !service.Owned || !service.RoutingShapeValid || service.ServiceUID != intent.ServiceUID || service.ResourceVersion == "" ||
			(service.Fleet != intent.SourceFleet && service.Fleet != intent.TargetFleet) || service.Fleet != rollback.Spec.ExpectedServingFleetByRole[intent.Role] ||
			(intent.Phase == rollout.Applied && service.Fleet != intent.TargetFleet) ||
			(intent.Role == "ingest" && service.Fleet == intent.TargetFleet && !progress[0].Converged) {
			return false, fmt.Errorf("%s Service differs from the authorized rollback mapping", intent.Role)
		}
		adopt := service.Fleet == intent.SourceFleet
		switched = switched || !adopt
		projected.Status.ServingFleetIntentByRole[intent.Role] = service.Fleet
		servingRevision := intent.CandidateRevision
		if adopt {
			servingRevision = revision
		}
		pinServingRevision(projected, intent.Role, service.Fleet, servingRevision)
		roles[intent.Role] = v1alpha1.RolloutRoleStatus{
			ServiceUID: intent.ServiceUID, SourceFleet: service.Fleet, TargetFleet: intent.SourceFleet,
			CandidateRevision: revision, AdoptExisting: adopt, Phase: string(rollout.Prepared),
		}
	}
	if !switched {
		return false, fmt.Errorf("no promotion selector change was observed")
	}
	return r.persistRollbackHandoff(ctx, reader, &instance, projected, &rollback, roles, &promotion)
}

// A terminating promotion may only be observed for recovery. Deletion can
// increment generation, but admission keeps the operation spec immutable.
func recoverablePromotionProgress(instance *v1alpha1.OpenSearchServerless, promotion *v1alpha1.OpenSearchServerlessOperation) ([]rollout.RoleProgress, error) {
	claim := instance.Status.ActiveOperation
	if promotion.DeletionTimestamp == nil || claim == nil || claim.UID != string(promotion.UID) || promotion.Generation < claim.Generation {
		return rolloutProgress(instance, promotion)
	}
	pinned := promotion.DeepCopyObject().(*v1alpha1.OpenSearchServerlessOperation)
	pinned.Generation = claim.Generation
	pinned.DeletionTimestamp = nil
	return rolloutProgress(instance, pinned)
}

// prepareReleasedPromotionRollback uses the durable instance record once the
// completed promotion has released its claim. The old operation may have been
// deleted, so the saved Service UIDs and revisions fence the recovery.
func (r *OperationReconciler) prepareReleasedPromotionRollback(ctx context.Context, reader client.Reader, instance *v1alpha1.OpenSearchServerless, rollback *v1alpha1.OpenSearchServerlessOperation) (bool, error) {
	if instance.Status.FleetRetirement != nil {
		return false, fmt.Errorf("fleet retirement is reserved")
	}
	last := instance.Status.LastRolloutCommit
	target := instance.Status.RollbackTarget
	if last == nil || last.Action != "Promote" || last.UID != rollback.Spec.SupersedesOperationUID || !r.rollbackTargetEligible(target, last.UID) {
		return false, fmt.Errorf("rollback does not identify an eligible committed promotion")
	}
	if target.Weighted {
		return false, fmt.Errorf("the retained fleet requires a weighted rollback")
	}
	if instance.Status.PendingPodCreation != nil || instance.Status.PendingGateRelease != nil ||
		instance.Status.ProvisioningAttempt != nil || instance.Status.SearchWarmupSlot != nil {
		return false, fmt.Errorf("Pod lifecycle work must settle before claiming rollback")
	}
	roles := make(map[string]v1alpha1.RolloutRoleStatus, 2)
	for _, role := range []string{"search", "ingest"} {
		identity := target.Roles[role]
		source := instance.Status.ServingFleetIntentByRole[role]
		if identity.Fleet != rollback.Spec.TargetFleet || identity.Revision == "" || identity.ServiceUID == "" ||
			source == "" || source == identity.Fleet || source != rollback.Spec.ExpectedServingFleetByRole[role] {
			return false, fmt.Errorf("%s serving intent differs from the authorized rollback mapping", role)
		}
		intent := rollout.SwitchIntent{Role: role, SourceFleet: source, TargetFleet: identity.Fleet}
		observation, err := r.observeRoleSwitch(ctx, instance, rollback, intent, nil, nil, time.Second)
		if err != nil {
			return false, err
		}
		service := observation.Service
		if !service.Owned || !service.RoutingShapeValid || service.ServiceUID != identity.ServiceUID ||
			service.ResourceVersion == "" || service.Fleet != source {
			return false, fmt.Errorf("%s Service differs from the authorized rollback mapping", role)
		}
		roles[role] = v1alpha1.RolloutRoleStatus{
			ServiceUID: identity.ServiceUID, SourceFleet: source, TargetFleet: identity.Fleet,
			CandidateRevision: identity.Revision, Phase: string(rollout.Prepared),
		}
	}
	projected := instance.DeepCopyObject().(*v1alpha1.OpenSearchServerless)
	claim := operationClaimFor(rollback)
	projected.Status.ActiveOperation = &claim
	return r.persistRollbackHandoff(ctx, reader, instance, projected, rollback, roles, nil)
}

func (r *OperationReconciler) persistRollbackHandoff(ctx context.Context, reader client.Reader, instance, projected *v1alpha1.OpenSearchServerless, rollback *v1alpha1.OpenSearchServerlessOperation, roles map[string]v1alpha1.RolloutRoleStatus, predecessor *v1alpha1.OpenSearchServerlessOperation) (bool, error) {
	if rollback.Status.RolloutRoles == nil {
		// Validate against the prospective mapping. Only the operation status
		// is written here. The instance claim remains unchanged.
		return false, prepareRolloutProgress(ctx, r.Client, projected, rollback, roles)
	}
	if !reflect.DeepEqual(rollback.Status.RolloutRoles, roles) {
		return false, fmt.Errorf("staged rollback no longer matches the live role mapping or revision")
	}
	if _, err := rolloutProgress(projected, rollback); err != nil {
		return false, err
	}
	if err := r.verifyRollbackHandoff(ctx, reader, instance, rollback, predecessor); err != nil {
		return false, err
	}
	if err := r.Status().Patch(ctx, projected, client.MergeFromWithOptions(instance, client.MergeFromWithOptimisticLock{})); err != nil {
		return false, fmt.Errorf("transfer rollout claim: %w", err)
	}
	return true, nil
}

func (r *OperationReconciler) verifyRollbackHandoff(ctx context.Context, reader client.Reader, instance *v1alpha1.OpenSearchServerless, rollback, predecessor *v1alpha1.OpenSearchServerlessOperation) error {
	for _, observed := range []*v1alpha1.OpenSearchServerlessOperation{predecessor, rollback} {
		if observed == nil {
			continue
		}
		var current v1alpha1.OpenSearchServerlessOperation
		if err := reader.Get(ctx, client.ObjectKeyFromObject(observed), &current); err != nil {
			return err
		}
		if current.UID != observed.UID || current.ResourceVersion != observed.ResourceVersion {
			return fmt.Errorf("operation changed during rollback handoff")
		}
	}
	if last := instance.Status.LastRolloutCommit; last != nil && last.UID == rollback.Spec.SupersedesOperationUID &&
		!r.rollbackTargetEligible(instance.Status.RollbackTarget, last.UID) {
		return fmt.Errorf("rollback deadline expired before the claim transfer")
	}
	return nil
}

func (r *OperationReconciler) rollbackTargetEligible(target *v1alpha1.RollbackTarget, promotionUID string) bool {
	now := time.Now()
	if r.Now != nil {
		now = r.Now()
	}
	return target != nil && target.PromotionUID == promotionUID && (target.RetainUntil == nil || now.Before(target.RetainUntil.Time))
}

func committedRollbackTarget(instance *v1alpha1.OpenSearchServerless, operation *v1alpha1.OpenSearchServerlessOperation, now time.Time) (*v1alpha1.RollbackTarget, error) {
	target := &v1alpha1.RollbackTarget{PromotionUID: string(operation.UID), Roles: make(map[string]v1alpha1.RollbackRoleTarget, 2)}
	for _, role := range []string{"search", "ingest"} {
		state := operation.Status.RolloutRoles[role]
		if state.SourceRevision == "" {
			return nil, fmt.Errorf("promotion did not record the original %s revision", role)
		}
		target.Roles[role] = v1alpha1.RollbackRoleTarget{Fleet: state.SourceFleet, Revision: state.SourceRevision, ServiceUID: state.ServiceUID}
	}
	return target, setRollbackDeadline(instance, target, now)
}

func setRollbackDeadline(instance *v1alpha1.OpenSearchServerless, target *v1alpha1.RollbackTarget, now time.Time) error {
	if raw := instance.Spec.Rollout.RollbackWindow; raw != "" {
		window, err := time.ParseDuration(raw)
		if err != nil || window < 0 {
			return fmt.Errorf("invalid rollback window")
		}
		until := metav1.NewMicroTime(now.Add(window).UTC().Truncate(time.Microsecond))
		target.RetainUntil = &until
	}
	return nil
}
