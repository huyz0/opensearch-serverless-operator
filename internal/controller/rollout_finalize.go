package controller

import (
	"context"
	"fmt"
	"time"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/huyz0/opensearch-serverless-operator/api/v1alpha1"
)

// finalizeCommittedRollout records success before releasing the claim on a
// later pass. The caller supplies the DNS domain used by the Pod reconciler
// and serializes rollout writers with an uncached reader.
func (r *OperationReconciler) finalizeCommittedRollout(ctx context.Context, reader client.Reader, key types.NamespacedName, uid types.UID, domain string) (bool, error) {
	if reader == nil || r.Client == nil || uid == "" {
		return false, fmt.Errorf("live clients and rollout UID are required")
	}
	var operation v1alpha1.OpenSearchServerlessOperation
	if err := reader.Get(ctx, key, &operation); err != nil {
		return false, err
	}
	if operation.UID != uid {
		return false, fmt.Errorf("rollout was replaced")
	}
	var instance v1alpha1.OpenSearchServerless
	if err := reader.Get(ctx, types.NamespacedName{Namespace: key.Namespace, Name: operation.Spec.InstanceRef.Name}, &instance); err != nil {
		return false, err
	}
	claim := operationClaimFor(&operation)
	pinned := &operation
	if committed := instance.Status.LastRolloutCommit; committed != nil && operation.DeletionTimestamp != nil &&
		committed.UID == claim.UID && committed.Name == claim.Name && committed.Action == claim.Action &&
		claim.Generation >= committed.Generation {
		claim = *committed
		pinned = operation.DeepCopyObject().(*v1alpha1.OpenSearchServerlessOperation)
		pinned.Generation = committed.Generation
		pinned.DeletionTimestamp = nil
	}
	if instance.UID == "" || string(instance.UID) != operation.Spec.InstanceRef.UID || instance.Status.LastRolloutCommit == nil ||
		*instance.Status.LastRolloutCommit != claim {
		return false, fmt.Errorf("no matching rollout commit exists")
	}
	if instance.Status.ActiveOperation == nil || *instance.Status.ActiveOperation != claim {
		if operation.Status.Phase == "Succeeded" {
			return true, nil
		}
		return false, fmt.Errorf("committed rollout no longer owns the instance")
	}
	if _, err := rolloutProgress(&instance, pinned); err != nil {
		return false, err
	}
	if instance.Status.PendingPodCreation != nil || instance.Status.PendingGateRelease != nil ||
		instance.Status.ProvisioningAttempt != nil || instance.Status.SearchWarmupSlot != nil {
		return false, fmt.Errorf("Pod lifecycle work must settle before releasing rollout")
	}
	revisions, err := podRevisions(&instance, domain)
	if err != nil {
		return false, err
	}
	destination := revisions.forFleetTemplate(operation.Spec.TargetFleet)
	if !destination.accepts(operation.Status.RolloutRoles["search"].CandidateRevision) || !destination.accepts(operation.Status.RolloutRoles["ingest"].CandidateRevision) {
		return false, fmt.Errorf("instance template would provision a different revision from the serving fleet")
	}
	snapshots, err := r.observeRolloutServices(ctx, &instance, pinned, nil, nil, time.Minute)
	if err != nil {
		return false, err
	}
	for role, snapshot := range snapshots {
		service := snapshot.Service
		if !service.Owned || !service.RoutingShapeValid || service.ResourceVersion == "" ||
			service.ServiceUID != operation.Status.RolloutRoles[role].ServiceUID || service.Fleet != operation.Spec.TargetFleet {
			return false, fmt.Errorf("%s Service differs from the committed rollout", role)
		}
	}
	if operation.Status.Phase != "Succeeded" {
		before := operation.DeepCopyObject().(*v1alpha1.OpenSearchServerlessOperation)
		operation.Status.Phase = "Succeeded"
		operation.Status.ObservedGeneration = operation.Generation
		meta.SetStatusCondition(&operation.Status.Conditions, metav1.Condition{
			Type: "Ready", Status: metav1.ConditionTrue, Reason: "ServingFleetCommitted",
			Message: "Both production routes converged and the serving fleet was committed", ObservedGeneration: operation.Generation,
		})
		return false, r.Status().Patch(ctx, &operation, client.MergeFromWithOptions(before, client.MergeFromWithOptimisticLock{}))
	}
	var currentOperation v1alpha1.OpenSearchServerlessOperation
	if err := reader.Get(ctx, key, &currentOperation); err != nil {
		return false, err
	}
	if currentOperation.UID != operation.UID || currentOperation.ResourceVersion != operation.ResourceVersion {
		return false, fmt.Errorf("operation changed before claim release")
	}
	before := instance.DeepCopyObject().(*v1alpha1.OpenSearchServerless)
	instance.Status.ActiveOperation = nil
	if err := r.Status().Patch(ctx, &instance, client.MergeFromWithOptions(before, client.MergeFromWithOptimisticLock{})); err != nil {
		return false, err
	}
	return true, nil
}
