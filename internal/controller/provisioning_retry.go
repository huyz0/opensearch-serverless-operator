package controller

import (
	"context"
	"fmt"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	"github.com/huyz0/opensearch-serverless-operator/api/v1alpha1"
)

const provisioningRetryFinalizer = "serverless.opensearch.org/provisioning-retry"

// reconcileProvisioningRetry clears only a specifically acknowledged, fully
// withdrawn attempt. It never deletes a Pod or bypasses normal qualification.
func (r *OperationReconciler) reconcileProvisioningRetry(ctx context.Context, operation *v1alpha1.OpenSearchServerlessOperation) (ctrl.Result, error) {
	before := operation.DeepCopyObject().(*v1alpha1.OpenSearchServerlessOperation)
	hold := func(reason, message string) (ctrl.Result, error) {
		meta.SetStatusCondition(&operation.Status.Conditions, metav1.Condition{Type: "Ready", Status: metav1.ConditionFalse, Reason: reason, Message: message, ObservedGeneration: operation.Generation})
		return r.patchSettingsStatus(ctx, operation, before)
	}
	if err := operation.Spec.Validate(); err != nil {
		return hold("InvalidSpec", err.Error())
	}
	if r.Core == nil {
		return hold("Unavailable", "Pod client is unavailable")
	}
	var instance v1alpha1.OpenSearchServerless
	if err := r.Get(ctx, types.NamespacedName{Namespace: operation.Namespace, Name: operation.Spec.InstanceRef.Name}, &instance); err != nil {
		if client.IgnoreNotFound(err) != nil {
			return ctrl.Result{}, err
		}
		if operation.DeletionTimestamp != nil && controllerutil.ContainsFinalizer(operation, provisioningRetryFinalizer) {
			return r.updateOperationFinalizer(ctx, operation, provisioningRetryFinalizer, false)
		}
		return hold("InstanceMismatch", "Referenced instance is missing")
	}
	if string(instance.UID) != operation.Spec.InstanceRef.UID || instance.DeletionTimestamp != nil {
		if operation.DeletionTimestamp != nil && controllerutil.ContainsFinalizer(operation, provisioningRetryFinalizer) {
			return r.updateOperationFinalizer(ctx, operation, provisioningRetryFinalizer, false)
		}
		return hold("InstanceMismatch", "Referenced instance was replaced or is terminating")
	}
	want := operationClaimFor(operation)
	ownedClaim := instance.Status.ActiveOperation != nil && *instance.Status.ActiveOperation == want
	// Deleting a custom resource can advance its generation even though its
	// spec is immutable. Continue an already claimed operation by UID and
	// action so its finalizer cannot orphan that claim.
	if claim := instance.Status.ActiveOperation; !ownedClaim && operation.DeletionTimestamp != nil && claim != nil && claim.Name == operation.Name && claim.UID == string(operation.UID) && claim.Action == operation.Spec.Action && claim.Generation > 0 && operation.Generation >= claim.Generation {
		ownedClaim = true
	}
	committed := instance.Status.LastProvisioningRetry != nil && instance.Status.LastProvisioningRetry.OperationUID == string(operation.UID) && instance.Status.LastProvisioningRetry.PodUID == operation.Spec.ProvisioningRetry.PodUID
	if operation.Status.Phase == "Succeeded" && !ownedClaim {
		if controllerutil.ContainsFinalizer(operation, provisioningRetryFinalizer) {
			return r.updateOperationFinalizer(ctx, operation, provisioningRetryFinalizer, false)
		}
		return ctrl.Result{}, nil
	}
	if operation.DeletionTimestamp != nil && !ownedClaim && !committed {
		if controllerutil.ContainsFinalizer(operation, provisioningRetryFinalizer) {
			return r.updateOperationFinalizer(ctx, operation, provisioningRetryFinalizer, false)
		}
		return ctrl.Result{}, nil
	}
	if !controllerutil.ContainsFinalizer(operation, provisioningRetryFinalizer) {
		if operation.DeletionTimestamp != nil {
			return ctrl.Result{}, nil
		}
		return r.updateOperationFinalizer(ctx, operation, provisioningRetryFinalizer, true)
	}
	if committed {
		if operation.Status.Phase != "Succeeded" {
			operation.Status.Phase = "Succeeded"
			meta.SetStatusCondition(&operation.Status.Conditions, metav1.Condition{Type: "Ready", Status: metav1.ConditionTrue, Reason: "HoldCleared", Message: "The recorded provisioning hold was cleared; normal qualification still applies", ObservedGeneration: operation.Generation})
			return r.patchSettingsStatus(ctx, operation, before)
		}
		if ownedClaim {
			old := instance.DeepCopyObject().(*v1alpha1.OpenSearchServerless)
			instance.Status.ActiveOperation = nil
			if err := r.Status().Patch(ctx, &instance, client.MergeFromWithOptions(old, client.MergeFromWithOptimisticLock{})); err != nil {
				return ctrl.Result{}, err
			}
			return ctrl.Result{Requeue: true}, nil
		}
		return r.updateOperationFinalizer(ctx, operation, provisioningRetryFinalizer, false)
	}
	var attempt *v1alpha1.ProvisioningAttempt
	archivedIndex := -1
	if active := instance.Status.ProvisioningAttempt; active != nil && active.PodUID == operation.Spec.ProvisioningRetry.PodUID {
		attempt = active
	}
	for i := range instance.Status.BlockedProvisioningAttempts {
		candidate := &instance.Status.BlockedProvisioningAttempts[i]
		if candidate.PodUID != operation.Spec.ProvisioningRetry.PodUID {
			continue
		}
		if attempt != nil {
			return hold("HoldMismatch", "The acknowledged Pod matches more than one provisioning hold")
		}
		attempt, archivedIndex = candidate, i
	}
	if attempt == nil || attempt.Phase != "Blocked" || attempt.Name == "" || (attempt.Reason != "PodGone" && attempt.Reason != "PodTerminating" && attempt.Reason != "PodTerminal") {
		return hold("HoldMismatch", "The acknowledged Pod does not match a recoverable provisioning hold")
	}
	if instance.Status.PendingPodCreation != nil || instance.Status.PendingGateRelease != nil || instance.Status.SearchWarmupSlot != nil {
		return hold("InstanceBusy", "Other Pod creation, release or warm-up work must finish before retry")
	}
	_, err := r.Core.Pods(instance.Namespace).Get(ctx, attempt.Name, metav1.GetOptions{})
	if err == nil {
		return hold("PodStillExists", "The previously blocked Pod must be fully removed before retry")
	}
	if !apierrors.IsNotFound(err) {
		return ctrl.Result{}, fmt.Errorf("read blocked Pod: %w", err)
	}
	if instance.Status.ActiveOperation == nil {
		if operation.DeletionTimestamp != nil {
			return r.updateOperationFinalizer(ctx, operation, provisioningRetryFinalizer, false)
		}
		if err := ClaimOperation(ctx, r.Client, &instance, operation); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{Requeue: true}, nil
	}
	if !ownedClaim {
		return hold("InstanceBusy", "A different operation owns the instance claim")
	}
	if operation.Status.Phase == "" || operation.Status.Phase == "Pending" {
		operation.Status.Phase = "Prepared"
		return hold("Prepared", "The blocked Pod identity is recorded before clearing the hold")
	}
	if operation.Status.Phase != "Prepared" {
		return ctrl.Result{RequeueAfter: time.Minute}, fmt.Errorf("unknown provisioning retry phase")
	}
	old := instance.DeepCopyObject().(*v1alpha1.OpenSearchServerless)
	if archivedIndex >= 0 {
		instance.Status.BlockedProvisioningAttempts = append(instance.Status.BlockedProvisioningAttempts[:archivedIndex], instance.Status.BlockedProvisioningAttempts[archivedIndex+1:]...)
		if len(instance.Status.BlockedProvisioningAttempts) == 0 {
			meta.RemoveStatusCondition(&instance.Status.Conditions, "ProvisioningGroupsBlocked")
		}
	} else {
		instance.Status.ProvisioningAttempt = nil
		meta.RemoveStatusCondition(&instance.Status.Conditions, "ProvisioningBlocked")
	}
	instance.Status.LastProvisioningRetry = &v1alpha1.ProvisioningRetryRecord{OperationUID: string(operation.UID), PodUID: operation.Spec.ProvisioningRetry.PodUID}
	if err := r.Status().Patch(ctx, &instance, client.MergeFromWithOptions(old, client.MergeFromWithOptimisticLock{})); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{Requeue: true}, nil
}
