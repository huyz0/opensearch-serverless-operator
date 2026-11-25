package controller

import (
	"context"
	"fmt"

	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/huyz0/opensearch-serverless-operator/api/v1alpha1"
)

// ClaimOperation persists exclusive operation identity using the observed
// instance resource version. Both inputs must be freshly observed. A conflict
// or ambiguous write requires the caller to re-read both objects before retry.
// This primitive neither authorizes effects nor clears an existing claim.
func ClaimOperation(ctx context.Context, c client.Client, instance *v1alpha1.OpenSearchServerless, operation *v1alpha1.OpenSearchServerlessOperation) error {
	if c == nil || instance == nil || operation == nil || instance.UID == "" || operation.UID == "" || instance.ResourceVersion == "" || operation.ResourceVersion == "" {
		return fmt.Errorf("observed instance and operation identities and resource versions are required")
	}
	if instance.Namespace == "" || instance.Namespace != operation.Namespace || instance.Name != operation.Spec.InstanceRef.Name || string(instance.UID) != operation.Spec.InstanceRef.UID {
		return fmt.Errorf("operation target does not match the instance identity")
	}
	if instance.DeletionTimestamp != nil || operation.DeletionTimestamp != nil {
		return fmt.Errorf("terminating instance or operation cannot acquire a claim")
	}
	if err := operation.Spec.Validate(); err != nil {
		return fmt.Errorf("invalid operation: %w", err)
	}
	want := operationClaimFor(operation)
	if want.Name == "" || want.Generation <= 0 {
		return fmt.Errorf("operation name and generation are required")
	}
	if current := instance.Status.ActiveOperation; current != nil {
		if *current == want {
			return nil
		}
		return fmt.Errorf("another operation or a conflicting identity already holds the instance claim")
	}
	if instance.Status.EmergencyRetirement != nil {
		return fmt.Errorf("an emergency Pod retirement already holds the instance")
	}
	if instance.Status.ScaleDownRetirement != nil {
		return fmt.Errorf("a scale-down Pod retirement already holds the instance")
	}
	// Retrying a withdrawn attempt clears a provisioning hold without
	// reviving a displaced fleet or changing the serving mapping.
	if instance.Status.FleetRetirement != nil && operation.Spec.Action != "RetryProvisioning" {
		return fmt.Errorf("a fleet retirement reservation already holds the instance")
	}
	before := instance.DeepCopyObject().(*v1alpha1.OpenSearchServerless)
	instance.Status.ActiveOperation = &want
	if err := c.Status().Patch(ctx, instance, client.MergeFromWithOptions(before, client.MergeFromWithOptimisticLock{})); err != nil {
		return fmt.Errorf("persist operation claim: %w", err)
	}
	return nil
}

func operationClaimFor(operation *v1alpha1.OpenSearchServerlessOperation) v1alpha1.OperationClaim {
	return v1alpha1.OperationClaim{Name: operation.Name, UID: string(operation.UID), Action: operation.Spec.Action, Generation: operation.Generation}
}

// clearObservedOperationClaim clears the named operation's claim at the
// observed resource version. The caller decides when release is safe.
func (r *OperationReconciler) clearObservedOperationClaim(ctx context.Context, instance *v1alpha1.OpenSearchServerless, operationUID string) (ctrl.Result, error) {
	if operationUID == "" || instance.Status.ActiveOperation == nil || instance.Status.ActiveOperation.UID != operationUID {
		return ctrl.Result{}, fmt.Errorf("observed claim does not belong to the operation")
	}
	before := instance.DeepCopy()
	instance.Status.ActiveOperation = nil
	return ctrl.Result{Requeue: true}, r.Status().Patch(ctx, instance, client.MergeFromWithOptions(before, client.MergeFromWithOptimisticLock{}))
}
