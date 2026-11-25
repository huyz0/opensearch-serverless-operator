package controller

import (
	"context"
	"fmt"

	typedcore "k8s.io/client-go/kubernetes/typed/core/v1"

	"github.com/huyz0/opensearch-serverless-operator/api/v1alpha1"
	"github.com/huyz0/opensearch-serverless-operator/internal/storage"
)

// checkPinnedPrimaryStore re-resolves external input and compares it to the
// persisted store identity. Callers still need their own mutation preconditions.
func (r *InstanceReconciler) checkPinnedPrimaryStore(ctx context.Context, instance *v1alpha1.OpenSearchServerless) error {
	return checkPinnedPrimaryStore(ctx, r.Core, instance)
}

func checkPinnedPrimaryStore(ctx context.Context, core typedcore.CoreV1Interface, instance *v1alpha1.OpenSearchServerless) error {
	if instance == nil || instance.Status.PrimaryStore == nil || core == nil {
		return fmt.Errorf("pinned primary store is required")
	}
	resolved, err := storage.ResolveS3(ctx, core, instance.Namespace, instance.Spec.Storage)
	if err != nil {
		return fmt.Errorf("primary store cannot be revalidated: %w", err)
	}
	current := resolved.PrimaryStoreStatus()
	if current != *instance.Status.PrimaryStore {
		return fmt.Errorf("resolved primary store differs from pinned identity")
	}
	return nil
}
