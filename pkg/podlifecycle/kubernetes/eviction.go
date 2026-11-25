// Package kubernetes executes guarded Pod lifecycle actions.
package kubernetes

import (
	"context"
	"fmt"

	policyv1 "k8s.io/api/policy/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/typed/core/v1"

	"github.com/huyz0/opensearch-serverless-operator/pkg/podlifecycle"
)

// Evict submits a PDB-aware eviction only for the observed Pod and owner.
// An error can be ambiguous. The caller must re-read the Pod before retrying.
func Evict(ctx context.Context, core v1.CoreV1Interface, target podlifecycle.Identity) error {
	return evict(ctx, core, target, "")
}

// EvictAtVersion also requires the resource version observed during qualification.
func EvictAtVersion(ctx context.Context, core v1.CoreV1Interface, target podlifecycle.Identity, version string) error {
	if version == "" {
		return fmt.Errorf("qualified Pod resource version is required")
	}
	return evict(ctx, core, target, version)
}

func evict(ctx context.Context, core v1.CoreV1Interface, target podlifecycle.Identity, expectedVersion string) error {
	pods, pod, err := readMutationTarget(ctx, core, target, expectedVersion)
	if err != nil {
		return fmt.Errorf("qualify eviction: %w", err)
	}
	options := deleteOptionsForPod(pod)
	eviction := &policyv1.Eviction{
		ObjectMeta:    metav1.ObjectMeta{Name: pod.Name, Namespace: pod.Namespace},
		DeleteOptions: &options,
	}
	if err := pods.EvictV1(ctx, eviction); err != nil {
		return fmt.Errorf("submit eviction: %w", err)
	}
	return nil
}
