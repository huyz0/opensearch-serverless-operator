package kubernetes

import (
	"context"
	"fmt"

	"k8s.io/client-go/kubernetes/typed/core/v1"

	"github.com/huyz0/opensearch-serverless-operator/pkg/podlifecycle"
)

// WithdrawUnbound deletes a pending Pod only if its UID, controlling owner,
// binding state and resource version still match the observation. The caller
// must persist the withdrawal intent and stop retries before invoking it.
func WithdrawUnbound(ctx context.Context, core v1.CoreV1Interface, target podlifecycle.Identity) error {
	return withdrawUnbound(ctx, core, target, "", "")
}

// WithdrawUnboundAtVersion also binds the deletion to the caller's exact Pod
// observation so a change after qualification invalidates that decision.
func WithdrawUnboundAtVersion(ctx context.Context, core v1.CoreV1Interface, target podlifecycle.Identity, expectedVersion string) error {
	if expectedVersion == "" {
		return fmt.Errorf("observed Pod resource version is required")
	}
	return withdrawUnbound(ctx, core, target, expectedVersion, "")
}

// WithdrawGatedAtVersion removes only the exact unbound Pod whose named gate
// and resource version the caller observed. The caller must persist its
// withdrawal intent before invoking this function and resolve ambiguous API
// outcomes by reading the target again.
func WithdrawGatedAtVersion(ctx context.Context, core v1.CoreV1Interface, target podlifecycle.Identity, gateName, expectedVersion string) error {
	if gateName == "" || expectedVersion == "" {
		return fmt.Errorf("gate and observed Pod resource version are required")
	}
	return withdrawUnbound(ctx, core, target, expectedVersion, gateName)
}

func withdrawUnbound(ctx context.Context, core v1.CoreV1Interface, target podlifecycle.Identity, expectedVersion, gateName string) error {
	pods, pod, err := readMutationTarget(ctx, core, target, expectedVersion)
	if err != nil {
		return fmt.Errorf("qualify withdrawal: %w", err)
	}
	if pod.Spec.NodeName != "" {
		return fmt.Errorf("withdrawal target is bound")
	}
	if gateName != "" && schedulingGateIndex(pod, gateName) == -1 {
		return fmt.Errorf("withdrawal target no longer has the required gate")
	}
	if err := pods.Delete(ctx, target.Name, deleteOptionsForPod(pod)); err != nil {
		return fmt.Errorf("delete unbound Pod: %w", err)
	}
	return nil
}
