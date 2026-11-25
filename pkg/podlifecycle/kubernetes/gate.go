package kubernetes

import (
	"context"
	"fmt"
	"slices"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/typed/core/v1"

	"github.com/huyz0/opensearch-serverless-operator/pkg/podlifecycle"
)

// ReleaseSchedulingGate removes a gate from an owned, unbound Pod after its caller has
// revalidated capacity and persisted its intent. An update conflict requires a
// new observation and a new budget decision before retrying.
func ReleaseSchedulingGate(ctx context.Context, core v1.CoreV1Interface, target podlifecycle.Identity, gateName string) error {
	return releaseSchedulingGate(ctx, core, target, gateName, "")
}

// ReleaseSchedulingGateAtVersion additionally requires the exact Pod version
// evaluated by the caller, preventing a stale admission or capacity check from
// authorizing a changed Pod. An already removed gate also requires a fresh
// version match; callers recover ambiguous outcomes by re-reading first.
func ReleaseSchedulingGateAtVersion(ctx context.Context, core v1.CoreV1Interface, target podlifecycle.Identity, gateName, expectedVersion string) error {
	if expectedVersion == "" {
		return fmt.Errorf("observed Pod resource version is required")
	}
	return releaseSchedulingGate(ctx, core, target, gateName, expectedVersion)
}

func releaseSchedulingGate(ctx context.Context, core v1.CoreV1Interface, target podlifecycle.Identity, gateName, expectedVersion string) error {
	if gateName == "" {
		return fmt.Errorf("gate name is required")
	}
	pods, pod, err := readMutationTarget(ctx, core, target, expectedVersion)
	if err != nil {
		return fmt.Errorf("qualify scheduling gate release: %w", err)
	}
	i := schedulingGateIndex(pod, gateName)
	if i == -1 {
		return nil
	}
	if pod.Spec.NodeName != "" {
		return fmt.Errorf("scheduling gate release target is already bound")
	}
	pod.Spec.SchedulingGates = slices.Delete(pod.Spec.SchedulingGates, i, i+1)
	if _, err := pods.Update(ctx, pod, metav1.UpdateOptions{}); err != nil {
		return fmt.Errorf("release scheduling gate: %w", err)
	}
	return nil
}

func schedulingGateIndex(pod *corev1.Pod, name string) int {
	return slices.IndexFunc(pod.Spec.SchedulingGates, func(gate corev1.PodSchedulingGate) bool {
		return gate.Name == name
	})
}
