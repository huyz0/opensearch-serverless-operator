package workload

import (
	"fmt"

	corev1 "k8s.io/api/core/v1"

	"github.com/huyz0/opensearch-serverless-operator/api/v1alpha1"
	"github.com/huyz0/opensearch-serverless-operator/internal/runtimeconfig"
)

// RuntimePod resolves qualified custom JVM settings and managed process inputs
// before building a gated Pod. It does not apply or release the Pod.
func RuntimePod(instance *v1alpha1.OpenSearchServerless, input PodInput, managed runtimeconfig.ManagedInputs, catalog runtimeconfig.Catalog) (*corev1.Pod, error) {
	if instance == nil {
		return nil, fmt.Errorf("instance is required")
	}
	pool, ok := instance.Spec.Pools[input.Role]
	if !ok {
		return nil, fmt.Errorf("role pool %q is missing", input.Role)
	}
	if managed.NodeName != input.Name || managed.Role != input.Role || managed.Zone != input.Zone || managed.JVMHeap != pool.JVMHeap || managed.WarmOnJoin != pool.WarmOnJoin || managed.RequireWarmupCompletion != (input.WarmupMode == "Strict") || len(input.Args) != 0 {
		return nil, fmt.Errorf("managed node, role, zone or heap does not match Pod inputs, or raw arguments were supplied")
	}
	custom, err := runtimeconfig.Resolve(instance.Spec.Runtime, pool.Runtime, catalog)
	if err != nil {
		return nil, err
	}
	args, err := runtimeconfig.ManagedPodArguments(managed, custom)
	if err != nil {
		return nil, err
	}
	input.Args = args
	return Pod(instance, input)
}
