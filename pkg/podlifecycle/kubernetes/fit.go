package kubernetes

import (
	"fmt"
	"sort"

	corev1 "k8s.io/api/core/v1"
)

// InsufficientResources compares a Pod's effective requests against allocatable
// resources after subtracting requests already reserved on the node. The caller
// supplies an up-to-date, complete reservation sum and separately evaluates
// node selectors, taints, Pod slots, topology, and other scheduler predicates.
func InsufficientResources(pod *corev1.Pod, allocatable, reserved corev1.ResourceList) ([]corev1.ResourceName, error) {
	if pod == nil {
		return nil, fmt.Errorf("Pod is required")
	}
	requests, err := EffectiveRequests(pod)
	if err != nil {
		return nil, err
	}
	insufficient := make([]corev1.ResourceName, 0)
	for name, request := range requests {
		capacity, exists := allocatable[name]
		capacity = capacity.DeepCopy()
		used := reserved[name]
		if request.Sign() < 0 || used.Sign() < 0 {
			return nil, fmt.Errorf("negative request or reservation for %s", name)
		}
		if !exists {
			if !request.IsZero() {
				insufficient = append(insufficient, name)
			}
			continue
		}
		capacity.Sub(used)
		if capacity.Cmp(request) < 0 {
			insufficient = append(insufficient, name)
		}
	}
	sort.Slice(insufficient, func(i, j int) bool { return insufficient[i] < insufficient[j] })
	return insufficient, nil
}
