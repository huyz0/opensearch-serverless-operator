package kubernetes

import (
	"fmt"
	corev1 "k8s.io/api/core/v1"
)

// EffectiveRequests calculates the scheduler request for traditional and
// restartable init containers and Pod overhead. Pod-level requests are treated
// conservatively: the greater of the Pod request and aggregate container
// request is used. The input must be admitted, after defaults and injection.
// Dynamic resource claims need separate placement qualification and fail closed.
func EffectiveRequests(pod *corev1.Pod) (corev1.ResourceList, error) {
	if pod == nil {
		return nil, fmt.Errorf("Pod is required")
	}
	if len(pod.Spec.ResourceClaims) != 0 {
		return nil, fmt.Errorf("dynamic resource claims are not supported by the fit calculation")
	}
	check := func(c corev1.Container) error {
		if len(c.Resources.Claims) != 0 {
			return fmt.Errorf("container %s uses unsupported dynamic resource claims", c.Name)
		}
		for name, quantity := range c.Resources.Requests {
			if quantity.Sign() < 0 {
				return fmt.Errorf("container %s has negative %s request", c.Name, name)
			}
		}
		return nil
	}
	regular := corev1.ResourceList{}
	for _, container := range pod.Spec.Containers {
		if err := check(container); err != nil {
			return nil, err
		}
		addResources(regular, container.Resources.Requests)
	}
	initMaximum := corev1.ResourceList{}
	restartable := corev1.ResourceList{}
	for _, container := range pod.Spec.InitContainers {
		if err := check(container); err != nil {
			return nil, err
		}
		current := restartable.DeepCopy()
		addResources(current, container.Resources.Requests)
		maxResources(initMaximum, current)
		if container.RestartPolicy != nil && *container.RestartPolicy == corev1.ContainerRestartPolicyAlways {
			addResources(restartable, container.Resources.Requests)
		}
	}
	addResources(regular, restartable)
	maxResources(regular, initMaximum)
	if pod.Spec.Resources != nil {
		if len(pod.Spec.Resources.Claims) != 0 {
			return nil, fmt.Errorf("Pod-level dynamic resource claims are not supported")
		}
		for name, quantity := range pod.Spec.Resources.Requests {
			if quantity.Sign() < 0 {
				return nil, fmt.Errorf("negative Pod-level %s request", name)
			}
		}
		maxResources(regular, pod.Spec.Resources.Requests)
		// Admitted Pods normally default a limit into a missing request. Keep
		// this conservative if a supplied object did not receive that default.
		for name, limit := range pod.Spec.Resources.Limits {
			if limit.Sign() < 0 {
				return nil, fmt.Errorf("negative Pod-level %s limit", name)
			}
			if _, exists := pod.Spec.Resources.Requests[name]; !exists {
				maxResources(regular, corev1.ResourceList{name: limit})
			}
		}
	}
	for name, quantity := range pod.Spec.Overhead {
		if quantity.Sign() < 0 {
			return nil, fmt.Errorf("negative Pod overhead for %s", name)
		}
	}
	addResources(regular, pod.Spec.Overhead)
	return regular, nil
}

func addResources(destination, source corev1.ResourceList) {
	for name, quantity := range source {
		current := destination[name]
		current.Add(quantity)
		destination[name] = current
	}
}

func maxResources(destination, source corev1.ResourceList) {
	for name, quantity := range source {
		current, found := destination[name]
		if !found || current.Cmp(quantity) < 0 {
			destination[name] = quantity.DeepCopy()
		}
	}
}
