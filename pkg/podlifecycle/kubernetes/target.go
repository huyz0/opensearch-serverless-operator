package kubernetes

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/typed/core/v1"

	"github.com/huyz0/opensearch-serverless-operator/pkg/podlifecycle"
)

// readMutationTarget returns the live, owned Pod and the client for its
// namespace. Callers still use this resource version on the mutation.
func readMutationTarget(ctx context.Context, core v1.CoreV1Interface, target podlifecycle.Identity, expectedVersion string) (v1.PodInterface, *corev1.Pod, error) {
	if core == nil || target.Namespace == "" || target.Name == "" || target.UID == "" || target.OwnerUID == "" {
		return nil, nil, fmt.Errorf("Kubernetes client and complete Pod and owner identity are required")
	}
	pods := core.Pods(target.Namespace)
	pod, err := pods.Get(ctx, target.Name, metav1.GetOptions{})
	if err != nil {
		return nil, nil, fmt.Errorf("read Pod target: %w", err)
	}
	owner := metav1.GetControllerOf(pod)
	if pod.Namespace != target.Namespace || pod.Name != target.Name || string(pod.UID) != target.UID || owner == nil || string(owner.UID) != target.OwnerUID {
		return nil, nil, fmt.Errorf("Pod target identity or controlling owner changed")
	}
	if pod.DeletionTimestamp != nil || pod.ResourceVersion == "" {
		return nil, nil, fmt.Errorf("Pod target is terminating or has no resource version")
	}
	if expectedVersion != "" && pod.ResourceVersion != expectedVersion {
		return nil, nil, fmt.Errorf("Pod target changed after observation")
	}
	return pods, pod, nil
}

func deleteOptionsForPod(pod *corev1.Pod) metav1.DeleteOptions {
	uid, version := pod.UID, pod.ResourceVersion
	return metav1.DeleteOptions{Preconditions: &metav1.Preconditions{UID: &uid, ResourceVersion: &version}}
}
