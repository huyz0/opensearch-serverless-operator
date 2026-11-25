package kubernetes

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/typed/core/v1"
)

// CreatePod creates a deterministically named Pod or recovers a previous
// successful create. The intent key and value must be persisted by the caller
// before this call. A create response or an existing Pod is accepted only
// when its identity, controlling owner and intent match the request.
func CreatePod(ctx context.Context, core v1.CoreV1Interface, desired *corev1.Pod, intentKey, intentValue string) (*corev1.Pod, error) {
	if core == nil {
		return nil, fmt.Errorf("Kubernetes client is required")
	}
	if desired == nil || desired.Namespace == "" || desired.Name == "" || intentKey == "" || intentValue == "" {
		return nil, fmt.Errorf("namespace, name and creation intent are required")
	}
	owner := metav1.GetControllerOf(desired)
	if owner == nil || owner.UID == "" || desired.Annotations[intentKey] != intentValue {
		return nil, fmt.Errorf("desired Pod must carry the controlling owner and creation intent")
	}
	pods := core.Pods(desired.Namespace)
	created, err := pods.Create(ctx, desired, metav1.CreateOptions{})
	if apierrors.IsAlreadyExists(err) {
		created, err = pods.Get(ctx, desired.Name, metav1.GetOptions{})
		if err != nil {
			return nil, fmt.Errorf("read existing Pod after create: %w", err)
		}
	} else if err != nil {
		return nil, fmt.Errorf("create Pod: %w", err)
	}
	if created == nil || created.Name != desired.Name || created.Namespace != desired.Namespace ||
		created.UID == "" || created.DeletionTimestamp != nil || created.Annotations[intentKey] != intentValue {
		return nil, fmt.Errorf("admitted Pod identity conflicts with creation intent")
	}
	admittedOwner := metav1.GetControllerOf(created)
	if admittedOwner == nil || admittedOwner.UID != owner.UID || admittedOwner.Name != owner.Name ||
		admittedOwner.Kind != owner.Kind || admittedOwner.APIVersion != owner.APIVersion {
		return nil, fmt.Errorf("admitted Pod controller reference conflicts with creation intent")
	}
	return created, nil
}
