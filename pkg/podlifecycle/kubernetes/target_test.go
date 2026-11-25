package kubernetes_test

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/huyz0/opensearch-serverless-operator/pkg/podlifecycle"
	"github.com/huyz0/opensearch-serverless-operator/pkg/podlifecycle/kubernetes"
)

func TestMutationsRequireKubernetesClient(t *testing.T) {
	ctx := context.Background()
	target := podlifecycle.Identity{Namespace: "n", Name: "p", UID: "pod", OwnerUID: "owner"}
	controller := true
	desired := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
		Namespace: "n", Name: "p",
		Annotations:     map[string]string{"intent": "planned"},
		OwnerReferences: []metav1.OwnerReference{{UID: "owner", Controller: &controller}},
	}}
	for _, test := range []struct {
		name string
		run  func() error
	}{
		{name: "create", run: func() error {
			_, err := kubernetes.CreatePod(ctx, nil, desired, "intent", "planned")
			return err
		}},
		{name: "release", run: func() error {
			return kubernetes.ReleaseSchedulingGateAtVersion(ctx, nil, target, "example.org/hold", "12")
		}},
		{name: "withdraw", run: func() error {
			return kubernetes.WithdrawGatedAtVersion(ctx, nil, target, "example.org/hold", "12")
		}},
		{name: "evict", run: func() error { return kubernetes.EvictAtVersion(ctx, nil, target, "12") }},
	} {
		t.Run(test.name, func(t *testing.T) {
			if err := test.run(); err == nil {
				t.Fatal("mutation accepted an absent Kubernetes client")
			}
		})
	}
}
