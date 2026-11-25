package kubernetes_test

import (
	"testing"

	"gopkg.in/inf.v0"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/huyz0/opensearch-serverless-operator/pkg/podlifecycle/kubernetes"
)

func TestInsufficientResourcesAccountsForReservedCapacity(t *testing.T) {
	pod := &corev1.Pod{Spec: corev1.PodSpec{Containers: []corev1.Container{{Resources: corev1.ResourceRequirements{
		Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("2"), corev1.ResourceMemory: resource.MustParse("4Gi")},
	}}}}}
	allocatable := corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("4"), corev1.ResourceMemory: resource.MustParse("6Gi")}
	reserved := corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("2"), corev1.ResourceMemory: resource.MustParse("3Gi")}
	insufficient, err := kubernetes.InsufficientResources(pod, allocatable, reserved)
	if err != nil || len(insufficient) != 1 || insufficient[0] != corev1.ResourceMemory {
		t.Fatalf("expected memory shortfall, got %v: %v", insufficient, err)
	}
	reserved[corev1.ResourceMemory] = resource.MustParse("2Gi")
	insufficient, err = kubernetes.InsufficientResources(pod, allocatable, reserved)
	if err != nil || len(insufficient) != 0 {
		t.Fatalf("exact fit should pass, got %v: %v", insufficient, err)
	}
}

func TestFitDoesNotMutateAllocatable(t *testing.T) {
	capacity := *resource.NewDecimalQuantity(*inf.NewDec(2000, 3), resource.DecimalSI)
	allocatable := corev1.ResourceList{corev1.ResourceCPU: capacity}
	reserved := corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("500m")}
	pod := &corev1.Pod{Spec: corev1.PodSpec{Containers: []corev1.Container{{Resources: corev1.ResourceRequirements{
		Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("1500m")},
	}}}}}
	for i := 0; i < 2; i++ {
		insufficient, err := kubernetes.InsufficientResources(pod, allocatable, reserved)
		if err != nil || len(insufficient) != 0 {
			t.Fatalf("fit changed across observations: %v, %v", insufficient, err)
		}
	}
	want := resource.MustParse("2")
	got := allocatable[corev1.ResourceCPU]
	if got.Cmp(want) != 0 {
		t.Fatalf("allocatable changed to %s", got.String())
	}
}
