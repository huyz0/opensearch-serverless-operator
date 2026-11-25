package kubernetes_test

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/huyz0/opensearch-serverless-operator/pkg/podlifecycle/kubernetes"
)

func TestEffectiveRequestsIncludesNativeSidecarAndOverhead(t *testing.T) {
	requests := func(memory string) corev1.ResourceRequirements {
		return corev1.ResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceMemory: resource.MustParse(memory)}}
	}
	always := corev1.ContainerRestartPolicyAlways
	pod := &corev1.Pod{Spec: corev1.PodSpec{
		Containers: []corev1.Container{{Name: "app", Resources: requests("4Gi")}},
		InitContainers: []corev1.Container{
			{Name: "sidecar", RestartPolicy: &always, Resources: requests("1Gi")},
			{Name: "installer", Resources: requests("7Gi")},
		},
		Overhead: corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("1Gi")},
	}}
	got, err := kubernetes.EffectiveRequests(pod)
	if err != nil {
		t.Fatal(err)
	}
	want := resource.MustParse("9Gi")
	actual := got[corev1.ResourceMemory]
	if actual.Cmp(want) != 0 {
		t.Fatalf("got %s, want %s", actual.String(), want.String())
	}
}

func TestEffectiveRequestsConservativelyAccountsForPodLevelResources(t *testing.T) {
	pod := &corev1.Pod{Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "app", Resources: corev1.ResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("1"), corev1.ResourceMemory: resource.MustParse("4Gi")}}}}, Resources: &corev1.ResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("3"), corev1.ResourceMemory: resource.MustParse("2Gi")}}, Overhead: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("250m"), corev1.ResourceMemory: resource.MustParse("1Gi")}}}
	got, err := kubernetes.EffectiveRequests(pod)
	if err != nil {
		t.Fatal(err)
	}
	cpu, memory := got[corev1.ResourceCPU], got[corev1.ResourceMemory]
	if cpu.Cmp(resource.MustParse("3250m")) != 0 || memory.Cmp(resource.MustParse("5Gi")) != 0 {
		t.Fatalf("unexpected conservative requests: %v", got)
	}
	pod.Spec.Resources.Requests = nil
	pod.Spec.Resources.Limits = corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("5")}
	got, err = kubernetes.EffectiveRequests(pod)
	if err != nil {
		t.Fatal(err)
	}
	cpu = got[corev1.ResourceCPU]
	if cpu.Cmp(resource.MustParse("5250m")) != 0 {
		t.Fatalf("missing Pod request did not conservatively account for limit: %s", cpu.String())
	}
}

func TestEffectiveRequestsRejectsUnqualifiedClaimsAndNegativeResources(t *testing.T) {
	pod := &corev1.Pod{Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "app"}}}}
	pod.Spec.ResourceClaims = []corev1.PodResourceClaim{{Name: "device"}}
	if _, err := kubernetes.EffectiveRequests(pod); err == nil {
		t.Fatal("Pod dynamic claim must fail closed")
	}
	pod.Spec.ResourceClaims = nil
	pod.Spec.Containers[0].Resources.Claims = []corev1.ResourceClaim{{Name: "device"}}
	if _, err := kubernetes.EffectiveRequests(pod); err == nil {
		t.Fatal("container dynamic claim must fail closed")
	}
	pod.Spec.Containers[0].Resources.Claims = nil
	pod.Spec.Containers[0].Resources.Requests = corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("-1")}
	if _, err := kubernetes.EffectiveRequests(pod); err == nil {
		t.Fatal("negative container request must fail closed")
	}
}
