package controller

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	kubernetesfake "k8s.io/client-go/kubernetes/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/huyz0/opensearch-serverless-operator/api/v1alpha1"
	"github.com/huyz0/opensearch-serverless-operator/internal/java"
)

func TestWarmupSlotSurvivesTimeoutAndProcessRestart(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := v1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	instance := &v1alpha1.OpenSearchServerless{ObjectMeta: metav1.ObjectMeta{Name: "instance", Namespace: "n", UID: "owner"}, Status: v1alpha1.OpenSearchServerlessStatus{SearchWarmupSlot: &v1alpha1.SearchWarmupSlot{Name: "pod", PodUID: "pod-uid"}}}
	owner := true
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "pod", Namespace: "n", UID: "pod-uid", ResourceVersion: "1", OwnerReferences: []metav1.OwnerReference{{UID: "owner", Controller: &owner}}}, Spec: corev1.PodSpec{NodeName: "node"}, Status: corev1.PodStatus{PodIP: "127.0.0.1", ContainerStatuses: []corev1.ContainerStatus{{Name: "opensearch", ContainerID: "container-1", State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}}}}}}
	core := kubernetesfake.NewSimpleClientset(pod)
	kube := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(instance).WithObjects(instance).Build()
	observation := java.Warmup{ProcessIncarnation: "process-1", Result: "running", Ready: true, TimedOut: true}
	r := &InstanceReconciler{Client: kube, Core: core.CoreV1(), ObserveWarmup: func(context.Context, *corev1.Pod) (java.Warmup, error) { return observation, nil }}
	key := types.NamespacedName{Namespace: "n", Name: "instance"}
	step := func() {
		t.Helper()
		if err := kube.Get(context.Background(), key, instance); err != nil {
			t.Fatal(err)
		}
		if _, _, err := r.reconcileWarmupSlot(context.Background(), instance); err != nil {
			t.Fatal(err)
		}
	}
	step()
	step()
	if err := kube.Get(context.Background(), key, instance); err != nil || instance.Status.SearchWarmupSlot == nil {
		t.Fatal("timed-out running worker released its slot")
	}
	observation.Result = "completed"
	step() // Persist completion, but do not release on a single sample.
	pod.Status.ContainerStatuses[0].ContainerID = "container-2"
	pod.Status.ContainerStatuses[0].RestartCount = 1
	if _, err := core.CoreV1().Pods("n").Update(context.Background(), pod, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	observation = java.Warmup{ProcessIncarnation: "process-2", Result: "running"}
	step()
	if err := kube.Get(context.Background(), key, instance); err != nil || instance.Status.SearchWarmupSlot == nil || instance.Status.SearchWarmupSlot.ProcessIncarnation != "process-2" {
		t.Fatal("new process must replace stale completion evidence")
	}
	observation.Result = "degraded"
	step()
	step()
	if err := kube.Get(context.Background(), key, instance); err != nil || instance.Status.SearchWarmupSlot != nil {
		t.Fatal("confirmed worker completion should release the slot")
	}
	if instance.Status.LastSearchWarmup == nil || instance.Status.LastSearchWarmup.Result != "degraded" || instance.Status.LastSearchWarmup.ProcessIncarnation != "process-2" {
		t.Fatal("completion history did not preserve the final process and result")
	}
}
