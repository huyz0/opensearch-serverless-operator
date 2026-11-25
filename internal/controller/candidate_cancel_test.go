package controller

import (
	"context"
	"fmt"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	kubernetesfake "k8s.io/client-go/kubernetes/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/huyz0/opensearch-serverless-operator/api/v1alpha1"
	"github.com/huyz0/opensearch-serverless-operator/internal/workload"
)

func TestCancelledCandidateWithdrawalPinsIntentBeforeEffect(t *testing.T) {
	ctx := context.Background()
	instance := &v1alpha1.OpenSearchServerless{ObjectMeta: metav1.ObjectMeta{Name: "catalog", Namespace: "n", UID: "owner", Generation: 2}, Status: v1alpha1.OpenSearchServerlessStatus{PodCreationSequence: 1, ServingFleetIntentByRole: map[string]string{"search": "blue", "ingest": "blue"}, CandidateFleet: &v1alpha1.CandidateFleetStatus{Fleet: "green", Revision: "revision"}}}
	owner := true
	name := podNameForSequence(instance, 1)
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "n", UID: "pod", ResourceVersion: "1", OwnerReferences: []metav1.OwnerReference{{APIVersion: v1alpha1.GroupVersion.String(), Kind: "OpenSearchServerless", Name: "catalog", UID: "owner", Controller: &owner}}, Labels: map[string]string{workload.InstanceLabel: "owner", workload.RoleLabel: "search", workload.ZoneLabel: "a", workload.FleetLabel: "green", "serverless.opensearch.org/revision": "revision"}, Annotations: map[string]string{creationIntentKey: fmt.Sprintf("%s/%d", instance.UID, 1)}}, Spec: corev1.PodSpec{SchedulingGates: []corev1.PodSchedulingGate{{Name: "serverless.opensearch.org/placement"}}}}
	scheme := runtime.NewScheme()
	if err := v1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	kube := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(instance).WithObjects(instance).Build()
	core := kubernetesfake.NewSimpleClientset(pod)
	r := &InstanceReconciler{Client: kube, Core: core.CoreV1()}
	if err := kube.Get(ctx, types.NamespacedName{Namespace: "n", Name: "catalog"}, instance); err != nil {
		t.Fatal(err)
	}
	instance.Status.ServingFleetIntentByRole["search"] = "green"
	if changed, err := r.retireCancelledCandidateGated(ctx, instance); err != nil || changed || len(instance.Status.RetiredPodCreations) != 0 {
		t.Fatalf("serving candidate must bypass gated withdrawal without recording a retirement: %v", err)
	}
	instance.Status.ServingFleetIntentByRole["search"] = "blue"
	if changed, err := r.retireCancelledCandidateGated(ctx, instance); !changed || err != nil || len(instance.Status.RetiredPodCreations) != 1 {
		t.Fatalf("retirement was not recorded: %v", err)
	}
	if _, err := core.CoreV1().Pods("n").Get(ctx, name, metav1.GetOptions{}); err != nil {
		t.Fatal("Pod deleted before the recorded withdrawal was observed")
	}
	pod.Spec.SchedulingGates = nil
	if _, err := core.CoreV1().Pods("n").Update(ctx, pod, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, _, err := r.reconcileRetiredPodCreations(ctx, instance); err == nil {
		t.Fatal("released candidate Pod accepted for gated withdrawal")
	}
	if _, err := core.CoreV1().Pods("n").Get(ctx, name, metav1.GetOptions{}); err != nil {
		t.Fatal("released Pod was deleted")
	}
}

func TestCancelledCandidateBindingRaceDoesNotDeleteBoundPod(t *testing.T) {
	ctx := context.Background()
	instance := &v1alpha1.OpenSearchServerless{ObjectMeta: metav1.ObjectMeta{Name: "catalog", Namespace: "n", UID: "owner", Generation: 2}, Status: v1alpha1.OpenSearchServerlessStatus{PodCreationSequence: 1, ServingFleetIntentByRole: map[string]string{"search": "blue", "ingest": "blue"}, CandidateFleet: &v1alpha1.CandidateFleetStatus{Fleet: "green", Revision: "revision"}}}
	owner := true
	name := podNameForSequence(instance, 1)
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "n", UID: "pod", ResourceVersion: "1", OwnerReferences: []metav1.OwnerReference{{APIVersion: v1alpha1.GroupVersion.String(), Kind: "OpenSearchServerless", Name: "catalog", UID: "owner", Controller: &owner}}, Labels: map[string]string{workload.InstanceLabel: "owner", workload.RoleLabel: "search", workload.ZoneLabel: "a", workload.FleetLabel: "green", "serverless.opensearch.org/revision": "revision"}, Annotations: map[string]string{creationIntentKey: "owner/1"}}}
	instance.Status.ProvisioningAttempt = &v1alpha1.ProvisioningAttempt{Name: name, PodUID: "pod", Role: "search", Zone: "a", StartedAt: metav1.NewTime(time.Now()), Phase: "Watching"}
	scheme := runtime.NewScheme()
	if err := v1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	kube := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(instance).WithObjects(instance).Build()
	core := kubernetesfake.NewSimpleClientset(pod)
	r := &InstanceReconciler{Client: kube, Core: core.CoreV1()}
	if err := kube.Get(ctx, types.NamespacedName{Namespace: "n", Name: "catalog"}, instance); err != nil {
		t.Fatal(err)
	}
	if _, changed, err := r.reconcileProvisioningAttempt(ctx, instance); err != nil || !changed || instance.Status.ProvisioningAttempt.Reason != "CandidateCancelled" {
		t.Fatalf("cancellation not persisted: %v", err)
	}
	pod.Spec.NodeName = "node-a"
	pod.ResourceVersion = "2"
	if _, err := core.CoreV1().Pods("n").Update(ctx, pod, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, _, err := r.reconcileProvisioningAttempt(ctx, instance); err != nil {
		t.Fatal(err)
	}
	still, err := core.CoreV1().Pods("n").Get(ctx, name, metav1.GetOptions{})
	if err != nil || still.Spec.NodeName != "node-a" {
		t.Fatalf("bound Pod was withdrawn: %v", err)
	}
}
