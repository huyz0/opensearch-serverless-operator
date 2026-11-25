package kubernetes_test

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/huyz0/opensearch-serverless-operator/pkg/podlifecycle"
	"github.com/huyz0/opensearch-serverless-operator/pkg/podlifecycle/kubernetes"
)

func TestReleaseSchedulingGatePreservesOtherGates(t *testing.T) {
	controller := true
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
		Namespace: "n", Name: "p", UID: "pod-uid", ResourceVersion: "12",
		OwnerReferences: []metav1.OwnerReference{{UID: "owner-uid", Controller: &controller}},
	}, Spec: corev1.PodSpec{SchedulingGates: []corev1.PodSchedulingGate{{Name: "operator.example/hold"}, {Name: "other.example/hold"}}}}
	client := fake.NewSimpleClientset(pod)
	target := podlifecycle.Identity{Namespace: "n", Name: "p", UID: "pod-uid", OwnerUID: "owner-uid"}
	if err := kubernetes.ReleaseSchedulingGate(context.Background(), client.CoreV1(), target, "operator.example/hold"); err != nil {
		t.Fatal(err)
	}
	updated, err := client.CoreV1().Pods("n").Get(context.Background(), "p", metav1.GetOptions{})
	if err != nil || len(updated.Spec.SchedulingGates) != 1 || updated.Spec.SchedulingGates[0].Name != "other.example/hold" {
		t.Fatalf("other scheduling gate must remain: pod=%v err=%v", updated, err)
	}
	stale := target
	stale.UID = "old-uid"
	if err := kubernetes.ReleaseSchedulingGate(context.Background(), client.CoreV1(), stale, "other.example/hold"); err == nil {
		t.Fatal("stale UID must be rejected")
	}
}

func TestReleaseSchedulingGateRejectsChangedVersion(t *testing.T) {
	controller := true
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: "n", Name: "p", UID: "pod", ResourceVersion: "new", OwnerReferences: []metav1.OwnerReference{{UID: "owner", Controller: &controller}}}, Spec: corev1.PodSpec{SchedulingGates: []corev1.PodSchedulingGate{{Name: "operator.example/hold"}}}}
	client := fake.NewSimpleClientset(pod)
	target := podlifecycle.Identity{Namespace: "n", Name: "p", UID: "pod", OwnerUID: "owner"}
	if err := kubernetes.ReleaseSchedulingGateAtVersion(context.Background(), client.CoreV1(), target, "operator.example/hold", "old"); err == nil {
		t.Fatal("changed resource version must block gate release")
	}
	if len(client.Actions()) != 1 {
		t.Fatal("stale version must not submit an update")
	}
}

func TestReleaseSchedulingGateRefusesBoundTarget(t *testing.T) {
	controller := true
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Namespace: "n", Name: "p", UID: "pod", ResourceVersion: "12",
			OwnerReferences: []metav1.OwnerReference{{UID: "owner", Controller: &controller}}},
		Spec: corev1.PodSpec{NodeName: "node-a", SchedulingGates: []corev1.PodSchedulingGate{{Name: "operator.example/hold"}}},
	}
	client := fake.NewSimpleClientset(pod)
	target := podlifecycle.Identity{Namespace: pod.Namespace, Name: pod.Name, UID: string(pod.UID), OwnerUID: "owner"}
	if err := kubernetes.ReleaseSchedulingGateAtVersion(context.Background(), client.CoreV1(), target, "operator.example/hold", pod.ResourceVersion); err == nil {
		t.Fatal("an already-bound Pod still holding its gate must be rejected")
	}
	if len(client.Actions()) != 1 {
		t.Fatal("a bound Pod must not be updated")
	}
	pod.Spec.SchedulingGates = nil
	client = fake.NewSimpleClientset(pod)
	if err := kubernetes.ReleaseSchedulingGateAtVersion(context.Background(), client.CoreV1(), target, "operator.example/hold", pod.ResourceVersion); err != nil {
		t.Fatalf("a retry after release and binding should be a no-op: %v", err)
	}
	if len(client.Actions()) != 1 {
		t.Fatal("a retry must only observe the released Pod")
	}
}
