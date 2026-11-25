package kubernetes_test

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
	clienttesting "k8s.io/client-go/testing"

	"github.com/huyz0/opensearch-serverless-operator/pkg/podlifecycle"
	"github.com/huyz0/opensearch-serverless-operator/pkg/podlifecycle/kubernetes"
)

func TestWithdrawUnboundUsesVersionPrecondition(t *testing.T) {
	controller := true
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
		Namespace: "n", Name: "p", UID: "pod-uid", ResourceVersion: "12",
		OwnerReferences: []metav1.OwnerReference{{UID: "owner-uid", Controller: &controller}},
	}}
	target := podlifecycle.Identity{Namespace: "n", Name: "p", UID: "pod-uid", OwnerUID: "owner-uid"}
	client := fake.NewSimpleClientset(pod)
	if err := kubernetes.WithdrawUnboundAtVersion(context.Background(), client.CoreV1(), target, "11"); err == nil || len(client.Actions()) != 1 {
		t.Fatalf("a stale observation reached deletion: %v", err)
	}
	client.ClearActions()
	if err := kubernetes.WithdrawUnboundAtVersion(context.Background(), client.CoreV1(), target, "12"); err != nil {
		t.Fatal(err)
	}
	action := client.Actions()[1].(clienttesting.DeleteAction)
	conditions := action.GetDeleteOptions().Preconditions
	if conditions == nil || conditions.UID == nil || conditions.ResourceVersion == nil || string(*conditions.UID) != target.UID || *conditions.ResourceVersion != "12" {
		t.Fatalf("unexpected delete preconditions: %#v", conditions)
	}
}

func TestWithdrawRejectsBoundPod(t *testing.T) {
	controller := true
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
		Namespace: "n", Name: "p", UID: "pod-uid", ResourceVersion: "12",
		OwnerReferences: []metav1.OwnerReference{{UID: "owner-uid", Controller: &controller}},
	}, Spec: corev1.PodSpec{NodeName: "node-a"}}
	client := fake.NewSimpleClientset(pod)
	err := kubernetes.WithdrawUnbound(context.Background(), client.CoreV1(), podlifecycle.Identity{Namespace: "n", Name: "p", UID: "pod-uid", OwnerUID: "owner-uid"})
	if err == nil || len(client.Actions()) != 1 {
		t.Fatalf("bound Pod must not be deleted, err=%v actions=%v", err, client.Actions())
	}
}

func TestWithdrawGatedAtVersionRequiresExactGateAndIdentity(t *testing.T) {
	controller := true
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: "n", Name: "p", UID: "pod-uid", ResourceVersion: "12", OwnerReferences: []metav1.OwnerReference{{UID: "owner-uid", Controller: &controller}}}, Spec: corev1.PodSpec{SchedulingGates: []corev1.PodSchedulingGate{{Name: "example.org/gate"}}}}
	target := podlifecycle.Identity{Namespace: "n", Name: "p", UID: "pod-uid", OwnerUID: "owner-uid"}
	for _, scenario := range []struct {
		name    string
		mutate  func(*corev1.Pod)
		version string
	}{
		{"gate removed", func(p *corev1.Pod) { p.Spec.SchedulingGates = nil }, "12"},
		{"bound", func(p *corev1.Pod) { p.Spec.NodeName = "node" }, "12"},
		{"UID changed", func(p *corev1.Pod) { p.UID = "replacement" }, "12"},
		{"owner changed", func(p *corev1.Pod) { p.OwnerReferences[0].UID = "other" }, "12"},
		{"version changed", func(p *corev1.Pod) { p.ResourceVersion = "13" }, "12"},
		{"missing expected version", func(p *corev1.Pod) {}, ""},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			changed := pod.DeepCopy()
			scenario.mutate(changed)
			client := fake.NewSimpleClientset(changed)
			if err := kubernetes.WithdrawGatedAtVersion(context.Background(), client.CoreV1(), target, "example.org/gate", scenario.version); err == nil {
				t.Fatal("unsafe gated withdrawal accepted")
			}
			for _, action := range client.Actions() {
				if action.GetVerb() == "delete" {
					t.Fatal("unsafe withdrawal reached delete")
				}
			}
		})
	}
	client := fake.NewSimpleClientset(pod)
	if err := kubernetes.WithdrawGatedAtVersion(context.Background(), client.CoreV1(), target, "example.org/gate", "12"); err != nil {
		t.Fatal(err)
	}
	conditions := client.Actions()[1].(clienttesting.DeleteAction).GetDeleteOptions().Preconditions
	if conditions == nil || conditions.UID == nil || *conditions.UID != pod.UID || conditions.ResourceVersion == nil || *conditions.ResourceVersion != "12" {
		t.Fatal("missing deletion preconditions")
	}
}
