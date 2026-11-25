package kubernetes_test

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
	clienttesting "k8s.io/client-go/testing"

	"github.com/huyz0/opensearch-serverless-operator/pkg/podlifecycle"
	"github.com/huyz0/opensearch-serverless-operator/pkg/podlifecycle/kubernetes"
)

func TestEvictUsesUIDAndResourceVersionPreconditions(t *testing.T) {
	controller := true
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
		Namespace: "n", Name: "p", UID: "pod-uid", ResourceVersion: "12",
		OwnerReferences: []metav1.OwnerReference{{UID: "owner-uid", Controller: &controller}},
	}}
	client := fake.NewSimpleClientset(pod)
	target := podlifecycle.Identity{Namespace: "n", Name: "p", UID: "pod-uid", OwnerUID: "owner-uid"}
	if err := kubernetes.Evict(context.Background(), client.CoreV1(), target); err != nil {
		t.Fatal(err)
	}
	actions := client.Actions()
	if len(actions) != 2 || actions[1].GetSubresource() != "eviction" {
		t.Fatalf("expected read and eviction, got %#v", actions)
	}
	eviction := actions[1].(clienttesting.CreateAction).GetObject().(*policyv1.Eviction)
	preconditions := eviction.DeleteOptions.Preconditions
	if preconditions == nil || string(*preconditions.UID) != target.UID || *preconditions.ResourceVersion != "12" {
		t.Fatalf("unexpected preconditions: %#v", preconditions)
	}
}

func TestEvictRejectsStaleOwner(t *testing.T) {
	controller := true
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
		Namespace: "n", Name: "p", UID: "pod-uid", ResourceVersion: "12",
		OwnerReferences: []metav1.OwnerReference{{UID: "new-owner", Controller: &controller}},
	}}
	client := fake.NewSimpleClientset(pod)
	target := podlifecycle.Identity{Namespace: "n", Name: "p", UID: "pod-uid", OwnerUID: "old-owner"}
	if err := kubernetes.Evict(context.Background(), client.CoreV1(), target); err == nil {
		t.Fatal("expected stale owner to be rejected")
	}
	if len(client.Actions()) != 1 {
		t.Fatal("stale owner must not submit an eviction")
	}
}
