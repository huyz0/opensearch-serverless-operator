package controller

import (
	"context"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/huyz0/opensearch-serverless-operator/api/v1alpha1"
	"github.com/huyz0/opensearch-serverless-operator/internal/workload"
)

func TestPeerDNSServiceCreationAndConflict(t *testing.T) {
	ctx := context.Background()
	instance := &v1alpha1.OpenSearchServerless{ObjectMeta: metav1.ObjectMeta{Name: "catalog", Namespace: "n", UID: "owner"}}
	core := fake.NewSimpleClientset().CoreV1()
	if err := ensurePeerDNSService(ctx, core, instance); err != nil {
		t.Fatal(err)
	}
	want, _ := workload.PeerDNSService(instance)
	service, err := core.Services("n").Get(ctx, want.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if service.Spec.ClusterIP != "None" || !service.Spec.PublishNotReadyAddresses || service.Spec.Selector[workload.InstanceLabel] != "owner" {
		t.Fatal("incorrect peer Service")
	}
	service.Spec.PublishNotReadyAddresses = false
	if _, err := core.Services("n").Update(ctx, service, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	if err := ensurePeerDNSService(ctx, core, instance); err == nil {
		t.Fatal("conflicting Service accepted")
	}
	service, err = core.Services("n").Get(ctx, want.Name, metav1.GetOptions{})
	if err != nil || service.Spec.PublishNotReadyAddresses {
		t.Fatal("conflicting Service was overwritten")
	}
}
