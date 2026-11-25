package workload_test

import (
	"context"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/huyz0/opensearch-serverless-operator/api/v1alpha1"
	"github.com/huyz0/opensearch-serverless-operator/internal/workload"
)

func TestSwitchServiceFleetRequiresExpectedSelector(t *testing.T) {
	instance := &v1alpha1.OpenSearchServerless{ObjectMeta: metav1.ObjectMeta{Name: "catalog", Namespace: "n", UID: "owner"}}
	service, err := workload.RoleService(instance, "search", "blue", 9200)
	if err != nil {
		t.Fatal(err)
	}
	service.UID = "service-uid"
	service.ResourceVersion = "12"
	client := fake.NewSimpleClientset(service)
	if err := workload.SwitchServiceFleet(context.Background(), client.CoreV1(), "n", service.Name, service.UID, instance.UID, "search", "blue", "green"); err != nil {
		t.Fatal(err)
	}
	updated, err := client.CoreV1().Services("n").Get(context.Background(), service.Name, metav1.GetOptions{})
	if err != nil || updated.Spec.Selector[workload.FleetLabel] != "green" {
		t.Fatalf("expected green selector, got %v: %v", updated, err)
	}
	if err := workload.SwitchServiceFleet(context.Background(), client.CoreV1(), "n", service.Name, service.UID, instance.UID, "search", "blue", "green"); err == nil {
		t.Fatal("stale expected fleet must be rejected")
	}
}
