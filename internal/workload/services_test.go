package workload_test

import (
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/huyz0/opensearch-serverless-operator/api/v1alpha1"
	"github.com/huyz0/opensearch-serverless-operator/internal/workload"
)

func TestRoleServicesSelectDifferentRoles(t *testing.T) {
	instance := &v1alpha1.OpenSearchServerless{ObjectMeta: metav1.ObjectMeta{Name: "catalog", Namespace: "n", UID: "instance-uid"}}
	search, err := workload.RoleService(instance, "search", "blue", 9200)
	if err != nil {
		t.Fatal(err)
	}
	ingest, err := workload.RoleService(instance, "ingest", "blue", 9200)
	if err != nil {
		t.Fatal(err)
	}
	if search.Name != "catalog-search" || ingest.Name != "catalog-index" || search.Spec.Selector[workload.RoleLabel] != "search" || ingest.Spec.Selector[workload.RoleLabel] != "ingest" {
		t.Fatalf("unexpected role services: %v %v", search, ingest)
	}
	if search.Spec.Selector[workload.InstanceLabel] != "instance-uid" || search.Spec.Selector[workload.FleetLabel] != "blue" || metav1.GetControllerOf(search).UID != instance.UID {
		t.Fatal("Service must identify the owning instance and serving fleet")
	}
}

func TestPreviewServicesAreDistinctAndSelectOnlyCandidate(t *testing.T) {
	instance := &v1alpha1.OpenSearchServerless{ObjectMeta: metav1.ObjectMeta{Name: "catalog", Namespace: "n", UID: "instance-uid"}}
	search, err := workload.PreviewService(instance, "search", "green")
	if err != nil {
		t.Fatal(err)
	}
	ingest, err := workload.PreviewService(instance, "ingest", "green")
	if err != nil {
		t.Fatal(err)
	}
	other, err := workload.PreviewService(instance, "search", "blue")
	if err != nil {
		t.Fatal(err)
	}
	stable, err := workload.RoleService(instance, "search", "blue", 9200)
	if err != nil {
		t.Fatal(err)
	}
	if search.Name == ingest.Name || search.Name == other.Name || search.Name == stable.Name || search.Spec.Selector[workload.FleetLabel] != "green" || search.Spec.Selector[workload.RoleLabel] != "search" || ingest.Spec.Selector[workload.RoleLabel] != "ingest" || metav1.GetControllerOf(search).UID != instance.UID {
		t.Fatal("preview identity or selector differs")
	}
	if _, err := workload.PreviewService(instance, "search", "bad fleet"); err == nil {
		t.Fatal("invalid fleet label accepted")
	}
}
