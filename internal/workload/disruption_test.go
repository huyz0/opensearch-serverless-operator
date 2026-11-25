package workload_test

import (
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"

	"github.com/huyz0/opensearch-serverless-operator/api/v1alpha1"
	"github.com/huyz0/opensearch-serverless-operator/internal/workload"
)

func TestDisruptionBudgetsDoNotOverlapAcrossZones(t *testing.T) {
	instance := &v1alpha1.OpenSearchServerless{ObjectMeta: metav1.ObjectMeta{Name: "catalog", Namespace: "n", UID: "uid"}}
	budget, err := workload.DisruptionBudget(instance, "catalog-search-a", "blue", "search", "zone-a", 2)
	if err != nil {
		t.Fatal(err)
	}
	selector, err := metav1.LabelSelectorAsSelector(budget.Spec.Selector)
	if err != nil {
		t.Fatal(err)
	}
	podLabels := labels.Set{workload.InstanceLabel: "uid", workload.FleetLabel: "blue", workload.RoleLabel: "search", workload.ZoneLabel: "zone-b"}
	if selector.Matches(podLabels) || budget.Spec.MinAvailable.IntVal != 2 {
		t.Fatal("PDB must use an integer floor and only select its own zone")
	}
	podLabels[workload.ZoneLabel] = "zone-a"
	if !selector.Matches(podLabels) {
		t.Fatal("PDB must match its own group")
	}
}
