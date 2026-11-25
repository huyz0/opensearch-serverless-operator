package workload

import (
	"fmt"

	policyv1 "k8s.io/api/policy/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"

	"github.com/huyz0/opensearch-serverless-operator/api/v1alpha1"
)

// DisruptionBudget builds one non-overlapping PDB for a fleet, role and zone.
// The caller supplies a deterministic Kubernetes-valid name and validates the
// global availability floor separately.
func DisruptionBudget(instance *v1alpha1.OpenSearchServerless, name, fleet, role, zone string, minServing int32) (*policyv1.PodDisruptionBudget, error) {
	if instance == nil || instance.UID == "" || instance.Name == "" || instance.Namespace == "" || name == "" || fleet == "" || zone == "" || minServing < 0 {
		return nil, fmt.Errorf("instance identity, budget name, fleet, zone and nonnegative floor are required")
	}
	if role != "search" && role != "ingest" {
		return nil, fmt.Errorf("unsupported role %q", role)
	}
	controller := true
	minimum := intstr.FromInt32(minServing)
	return &policyv1.PodDisruptionBudget{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: instance.Namespace, OwnerReferences: []metav1.OwnerReference{{
			APIVersion: v1alpha1.GroupVersion.String(), Kind: "OpenSearchServerless", Name: instance.Name, UID: instance.UID,
			Controller: &controller,
		}}},
		Spec: policyv1.PodDisruptionBudgetSpec{
			MinAvailable: &minimum,
			Selector: &metav1.LabelSelector{MatchLabels: map[string]string{
				InstanceLabel: string(instance.UID), FleetLabel: fleet, RoleLabel: role, ZoneLabel: zone,
			}},
		},
	}, nil
}
