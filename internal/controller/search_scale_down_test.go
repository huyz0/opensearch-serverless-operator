package controller

import (
	"context"
	"testing"

	coreapi "k8s.io/api/core/v1"
	policyapi "k8s.io/api/policy/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/huyz0/opensearch-serverless-operator/api/v1alpha1"
	"github.com/huyz0/opensearch-serverless-operator/internal/workload"
	"github.com/huyz0/opensearch-serverless-operator/pkg/podlifecycle"
)

func TestSearchScaleDownBootstrapSurplus(t *testing.T) {
	for _, test := range []struct {
		name         string
		regularA     int
		regularB     int
		ready        bool
		regional     int32
		wantEviction bool
	}{
		{name: "both zones retain target including reserve", regularA: 2, regularB: 1, regional: 3, ready: true, wantEviction: true},
		{name: "bootstrap does not count toward its zone reserve", regularA: 1, regularB: 2, regional: 3, ready: true},
		{name: "other zone target is missing", regularA: 3, regularB: 0, regional: 3, ready: true},
		{name: "regional floor is not met", regularA: 2, regularB: 1, regional: 4, ready: true},
		{name: "bootstrap endpoint is absent", regularA: 2, regularB: 1, regional: 3},
	} {
		t.Run(test.name, func(t *testing.T) {
			instance := &v1alpha1.OpenSearchServerless{
				ObjectMeta: metav1.ObjectMeta{Name: "catalog", Namespace: "work", UID: "instance"},
				Spec: v1alpha1.OpenSearchServerlessSpec{
					Availability: &v1alpha1.Availability{MaxConcurrentRetirements: 1, MinServingTotal: map[string]int32{"search": test.regional}},
					Pools: map[string]v1alpha1.Pool{"search": {Zones: map[string]v1alpha1.Zone{
						"zone-a": {Replicas: 1, WarmSpare: 1, MinServing: 1},
						"zone-b": {Replicas: 1, MinServing: 1},
					}}},
				},
			}
			bootstrap := podlifecycle.Identity{Namespace: "work", Name: "bootstrap", UID: "bootstrap-uid", OwnerUID: "instance"}
			observed := servingRoleProtection{
				Fleet: "initial", BootstrapPod: &emergencyFailbackPod{Identity: bootstrap, Zone: "zone-a"},
				RegularEndpointsByZone: map[string]int{"zone-a": test.regularA, "zone-b": test.regularB},
			}
			if test.ready {
				observed.Endpoints.ReadyPodVersions = map[types.UID]string{"bootstrap-uid": "10"}
			}
			action, err := planSearchScaleDown(instance, observed)
			if err != nil {
				t.Fatal(err)
			}
			if test.wantEviction {
				if action.Kind != podlifecycle.EvictPod || action.Pod != bootstrap || action.Group.Zone != "zone-a" {
					t.Fatalf("expected the exact bootstrap Pod, got %+v", action)
				}
			} else if action.Kind != podlifecycle.Wait {
				t.Fatalf("bootstrap must stay in service, got %+v", action)
			}
		})
	}
}

func TestSingleScaleDownBudget(t *testing.T) {
	instance := &v1alpha1.OpenSearchServerless{
		ObjectMeta: metav1.ObjectMeta{Name: "catalog", Namespace: "work", UID: "instance"},
		Spec: v1alpha1.OpenSearchServerlessSpec{Pools: map[string]v1alpha1.Pool{
			"search": {Zones: map[string]v1alpha1.Zone{"zone-a": {Replicas: 1, MinServing: 1}}},
		}},
	}
	group := podlifecycle.Group{Workload: "instance", Fleet: "initial", Role: "search", Zone: "zone-a"}
	name := disruptionBudgetName(instance, group.Fleet, group.Role, group.Zone)
	expected, err := workload.DisruptionBudget(instance, name, group.Fleet, group.Role, group.Zone, 1)
	if err != nil {
		t.Fatal(err)
	}
	expected.Generation = 1
	expected.Status = policyapi.PodDisruptionBudgetStatus{ObservedGeneration: 1, DisruptionsAllowed: 1}
	pod := coreapi.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: instance.Namespace, Name: "search", UID: "victim",
		Labels: expected.Spec.Selector.MatchLabels}}
	action := podlifecycle.Action{Group: group, Pod: failbackPodIdentity(instance, &pod).Identity}
	external := expected.DeepCopy()
	external.Name = "external-overlap"
	external.Spec.Selector = &metav1.LabelSelector{}
	unrelated := expected.DeepCopy()
	unrelated.Name = "unrelated"
	unrelated.Spec.Selector.MatchLabels[workload.RoleLabel] = "ingest"
	for _, test := range []struct {
		name    string
		budgets []runtime.Object
		allowed bool
	}{
		{name: "one matching managed budget", budgets: []runtime.Object{expected, unrelated}, allowed: true},
		{name: "overlapping namespace-wide budget", budgets: []runtime.Object{expected, external}},
		{name: "external budget replaces the expected budget", budgets: []runtime.Object{external}},
	} {
		t.Run(test.name, func(t *testing.T) {
			r := &InstanceReconciler{Policy: fake.NewSimpleClientset(test.budgets...).PolicyV1()}
			if allowed := r.singleScaleDownBudget(context.Background(), instance, []coreapi.Pod{pod}, action); allowed != test.allowed {
				t.Fatalf("retirement allowed=%t, want %t", allowed, test.allowed)
			}
		})
	}
}
