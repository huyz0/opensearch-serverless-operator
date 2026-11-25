package controller

import (
	"context"
	"testing"

	policyv1 "k8s.io/api/policy/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/kubernetes/fake"
	clienttesting "k8s.io/client-go/testing"
	clientfake "sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/huyz0/opensearch-serverless-operator/api/v1alpha1"
	"github.com/huyz0/opensearch-serverless-operator/internal/workload"
)

func TestSearchBudgetCannotLowerAnUnobservedFloor(t *testing.T) {
	ctx := context.Background()
	scheme := runtime.NewScheme()
	if err := v1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	for _, initialFloor := range []int32{0, 1} {
		name := "new positive floor"
		if initialFloor > 0 {
			name = "increased positive floor"
		}
		t.Run(name, func(t *testing.T) {
			instance := &v1alpha1.OpenSearchServerless{
				ObjectMeta: metav1.ObjectMeta{Name: "catalog", Namespace: "n", UID: "owner", Generation: 1, ResourceVersion: "1"},
				Spec:       v1alpha1.OpenSearchServerlessSpec{Pools: map[string]v1alpha1.Pool{"search": {Zones: map[string]v1alpha1.Zone{"a": {}}}}},
				Status:     v1alpha1.OpenSearchServerlessStatus{ServingFleetIntentByRole: map[string]string{"search": "blue"}},
			}
			pdb, err := workload.DisruptionBudget(instance, disruptionBudgetName(instance, "blue", "search", "a"), "blue", "search", "a", initialFloor)
			if err != nil {
				t.Fatal(err)
			}
			pdb.UID, pdb.ResourceVersion = "budget", "1"
			core := fake.NewSimpleClientset(pdb)
			kube := clientfake.NewClientBuilder().WithScheme(scheme).WithObjects(instance).WithStatusSubresource(instance).Build()
			r := &InstanceReconciler{Client: kube, Policy: core.PolicyV1(), DevelopmentSearchScaleDown: true}
			if result, ready, err := r.reconcileDisruptionBudgets(ctx, instance); err != nil || ready || !result.Requeue {
				t.Fatalf("record initial floor: %+v, %t, %v", result, ready, err)
			}
			reads := 0
			core.PrependReactor("get", "poddisruptionbudgets", func(clienttesting.Action) (bool, runtime.Object, error) {
				reads++
				if reads == 1 {
					return false, nil, nil
				}
				changed := pdb.DeepCopy()
				changed.ResourceVersion = "2"
				floor := intstr.FromInt32(initialFloor + 1)
				changed.Spec.MinAvailable = &floor
				return true, changed, nil
			})
			if result, ready, err := r.reconcileDisruptionBudgets(ctx, instance); err != nil || ready || !result.Requeue || reads != 2 {
				t.Fatalf("expected replan after the floor changed: %+v, %t, %v (reads=%d)", result, ready, err, reads)
			}
			for _, action := range core.Actions() {
				if action.Matches("update", "poddisruptionbudgets") {
					budget := action.(clienttesting.UpdateAction).GetObject().(*policyv1.PodDisruptionBudget)
					t.Fatalf("lowered a floor without evidence: %+v", budget.Spec)
				}
			}
		})
	}
}

func TestBudgetReconcileStopsWhenAnOperationClaimsTheInstance(t *testing.T) {
	ctx := context.Background()
	scheme := runtime.NewScheme()
	if err := v1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	instance := &v1alpha1.OpenSearchServerless{
		ObjectMeta: metav1.ObjectMeta{Name: "catalog", Namespace: "n", UID: "owner", Generation: 1, ResourceVersion: "1"},
		Spec:       v1alpha1.OpenSearchServerlessSpec{Pools: map[string]v1alpha1.Pool{"search": {Zones: map[string]v1alpha1.Zone{"a": {Replicas: 1, MinServing: 1}}}}},
		Status:     v1alpha1.OpenSearchServerlessStatus{ServingFleetIntentByRole: map[string]string{"search": "blue"}},
	}
	core := fake.NewSimpleClientset()
	kube := clientfake.NewClientBuilder().WithScheme(scheme).WithObjects(instance).WithStatusSubresource(instance).Build()
	r := &InstanceReconciler{Client: kube, Policy: core.PolicyV1()}
	if result, ready, err := r.reconcileDisruptionBudgets(ctx, instance); err != nil || ready || !result.Requeue {
		t.Fatalf("record budget intent: %+v, %t, %v", result, ready, err)
	}
	claimed := instance.DeepCopy()
	claimed.Status.ActiveOperation = &v1alpha1.OperationClaim{Name: "promote", UID: "operation", Action: "Promote", Generation: 1}
	if err := kube.Status().Update(ctx, claimed); err != nil {
		t.Fatal(err)
	}
	if result, ready, err := r.reconcileDisruptionBudgets(ctx, instance); err != nil || ready || !result.Requeue {
		t.Fatalf("budget reconciliation did not stop at the claim: %+v, %t, %v", result, ready, err)
	}
	for _, action := range core.Actions() {
		if action.Matches("create", "poddisruptionbudgets") {
			t.Fatal("stale instance observation allowed budget creation")
		}
	}
}
