package controller

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	kubernetesfake "k8s.io/client-go/kubernetes/fake"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/huyz0/opensearch-serverless-operator/api/v1alpha1"
	"github.com/huyz0/opensearch-serverless-operator/internal/workload"
)

func TestRollbackHandoffRecoversMixedSelectors(t *testing.T) {
	ctx := context.Background()
	instance := &v1alpha1.OpenSearchServerless{ObjectMeta: metav1.ObjectMeta{Name: "catalog", Namespace: "n", UID: "instance", Generation: 1}}
	promotion := &v1alpha1.OpenSearchServerlessOperation{
		ObjectMeta: metav1.ObjectMeta{Name: "promotion", Namespace: "n", UID: "promotion", Generation: 1},
		Spec: v1alpha1.OperationSpec{
			Action: "Promote", InstanceRef: v1alpha1.InstanceReference{Name: instance.Name, UID: string(instance.UID)}, TargetFleet: "green",
			ExpectedServingFleetByRole: map[string]string{"search": "blue", "ingest": "blue"},
		},
	}
	instance.Status.ServingFleetIntentByRole = map[string]string{"search": "blue", "ingest": "blue"}
	claim := operationClaimFor(promotion)
	instance.Status.ActiveOperation = &claim
	promotion.Status.RolloutRoles = map[string]v1alpha1.RolloutRoleStatus{
		"search": {ServiceUID: "search", SourceFleet: "blue", SourceRevision: "previous-search", TargetFleet: "green", CandidateRevision: "new", Phase: "Prepared"},
		"ingest": {ServiceUID: "ingest", SourceFleet: "blue", SourceRevision: "previous-ingest", TargetFleet: "green", CandidateRevision: "new", Phase: "Prepared"},
	}
	rollback := &v1alpha1.OpenSearchServerlessOperation{
		ObjectMeta: metav1.ObjectMeta{Name: "rollback", Namespace: "n", UID: "rollback", Generation: 1},
		Spec: v1alpha1.OperationSpec{
			Action: "Rollback", InstanceRef: promotion.Spec.InstanceRef, TargetFleet: "blue",
			ExpectedServingFleetByRole: map[string]string{"search": "green", "ingest": "blue"}, SupersedesOperationUID: string(promotion.UID),
		},
	}
	// Search switched, but the promotion did not record the result yet.
	search, err := workload.RoleService(instance, "search", "green", 9200)
	if err != nil {
		t.Fatal(err)
	}
	ingest, err := workload.RoleService(instance, "ingest", "blue", 9200)
	if err != nil {
		t.Fatal(err)
	}
	search.UID, search.ResourceVersion = "search", "1"
	search.Spec.Type, search.Spec.ClusterIP = corev1.ServiceTypeClusterIP, "10.0.0.1"
	ingest.UID, ingest.ResourceVersion = "ingest", "1"
	ingest.Spec.Type, ingest.Spec.ClusterIP = corev1.ServiceTypeClusterIP, "10.0.0.2"
	core := kubernetesfake.NewSimpleClientset(search, ingest)
	scheme := runtime.NewScheme()
	if err := v1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	c := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(instance, promotion, rollback).WithObjects(instance, promotion, rollback).Build()
	r := &OperationReconciler{Client: c, Core: core.CoreV1()}
	key := client.ObjectKeyFromObject(rollback)
	if transferred, err := r.prepareRollbackHandoff(ctx, c, key, rollback.UID); err != nil || transferred {
		t.Fatalf("expected a staged rollback only: transferred=%t err=%v", transferred, err)
	}
	if err := c.Get(ctx, client.ObjectKeyFromObject(instance), instance); err != nil {
		t.Fatal(err)
	}
	if *instance.Status.ActiveOperation != claim || instance.Status.ServingFleetIntentByRole["search"] != "blue" {
		t.Fatalf("staging changed instance claim or serving mapping: %#v", instance.Status)
	}
	restarted := &OperationReconciler{Client: c, Core: core.CoreV1()}
	services := core.CoreV1().Services(instance.Namespace)
	search.Spec.Selector[workload.FleetLabel] = "blue"
	if _, err := services.Update(ctx, search, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	if transferred, err := restarted.prepareRollbackHandoff(ctx, c, key, rollback.UID); err == nil || transferred {
		t.Fatalf("handoff accepted a stale route mapping: transferred=%t err=%v", transferred, err)
	}
	search.Spec.Selector[workload.FleetLabel] = "green"
	if _, err := services.Update(ctx, search, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	core.ClearActions()
	for i := 0; i < 2; i++ {
		if transferred, err := restarted.prepareRollbackHandoff(ctx, c, key, rollback.UID); err != nil || !transferred {
			t.Fatalf("handoff or retry failed: transferred=%t err=%v", transferred, err)
		}
	}
	if err := c.Get(ctx, client.ObjectKeyFromObject(instance), instance); err != nil {
		t.Fatal(err)
	}
	if err := c.Get(ctx, key, rollback); err != nil {
		t.Fatal(err)
	}
	if *instance.Status.ActiveOperation != operationClaimFor(rollback) || instance.Status.ServingFleetIntentByRole["search"] != "green" || instance.Status.ServingFleetIntentByRole["ingest"] != "blue" {
		t.Fatalf("handoff did not preserve live routing: %#v", instance.Status)
	}
	roles, err := rolloutProgress(instance, rollback)
	if err != nil || roles[0].Intent.AdoptExisting || !roles[1].Intent.AdoptExisting {
		t.Fatalf("expected only search to require a patch: %#v %v", roles, err)
	}
	if roles[0].Intent.CandidateRevision != "previous-search" || roles[1].Intent.CandidateRevision != "previous-ingest" {
		t.Fatalf("rollback did not recover the recorded original revisions: %#v", roles)
	}
	if _, err := rolloutProgress(instance, promotion); err == nil {
		t.Fatal("superseded promotion retained authority")
	}
	for _, action := range core.Actions() {
		if action.GetVerb() != "get" {
			t.Fatalf("rollback handoff changed a Service: %#v", action)
		}
	}
}
