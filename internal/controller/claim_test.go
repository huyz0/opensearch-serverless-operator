package controller_test

import (
	"context"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/huyz0/opensearch-serverless-operator/api/v1alpha1"
	"github.com/huyz0/opensearch-serverless-operator/internal/controller"
)

func TestClaimRejectsStaleTargetAndConflictingIdentity(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := v1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	instance := &v1alpha1.OpenSearchServerless{ObjectMeta: metav1.ObjectMeta{Namespace: "n", Name: "catalog", UID: "current", ResourceVersion: "1"}}
	op := &v1alpha1.OpenSearchServerlessOperation{ObjectMeta: metav1.ObjectMeta{Namespace: "n", Name: "work", UID: "op", ResourceVersion: "1", Generation: 1}, Spec: v1alpha1.OperationSpec{InstanceRef: v1alpha1.InstanceReference{Name: "catalog", UID: "old"}, Action: "Decommission"}}
	client := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(instance).WithObjects(instance).Build()
	if err := controller.ClaimOperation(context.Background(), client, instance, op); err == nil {
		t.Fatal("claim accepted a recreated target")
	}
	op.Spec.InstanceRef.UID = "current"
	instance.Status.ActiveOperation = &v1alpha1.OperationClaim{Name: op.Name, UID: string(op.UID), Action: "Promote", Generation: op.Generation}
	if err := controller.ClaimOperation(context.Background(), client, instance, op); err == nil {
		t.Fatal("claim accepted the same UID with a conflicting action")
	}
}
