package controller_test

import (
	"context"
	"testing"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/huyz0/opensearch-serverless-operator/api/v1alpha1"
	"github.com/huyz0/opensearch-serverless-operator/internal/controller"
)

func TestOperationRejectsRecreatedInstance(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := v1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	instance := &v1alpha1.OpenSearchServerless{ObjectMeta: metav1.ObjectMeta{Namespace: "n", Name: "catalog", UID: "new-uid"}}
	operation := &v1alpha1.OpenSearchServerlessOperation{
		ObjectMeta: metav1.ObjectMeta{Namespace: "n", Name: "stop"},
		Spec:       v1alpha1.OperationSpec{InstanceRef: v1alpha1.InstanceReference{Name: "catalog", UID: "old-uid"}, Action: "Decommission"},
	}
	client := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(operation).WithObjects(instance, operation).Build()
	reconciler := controller.OperationReconciler{Client: client}
	key := types.NamespacedName{Namespace: "n", Name: "stop"}
	if _, err := reconciler.Reconcile(context.Background(), ctrl.Request{NamespacedName: key}); err != nil {
		t.Fatal(err)
	}
	var observed v1alpha1.OpenSearchServerlessOperation
	if err := client.Get(context.Background(), key, &observed); err != nil {
		t.Fatal(err)
	}
	if condition := meta.FindStatusCondition(observed.Status.Conditions, "Ready"); condition == nil || condition.Reason != "InstanceMismatch" {
		t.Fatalf("expected UID mismatch, got %#v", observed.Status)
	}
}
