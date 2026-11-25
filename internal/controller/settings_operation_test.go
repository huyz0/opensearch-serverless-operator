package controller

import (
	"context"
	"testing"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/huyz0/opensearch-serverless-operator/api/v1alpha1"
)

func TestUnresolvedSettingsWriteSurvivesMissingInstance(t *testing.T) {
	ctx := context.Background()
	scheme := runtime.NewScheme()
	if err := v1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	for _, phase := range []string{settingsWriting, "Prepared"} {
		t.Run(phase, func(t *testing.T) {
			operation := &v1alpha1.OpenSearchServerlessOperation{
				ObjectMeta: metav1.ObjectMeta{Name: "settings", Namespace: "n", UID: "op", Generation: 1, Finalizers: []string{settingsFinalizer}},
				Spec: v1alpha1.OperationSpec{Action: "ApplyClusterSettings", InstanceRef: v1alpha1.InstanceReference{Name: "catalog", UID: "original"},
					ClusterSettings: &v1alpha1.ClusterSettingsPatch{Set: map[string]string{"serverless.bucket": "off"}}},
				Status: v1alpha1.OperationStatus{Phase: phase, SettingsServiceUID: "original-service"},
			}
			kube := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(operation).WithObjects(operation).Build()
			r := &OperationReconciler{Client: kube}
			request := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(operation)}
			if err := kube.Delete(ctx, operation); err != nil {
				t.Fatal(err)
			}
			assertHeld := func() {
				t.Helper()
				if _, err := r.Reconcile(ctx, request); err != nil {
					t.Fatal(err)
				}
				if err := kube.Get(ctx, request.NamespacedName, operation); err != nil {
					t.Fatalf("unresolved settings operation disappeared: %v", err)
				}
				ready := meta.FindStatusCondition(operation.Status.Conditions, "Ready")
				if operation.DeletionTimestamp == nil || len(operation.Finalizers) != 1 || operation.Status.Phase != phase || ready == nil || ready.Reason != "WriteOutcomeUnknown" {
					t.Fatalf("unresolved write lost its recovery record: %+v", operation.Status)
				}
			}
			assertHeld()
			replacement := &v1alpha1.OpenSearchServerless{ObjectMeta: metav1.ObjectMeta{Name: "catalog", Namespace: "n", UID: "replacement"}}
			if err := kube.Create(ctx, replacement); err != nil {
				t.Fatal(err)
			}
			assertHeld()
		})
	}
}
