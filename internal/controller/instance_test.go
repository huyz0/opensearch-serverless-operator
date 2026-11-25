package controller_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	kubernetesfake "k8s.io/client-go/kubernetes/fake"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	"github.com/huyz0/opensearch-serverless-operator/api/v1alpha1"
	"github.com/huyz0/opensearch-serverless-operator/internal/controller"
)

func TestReconcileReportsUnverifiedAvailability(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := v1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	instance := &v1alpha1.OpenSearchServerless{
		ObjectMeta: metav1.ObjectMeta{Namespace: "n", Name: "catalog", Generation: 2, UID: "instance"},
		Spec: v1alpha1.OpenSearchServerlessSpec{
			Image: "repo/image@sha256:" + strings.Repeat("a", 64), ServiceAccountName: "runtime",
			Storage: v1alpha1.Storage{Type: "s3", Bucket: "bucket", Region: "region"},
			Pools:   map[string]v1alpha1.Pool{"search": {Zones: map[string]v1alpha1.Zone{"a": {Replicas: 1}}}},
		},
	}
	client := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(instance).WithObjects(instance).Build()
	reconciler := controller.InstanceReconciler{Client: client, Core: kubernetesfake.NewSimpleClientset().CoreV1()}
	request := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "n", Name: "catalog"}}
	if _, err := reconciler.Reconcile(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	var observed v1alpha1.OpenSearchServerless
	if err := client.Get(context.Background(), request.NamespacedName, &observed); err != nil {
		t.Fatal(err)
	}
	condition := meta.FindStatusCondition(observed.Status.Conditions, "Available")
	if condition == nil || condition.Status != metav1.ConditionUnknown || condition.Reason != "EndpointObservationDisabled" || observed.Status.ObservedGeneration != 2 {
		t.Fatalf("unexpected status: %#v", observed.Status)
	}
	if observed.Status.PrimaryStore == nil || observed.Status.PrimaryStore.Bucket != "bucket" {
		t.Fatalf("primary store was not pinned: %#v", observed.Status)
	}
	observed.Spec.Storage.Bucket = "other"
	if err := client.Update(context.Background(), &observed); err != nil {
		t.Fatal(err)
	}
	if _, err := reconciler.Reconcile(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	if err := client.Get(context.Background(), request.NamespacedName, &observed); err != nil {
		t.Fatal(err)
	}
	if condition = meta.FindStatusCondition(observed.Status.Conditions, "Available"); condition == nil || condition.Reason != "StorageIdentityChanged" || observed.Status.PrimaryStore.Bucket != "bucket" {
		t.Fatalf("store redirection was not blocked: %#v", observed.Status)
	}
	observed.Spec.Image = "unversioned"
	if err := client.Update(context.Background(), &observed); err != nil {
		t.Fatal(err)
	}
	if _, err := reconciler.Reconcile(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	if err := client.Get(context.Background(), request.NamespacedName, &observed); err != nil {
		t.Fatal(err)
	}
	if condition = meta.FindStatusCondition(observed.Status.Conditions, "Available"); condition == nil || condition.Reason != "InvalidSpec" {
		t.Fatalf("invalid spec was not reported: %#v", observed.Status)
	}
}

func TestReconcileReturnsStatusConflict(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := v1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	instance := &v1alpha1.OpenSearchServerless{ObjectMeta: metav1.ObjectMeta{Namespace: "n", Name: "catalog"}}
	conflict := apierrors.NewConflict(v1alpha1.GroupVersion.WithResource("opensearchserverlesses").GroupResource(), "catalog", errors.New("concurrent status update"))
	client := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(instance).WithObjects(instance).WithInterceptorFuncs(interceptor.Funcs{
		SubResourcePatch: func(ctx context.Context, c client.Client, subResource string, obj client.Object, patch client.Patch, opts ...client.SubResourcePatchOption) error {
			return conflict
		},
	}).Build()
	reconciler := controller.InstanceReconciler{Client: client}
	_, err := reconciler.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "n", Name: "catalog"}})
	if !apierrors.IsConflict(err) {
		t.Fatalf("expected conflict for retry, got %v", err)
	}
}

func TestReconcileSkipsUnchangedStatus(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := v1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	instance := &v1alpha1.OpenSearchServerless{ObjectMeta: metav1.ObjectMeta{Namespace: "n", Name: "catalog"}}
	patches := 0
	client := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(instance).WithObjects(instance).WithInterceptorFuncs(interceptor.Funcs{
		SubResourcePatch: func(ctx context.Context, c client.Client, subResource string, obj client.Object, patch client.Patch, opts ...client.SubResourcePatchOption) error {
			patches++
			return c.SubResource(subResource).Patch(ctx, obj, patch, opts...)
		},
	}).Build()
	reconciler := controller.InstanceReconciler{Client: client}
	request := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "n", Name: "catalog"}}
	for i := 0; i < 2; i++ {
		if _, err := reconciler.Reconcile(context.Background(), request); err != nil {
			t.Fatal(err)
		}
	}
	if patches != 1 {
		t.Fatalf("expected one status patch, got %d", patches)
	}
}
