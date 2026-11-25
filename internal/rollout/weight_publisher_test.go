package rollout

import (
	"context"
	"crypto/ed25519"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	kubernetesfake "k8s.io/client-go/kubernetes/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/huyz0/opensearch-serverless-operator/api/v1alpha1"
)

func TestDelayedWeightPublisherPreservesNewerRouteEvidence(t *testing.T) {
	ctx := context.Background()
	const namespace = "tenant"
	instance := &v1alpha1.OpenSearchServerless{
		ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: "catalog", UID: "instance", Generation: 1},
		Spec:       v1alpha1.OpenSearchServerlessSpec{Rollout: &v1alpha1.Rollout{GatewayBackends: true}},
		Status: v1alpha1.OpenSearchServerlessStatus{
			ActiveOperation: &v1alpha1.OperationClaim{Name: "promote", UID: "operation", Action: "Promote", Generation: 1},
		},
	}
	operation := &v1alpha1.OpenSearchServerlessOperation{
		ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: "promote", UID: "operation", Generation: 1},
		Spec: v1alpha1.OperationSpec{
			InstanceRef: v1alpha1.InstanceReference{Name: instance.Name, UID: string(instance.UID)},
			Action:      "Promote", RoutingMode: "WeightedGateway", TargetFleet: "green",
			ExpectedServingFleetByRole: map[string]string{"search": "blue", "ingest": "blue"},
		},
	}
	route := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "gateway.networking.k8s.io/v1", "kind": "HTTPRoute"}}
	route.SetNamespace(namespace)
	route.SetName("search")
	route.SetUID("route")
	route.SetResourceVersion("old-version")
	scheme := runtime.NewScheme()
	if err := v1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	reader := fake.NewClientBuilder().WithScheme(scheme).WithObjects(instance, operation, route).Build()
	now := time.Now()
	old := WeightEvidence{
		WeightEvidenceScope: WeightEvidenceScope{
			Namespace: namespace, InstanceUID: instance.UID, InstanceGeneration: 1,
			OperationUID: operation.UID, Role: "search", RouteName: route.GetName(),
			RouteUID: route.GetUID(), RouteVersion: route.GetResourceVersion(),
		},
		InventoryObservedAt: now.Add(-2 * time.Second), Weights: map[string]int{"blue": 80, "green": 20},
		ServingProxyUIDs: []string{"proxy"},
		Proxies:          []WeightProxyReport{{ProxyUID: "proxy", RouteVersion: route.GetResourceVersion(), ObservedAt: now.Add(-2 * time.Second)}},
	}
	newer := old
	newer.RouteVersion = "new-version"
	newer.InventoryObservedAt = now.Add(-time.Second)
	newer.Proxies = []WeightProxyReport{{ProxyUID: "proxy", RouteVersion: newer.RouteVersion, ObservedAt: newer.InventoryObservedAt}}
	public, signer, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatal(err)
	}
	signed, err := signEvidence(weightEvidenceDomain, newer, signer)
	if err != nil {
		t.Fatal(err)
	}
	name := WeightEvidenceName(operation.UID, "search")
	kube := kubernetesfake.NewSimpleClientset(&corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: name},
		Data:       signed,
	})
	// The delayed reader still sees the route version checked before the cutover.
	if err := PublishWeightEvidence(ctx, reader, kube.CoreV1(), namespace, operation.Name, old, signer); err != nil {
		t.Fatal(err)
	}
	stored, err := kube.CoreV1().ConfigMaps(namespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	report, err := readSignedWeightEvidence(stored.Data, public)
	if err != nil || report.RouteVersion != newer.RouteVersion {
		t.Fatalf("the newer route report was replaced: revision=%q error=%v", report.RouteVersion, err)
	}
}
