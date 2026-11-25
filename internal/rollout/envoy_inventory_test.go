package rollout

import (
	"context"
	"crypto/ed25519"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	kubernetesfake "k8s.io/client-go/kubernetes/fake"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/huyz0/opensearch-serverless-operator/api/v1alpha1"
	"github.com/huyz0/opensearch-serverless-operator/internal/workload"
)

func TestEnvoyInventoryRequiresServingReplicasAndCurrentApprovedPods(t *testing.T) {
	ctx := context.Background()
	key, signer, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatal(err)
	}
	instance := &v1alpha1.OpenSearchServerless{
		ObjectMeta: metav1.ObjectMeta{Name: "catalog", Namespace: "tenant", UID: "instance", Generation: 3},
		Spec:       v1alpha1.OpenSearchServerlessSpec{Rollout: &v1alpha1.Rollout{GatewayBackends: true}},
	}
	op := &v1alpha1.OpenSearchServerlessOperation{
		ObjectMeta: metav1.ObjectMeta{Name: "promote", Namespace: "tenant", UID: "operation", Generation: 1},
		Spec: v1alpha1.OperationSpec{
			InstanceRef: v1alpha1.InstanceReference{Name: instance.Name, UID: string(instance.UID)},
			Action:      "Promote", TargetFleet: "green", RoutingMode: "WeightedGateway",
			ExpectedServingFleetByRole: map[string]string{"search": "blue", "ingest": "blue"},
		},
		Status: v1alpha1.OperationStatus{WeightedRollout: &v1alpha1.WeightedRolloutStatus{
			InstanceGeneration: instance.Generation, Roles: map[string]v1alpha1.WeightedRoleStatus{
				"search": {RouteName: "catalog-search", RouteUID: "route",
					BackendUIDs: map[string]string{"blue": "blue-service", "green": "green-service"}, Revisions: map[string]string{"blue": "revision"}},
			},
		}},
	}
	instance.Status.ActiveOperation = &v1alpha1.OperationClaim{Name: op.Name, UID: string(op.UID), Action: op.Spec.Action, Generation: op.Generation}
	instance.Status.GatewayBackends = map[string]v1alpha1.GatewayBackendStatus{
		"search/blue":  {Role: "search", Fleet: "blue", Name: "blue-backend", ServiceUID: "blue-service"},
		"search/green": {Role: "search", Fleet: "green", Name: "green-backend", ServiceUID: "green-service"},
	}
	route := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "gateway.networking.k8s.io/v1", "kind": "HTTPRoute",
		"metadata": map[string]any{"namespace": "tenant", "name": "catalog-search", "uid": "route", "resourceVersion": "20"},
		"spec": map[string]any{"rules": []any{map[string]any{"backendRefs": []any{
			map[string]any{"name": "blue-backend", "port": int64(9200), "weight": int64(100)},
			map[string]any{"name": "green-backend", "port": int64(9200), "weight": int64(0)},
		}}}},
	}}
	svc := &corev1.Service{ObjectMeta: metav1.ObjectMeta{Namespace: "gateway", Name: "envoy", UID: "gateway-service", ResourceVersion: "2"}}
	slice := &discoveryv1.EndpointSlice{
		ObjectMeta: metav1.ObjectMeta{Namespace: svc.Namespace, Name: "envoy-abc", UID: "slice", ResourceVersion: "5",
			OwnerReferences: []metav1.OwnerReference{*metav1.NewControllerRef(svc, corev1.SchemeGroupVersion.WithKind("Service"))},
			Labels:          map[string]string{discoveryv1.LabelServiceName: svc.Name, discoveryv1.LabelManagedBy: "endpointslice-controller.k8s.io"},
		},
		Endpoints: []discoveryv1.Endpoint{
			{Conditions: discoveryv1.EndpointConditions{Ready: ptr.To(true)},
				TargetRef: &corev1.ObjectReference{Kind: "Pod", Namespace: "gateway", Name: "envoy-one", UID: "first"}},
			{Conditions: discoveryv1.EndpointConditions{Ready: ptr.To(true)},
				TargetRef: &corev1.ObjectReference{Kind: "Pod", Namespace: "gateway", Name: "envoy-two", UID: "second"}},
		},
	}
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Namespace: "tenant", Name: "blue-node", UID: "blue-pod", ResourceVersion: "8",
			OwnerReferences: []metav1.OwnerReference{*metav1.NewControllerRef(instance, v1alpha1.GroupVersion.WithKind("OpenSearchServerless"))},
			Labels: map[string]string{workload.InstanceLabel: string(instance.UID), workload.RoleLabel: "search", workload.FleetLabel: "blue",
				workload.ZoneLabel: "az-a", "serverless.opensearch.org/revision": "revision"},
		},
		Spec:   corev1.PodSpec{NodeName: "worker"},
		Status: corev1.PodStatus{Phase: corev1.PodRunning, PodIP: "10.0.0.10"},
	}
	approval := PlatformEvidence{
		Namespace: "tenant", InstanceUID: instance.UID, InstanceGeneration: instance.Generation, OperationUID: op.UID,
		Role: "search", Fleet: "blue", Revision: "revision", ObservedAt: time.Now(),
		PeerQualified: true, SecurityQualified: true, RollbackCompatible: true,
		Pods: []PlatformPod{{UID: pod.UID, ResourceVersion: pod.ResourceVersion, NodeName: pod.Spec.NodeName, Zone: "az-a", ProcessIncarnation: "jvm"}},
	}
	data, err := SignPlatformEvidence(approval, signer)
	if err != nil {
		t.Fatal(err)
	}
	cm := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{
		Namespace: instance.Namespace, Name: FleetPlatformEvidenceName(op.UID, "search", "blue", "green"),
	}, Data: data}
	scheme := runtime.NewScheme()
	if err := v1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	reader := fake.NewClientBuilder().WithScheme(scheme).WithObjects(instance, op, route).Build()
	kube := kubernetesfake.NewSimpleClientset(svc, slice, pod, cm)
	input := KubernetesEnvoyInventory{
		Reader: reader, Kube: kube, Namespace: instance.Namespace, Operation: op.Name, Role: "search",
		GatewayService: client.ObjectKeyFromObject(svc), GatewayServiceUID: svc.UID, PlatformKey: key,
		Tunnels: map[string]EnvoyTunnel{"first": {AdminAddress: "127.0.0.1:19000", ClientAddress: "127.0.0.1:10443"}},
	}
	if _, err := input.Read(ctx); err == nil || !strings.Contains(err.Error(), "no verified tunnel") {
		t.Fatalf("observer accepted a subset of the serving replicas: %v", err)
	}
	input.Tunnels["second"] = EnvoyTunnel{AdminAddress: "127.0.0.1:19001", ClientAddress: "127.0.0.1:10444"}
	actual, err := input.Read(ctx)
	if err != nil || len(actual.Proxies) != 2 || actual.Weights["blue"] != 100 || actual.Pods[pod.Name].ProcessIncarnation != "jvm" ||
		actual.GatewayVersions["slice/slice"] != "5" {
		t.Fatalf("unexpected inventory: %#v, %v", actual, err)
	}
	pod.ResourceVersion = "9"
	if _, err := kube.CoreV1().Pods(pod.Namespace).Update(ctx, pod, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := input.Read(ctx); err == nil || !strings.Contains(err.Error(), "changed or is unavailable") {
		t.Fatalf("observer accepted a changed Pod using the old signed approval: %v", err)
	}
}
