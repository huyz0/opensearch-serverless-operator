package rollout

import (
	"context"
	"fmt"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	clientfake "sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/huyz0/opensearch-serverless-operator/api/v1alpha1"
	"github.com/huyz0/opensearch-serverless-operator/internal/java"
	"github.com/huyz0/opensearch-serverless-operator/internal/workload"
)

type writerInventoryFixture struct {
	operation *v1alpha1.OpenSearchServerlessOperation
	instance  *v1alpha1.OpenSearchServerless
	victim    *corev1.Pod
	survivor  *corev1.Pod
	index     *discoveryv1.EndpointSlice
	gateway   *discoveryv1.EndpointSlice
	route     *unstructured.Unstructured
	probe     func(context.Context, *v1alpha1.OpenSearchServerless, *corev1.Pod, *RouteCredential) (java.Lease, error)
}

func TestKubernetesEnvoyWriterInventory(t *testing.T) {
	for _, test := range []struct {
		name string
		edit func(*writerInventoryFixture)
	}{
		{name: "exact claim and live survivor"},
		{name: "operation no longer holds the claim", edit: func(f *writerInventoryFixture) {
			f.instance.Status.ActiveOperation.UID = "different-operation"
		}},
		{name: "writer withdrawal not confirmed", edit: func(f *writerInventoryFixture) {
			f.operation.Status.WriterRetirement.WithdrawnAt = nil
		}},
		{name: "writer container replaced", edit: func(f *writerInventoryFixture) {
			f.victim.Status.ContainerStatuses[0].ContainerID = "replacement"
		}},
		{name: "writer JVM replaced", edit: func(f *writerInventoryFixture) {
			probe := f.probe
			f.probe = func(ctx context.Context, i *v1alpha1.OpenSearchServerless, pod *corev1.Pod, auth *RouteCredential) (java.Lease, error) {
				lease, err := probe(ctx, i, pod, auth)
				if pod.Name == "victim" {
					lease.NodeEphemeralID = "replacement"
				}
				return lease, err
			}
		}},
		{name: "writer remains potentially serving", edit: func(f *writerInventoryFixture) {
			f.index.Endpoints[0].Conditions.Serving = ptr.To(true)
		}},
		{name: "survivor address belongs to victim", edit: func(f *writerInventoryFixture) {
			f.index.Endpoints[1].Addresses = []string{"10.1.0.1"}
		}},
		{name: "survivor EndpointSlice Pod UID is stale", edit: func(f *writerInventoryFixture) {
			f.index.Endpoints[1].TargetRef.UID = "old-survivor"
		}},
		{name: "route changed to a different Service", edit: func(f *writerInventoryFixture) {
			rules, _, _ := unstructured.NestedSlice(f.route.Object, "spec", "rules")
			rules[0].(map[string]any)["backendRefs"] = []any{map[string]any{"name": "different", "port": int64(9200)}}
			if err := unstructured.SetNestedSlice(f.route.Object, rules, "spec", "rules"); err != nil {
				panic(err)
			}
		}},
		{name: "route restricts the catch-all to GET", edit: func(f *writerInventoryFixture) {
			rules, _, _ := unstructured.NestedSlice(f.route.Object, "spec", "rules")
			match := rules[0].(map[string]any)["matches"].([]any)[0].(map[string]any)
			match["method"] = "GET"
			if err := unstructured.SetNestedSlice(f.route.Object, rules, "spec", "rules"); err != nil {
				panic(err)
			}
		}},
		{name: "new Envoy replica lacks a tunnel", edit: func(f *writerInventoryFixture) {
			endpoint := f.gateway.Endpoints[0].DeepCopy()
			endpoint.TargetRef.Name, endpoint.TargetRef.UID = "new-proxy", "new-proxy-uid"
			f.gateway.Endpoints = append(f.gateway.Endpoints, *endpoint)
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			instance := &v1alpha1.OpenSearchServerless{
				ObjectMeta: metav1.ObjectMeta{Name: "catalog", Namespace: "catalog-ns", UID: "instance-uid", ResourceVersion: "1", Generation: 1},
				Spec: v1alpha1.OpenSearchServerlessSpec{
					Pools:    map[string]v1alpha1.Pool{"ingest": {Zones: map[string]v1alpha1.Zone{"zone-a": {Replicas: 2, MinServing: 1}}}},
					Security: &v1alpha1.SecurityConfig{Provider: "ServerlessBasicAuth"},
				},
				Status: v1alpha1.OpenSearchServerlessStatus{
					ServingFleetIntentByRole: map[string]string{"ingest": "initial"},
					BasicAuthBaseline:        &v1alpha1.BasicAuthBaseline{SecretName: "bootstrap", SecretUID: "bootstrap-uid"},
				},
			}
			op := &v1alpha1.OpenSearchServerlessOperation{
				ObjectMeta: metav1.ObjectMeta{Name: "retire-one", Namespace: instance.Namespace, UID: "operation-uid", ResourceVersion: "2", Generation: 1,
					Finalizers: []string{"serverless.opensearch.org/writer-retirement"}},
				Spec: v1alpha1.OperationSpec{
					Action: "RetireWriter", InstanceRef: v1alpha1.InstanceReference{Name: instance.Name, UID: string(instance.UID)},
					WriterRetirement: &v1alpha1.WriterRetirementTarget{PodName: "victim", PodUID: "victim-uid"},
				},
				Status: v1alpha1.OperationStatus{WriterRetirement: &v1alpha1.WriterRetirementStatus{
					InstanceGeneration: instance.Generation, ServiceUID: "index-uid", RouteUID: "route-uid", GatewayUID: "gateway-uid", GatewayServiceUID: "frontend-uid",
					WithdrawnAt: ptr.To(metav1.NewMicroTime(time.Now().Add(-time.Minute).UTC().Truncate(time.Microsecond))), WithdrawalArmed: true,
					BindingDigest: "binding", ServingFleet: "initial", ContainerID: "container-victim", NodeID: "node-victim", NodeEphemeralID: "ephemeral-victim",
				}},
			}
			instance.Status.ActiveOperation = &v1alpha1.OperationClaim{Name: op.Name, UID: string(op.UID), Generation: op.Generation, Action: op.Spec.Action}
			index, err := workload.RoleService(instance, "ingest", "initial", 9200)
			if err != nil {
				t.Fatal(err)
			}
			index.UID, index.ResourceVersion, index.Spec.Type, index.Spec.ClusterIP = "index-uid", "3", corev1.ServiceTypeClusterIP, "10.0.0.1"
			frontend := &corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: "frontend", Namespace: "gateway-ns", UID: "frontend-uid", ResourceVersion: "4",
				Labels: map[string]string{"gateway.envoyproxy.io/owning-gateway-name": "gateway", "gateway.envoyproxy.io/owning-gateway-namespace": "gateway-ns"}}}
			route := writerInventoryGatewayObject("HTTPRoute", "index", instance.Namespace, "route-uid", map[string]any{
				"hostnames":  []any{"index.example.com"},
				"parentRefs": []any{map[string]any{"name": "gateway", "namespace": "gateway-ns", "sectionName": "https"}},
				"rules": []any{map[string]any{
					"backendRefs": []any{map[string]any{"name": index.Name, "port": int64(9200)}},
					"matches":     []any{map[string]any{"path": map[string]any{"type": "PathPrefix", "value": "/"}}},
				}},
			})
			gateway := writerInventoryGatewayObject("Gateway", "gateway", "gateway-ns", "gateway-uid", map[string]any{
				"listeners": []any{map[string]any{"name": "https", "protocol": "HTTPS", "port": int64(443)}},
			})
			pod := func(name, address string, status corev1.ConditionStatus) *corev1.Pod {
				return &corev1.Pod{
					ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: instance.Namespace, UID: types.UID(name + "-uid"), ResourceVersion: "6",
						OwnerReferences: index.OwnerReferences,
						Labels:          map[string]string{workload.InstanceLabel: string(instance.UID), workload.RoleLabel: "ingest", workload.FleetLabel: "initial", workload.ZoneLabel: "zone-a"},
						Annotations:     map[string]string{"serverless.opensearch.org/keystore-uid": "bootstrap-uid"}},
					Spec: corev1.PodSpec{NodeName: "node"},
					Status: corev1.PodStatus{Phase: corev1.PodRunning, PodIP: address,
						Conditions:        []corev1.PodCondition{{Type: corev1.PodReady, Status: status}},
						ContainerStatuses: []corev1.ContainerStatus{{Name: "opensearch", ContainerID: "container-" + name, State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}}}}},
				}
			}
			victim := pod("victim", "10.1.0.1", corev1.ConditionFalse)
			survivor := pod("survivor", "10.1.0.2", corev1.ConditionTrue)
			makeSlice := func(service *corev1.Service, endpoint ...discoveryv1.Endpoint) *discoveryv1.EndpointSlice {
				return &discoveryv1.EndpointSlice{ObjectMeta: metav1.ObjectMeta{Name: service.Name + "-slice", Namespace: service.Namespace,
					UID: service.UID + "-slice", ResourceVersion: "7",
					OwnerReferences: []metav1.OwnerReference{{APIVersion: "v1", Kind: "Service", Name: service.Name, UID: service.UID, Controller: ptr.To(true)}},
					Labels:          map[string]string{discoveryv1.LabelManagedBy: "endpointslice-controller.k8s.io", discoveryv1.LabelServiceName: service.Name}},
					AddressType: discoveryv1.AddressTypeIPv4, Endpoints: endpoint,
					Ports: []discoveryv1.EndpointPort{{Name: ptr.To("http"), Port: ptr.To(int32(9200)), Protocol: ptr.To(corev1.ProtocolTCP)}},
				}
			}
			makeEndpoint := func(pod *corev1.Pod, ready bool) discoveryv1.Endpoint {
				return discoveryv1.Endpoint{Addresses: []string{pod.Status.PodIP},
					Conditions: discoveryv1.EndpointConditions{Ready: ptr.To(ready), Serving: ptr.To(ready)},
					TargetRef:  &corev1.ObjectReference{Kind: "Pod", Namespace: pod.Namespace, Name: pod.Name, UID: pod.UID}}
			}
			indexSlice := makeSlice(index, makeEndpoint(victim, false), makeEndpoint(survivor, true))
			gatewaySlice := makeSlice(frontend, discoveryv1.Endpoint{Addresses: []string{"10.2.0.1"},
				Conditions: discoveryv1.EndpointConditions{Ready: ptr.To(true)},
				TargetRef:  &corev1.ObjectReference{Kind: "Pod", Namespace: frontend.Namespace, Name: "proxy", UID: "proxy-uid"}})
			credential := func(name string) *corev1.Secret {
				return &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: instance.Namespace, UID: types.UID(name + "-uid"), ResourceVersion: "8"},
					Type: RouteCredentialSecretType, Immutable: ptr.To(true), Data: map[string][]byte{"username": []byte(name), "password": []byte(name + "-password")}}
			}
			fixture := writerInventoryFixture{operation: op, instance: instance, victim: victim, survivor: survivor, index: indexSlice, gateway: gatewaySlice, route: route,
				probe: func(_ context.Context, _ *v1alpha1.OpenSearchServerless, pod *corev1.Pod, auth *RouteCredential) (java.Lease, error) {
					if auth.Username != "java-reader" {
						return java.Lease{}, fmt.Errorf("wrong direct Java credential")
					}
					return java.Lease{NodeID: "node-" + pod.Name, NodeEphemeralID: "ephemeral-" + pod.Name, NodeName: pod.Name,
						ProcessIncarnation: "process-" + pod.Name, Roles: []string{"ingest"}, Valid: true, TTLMillis: 300_000, ExpiresInMillis: 200_000}, nil
				},
			}
			if test.edit != nil {
				test.edit(&fixture)
			}
			scheme := runtime.NewScheme()
			if err := v1alpha1.AddToScheme(scheme); err != nil {
				t.Fatal(err)
			}
			reader := KubernetesEnvoyWriterInventory{
				Reader:    clientfake.NewClientBuilder().WithScheme(scheme).WithObjects(instance, op, route, gateway).Build(),
				Kube:      fake.NewSimpleClientset(index, frontend, victim, survivor, indexSlice, gatewaySlice, credential("edge-reader"), credential("java-reader")),
				Namespace: instance.Namespace, Operation: op.Name, InstanceName: instance.Name, RouteName: route.GetName(),
				RouteURL: "https://index.example.com/", RouteAuthSecret: "edge-reader", ReaderSecret: "java-reader",
				Gateway: client.ObjectKeyFromObject(gateway), GatewayService: client.ObjectKeyFromObject(frontend), GatewayListener: "https",
				Tunnels: map[string]EnvoyTunnel{"proxy-uid": {AdminAddress: "127.0.0.1:19000", ClientAddress: "127.0.0.1:18443"}}, ProbeLease: fixture.probe,
			}
			inventory, err := reader.Read(ctx)
			if test.edit != nil {
				if err == nil {
					t.Fatalf("accepted mismatched inventory %+v", inventory.Scope)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if inventory.Scope.Writer.EphemeralID != "ephemeral-victim" || inventory.Scope.VictimPodUID != "victim-uid" ||
				inventory.Scope.AuthProbeURL != "https://index.example.com" || inventory.Scope.OperationUID != op.UID ||
				!inventory.WithdrawnAt.Equal(op.Status.WriterRetirement.WithdrawnAt.Time) || len(inventory.Proxies) != 1 ||
				len(inventory.Pods) != 1 || inventory.Pods["survivor"].ProcessIncarnation != "process-survivor" ||
				inventory.EndpointVersions[indexSlice.UID] != indexSlice.ResourceVersion {
				t.Fatalf("wrong verified scope or inventory %+v", inventory.Scope)
			}
		})
	}
}

func writerInventoryGatewayObject(kind, name, namespace, uid string, spec map[string]any) *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]any{"apiVersion": "gateway.networking.k8s.io/v1", "kind": kind,
		"metadata": map[string]any{"name": name, "namespace": namespace, "uid": uid, "resourceVersion": "5"}, "spec": spec}}
}
