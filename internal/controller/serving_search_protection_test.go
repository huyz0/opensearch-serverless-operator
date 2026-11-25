package controller

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	clientfake "sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/huyz0/opensearch-serverless-operator/api/v1alpha1"
	"github.com/huyz0/opensearch-serverless-operator/internal/java"
	"github.com/huyz0/opensearch-serverless-operator/internal/workload"
)

func TestServingSearchProtectionRequiresWarmProcessesAndMatchingEndpoints(t *testing.T) {
	ctx := context.Background()
	instance := &v1alpha1.OpenSearchServerless{ObjectMeta: metav1.ObjectMeta{Name: "catalog", Namespace: "n", UID: "owner", Generation: 1}, Spec: v1alpha1.OpenSearchServerlessSpec{Image: "image", Pools: map[string]v1alpha1.Pool{"search": {WarmOnJoin: true, WarmReadiness: "Strict", Zones: map[string]v1alpha1.Zone{"a": {Replicas: 1}}}}}, Status: v1alpha1.OpenSearchServerlessStatus{ServingFleetIntentByRole: map[string]string{"search": "blue"}}}
	revision, err := podRevision(instance, "")
	if err != nil {
		t.Fatal(err)
	}
	service, err := workload.RoleService(instance, "search", "blue", 9200)
	if err != nil {
		t.Fatal(err)
	}
	service.UID, service.ResourceVersion = "service", "10"
	service.Spec.Type, service.Spec.ClusterIP = corev1.ServiceTypeClusterIP, "10.0.0.1"
	controller, ready, serving, terminating := true, true, true, false
	port, portName, protocol, node := int32(9200), "http", corev1.ProtocolTCP, "node-a"
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "source", Namespace: "n", UID: "pod", ResourceVersion: "1", OwnerReferences: []metav1.OwnerReference{{APIVersion: v1alpha1.GroupVersion.String(), Kind: "OpenSearchServerless", Name: instance.Name, UID: instance.UID, Controller: &controller}}, Labels: map[string]string{workload.InstanceLabel: "owner", workload.RoleLabel: "search", workload.FleetLabel: "blue", workload.ZoneLabel: "a", "serverless.opensearch.org/revision": revision}}, Spec: corev1.PodSpec{NodeName: node}, Status: corev1.PodStatus{Phase: corev1.PodRunning, PodIP: "10.1.0.2", Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}, ContainerStatuses: []corev1.ContainerStatus{{Name: "opensearch", ContainerID: "container", Ready: true, State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}}}}}}
	slice := &discoveryv1.EndpointSlice{ObjectMeta: metav1.ObjectMeta{Name: "slice", Namespace: "n", Labels: map[string]string{discoveryv1.LabelServiceName: service.Name, discoveryv1.LabelManagedBy: "endpointslice-controller.k8s.io"}, OwnerReferences: []metav1.OwnerReference{{APIVersion: "v1", Kind: "Service", Name: service.Name, UID: service.UID, Controller: &controller}}}, AddressType: discoveryv1.AddressTypeIPv4, Ports: []discoveryv1.EndpointPort{{Name: &portName, Port: &port, Protocol: &protocol}}, Endpoints: []discoveryv1.Endpoint{{Addresses: []string{"10.1.0.2"}, NodeName: &node, Conditions: discoveryv1.EndpointConditions{Ready: &ready, Serving: &serving, Terminating: &terminating}, TargetRef: &corev1.ObjectReference{APIVersion: "v1", Kind: "Pod", Namespace: "n", Name: pod.Name, UID: pod.UID}}}}
	slice.UID, slice.ResourceVersion = "slice-uid", "1"
	core := fake.NewSimpleClientset(service, pod, slice)
	var timedOut atomic.Bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if req.URL.Path == "/_serverless/stats" {
			_, _ = fmt.Fprint(w, `{"node":"id","name":"source","roles":["search"],"lease":{"valid_now":true,"expires_in_millis":30000,"ttl_millis":30000}}`)
			return
		}
		_, _ = fmt.Fprintf(w, `{"ready":true,"state":"ready","serves_searches":true,"process_incarnation":"9aa977f3-4985-4cb5-9692-5ad1e3e243fe","warmup_result":"completed","warmup_timed_out":%t}`, timedOut.Load())
	}))
	defer server.Close()
	r := &InstanceReconciler{Core: core.CoreV1(), Discovery: core.DiscoveryV1(), DevelopmentWarmupImage: "image", ObserveSearchQualification: func(ctx context.Context, pod *corev1.Pod) (java.SearchQualification, error) {
		return java.QualifySearch(ctx, server.Client(), server.URL, pod.Name, time.Second)
	}}
	check := func() (servingRoleProtection, error) {
		return r.observeServingSearchProtection(ctx, instance, time.Minute)
	}
	evidence, err := check()
	if err != nil || len(evidence.Pods) != 1 || evidence.ServiceUID != "service" || evidence.ServiceVersion != "10" || evidence.ObservedAt.IsZero() {
		t.Fatalf("valid serving evidence rejected: %#v %v", evidence, err)
	}
	scheme := runtime.NewScheme()
	if err := v1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	kube := clientfake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(instance).WithObjects(instance).Build()
	if err := kube.Get(ctx, types.NamespacedName{Namespace: "n", Name: "catalog"}, instance); err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	r.Client, r.DevelopmentObserveEndpoints, r.Now = kube, true, func() time.Time { return now }
	if changed, err := r.sampleServingSearchProtection(ctx, instance); err != nil || !changed || instance.Status.LastServingSearchProtection.Result != "WarmupAndEndpointsObserved" || instance.Status.LastServingSearchProtection.QualifiedPods != 1 {
		t.Fatalf("successful historical observation missing: %v", err)
	}
	if changed, err := r.sampleServingSearchProtection(ctx, instance); err != nil || changed {
		t.Fatal("historical sample was not rate limited")
	}
	timedOut.Store(true)
	if _, err := check(); err == nil {
		t.Fatal("readiness green after warm-up timeout was accepted")
	}
	now = now.Add(time.Minute)
	if changed, err := r.sampleServingSearchProtection(ctx, instance); err != nil || !changed || instance.Status.LastServingSearchProtection.Result != "Unavailable" || instance.Status.LastServingSearchProtection.QualifiedPods != 0 || instance.Status.LastServingSearchProtection.ServiceUID != "" {
		t.Fatalf("failed observation retained success details: %v", err)
	}
	timedOut.Store(false)
	slice.Endpoints[0].TargetRef.UID = "foreign"
	if _, err := core.DiscoveryV1().EndpointSlices("n").Update(ctx, slice, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := check(); err == nil {
		t.Fatal("foreign endpoint accepted")
	}
	slice.Endpoints[0].TargetRef.UID = pod.UID
	if _, err := core.DiscoveryV1().EndpointSlices("n").Update(ctx, slice, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	service.Spec.Selector[workload.FleetLabel] = "green"
	if _, err := core.CoreV1().Services("n").Update(ctx, service, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := check(); err == nil {
		t.Fatal("changed production selector accepted")
	}
	service.Spec.Selector[workload.FleetLabel] = "blue"
	if _, err := core.CoreV1().Services("n").Update(ctx, service, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	instance.Status.ColdBootstrap = &v1alpha1.ColdBootstrapStatus{Name: pod.Name, PodUID: string(pod.UID), Fleet: "blue"}
	instance.Spec.Availability = &v1alpha1.Availability{MaxConcurrentRetirements: 1, MinServingTotal: map[string]int32{"search": 1}}
	updatedRevision, err := podRevision(instance, "")
	if err != nil {
		t.Fatal(err)
	}
	pod.Labels["serverless.opensearch.org/revision"] = updatedRevision
	pod.Annotations = map[string]string{warmupModeAnnotation: "Bootstrap"}
	if _, err := core.CoreV1().Pods("n").Update(ctx, pod, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	r.ObserveSearchQualification = func(_ context.Context, target *corev1.Pod) (java.SearchQualification, error) {
		if target.UID == pod.UID {
			return java.SearchQualification{}, fmt.Errorf("bootstrap is degraded")
		}
		return java.SearchQualification{ProcessIncarnation: "9aa977f3-4985-4cb5-9692-5ad1e3e243fe"}, nil
	}
	addEndpoint := func(name, address string, annotations map[string]string) {
		t.Helper()
		other := pod.DeepCopy()
		other.Name, other.UID, other.Status.PodIP = name, types.UID(name), address
		other.Annotations = annotations
		if _, err := core.CoreV1().Pods("n").Create(ctx, other, metav1.CreateOptions{}); err != nil {
			t.Fatal(err)
		}
		endpoint := slice.Endpoints[0].DeepCopy()
		endpoint.Addresses, endpoint.TargetRef.Name, endpoint.TargetRef.UID = []string{address}, name, other.UID
		slice.Endpoints = append(slice.Endpoints, *endpoint)
		if _, err := core.DiscoveryV1().EndpointSlices("n").Update(ctx, slice, metav1.UpdateOptions{}); err != nil {
			t.Fatal(err)
		}
	}
	addEndpoint("temporary", "10.1.0.3", map[string]string{emergencyCapacityAnnotation: "true"})
	observeFailback := func() servingRoleProtection {
		t.Helper()
		observed, err := r.observeSearchProtection(ctx, instance, 5*time.Second, instance.Status.ColdBootstrap)
		if err != nil {
			t.Fatal(err)
		}
		return observed
	}
	if _, ok := observeFailback().emergencyFailbackCandidate(instance, "search", time.Now()); ok {
		t.Fatal("bootstrap and temporary endpoints masked the regular warm shortfall")
	}
	addEndpoint("regular", "10.1.0.4", map[string]string{warmupModeAnnotation: "Strict", bootstrapReplacementAnnotation: string(pod.UID)})
	if candidate, ok := observeFailback().emergencyFailbackCandidate(instance, "search", time.Now()); !ok || candidate.UID != string(pod.UID) {
		t.Fatalf("expected the exact bootstrap Pod as the first retirement: %#v", candidate)
	}
	bootstrap := instance.Status.ColdBootstrap
	bootstrap.PodUID = "different-uid"
	if _, err := r.observeSearchProtection(ctx, instance, 5*time.Second, bootstrap); err == nil {
		t.Fatal("a same-name Pod with another UID inherited the bootstrap exemption")
	}
	bootstrap.PodUID = string(pod.UID)
	originalProbe := r.ObserveSearchQualification
	r.ObserveSearchQualification = func(ctx context.Context, target *corev1.Pod) (java.SearchQualification, error) {
		if target.Name == "regular" {
			return java.SearchQualification{}, fmt.Errorf("warm-up expired")
		}
		return originalProbe(ctx, target)
	}
	if _, err := r.observeSearchProtection(ctx, instance, 5*time.Second, bootstrap); err == nil {
		t.Fatal("Kubernetes readiness masked a failed regular JVM warm-up")
	}
}
