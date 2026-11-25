package controller

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
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

func TestCandidateWarmthRequiresStableCompletePodSet(t *testing.T) {
	ctx := context.Background()
	instance := &v1alpha1.OpenSearchServerless{ObjectMeta: metav1.ObjectMeta{Name: "cluster", Namespace: "n", UID: "owner", Generation: 1}, Spec: v1alpha1.OpenSearchServerlessSpec{Image: "image", Pools: map[string]v1alpha1.Pool{"search": {WarmOnJoin: true, WarmReadiness: "Strict", Zones: map[string]v1alpha1.Zone{"a": {Replicas: 1}, "b": {Replicas: 1}}}}, Rollout: &v1alpha1.Rollout{Strategy: "BlueGreen", MaxPreviewReplicas: 2, Candidate: &v1alpha1.FleetCandidate{Fleet: "green", Image: "image"}}}, Status: v1alpha1.OpenSearchServerlessStatus{ServingFleetIntentByRole: map[string]string{"search": "blue"}, CandidateFleet: &v1alpha1.CandidateFleetStatus{Fleet: "green", Image: "image", Revision: "revision", Generation: 1, SearchServiceUID: "s", IngestServiceUID: "i"}, Conditions: []metav1.Condition{{Type: "PreviewServicesConfigured", Status: metav1.ConditionTrue, ObservedGeneration: 1}}}}
	owner := true
	addresses := map[string]string{"a": "10.0.0.1", "b": "10.0.0.2"}
	makePod := func(name, zone string) *corev1.Pod {
		return &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "n", UID: types.UID(name), ResourceVersion: "1", OwnerReferences: []metav1.OwnerReference{{APIVersion: v1alpha1.GroupVersion.String(), Kind: "OpenSearchServerless", Name: "cluster", UID: "owner", Controller: &owner}}, Labels: map[string]string{workload.InstanceLabel: "owner", workload.RoleLabel: "search", workload.FleetLabel: "green", workload.ZoneLabel: zone, "serverless.opensearch.org/revision": "revision"}, Annotations: map[string]string{warmupModeAnnotation: "Strict"}}, Spec: corev1.PodSpec{NodeName: "node-" + zone}, Status: corev1.PodStatus{Phase: corev1.PodRunning, PodIP: addresses[zone], Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}, ContainerStatuses: []corev1.ContainerStatus{{Name: "opensearch", ContainerID: "container-" + name, Ready: true, State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}}}}}}
	}
	core := fake.NewSimpleClientset(makePod("a", "a"), makePod("b", "b"))
	fail, mutate := false, false
	r := &InstanceReconciler{Core: core.CoreV1(), DevelopmentWarmupImage: "image", ObserveSearchQualification: func(ctx context.Context, pod *corev1.Pod) (java.SearchQualification, error) {
		if fail && pod.Name == "b" {
			return java.SearchQualification{}, fmt.Errorf("warm-up timed out")
		}
		if mutate && pod.Name == "b" {
			prior, err := core.CoreV1().Pods("n").Get(ctx, "a", metav1.GetOptions{})
			if err != nil {
				return java.SearchQualification{}, err
			}
			prior.ResourceVersion = "2"
			if _, err := core.CoreV1().Pods("n").Update(ctx, prior, metav1.UpdateOptions{}); err != nil {
				return java.SearchQualification{}, err
			}
		}
		return java.SearchQualification{ProcessIncarnation: "9aa977f3-4985-4cb5-9692-5ad1e3e243fe"}, nil
	}}
	observed, err := r.observeCandidateSearchWarmth(ctx, instance, time.Minute)
	if err != nil || len(observed.Pods) != 2 || observed.Fleet != "green" || observed.ObservedAt.IsZero() {
		t.Fatalf("valid observations rejected: %#v %v", observed, err)
	}
	instance.ResourceVersion = "1"
	scheme := runtime.NewScheme()
	if err := v1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	r.Client = clientfake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(instance).WithObjects(instance.DeepCopyObject().(*v1alpha1.OpenSearchServerless)).Build()
	now := time.Now()
	r.Now = func() time.Time { return now }
	if changed, err := r.sampleCandidateSearch(ctx, instance); err != nil || !changed || instance.Status.LastCandidateSearchObservation.Result != "WarmthObserved" || instance.Status.LastCandidateSearchObservation.QualifiedPods != 2 {
		t.Fatalf("search sample: %#v %v", instance.Status.LastCandidateSearchObservation, err)
	}
	service, err := workload.PreviewService(instance, "search", "green")
	if err != nil {
		t.Fatal(err)
	}
	service.UID, service.ResourceVersion = "s", "1"
	service.Spec.Type, service.Spec.ClusterIP = corev1.ServiceTypeClusterIP, "10.0.0.10"
	ready, serving, terminating := true, true, false
	name, port, protocol := "http", int32(9200), corev1.ProtocolTCP
	endpoints := []discoveryv1.Endpoint{}
	for _, zone := range []string{"a", "b"} {
		node := "node-" + zone
		endpoints = append(endpoints, discoveryv1.Endpoint{Addresses: []string{addresses[zone]}, NodeName: &node, Conditions: discoveryv1.EndpointConditions{Ready: &ready, Serving: &serving, Terminating: &terminating}, TargetRef: &corev1.ObjectReference{APIVersion: "v1", Kind: "Pod", Namespace: "n", Name: zone, UID: types.UID(zone)}})
	}
	slice := &discoveryv1.EndpointSlice{ObjectMeta: metav1.ObjectMeta{Name: "preview-slice", Namespace: "n", UID: "slice", ResourceVersion: "1", Labels: map[string]string{discoveryv1.LabelServiceName: service.Name, discoveryv1.LabelManagedBy: "endpointslice-controller.k8s.io"}, OwnerReferences: []metav1.OwnerReference{{APIVersion: "v1", Kind: "Service", Name: service.Name, UID: service.UID, Controller: &owner}}}, AddressType: discoveryv1.AddressTypeIPv4, Ports: []discoveryv1.EndpointPort{{Name: &name, Port: &port, Protocol: &protocol}}, Endpoints: endpoints}
	previewCore := fake.NewSimpleClientset(makePod("a", "a"), makePod("b", "b"), service, slice)
	r.Core, r.Discovery, r.DevelopmentObserveEndpoints = previewCore.CoreV1(), previewCore.DiscoveryV1(), true
	now = now.Add(2 * time.Minute)
	if changed, err := r.sampleCandidateSearch(ctx, instance); err != nil || !changed || instance.Status.LastCandidateSearchObservation.Result != "WarmthAndPreviewEndpointsObserved" {
		t.Fatalf("search preview sample: %#v %v", instance.Status.LastCandidateSearchObservation, err)
	}
	r.Core, r.Discovery, r.DevelopmentObserveEndpoints = core.CoreV1(), nil, false
	fail = true
	now = now.Add(2 * time.Minute)
	if changed, err := r.sampleCandidateSearch(ctx, instance); err != nil || !changed || instance.Status.LastCandidateSearchObservation.Result != "Unavailable" {
		t.Fatalf("failed search sample: %#v %v", instance.Status.LastCandidateSearchObservation, err)
	}
	fail = false
	originalProbe := r.ObserveSearchQualification
	var timedOut atomic.Bool
	server := func(name string) *httptest.Server {
		return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			if req.URL.Path == "/_serverless/stats" {
				_, _ = fmt.Fprintf(w, `{"node":"id","name":%q,"roles":["search","ingest"],"lease":{"valid_now":true,"expires_in_millis":30000,"ttl_millis":30000}}`, name)
				return
			}
			_, _ = fmt.Fprintf(w, `{"ready":true,"state":"ready","serves_searches":true,"process_incarnation":"9aa977f3-4985-4cb5-9692-5ad1e3e243fe","warmup_result":"completed","warmup_timed_out":%t}`, name == "b" && timedOut.Load())
		}))
	}
	firstServer, secondServer := server("a"), server("b")
	defer firstServer.Close()
	defer secondServer.Close()
	r.ObserveSearchQualification = func(ctx context.Context, pod *corev1.Pod) (java.SearchQualification, error) {
		target := firstServer
		if pod.Name == "b" {
			target = secondServer
		}
		return java.QualifySearch(ctx, target.Client(), target.URL, pod.Name, time.Second)
	}
	if _, err := r.observeCandidateSearchWarmth(ctx, instance, time.Minute); err != nil {
		t.Fatalf("HTTP warm-up evidence rejected: %v", err)
	}
	timedOut.Store(true)
	if _, err := r.observeCandidateSearchWarmth(ctx, instance, time.Minute); err == nil {
		t.Fatal("green readiness with warm-up timeout accepted")
	}
	r.ObserveSearchQualification = originalProbe
	fail = true
	if _, err := r.observeCandidateSearchWarmth(ctx, instance, time.Minute); err == nil {
		t.Fatal("partial warm-up accepted")
	}
	fail, mutate = false, true
	if _, err := r.observeCandidateSearchWarmth(ctx, instance, time.Minute); err == nil {
		t.Fatal("earlier Pod change accepted")
	}
	mutate = false
	if err := core.CoreV1().Pods("n").Delete(ctx, "b", metav1.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := r.observeCandidateSearchWarmth(ctx, instance, time.Minute); err == nil {
		t.Fatal("missing AZ candidate accepted")
	}
	instance.Spec.Rollout.Candidate.Image = "different"
	if _, err := r.observeCandidateSearchWarmth(ctx, instance, time.Minute); err == nil {
		t.Fatal("unqualified image accepted")
	}
}

func TestCandidateIngestObservationReportsFailureAfterSuccess(t *testing.T) {
	ctx := context.Background()
	image := "example.invalid/java@sha256:" + strings.Repeat("a", 64)
	instance := &v1alpha1.OpenSearchServerless{ObjectMeta: metav1.ObjectMeta{Name: "cluster", Namespace: "n", UID: "owner", Generation: 1, ResourceVersion: "1"}, Spec: v1alpha1.OpenSearchServerlessSpec{Image: image, Pools: map[string]v1alpha1.Pool{"ingest": {Zones: map[string]v1alpha1.Zone{"a": {Replicas: 1}}}}, Rollout: &v1alpha1.Rollout{Strategy: "BlueGreen", MaxPreviewReplicas: 1, Candidate: &v1alpha1.FleetCandidate{Fleet: "green", Image: image}}}, Status: v1alpha1.OpenSearchServerlessStatus{ServingFleetIntentByRole: map[string]string{"ingest": "blue"}, CandidateFleet: &v1alpha1.CandidateFleetStatus{Fleet: "green", Image: image, Revision: "revision", Generation: 1, SearchServiceUID: "s", IngestServiceUID: "i"}, Conditions: []metav1.Condition{{Type: "PreviewServicesConfigured", Status: metav1.ConditionTrue, ObservedGeneration: 1}}}}
	owner := true
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "candidate", Namespace: "n", UID: "pod", ResourceVersion: "1", OwnerReferences: []metav1.OwnerReference{{APIVersion: v1alpha1.GroupVersion.String(), Kind: "OpenSearchServerless", Name: "cluster", UID: "owner", Controller: &owner}}, Labels: map[string]string{workload.InstanceLabel: "owner", workload.RoleLabel: "ingest", workload.FleetLabel: "green", workload.ZoneLabel: "a", "serverless.opensearch.org/revision": "revision"}}, Spec: corev1.PodSpec{NodeName: "node"}, Status: corev1.PodStatus{Phase: corev1.PodRunning, PodIP: "10.0.0.1", Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}, ContainerStatuses: []corev1.ContainerStatus{{Name: "opensearch", ContainerID: "container", Ready: true, State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}}}}}}
	scheme := runtime.NewScheme()
	if err := v1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	client := clientfake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(instance).WithObjects(instance.DeepCopyObject().(*v1alpha1.OpenSearchServerless)).Build()
	fail := false
	now := time.Now()
	r := &InstanceReconciler{Client: client, Core: fake.NewSimpleClientset(pod).CoreV1(), DevelopmentServiceProbeImage: image, Now: func() time.Time { return now }, ObserveIngestQualification: func(context.Context, *corev1.Pod) (java.IngestQualification, error) {
		if fail {
			return java.IngestQualification{}, fmt.Errorf("lease unavailable")
		}
		return java.IngestQualification{ProcessIncarnation: "9aa977f3-4985-4cb5-9692-5ad1e3e243fe"}, nil
	}}
	if changed, err := r.sampleCandidateIngest(ctx, instance); err != nil || !changed || instance.Status.LastCandidateIngestObservation.Result != "LeaseObserved" || instance.Status.LastCandidateIngestObservation.QualifiedPods != 1 {
		t.Fatalf("successful sample: %#v %v", instance.Status.LastCandidateIngestObservation, err)
	}
	service, err := workload.PreviewService(instance, "ingest", "green")
	if err != nil {
		t.Fatal(err)
	}
	service.UID, service.ResourceVersion = "i", "1"
	service.Spec.Type, service.Spec.ClusterIP = corev1.ServiceTypeClusterIP, "10.0.0.10"
	ready, serving, terminating := true, true, false
	name, port, protocol, node := "http", int32(9200), corev1.ProtocolTCP, "node"
	slice := &discoveryv1.EndpointSlice{ObjectMeta: metav1.ObjectMeta{Name: "preview-slice", Namespace: "n", UID: "slice", ResourceVersion: "1", Labels: map[string]string{discoveryv1.LabelServiceName: service.Name, discoveryv1.LabelManagedBy: "endpointslice-controller.k8s.io"}, OwnerReferences: []metav1.OwnerReference{{APIVersion: "v1", Kind: "Service", Name: service.Name, UID: service.UID, Controller: &owner}}}, AddressType: discoveryv1.AddressTypeIPv4, Ports: []discoveryv1.EndpointPort{{Name: &name, Port: &port, Protocol: &protocol}}, Endpoints: []discoveryv1.Endpoint{{Addresses: []string{pod.Status.PodIP}, NodeName: &node, Conditions: discoveryv1.EndpointConditions{Ready: &ready, Serving: &serving, Terminating: &terminating}, TargetRef: &corev1.ObjectReference{APIVersion: "v1", Kind: "Pod", Namespace: "n", Name: pod.Name, UID: pod.UID}}}}
	kube := fake.NewSimpleClientset(pod.DeepCopy(), service, slice)
	r.Core, r.Discovery, r.DevelopmentObserveEndpoints = kube.CoreV1(), kube.DiscoveryV1(), true
	now = now.Add(2 * time.Minute)
	if changed, err := r.sampleCandidateIngest(ctx, instance); err != nil || !changed || instance.Status.LastCandidateIngestObservation.Result != "LeaseAndPreviewEndpointsObserved" {
		t.Fatalf("preview endpoint sample: %#v %v", instance.Status.LastCandidateIngestObservation, err)
	}
	if err := kube.DiscoveryV1().EndpointSlices("n").Delete(ctx, slice.Name, metav1.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	now = now.Add(2 * time.Minute)
	if changed, err := r.sampleCandidateIngest(ctx, instance); err != nil || !changed || instance.Status.LastCandidateIngestObservation.Result != "LeaseObserved" {
		t.Fatalf("missing preview endpoints accepted: %#v %v", instance.Status.LastCandidateIngestObservation, err)
	}
	fail = true
	now = now.Add(2 * time.Minute)
	if changed, err := r.sampleCandidateIngest(ctx, instance); err != nil || !changed || instance.Status.LastCandidateIngestObservation.Result != "Unavailable" || instance.Status.LastCandidateIngestObservation.QualifiedPods != 0 {
		t.Fatalf("failed sample: %#v %v", instance.Status.LastCandidateIngestObservation, err)
	}
}
