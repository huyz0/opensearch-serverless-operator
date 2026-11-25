package controller

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	kubernetesfake "k8s.io/client-go/kubernetes/fake"
	kubetesting "k8s.io/client-go/testing"
	clientfake "sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/huyz0/opensearch-serverless-operator/api/v1alpha1"
	"github.com/huyz0/opensearch-serverless-operator/internal/java"
	"github.com/huyz0/opensearch-serverless-operator/internal/workload"
)

func TestServingIngestProtectionBindsLocalProcessAndEndpoints(t *testing.T) {
	ctx := context.Background()
	image := "registry/runtime@sha256:" + strings.Repeat("a", 64)
	instance := &v1alpha1.OpenSearchServerless{ObjectMeta: metav1.ObjectMeta{Name: "catalog", Namespace: "n", UID: "owner", Generation: 1}, Spec: v1alpha1.OpenSearchServerlessSpec{Image: image, Pools: map[string]v1alpha1.Pool{"ingest": {Zones: map[string]v1alpha1.Zone{"a": {Replicas: 1}}}}}, Status: v1alpha1.OpenSearchServerlessStatus{ServingFleetIntentByRole: map[string]string{"ingest": "blue"}}}
	instance.Spec.ServiceAccountName = "runtime"
	instance.Spec.Storage = v1alpha1.Storage{Type: "s3", Bucket: "catalog", Region: "us-east-1"}
	instance.Spec.Pools["ingest"] = v1alpha1.Pool{CapacityProfileRef: "fit", Zones: map[string]v1alpha1.Zone{"a": {Replicas: 1, MinServing: 1}}}
	instance.Spec.Availability = &v1alpha1.Availability{MaxConcurrentRetirements: 1, MaxPendingProvisionablePods: 1, MinServingTotal: map[string]int32{"ingest": 1},
		EmergencyCapacity: &v1alpha1.EmergencyCapacityPolicy{EvidenceRef: v1alpha1.LocalReference{Name: "outage"}, MaxTemporaryPerZone: 1, MaxTemporaryByRole: map[string]int32{"ingest": 1}}}
	instance.Status.PrimaryStore = &v1alpha1.PrimaryStoreStatus{Bucket: "catalog", Region: "us-east-1"}
	revision, err := podRevision(instance, "")
	if err != nil {
		t.Fatal(err)
	}
	const legacyRevision = "legacy-revision"
	instance.Status.TemplateRevisionAliases = map[string]string{legacyRevision: revision}
	pinServingRevision(instance, "ingest", "blue", legacyRevision)
	service, err := workload.RoleService(instance, "ingest", "blue", 9200)
	if err != nil {
		t.Fatal(err)
	}
	service.UID, service.ResourceVersion = "service", "10"
	service.Spec.Type, service.Spec.ClusterIP = corev1.ServiceTypeClusterIP, "10.0.0.1"
	controller, ready, serving, terminating := true, true, true, false
	port, portName, protocol, node := int32(9200), "http", corev1.ProtocolTCP, "node-a"
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "writer", Namespace: "n", UID: "pod", ResourceVersion: "1", OwnerReferences: []metav1.OwnerReference{{APIVersion: v1alpha1.GroupVersion.String(), Kind: "OpenSearchServerless", Name: instance.Name, UID: instance.UID, Controller: &controller}}, Labels: map[string]string{workload.InstanceLabel: "owner", workload.RoleLabel: "ingest", workload.FleetLabel: "blue", workload.ZoneLabel: "a", "serverless.opensearch.org/revision": revision}}, Spec: corev1.PodSpec{NodeName: node}, Status: corev1.PodStatus{Phase: corev1.PodRunning, PodIP: "10.1.0.2", Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}, ContainerStatuses: []corev1.ContainerStatus{{Name: "opensearch", ContainerID: "container", Ready: true, State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}}}}}}
	slice := &discoveryv1.EndpointSlice{ObjectMeta: metav1.ObjectMeta{Name: "slice", Namespace: "n", Labels: map[string]string{discoveryv1.LabelServiceName: service.Name, discoveryv1.LabelManagedBy: "endpointslice-controller.k8s.io"}, OwnerReferences: []metav1.OwnerReference{{APIVersion: "v1", Kind: "Service", Name: service.Name, UID: service.UID, Controller: &controller}}}, AddressType: discoveryv1.AddressTypeIPv4, Ports: []discoveryv1.EndpointPort{{Name: &portName, Port: &port, Protocol: &protocol}}, Endpoints: []discoveryv1.Endpoint{{Addresses: []string{"10.1.0.2"}, NodeName: &node, Conditions: discoveryv1.EndpointConditions{Ready: &ready, Serving: &serving, Terminating: &terminating}, TargetRef: &corev1.ObjectReference{APIVersion: "v1", Kind: "Pod", Namespace: "n", Name: pod.Name, UID: pod.UID}}}}
	slice.UID, slice.ResourceVersion = "slice-uid", "1"
	core := kubernetesfake.NewSimpleClientset(service, pod, slice)
	var wrongRole atomic.Bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if req.URL.Path == "/_serverless/stats" {
			role := "ingest"
			if wrongRole.Load() {
				role = "search"
			}
			_, _ = fmt.Fprintf(w, `{"node":"id","name":"writer","roles":[%q],"lease":{"valid_now":true,"expires_in_millis":30000,"ttl_millis":30000}}`, role)
			return
		}
		_, _ = fmt.Fprint(w, `{"ready":true,"state":"ready","serves_searches":false,"process_incarnation":"9aa977f3-4985-4cb5-9692-5ad1e3e243fe","warmup_result":"not_requested","warmup_timed_out":false}`)
	}))
	defer server.Close()
	scheme := runtime.NewScheme()
	if err := v1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	kube := clientfake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(instance).WithObjects(instance).Build()
	if err := kube.Get(ctx, types.NamespacedName{Namespace: "n", Name: "catalog"}, instance); err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	r := &InstanceReconciler{Client: kube, Core: core.CoreV1(), Discovery: core.DiscoveryV1(), DevelopmentObserveEndpoints: true, DevelopmentServiceProbeImage: image, Now: func() time.Time { return now }, ObserveIngestQualification: func(ctx context.Context, pod *corev1.Pod) (java.IngestQualification, error) {
		return java.QualifyIngest(ctx, server.Client(), server.URL, pod.Name, time.Second)
	}}
	if changed, err := r.sampleServingIngestProtection(ctx, instance); err != nil || !changed || instance.Status.LastServingIngestProtection.Result != "LeaseAndEndpointsObserved" || instance.Status.LastServingIngestProtection.QualifiedPods != 1 {
		t.Fatalf("valid ingest sample rejected: %v", err)
	}
	if changed, err := r.sampleServingIngestProtection(ctx, instance); err != nil || changed {
		t.Fatal("ingest sample was not rate limited")
	}
	wrongRole.Store(true)
	now = now.Add(time.Minute)
	if changed, err := r.sampleServingIngestProtection(ctx, instance); err != nil || !changed || instance.Status.LastServingIngestProtection.Result != "Unavailable" || instance.Status.LastServingIngestProtection.QualifiedPods != 0 {
		t.Fatalf("wrong-role process retained success: %v", err)
	}
	wrongRole.Store(false)
	pod.Annotations = map[string]string{emergencyCapacityAnnotation: "true"}
	pod.Labels[podRevisionLabel] = legacyRevision
	if _, err := r.Core.Pods("n").Update(ctx, pod, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	observation, err := r.fleetQualification().observeServingIngestProtection(ctx, instance, 5*time.Second)
	if err != nil || len(observation.EmergencyPods) != 1 || observation.RegularEndpointsByZone["a"] != 0 {
		t.Fatalf("temporary serving capacity was miscounted: %#v, %v", observation, err)
	}
	if _, ok := observation.emergencyFailbackCandidate(instance, "ingest", time.Now()); ok {
		t.Fatal("a Ready emergency endpoint masked the loss of the regular writer")
	}
	replacement := pod.DeepCopy()
	replacement.Name, replacement.UID, replacement.ResourceVersion = "regular", "regular-uid", "2"
	replacement.Status.PodIP = "10.1.0.3"
	replacement.Annotations = nil
	replacement.Labels[podRevisionLabel] = revision
	if _, err := r.Core.Pods("n").Create(ctx, replacement, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	r.ObserveIngestQualification = func(context.Context, *corev1.Pod) (java.IngestQualification, error) {
		return java.IngestQualification{ProcessIncarnation: "9aa977f3-4985-4cb5-9692-5ad1e3e243fe", LeaseValidUntil: time.Now().Add(time.Minute)}, nil
	}
	observation, err = r.fleetQualification().observeServingIngestProtection(ctx, instance, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := observation.emergencyFailbackCandidate(instance, "ingest", time.Now()); ok {
		t.Fatal("a qualified regular Pod without a Service endpoint counted toward failback")
	}
	endpoint := slice.Endpoints[0].DeepCopy()
	endpoint.Addresses = []string{replacement.Status.PodIP}
	endpoint.TargetRef.Name, endpoint.TargetRef.UID = replacement.Name, replacement.UID
	slice.Endpoints = append(slice.Endpoints, *endpoint)
	if _, err := r.Discovery.EndpointSlices("n").Update(ctx, slice, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	unknownTemplate := instance.DeepCopy()
	unknownTemplate.Status.TemplateRevisionAliases[legacyRevision] = "different-template"
	if _, err := r.fleetQualification().observeServingIngestProtection(ctx, unknownTemplate, 5*time.Second); err == nil {
		t.Fatal("failback accepted an alias pointing at a different template")
	}
	now = now.Add(time.Minute)
	if changed, err := r.sampleServingIngestProtection(ctx, instance); err != nil || !changed || !instance.Status.LastServingIngestProtection.RegularCapacityRecovered {
		t.Fatalf("restored regular endpoints with equivalent revision labels did not qualify for failback planning: %v", err)
	}
	r.DevelopmentEmergencyFailback, r.Policy = true, core.PolicyV1()
	budget, err := workload.DisruptionBudget(instance, disruptionBudgetName(instance, "blue", "ingest", "a"), "blue", "ingest", "a", 1)
	if err != nil {
		t.Fatal(err)
	}
	budget.Generation, budget.Status.ObservedGeneration, budget.Status.DisruptionsAllowed = 1, 1, 1
	if _, err := r.Policy.PodDisruptionBudgets("n").Create(ctx, budget, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	outage := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "outage", Namespace: "n", UID: "evidence", ResourceVersion: "1"}, Data: map[string]string{}}
	setOutage := func(zones string) {
		outage.Data["evidence.json"] = fmt.Sprintf(`{"instanceUID":"owner","generation":1,"observedAt":%q,"expiresAt":%q,"unavailableZones":%s}`, now.Format(time.RFC3339), now.Add(time.Minute).Format(time.RFC3339), zones)
		if _, err := r.Core.ConfigMaps("n").Update(ctx, outage, metav1.UpdateOptions{}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := r.Core.ConfigMaps("n").Create(ctx, outage, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	setOutage(`[]`)
	evictions := 0
	responseLost := errors.New("eviction response lost")
	core.PrependReactor("create", "pods", func(action kubetesting.Action) (bool, runtime.Object, error) {
		if action.GetSubresource() != "eviction" {
			return false, nil, nil
		}
		evictions++
		if evictions == 1 {
			return true, nil, apierrors.NewTooManyRequests("PDB denied eviction", 1)
		}
		if err := core.Tracker().Delete(corev1.SchemeGroupVersion.WithResource("pods"), "n", pod.Name); err != nil {
			return true, nil, err
		}
		return true, nil, responseLost
	})
	step := func(expectedErr error) {
		if _, handled, err := r.reconcileEmergencyRetirement(ctx, instance); !handled || !errors.Is(err, expectedErr) {
			t.Fatalf("reconcile retirement handled=%t, err=%v, want %v", handled, err, expectedErr)
		}
		if err := kube.Get(ctx, types.NamespacedName{Namespace: "n", Name: "catalog"}, instance); err != nil {
			t.Fatal(err)
		}
	}
	step(nil)
	if instance.Status.EmergencyRetirement == nil || instance.Status.EmergencyRetirement.PodUID != string(pod.UID) || evictions != 0 {
		t.Fatal("the first reconcile must only persist the temporary Pod identity")
	}
	setOutage(`["a"]`)
	step(nil)
	if instance.Status.EmergencyRetirement != nil || evictions != 0 {
		t.Fatal("renewed outage must cancel the intent before eviction")
	}
	setOutage(`[]`)
	step(nil)
	step(nil)
	if instance.Status.EmergencyRetirement == nil || evictions != 1 {
		t.Fatal("PDB denial must preserve the intent")
	}
	step(responseLost)
	step(nil)
	if instance.Status.EmergencyRetirement != nil || evictions != 2 {
		t.Fatal("observing the victim gone must clear intent without replaying eviction")
	}
}
