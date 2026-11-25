package controller

import (
	"context"
	"crypto/tls"
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
	kubernetesfake "k8s.io/client-go/kubernetes/fake"
	clienttesting "k8s.io/client-go/testing"

	"github.com/huyz0/opensearch-serverless-operator/api/v1alpha1"
	"github.com/huyz0/opensearch-serverless-operator/internal/java"
	"github.com/huyz0/opensearch-serverless-operator/internal/rollout"
	"github.com/huyz0/opensearch-serverless-operator/internal/workload"
)

func TestRolloutEdgeProbeBindsHTTPSResponseToRoleService(t *testing.T) {
	ctx := context.Background()
	instance := &v1alpha1.OpenSearchServerless{ObjectMeta: metav1.ObjectMeta{Name: "catalog", Namespace: "n", UID: "owner"}}
	service, err := workload.RoleService(instance, "search", "green", 9200)
	if err != nil {
		t.Fatal(err)
	}
	service.UID, service.ResourceVersion = "service", "10"
	service.Spec.Type, service.Spec.ClusterIP = corev1.ServiceTypeClusterIP, "10.0.0.1"
	controller := true
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "pod", Namespace: "n", UID: "pod-uid", ResourceVersion: "20", OwnerReferences: []metav1.OwnerReference{{APIVersion: v1alpha1.GroupVersion.String(), Kind: "OpenSearchServerless", Name: "catalog", UID: "owner", Controller: &controller}}, Labels: map[string]string{workload.InstanceLabel: "owner", workload.RoleLabel: "search", workload.FleetLabel: "green", workload.ZoneLabel: "a"}}, Spec: corev1.PodSpec{NodeName: "node"}, Status: corev1.PodStatus{Phase: corev1.PodRunning, PodIP: "10.1.0.2", Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}}}
	ready, serving, terminating := true, true, false
	port, portName, protocol, node := int32(9200), "http", corev1.ProtocolTCP, "node"
	slice := &discoveryv1.EndpointSlice{ObjectMeta: metav1.ObjectMeta{Name: "slice", Namespace: "n", Labels: map[string]string{discoveryv1.LabelServiceName: service.Name, discoveryv1.LabelManagedBy: "endpointslice-controller.k8s.io"}, OwnerReferences: []metav1.OwnerReference{{APIVersion: "v1", Kind: "Service", Name: service.Name, UID: service.UID, Controller: &controller}}}, AddressType: discoveryv1.AddressTypeIPv4, Ports: []discoveryv1.EndpointPort{{Name: &portName, Port: &port, Protocol: &protocol}}, Endpoints: []discoveryv1.Endpoint{{Addresses: []string{"10.1.0.2"}, NodeName: &node, Conditions: discoveryv1.EndpointConditions{Ready: &ready, Serving: &serving, Terminating: &terminating}, TargetRef: &corev1.ObjectReference{APIVersion: "v1", Kind: "Pod", Namespace: "n", Name: "pod", UID: pod.UID}}}}
	slice.UID, slice.ResourceVersion = "slice-uid", "1"
	core := kubernetesfake.NewSimpleClientset(service, pod, slice)
	const incarnation = "9aa977f3-4985-4cb5-9692-5ad1e3e243fe"
	var redirect, changeService, wrongRole, staleProcess, missingProcess, requireAuth atomic.Bool
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		switch req.URL.Path {
		case "/search/_serverless/ready", "/ingest/_serverless/ready":
			_, _ = fmt.Fprint(w, `{"ready":true,"state":"ready","serves_searches":true,"process_incarnation":"9aa977f3-4985-4cb5-9692-5ad1e3e243fe","warmup_result":"completed","warmup_timed_out":false}`)
			return
		case "/ingest/_serverless/stats":
			_, _ = fmt.Fprintf(w, `{"node":"id","name":"ingest-pod","process_incarnation":%q,"roles":["ingest"],"lease":{"valid_now":true,"expires_in_millis":60000,"ttl_millis":60000}}`, incarnation)
			return
		}
		if req.URL.Path != "/search/_serverless/stats" {
			http.Error(w, "wrong path", 404)
			return
		}
		if req.Header.Get("Cache-Control") != "no-cache, no-store" {
			t.Error("stats request must disallow cache reuse")
		}
		if requireAuth.Load() {
			user, password, ok := req.BasicAuth()
			if !ok || user != "rollout-check" || password != "fixture-secret" {
				w.WriteHeader(http.StatusUnauthorized)
				return
			}
		}
		if redirect.Load() {
			http.Redirect(w, req, "/other", http.StatusFound)
			return
		}
		if changeService.Load() {
			current, err := core.CoreV1().Services("n").Get(ctx, service.Name, metav1.GetOptions{})
			if err != nil {
				t.Error(err)
				return
			}
			current.ResourceVersion = "11"
			if _, err := core.CoreV1().Services("n").Update(ctx, current, metav1.UpdateOptions{}); err != nil {
				t.Error(err)
				return
			}
		}
		roles := `"search","ingest"`
		if wrongRole.Load() {
			roles = `"ingest"`
		}
		process := incarnation
		if staleProcess.Load() {
			process = "9aa977f3-4985-4cb5-9692-5ad1e3e243ff"
		}
		if missingProcess.Load() {
			_, _ = fmt.Fprintf(w, `{"node":"java-id","name":"pod","roles":[%s],"lease":{"valid_now":true,"expires_in_millis":60000,"ttl_millis":60000}}`, roles)
			return
		}
		_, _ = fmt.Fprintf(w, `{"node":"java-id","name":"pod","process_incarnation":%q,"roles":[%s],"lease":{"valid_now":true,"expires_in_millis":60000,"ttl_millis":60000}}`, process, roles)
	}))
	defer server.Close()
	client := server.Client()
	client.Transport.(*http.Transport).Proxy = nil
	intent := rollout.SwitchIntent{InstanceUID: "owner", OperationUID: "operation", Role: "search", ServiceUID: "service", SourceFleet: "blue", TargetFleet: "green", CandidateRevision: "revision", Phase: rollout.Applied, AppliedAt: time.Now().Add(-time.Second)}
	allowed := map[types.UID]string{"pod-uid": incarnation}
	probe := func(url string, c *http.Client, pods map[types.UID]string) (externalRouteObservation, error) {
		return probeRolloutEdge(ctx, core.CoreV1(), instance, intent, rolloutRouteConfig{URL: url, ID: "ingress/search", Client: c}, pods, time.Second)
	}
	out, err := probe(server.URL+"/search", client, allowed)
	if err != nil || out.ServiceVersion != "10" || out.RespondingPodUID != "pod-uid" || out.RouteID != "ingress/search" {
		t.Fatalf("valid route rejected: %#v %v", out, err)
	}
	disrupted := pod.DeepCopy()
	disrupted.Status.Conditions = append(disrupted.Status.Conditions, corev1.PodCondition{Type: corev1.DisruptionTarget, Status: corev1.ConditionTrue})
	if _, err := core.CoreV1().Pods("n").UpdateStatus(ctx, disrupted, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := probe(server.URL+"/search", client, allowed); err == nil {
		t.Fatal("a valid HTTPS reply from a Ready Pod marked for disruption counted as convergence")
	}
	if _, err := core.CoreV1().Pods("n").UpdateStatus(ctx, pod, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	t.Run("authenticated edge requires an ordinary credential and anonymous denial", func(t *testing.T) {
		protected := instance.DeepCopyObject().(*v1alpha1.OpenSearchServerless)
		protected.Spec.Security = &v1alpha1.SecurityConfig{Provider: serverlessBasicAuth}
		route := rolloutRouteConfig{URL: server.URL + "/search", ID: "ingress/search", Client: client}
		if _, err := probeRolloutEdge(ctx, core.CoreV1(), protected, intent, route, allowed, time.Second); err == nil {
			t.Fatal("protected edge was accepted without a credential")
		}
		route.credential = &rollout.RouteCredential{Username: "rollout-check", Password: []byte("fixture-secret")}
		if _, err := probeRolloutEdge(ctx, core.CoreV1(), protected, intent, route, allowed, time.Second); err == nil {
			t.Fatal("edge accepted anonymous requests while claiming managed Basic Auth")
		}
		requireAuth.Store(true)
		defer requireAuth.Store(false)
		if result, err := probeRolloutEdge(ctx, core.CoreV1(), protected, intent, route, allowed, time.Second); err != nil || result.RespondingPodUID != "pod-uid" {
			t.Fatalf("authenticated edge rejected: %#v %v", result, err)
		}
		route.credential.Password = []byte("wrong")
		if _, err := probeRolloutEdge(ctx, core.CoreV1(), protected, intent, route, allowed, time.Second); err == nil {
			t.Fatal("edge accepted a wrong credential")
		}
	})
	if err := observeRollbackProcess(ctx, client, server.URL+"/search", "pod", "search"); err != nil {
		t.Fatalf("valid rollback source process rejected: %v", err)
	}
	instance.ResourceVersion = "1"
	instance.Status.ServingFleetIntentByRole = map[string]string{"search": "blue", "ingest": "blue"}
	instance.Status.ActiveOperation = &v1alpha1.OperationClaim{Name: "promote", UID: "operation", Action: "Promote", Generation: 1}
	op := &v1alpha1.OpenSearchServerlessOperation{ObjectMeta: metav1.ObjectMeta{Name: "promote", Namespace: "n", UID: "operation", ResourceVersion: "1", Generation: 1}, Spec: v1alpha1.OperationSpec{InstanceRef: v1alpha1.InstanceReference{Name: "catalog", UID: "owner"}, Action: "Promote", TargetFleet: "green", ExpectedServingFleetByRole: map[string]string{"search": "blue", "ingest": "blue"}}}
	appliedAt := metav1.NewMicroTime(intent.AppliedAt)
	op.Status.RolloutRoles = map[string]v1alpha1.RolloutRoleStatus{"search": {ServiceUID: "service", SourceFleet: "blue", TargetFleet: "green", CandidateRevision: "revision", Phase: "Applied", AppliedAt: &appliedAt}, "ingest": {ServiceUID: "ingest-service", SourceFleet: "blue", TargetFleet: "green", CandidateRevision: "revision", Phase: "Prepared"}}
	r := &OperationReconciler{Core: core.CoreV1(), Discovery: core.DiscoveryV1()}
	edgeRoute := rolloutRouteConfig{URL: server.URL + "/search", ID: "ingress/search", Client: client}
	evidence, err := r.probeAndObserveRolloutRoute(ctx, instance, op, "search", edgeRoute, map[string]int{"a": 1}, allowed, time.Second, time.Minute)
	if err != nil || !evidence.EndpointsConverged || !evidence.ClientProbePassed || evidence.ServiceVersion != "10" {
		t.Fatalf("combined route observation rejected: %#v %v", evidence, err)
	}
	pod.Status.ContainerStatuses = []corev1.ContainerStatus{{Name: "opensearch", ContainerID: "container-1", Ready: true, State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}}}}
	ingestPod := pod.DeepCopy()
	ingestPod.Name, ingestPod.UID = "ingest-pod", "ingest-pod-uid"
	ingestPod.Labels[workload.RoleLabel] = "ingest"
	pods := core.CoreV1().Pods("n")
	if _, err := pods.Update(ctx, pod, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := pods.Create(ctx, ingestPod, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	for _, target := range []*corev1.Pod{pod, ingestPod} {
		target.Labels["serverless.opensearch.org/revision"] = "revision"
		if _, err := pods.Update(ctx, target, metav1.UpdateOptions{}); err != nil {
			t.Fatal(err)
		}
	}
	instance.Spec.Pools = map[string]v1alpha1.Pool{
		"search": {Zones: map[string]v1alpha1.Zone{"a": {Replicas: 1, MinServing: 1}}},
		"ingest": {Zones: map[string]v1alpha1.Zone{"a": {Replicas: 1, MinServing: 1}}},
	}
	observer := &InstanceReconciler{Core: core.CoreV1()}
	probes := map[string]rolloutRoleProbe{
		"search": func(ctx context.Context, i *v1alpha1.OpenSearchServerless, name, uid, fleet, zone string, lease time.Duration) (searchPodQualification, error) {
			return qualifySearchPod(ctx, core.CoreV1(), i, name, uid, fleet, zone, func(ctx context.Context, pod *corev1.Pod) (java.SearchQualification, error) {
				return java.QualifySearch(ctx, client, server.URL+"/search", pod.Name, lease)
			})
		},
		"ingest": func(ctx context.Context, i *v1alpha1.OpenSearchServerless, name, uid, fleet, zone string, lease time.Duration) (searchPodQualification, error) {
			return qualifyRolePod(ctx, core.CoreV1(), i, name, uid, fleet, zone, "ingest", func(ctx context.Context, pod *corev1.Pod) (java.RoleQualification, error) {
				return java.QualifyIngest(ctx, client, server.URL+"/ingest", pod.Name, lease)
			})
		},
	}
	routeConfig := map[string]rolloutRouteConfig{
		"search": {URL: server.URL + "/search", ID: "ingress/search", Client: client},
		"ingest": {URL: server.URL + "/ingest", ID: "ingress/ingest", Client: client},
	}
	rejectIngest, replaceIngestObservation, replaceSearchPlatformReport := false, false, false
	var changeSearchWhileQualifyingIngest func()
	platformTime := time.Now().Add(-time.Second)
	read := r.liveRolloutEvidence(observer.fleetQualification(), rolloutTLSQualification{http: observer.qualifyHTTPRotation, peer: observer.qualifyPeerRotation}, probes, routeConfig, func(_ context.Context, _ *v1alpha1.OpenSearchServerless, _ *v1alpha1.OpenSearchServerlessOperation, role string, observed candidateRoleObservation) (rolloutPlatformObservation, error) {
		if len(observed.Pods) != 1 || len(observed.PodSnapshot) != 1 || observed.Pods[0].PodUID != string(observed.PodSnapshot[0].UID) ||
			(role == "ingest" && rejectIngest) {
			return rolloutPlatformObservation{}, fmt.Errorf("platform qualification unavailable")
		}
		if role == "ingest" && replaceIngestObservation {
			newVersion := observed.PodSnapshot[0].DeepCopy()
			newVersion.ResourceVersion = "21"
			if _, err := pods.Update(ctx, newVersion, metav1.UpdateOptions{}); err != nil {
				return rolloutPlatformObservation{}, err
			}
		}
		if role == "ingest" && changeSearchWhileQualifyingIngest != nil {
			changeSearchWhileQualifyingIngest()
		}
		return rolloutPlatformObservation{observedAt: platformTime, unchanged: func(context.Context) bool {
			return role != "search" || !replaceSearchPlatformReport
		}}, nil
	})
	checkedEvidence := func() (map[string]*rollout.CandidateEvidence, map[string]*rollout.RouteEvidence, error) {
		evidence, err := read(ctx, instance, op)
		if err == nil {
			evidence.revalidate(ctx, &evidence)
		}
		return evidence.Candidates, evidence.Routes, err
	}
	candidates, observedRoutes, err := checkedEvidence()
	if err != nil || len(candidates) != 2 || len(observedRoutes) != 1 || observedRoutes["search"] == nil ||
		!candidates["search"].ObservedAt.Equal(platformTime) {
		t.Fatalf("live evidence did not bind both roles and the applied route: %#v %#v %v", candidates, observedRoutes, err)
	}
	replaceSearchPlatformReport = true
	candidates, observedRoutes, err = checkedEvidence()
	if err != nil || candidates["search"] != nil || candidates["ingest"] == nil || observedRoutes["search"] != nil {
		t.Fatalf("changed platform report retained search evidence: %#v %#v %v", candidates, observedRoutes, err)
	}
	replaceSearchPlatformReport = false
	missingProcess.Store(true)
	candidates, observedRoutes, err = checkedEvidence()
	if err != nil || candidates["search"] != nil || observedRoutes["search"] != nil {
		t.Fatalf("a runtime without stats identity must not qualify for a selector switch: %#v %#v %v", candidates, observedRoutes, err)
	}
	missingProcess.Store(false)
	rejectIngest = true
	candidates, observedRoutes, err = checkedEvidence()
	if err != nil || len(candidates) != 1 || candidates["ingest"] != nil || observedRoutes["search"] == nil {
		t.Fatalf("unqualified indexing role should be omitted: %#v %#v %v", candidates, observedRoutes, err)
	}
	rejectIngest, replaceIngestObservation = false, true
	candidates, _, err = checkedEvidence()
	if err != nil || len(candidates) != 1 || candidates["ingest"] != nil {
		t.Fatalf("changed Pod must be requalified after platform verification: %#v %v", candidates, err)
	}
	replaceIngestObservation = false
	changeSearchWhileQualifyingIngest = func() {
		updated := pod.DeepCopy()
		updated.ResourceVersion = "22"
		if _, err := pods.Update(ctx, updated, metav1.UpdateOptions{}); err != nil {
			t.Fatal(err)
		}
	}
	candidates, observedRoutes, err = checkedEvidence()
	if err != nil || candidates["search"] != nil || candidates["ingest"] == nil || observedRoutes["search"] != nil {
		t.Fatalf("search Pod changed during ingest qualification but retained evidence: %#v %#v %v", candidates, observedRoutes, err)
	}
	changeSearchWhileQualifyingIngest = func() {
		updated := slice.DeepCopy()
		updated.ResourceVersion = "2"
		if _, err := core.DiscoveryV1().EndpointSlices("n").Update(ctx, updated, metav1.UpdateOptions{}); err != nil {
			t.Fatal(err)
		}
	}
	candidates, observedRoutes, err = checkedEvidence()
	if err != nil || candidates["search"] == nil || candidates["ingest"] == nil || observedRoutes["search"] != nil {
		t.Fatalf("search EndpointSlice changed during ingest qualification but retained route evidence: %#v %#v %v", candidates, observedRoutes, err)
	}
	var changePodAfterProbe atomic.Bool
	core.PrependReactor("list", "endpointslices", func(clienttesting.Action) (bool, runtime.Object, error) {
		if changePodAfterProbe.Swap(false) {
			updated := pod.DeepCopy()
			updated.ResourceVersion = "21"
			if err := core.Tracker().Update(corev1.SchemeGroupVersion.WithResource("pods"), updated, "n"); err != nil {
				return true, nil, err
			}
		}
		return false, nil, nil
	})
	changePodAfterProbe.Store(true)
	if _, err := r.probeAndObserveRolloutRoute(ctx, instance, op, "search", edgeRoute, map[string]int{"a": 1}, allowed, time.Second, time.Minute); err == nil || !strings.Contains(err.Error(), "responding Pod changed") {
		t.Fatalf("Pod version change between HTTPS and EndpointSlice reads was not rejected: %v", err)
	}
	if _, err := probe("http://example.com", client, allowed); err == nil {
		t.Fatal("plaintext accepted")
	}
	if _, err := probe(server.URL+"/search", client, map[types.UID]string{"other": incarnation}); err == nil {
		t.Fatal("unqualified Pod accepted")
	}
	staleProcess.Store(true)
	if _, err := probe(server.URL+"/search", client, allowed); err == nil || !strings.Contains(err.Error(), "does not match the qualified process") {
		t.Fatalf("different JVM with the same Pod name and UID was not rejected: %v", err)
	}
	staleProcess.Store(false)
	missingProcess.Store(true)
	if _, err := probe(server.URL+"/search", client, allowed); err == nil || !strings.Contains(err.Error(), "does not match the qualified process") {
		t.Fatalf("legacy response without a JVM identity was not rejected: %v", err)
	}
	if err := observeRollbackProcess(ctx, client, server.URL+"/search", "pod", "search"); err == nil {
		t.Fatal("legacy JVM must not qualify as a rollback source")
	}
	missingProcess.Store(false)
	wrongRole.Store(true)
	if _, err := probe(server.URL+"/search", client, allowed); err == nil {
		t.Fatal("response from node without search role accepted")
	}
	wrongRole.Store(false)
	insecure := &http.Client{Transport: &http.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: true}}}
	if _, err := probe(server.URL+"/search", insecure, allowed); err == nil {
		t.Fatal("unverified TLS accepted")
	}
	redirect.Store(true)
	if _, err := probe(server.URL+"/search", client, allowed); err == nil {
		t.Fatal("redirect accepted")
	}
	redirect.Store(false)
	changeService.Store(true)
	if _, err := probe(server.URL+"/search", client, allowed); err == nil {
		t.Fatal("Service changed during request")
	}
}
