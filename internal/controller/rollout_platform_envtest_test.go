//go:build envtest

package controller

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/x509"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/huyz0/opensearch-serverless-operator/api/v1alpha1"
	"github.com/huyz0/opensearch-serverless-operator/internal/java"
	"github.com/huyz0/opensearch-serverless-operator/internal/rollout"
	"github.com/huyz0/opensearch-serverless-operator/internal/workload"
)

type publishedRolloutFixture struct {
	readEvidence     rolloutEvidenceReader
	sourceProbe      func(context.Context, *v1alpha1.OpenSearchServerless, *corev1.Pod, string) error
	publishEndpoints func(string)
	oldSearchRoute   *atomic.Bool
	verifyData       func()
	verifyRollback   func()
}

func prepareWithPublishedEvidence(t *testing.T, ctx context.Context, c client.Client, kube kubernetes.Interface, instance *v1alpha1.OpenSearchServerless, op *v1alpha1.OpenSearchServerlessOperation) publishedRolloutFixture {
	t.Helper()
	instance.Status.ActiveOperation = nil
	if err := c.Status().Update(ctx, instance); err != nil {
		t.Fatal(err)
	}
	public, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	services := make(map[string]*corev1.Service, 2)
	for _, role := range []string{"search", "ingest"} {
		service, err := workload.RoleService(instance, role, "green", 9200)
		if err != nil {
			t.Fatal(err)
		}
		services[role] = service
	}
	var sourceSearchNames, searchNames []string
	sourceSearch := services["search"].Name + "-source"
	for zone := range instance.Spec.Pools["search"].Zones {
		searchNames = append(searchNames, "publisher-search-"+zone)
		if zone != "a" {
			sourceSearchNames = append(sourceSearchNames, sourceSearch+"-"+zone)
		}
	}
	javaEndpoints, stopGreenIngest := launchJavaRollout(t, sourceSearch, services["ingest"].Name+"-source", sourceSearchNames, searchNames)
	directClient := &http.Client{Timeout: 2 * time.Second}
	proxies := make(map[string]*httputil.ReverseProxy, len(javaEndpoints))
	for name, endpoint := range javaEndpoints {
		target, err := url.Parse(endpoint)
		if err != nil {
			t.Fatal(err)
		}
		proxies[name] = httputil.NewSingleHostReverseProxy(target)
	}
	var oldSearchRoute atomic.Bool
	// Stand in for a proxy that follows the live Service selector, with
	// a toggle to simulate search still serving the old target.
	edge := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		role, path, ok := strings.Cut(strings.TrimPrefix(req.URL.Path, "/"), "/")
		if !ok || services[role] == nil {
			http.NotFound(w, req)
			return
		}
		service, err := kube.CoreV1().Services(instance.Namespace).Get(req.Context(), services[role].Name, metav1.GetOptions{})
		if err != nil {
			http.Error(w, "service unavailable", http.StatusServiceUnavailable)
			return
		}
		podName := "publisher-" + role + "-a"
		if service.Spec.Selector[workload.FleetLabel] != "green" || (role == "search" && oldSearchRoute.Load()) {
			podName = service.Name + "-source"
		}
		if proxy := proxies[podName]; proxy != nil {
			routed := req.Clone(req.Context())
			routed.URL.Path, routed.URL.RawPath = "/"+path, ""
			proxy.ServeHTTP(w, routed)
			return
		}
		_, _ = fmt.Fprintf(w, `{"node":"fixture","name":%q,"process_incarnation":"9aa977f3-4985-4cb5-9692-5ad1e3e243fe","roles":[%q],"lease":{"valid_now":true,"expires_in_millis":60000,"ttl_millis":60000}}`, podName, role)
	}))
	t.Cleanup(edge.Close)
	roots := x509.NewCertPool()
	roots.AddCert(edge.Certificate())
	observer := &InstanceReconciler{
		Core: kube.CoreV1(), DevelopmentGatedPods: true, DevelopmentReleaseGates: true,
		DevelopmentWarmupImage: instance.Spec.Image, DevelopmentServiceProbeImage: instance.Spec.Image,
		ObserveSearchQualification: func(ctx context.Context, pod *corev1.Pod) (java.SearchQualification, error) {
			if javaEndpoints != nil {
				return java.QualifySearch(ctx, directClient, javaEndpoints[pod.Name], pod.Name, 5*time.Second)
			}
			return java.SearchQualification{ProcessIncarnation: "9aa977f3-4985-4cb5-9692-5ad1e3e243fe", StatsProcessVerified: true, LeaseValidUntil: time.Now().Add(time.Minute)}, nil
		},
		ObserveIngestQualification: func(ctx context.Context, pod *corev1.Pod) (java.IngestQualification, error) {
			if javaEndpoints != nil {
				return java.QualifyIngest(ctx, directClient, javaEndpoints[pod.Name], pod.Name, 5*time.Second)
			}
			return java.IngestQualification{ProcessIncarnation: "9aa977f3-4985-4cb5-9692-5ad1e3e243fe", StatsProcessVerified: true, LeaseValidUntil: time.Now().Add(time.Minute)}, nil
		},
	}
	reconciler := &OperationReconciler{Client: c, Core: kube.CoreV1(), Policy: kube.PolicyV1(), Discovery: kube.DiscoveryV1(), RolloutReader: c}
	if err := reconciler.ConfigureDevelopmentRollout(observer, instance.Name, edge.URL+"/search", edge.URL+"/ingest", public, roots, "", ""); err != nil {
		t.Fatal(err)
	}
	reconciler.rolloutSourceProbe = func(ctx context.Context, _ *v1alpha1.OpenSearchServerless, pod *corev1.Pod, role string) error {
		if javaEndpoints == nil {
			return nil
		}
		return observeRollbackProcess(ctx, directClient, javaEndpoints[pod.Name], pod.Name, role)
	}
	request := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(op)}
	reconcile := func() {
		t.Helper()
		if _, err := reconciler.Reconcile(ctx, request); err != nil {
			t.Fatal(err)
		}
	}
	reconcile() // Install the operation finalizer first.

	byRole := make(map[string][]rollout.PlatformPod, 2)
	endpoints := make(map[string][]discoveryv1.Endpoint, 2)
	ipIndex := 2
	for role, pool := range instance.Spec.Pools {
		for zone := range pool.Zones {
			name := "publisher-" + role + "-" + zone
			node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: name, Labels: map[string]string{corev1.LabelTopologyZone: zone}}}
			if _, err := kube.CoreV1().Nodes().Create(ctx, node, metav1.CreateOptions{}); err != nil {
				t.Fatal(err)
			}
			pod := &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: instance.Namespace,
					OwnerReferences: []metav1.OwnerReference{*metav1.NewControllerRef(instance, v1alpha1.GroupVersion.WithKind("OpenSearchServerless"))},
					Labels: map[string]string{
						workload.InstanceLabel: string(instance.UID), workload.RoleLabel: role, workload.ZoneLabel: zone,
						workload.FleetLabel: "green", "serverless.opensearch.org/revision": instance.Status.CandidateFleet.Revision,
					},
				},
				Spec: corev1.PodSpec{NodeName: name, Containers: []corev1.Container{{Name: "opensearch", Image: instance.Spec.Image}}},
			}
			pods := kube.CoreV1().Pods(instance.Namespace)
			pod, err := pods.Create(ctx, pod, metav1.CreateOptions{})
			if err != nil {
				t.Fatal(err)
			}
			pod.Status.Phase, pod.Status.PodIP = corev1.PodRunning, fmt.Sprintf("10.0.0.%d", ipIndex)
			ipIndex++
			pod.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}
			pod.Status.ContainerStatuses = []corev1.ContainerStatus{{
				Name: "opensearch", ContainerID: "containerd://" + name, Image: instance.Spec.Image, ImageID: instance.Spec.Image,
				Ready: true, State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}},
			}}
			pod, err = pods.UpdateStatus(ctx, pod, metav1.UpdateOptions{})
			if err != nil {
				t.Fatal(err)
			}
			incarnation := "9aa977f3-4985-4cb5-9692-5ad1e3e243fe"
			if javaEndpoints != nil {
				warm, err := java.GetWarmup(ctx, directClient, javaEndpoints[pod.Name])
				if err != nil {
					t.Fatal(err)
				}
				incarnation = warm.ProcessIncarnation
			}
			byRole[role] = append(byRole[role], rollout.PlatformPod{
				UID: pod.UID, ResourceVersion: pod.ResourceVersion, NodeName: name, Zone: zone,
				ProcessIncarnation: incarnation,
			})
			ready := true
			endpoints[role] = append(endpoints[role], discoveryv1.Endpoint{
				Addresses: []string{pod.Status.PodIP}, NodeName: &pod.Spec.NodeName,
				Conditions: discoveryv1.EndpointConditions{Ready: &ready, Serving: &ready},
				TargetRef:  &corev1.ObjectReference{APIVersion: "v1", Kind: "Pod", Namespace: pod.Namespace, Name: pod.Name, UID: pod.UID},
			})
		}
	}
	publish := func(role string) {
		t.Helper()
		evidence := rollout.PlatformEvidence{
			Namespace: instance.Namespace, InstanceUID: instance.UID, InstanceGeneration: instance.Generation,
			OperationUID: op.UID, Role: role, Fleet: "green", Revision: instance.Status.CandidateFleet.Revision,
			ObservedAt: time.Now(), PeerQualified: true, SecurityQualified: true, RollbackCompatible: true, Pods: byRole[role],
		}
		if err := rollout.PublishPlatformEvidence(ctx, c, kube.CoreV1(), instance.Namespace, op.Name, evidence, private); err != nil {
			t.Fatal(err)
		}
	}
	publish("search")
	reconcile()
	if err := c.Get(ctx, client.ObjectKeyFromObject(instance), instance); err != nil || instance.Status.ActiveOperation != nil {
		t.Fatalf("promotion acquired the claim before indexing evidence arrived: %v", err)
	}
	publish("ingest")
	reconcile() // Claim.
	reconcile() // Prepare.
	if err := c.Get(ctx, client.ObjectKeyFromObject(instance), instance); err != nil || instance.Status.ActiveOperation == nil || instance.Status.ActiveOperation.UID != string(op.UID) {
		t.Fatalf("publisher evidence did not allow the promotion claim: %v", err)
	}
	if err := c.Get(ctx, request.NamespacedName, op); err != nil || len(op.Status.RolloutRoles) != 2 {
		t.Fatalf("promotion did not prepare both roles: %#v %v", op.Status.RolloutRoles, err)
	}
	publishEndpoints := func(role string) {
		t.Helper()
		service, err := kube.CoreV1().Services(instance.Namespace).Get(ctx, services[role].Name, metav1.GetOptions{})
		if err != nil || service.Spec.Selector[workload.FleetLabel] != "green" {
			t.Fatalf("cannot publish EndpointSlice before %s switches: %v", role, err)
		}
		portName, port, protocol := "http", int32(9200), corev1.ProtocolTCP
		slice := &discoveryv1.EndpointSlice{
			ObjectMeta: metav1.ObjectMeta{Name: service.Name + "-green", Namespace: service.Namespace,
				Labels:          map[string]string{discoveryv1.LabelServiceName: service.Name, discoveryv1.LabelManagedBy: "endpointslice-controller.k8s.io"},
				OwnerReferences: []metav1.OwnerReference{*metav1.NewControllerRef(service, corev1.SchemeGroupVersion.WithKind("Service"))}},
			AddressType: discoveryv1.AddressTypeIPv4,
			Ports:       []discoveryv1.EndpointPort{{Name: &portName, Port: &port, Protocol: &protocol}},
			Endpoints:   endpoints[role],
		}
		if _, err := kube.DiscoveryV1().EndpointSlices(service.Namespace).Create(ctx, slice, metav1.CreateOptions{}); err != nil {
			t.Fatal(err)
		}
	}
	verifySearch := func() {
		t.Helper()
		if javaEndpoints == nil {
			return
		}
		deadline := time.Now().Add(10 * time.Second)
		for {
			result := sendRolloutJavaFixtureRequest(t, edge.Client(), http.MethodGet, edge.URL+"/search/rollout-proof/_search?q=message:promotion", "")
			if strings.Contains(result, "before promotion") && strings.Contains(result, "after promotion") {
				return
			}
			if time.Now().After(deadline) {
				t.Fatalf("the serving search route did not see both writes: %s", result)
			}
			time.Sleep(150 * time.Millisecond)
		}
	}
	var candidateOwnedIndex string
	verifyData := func() {
		t.Helper()
		if javaEndpoints != nil {
			sendRolloutJavaFixtureRequest(t, edge.Client(), http.MethodPut, edge.URL+"/ingest/rollout-proof/_doc/2?refresh=true", `{"message":"after promotion"}`)
			verifySearch()
			if os.Getenv("OPERATOR_ROLLOUT_S3_BINARY") != "" {
				candidateOwnedIndex = writeCandidateOwnedRolloutShard(t, ctx, directClient, edge.Client(),
					javaEndpoints[services["ingest"].Name+"-source"], javaEndpoints["publisher-ingest-a"], edge.URL)
			}
		}
	}
	verifyRollback := func() {
		t.Helper()
		if javaEndpoints != nil {
			verifySearch()
			if candidateOwnedIndex != "" {
				verifyCandidateOwnedRolloutDocument(t, edge.Client(), edge.URL, candidateOwnedIndex, "candidate owned write")
				t.Log("Restored search read the document written on the green-owned shard")
				verifyRolloutShardTakeover(t, directClient, edge.Client(), javaEndpoints[services["ingest"].Name+"-source"],
					javaEndpoints["publisher-ingest-a"], edge.URL, candidateOwnedIndex, stopGreenIngest)
			}
			t.Log("Restored search returned documents written before and after promotion")
		}
	}
	return publishedRolloutFixture{
		readEvidence: reconciler.RolloutEvidence, publishEndpoints: publishEndpoints, oldSearchRoute: &oldSearchRoute,
		sourceProbe: reconciler.rolloutSourceProbe, verifyData: verifyData, verifyRollback: verifyRollback,
	}
}
