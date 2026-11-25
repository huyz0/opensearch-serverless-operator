//go:build envtest

package controller

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"

	"github.com/huyz0/opensearch-serverless-operator/api/v1alpha1"
	"github.com/huyz0/opensearch-serverless-operator/internal/java"
	"github.com/huyz0/opensearch-serverless-operator/internal/rollout"
	"github.com/huyz0/opensearch-serverless-operator/internal/workload"
	"github.com/huyz0/opensearch-serverless-operator/pkg/podlifecycle"
	podkube "github.com/huyz0/opensearch-serverless-operator/pkg/podlifecycle/kubernetes"
)

type serviceRoundTripFunc func(*http.Request) (*http.Response, error)

func (f serviceRoundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestStrictBootstrapIntentOnAPIServer(t *testing.T) {
	environment := &envtest.Environment{CRDDirectoryPaths: []string{filepath.Join("..", "..", "config", "generated-dev")}, ErrorIfCRDPathMissing: true}
	config, err := environment.Start()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := environment.Stop(); err != nil {
			t.Error(err)
		}
	})
	scheme := runtime.NewScheme()
	if err := v1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	kube, err := client.New(config, client.Options{Scheme: scheme})
	if err != nil {
		t.Fatal(err)
	}
	core, err := kubernetes.NewForConfig(config)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	instance := &v1alpha1.OpenSearchServerless{ObjectMeta: metav1.ObjectMeta{Name: "strict", Namespace: "default"}, Spec: v1alpha1.OpenSearchServerlessSpec{Image: "example/image@sha256:" + strings.Repeat("a", 64), ServiceAccountName: "runtime", Storage: v1alpha1.Storage{Type: "s3", Bucket: "bucket", Region: "region"}, Pools: map[string]v1alpha1.Pool{"search": {JVMHeap: "512Mi", WarmOnJoin: true, WarmReadiness: "Strict", ColdStartPolicy: "AllowOneDegraded", Zones: map[string]v1alpha1.Zone{"a": {Replicas: 2}}, Resources: corev1.ResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("1"), corev1.ResourceMemory: resource.MustParse("1Gi")}}}}}}
	if err := kube.Create(ctx, instance); err != nil {
		t.Fatal(err)
	}
	instance.Status.PrimaryStore = &v1alpha1.PrimaryStoreStatus{Bucket: "bucket", Region: "region"}
	instance.Status.ServingFleetIntentByRole = map[string]string{"search": "initial"}
	if err := kube.Status().Update(ctx, instance); err != nil {
		t.Fatal(err)
	}
	key := types.NamespacedName{Namespace: instance.Namespace, Name: instance.Name}
	r := &InstanceReconciler{Client: kube, Core: core.CoreV1(), DevelopmentWarmupImage: instance.Spec.Image}
	step := func() {
		t.Helper()
		if err := kube.Get(ctx, key, instance); err != nil {
			t.Fatal(err)
		}
		if _, err := r.reconcileGatedPod(ctx, instance); err != nil {
			t.Fatal(err)
		}
	}
	step()
	if err := kube.Get(ctx, key, instance); err != nil {
		t.Fatal(err)
	}
	intent, slot := instance.Status.PendingPodCreation, instance.Status.ColdBootstrap
	if intent == nil || slot == nil || intent.WarmupMode != "Bootstrap" || slot.Name != intent.Name || slot.PodUID != "" {
		t.Fatalf("bootstrap intent was not durably reserved: %#v", instance.Status)
	}
	step()
	if err := kube.Get(ctx, key, instance); err != nil {
		t.Fatal(err)
	}
	slot = instance.Status.ColdBootstrap
	if instance.Status.PendingPodCreation != nil || slot == nil || slot.PodUID == "" {
		t.Fatalf("created bootstrap UID not committed: %#v", instance.Status)
	}
	pod, err := core.CoreV1().Pods("default").Get(ctx, slot.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	identity := podlifecycle.Identity{Namespace: "default", Name: pod.Name, UID: string(pod.UID), OwnerUID: string(instance.UID)}
	if err := podkube.ReleaseSchedulingGate(ctx, core.CoreV1(), identity, "serverless.opensearch.org/placement"); err != nil {
		t.Fatal(err)
	}
	if err := core.CoreV1().Pods("default").Bind(ctx, &corev1.Binding{ObjectMeta: metav1.ObjectMeta{Name: pod.Name, UID: pod.UID}, Target: corev1.ObjectReference{Kind: "Node", Name: "node-a"}}, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	step()
	if err := kube.Get(ctx, key, instance); err != nil {
		t.Fatal(err)
	}
	if instance.Status.PendingPodCreation == nil || instance.Status.PendingPodCreation.WarmupMode != "Strict" || instance.Status.ColdBootstrap.PodUID != identity.UID {
		t.Fatalf("second Pod did not preserve bootstrap and require strict mode: %#v", instance.Status)
	}
	step()
	bootstrap, err := core.CoreV1().Pods("default").Get(ctx, identity.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	bootstrap.Annotations[warmupModeAnnotation] = "Strict"
	if _, err := core.CoreV1().Pods("default").Update(ctx, bootstrap, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	if err := kube.Get(ctx, key, instance); err != nil {
		t.Fatal(err)
	}
	if _, err := r.reconcileGatedPod(ctx, instance); err == nil {
		t.Fatal("edited bootstrap mode did not hold reconciliation")
	}

	flow := &v1alpha1.OpenSearchServerless{ObjectMeta: metav1.ObjectMeta{Name: "strict-flow", Namespace: "default"}, Spec: instance.Spec}
	if err := kube.Create(ctx, flow); err != nil {
		t.Fatal(err)
	}
	flowKey := types.NamespacedName{Namespace: flow.Namespace, Name: flow.Name}
	flowReconciler := &InstanceReconciler{Client: kube, Core: core.CoreV1(), Policy: core.PolicyV1(), DevelopmentGatedPods: true, DevelopmentReleaseGates: true, DevelopmentWarmupImage: flow.Spec.Image, ObserveWarmup: func(context.Context, *corev1.Pod) (java.Warmup, error) {
		return java.Warmup{ProcessIncarnation: "9aa977f3-4985-4cb5-9692-5ad1e3e243fe", Result: "degraded", Ready: true}, nil
	}, ObserveSearchQualification: func(context.Context, *corev1.Pod) (java.SearchQualification, error) {
		return java.SearchQualification{}, fmt.Errorf("degraded")
	}}
	reconcile := func() {
		t.Helper()
		if _, err := flowReconciler.Reconcile(ctx, ctrl.Request{NamespacedName: flowKey}); err != nil {
			t.Fatal(err)
		}
		budgets, err := core.PolicyV1().PodDisruptionBudgets("default").List(ctx, metav1.ListOptions{})
		if err != nil {
			t.Fatal(err)
		}
		for i := range budgets.Items {
			budget := &budgets.Items[i]
			owner := metav1.GetControllerOf(budget)
			if owner != nil && owner.UID == flow.UID && budget.Status.ObservedGeneration != budget.Generation {
				budget.Status.ObservedGeneration = budget.Generation
				if _, err := core.PolicyV1().PodDisruptionBudgets("default").UpdateStatus(ctx, budget, metav1.UpdateOptions{}); err != nil {
					t.Fatal(err)
				}
			}
		}
		if err := kube.Get(ctx, flowKey, flow); err != nil {
			t.Fatal(err)
		}
	}
	var bootstrapPod *corev1.Pod
	for i := 0; i < 30; i++ {
		reconcile()
		if flow.Status.ColdBootstrap == nil || flow.Status.ColdBootstrap.PodUID == "" {
			continue
		}
		bootstrapPod, err = core.CoreV1().Pods("default").Get(ctx, flow.Status.ColdBootstrap.Name, metav1.GetOptions{})
		if err != nil {
			t.Fatal(err)
		}
		if !hasPlacementGate(bootstrapPod) && flow.Status.ProvisioningAttempt != nil {
			break
		}
	}
	if bootstrapPod == nil || hasPlacementGate(bootstrapPod) || flow.Status.SearchWarmupSlot == nil || flow.Status.ProvisioningAttempt == nil {
		t.Fatalf("normal reconciliation did not release the reserved bootstrap: %#v", flow.Status)
	}
	if err := core.CoreV1().Pods("default").Bind(ctx, &corev1.Binding{ObjectMeta: metav1.ObjectMeta{Name: bootstrapPod.Name, UID: bootstrapPod.UID}, Target: corev1.ObjectReference{Kind: "Node", Name: "node-a"}}, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	bootstrapPod, err = core.CoreV1().Pods("default").Get(ctx, bootstrapPod.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	bootstrapPod.Status = corev1.PodStatus{Phase: corev1.PodRunning, PodIP: "10.0.0.1", Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}, ContainerStatuses: []corev1.ContainerStatus{{Name: "opensearch", ContainerID: "container-1", Ready: true, State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}}}}}
	if _, err := core.CoreV1().Pods("default").UpdateStatus(ctx, bootstrapPod, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 20; i++ {
		reconcile()
		if flow.Status.PendingPodCreation != nil && flow.Status.PendingPodCreation.WarmupMode == "Strict" {
			break
		}
	}
	if flow.Status.PendingPodCreation == nil || flow.Status.PendingPodCreation.WarmupMode != "Strict" || flow.Status.SearchWarmupSlot != nil || flow.Status.LastSearchWarmup == nil || flow.Status.LastSearchWarmup.Result != "degraded" {
		t.Fatalf("normal reconciliation did not progress to strict successor: %#v", flow.Status)
	}
	strictName := flow.Status.PendingPodCreation.Name
	var strictPod *corev1.Pod
	for i := 0; i < 20; i++ {
		reconcile()
		pod, getErr := core.CoreV1().Pods("default").Get(ctx, strictName, metav1.GetOptions{})
		if getErr != nil {
			continue
		}
		strictPod = pod
		if !hasPlacementGate(pod) && flow.Status.SearchWarmupSlot != nil && flow.Status.SearchWarmupSlot.PodUID == string(pod.UID) {
			break
		}
	}
	if strictPod == nil || hasPlacementGate(strictPod) || flow.Status.SearchWarmupSlot == nil || flow.Status.SearchWarmupSlot.PodUID != string(strictPod.UID) {
		t.Fatalf("strict successor did not obtain warm-up slot and release its gate: %#v", flow.Status)
	}
	if strictPod.Annotations[warmupModeAnnotation] != "Strict" || !slices.Contains(strictPod.Spec.Containers[0].Args, "-Dserverless.search.require_warmup_completion=true") || !slices.Contains(strictPod.Spec.Containers[0].ReadinessProbe.Exec.Command, "-require-warmup-completion") {
		t.Fatal("strict successor lacks Java and kubelet completion gates")
	}

	flowService, err := core.CoreV1().Services("default").Get(ctx, "strict-flow-search", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	settingsBackend := developmentHTTPSettings{core: core.CoreV1(), http: &http.Client{Transport: serviceRoundTripFunc(func(req *http.Request) (*http.Response, error) {
		service, err := core.CoreV1().Services("default").Get(ctx, "strict-flow-index", metav1.GetOptions{})
		if err != nil {
			return nil, err
		}
		if service.Annotations == nil {
			service.Annotations = map[string]string{}
		}
		service.Annotations["test.example/change"] = "during-read"
		if _, err := core.CoreV1().Services("default").Update(ctx, service, metav1.UpdateOptions{}); err != nil {
			return nil, err
		}
		return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(`{"persistent":{"serverless.bucket":"auto"}}`))}, nil
	})}}
	if _, err := settingsBackend.Read(ctx, flow); err == nil {
		t.Fatal("settings read accepted a Service changed during the request")
	}
	isController, isReady, isServing, isTerminating := true, true, true, false
	portName, port, protocol, nodeName := "http", int32(9200), corev1.ProtocolTCP, "node-a"
	endpointSlice := &discoveryv1.EndpointSlice{ObjectMeta: metav1.ObjectMeta{Name: "strict-flow-test-slice", Namespace: "default", Labels: map[string]string{discoveryv1.LabelServiceName: flowService.Name, discoveryv1.LabelManagedBy: "endpointslice-controller.k8s.io"}, OwnerReferences: []metav1.OwnerReference{{APIVersion: "v1", Kind: "Service", Name: flowService.Name, UID: flowService.UID, Controller: &isController}}}, AddressType: discoveryv1.AddressTypeIPv4, Ports: []discoveryv1.EndpointPort{{Name: &portName, Port: &port, Protocol: &protocol}}, Endpoints: []discoveryv1.Endpoint{{Addresses: []string{"10.0.0.1"}, NodeName: &nodeName, Conditions: discoveryv1.EndpointConditions{Ready: &isReady, Serving: &isServing, Terminating: &isTerminating}, TargetRef: &corev1.ObjectReference{APIVersion: "v1", Kind: "Pod", Namespace: "default", Name: bootstrapPod.Name, UID: bootstrapPod.UID}}}}
	endpointSlice, err = core.DiscoveryV1().EndpointSlices("default").Create(ctx, endpointSlice, metav1.CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	qualified := map[types.UID]struct{}{bootstrapPod.UID: {}}
	observation, err := observeRoleEndpoints(ctx, core.CoreV1(), core.DiscoveryV1(), flow, "search", "initial", flowService.UID, map[string]int{"a": 1}, qualified)
	if err != nil || observation.ReadyByZone["a"] != 1 {
		t.Fatalf("valid endpoint observation failed: %#v %v", observation, err)
	}
	preview, err := workload.PreviewService(flow, "search", "initial")
	if err != nil {
		t.Fatal(err)
	}
	preview, err = core.CoreV1().Services("default").Create(ctx, preview, metav1.CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	previewSlice := endpointSlice.DeepCopy()
	previewSlice.Name, previewSlice.UID, previewSlice.ResourceVersion = "strict-preview-test-slice", "", ""
	previewSlice.Labels[discoveryv1.LabelServiceName] = preview.Name
	previewSlice.OwnerReferences[0].Name, previewSlice.OwnerReferences[0].UID = preview.Name, preview.UID
	if _, err := core.DiscoveryV1().EndpointSlices("default").Create(ctx, previewSlice, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := observeServiceEndpoints(ctx, core.CoreV1(), core.DiscoveryV1(), flow, "search", "initial", preview, preview.UID, map[string]int{"a": 1}, qualified); err != nil {
		t.Fatalf("preview endpoint observation failed: %v", err)
	}
	if _, err := observeServiceEndpoints(ctx, core.CoreV1(), core.DiscoveryV1(), flow, "search", "initial", preview, flowService.UID, map[string]int{"a": 1}, qualified); err == nil {
		t.Fatal("different preview Service UID accepted")
	}
	sampleNow := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	sampler := &InstanceReconciler{Client: kube, Core: core.CoreV1(), Discovery: core.DiscoveryV1(), Now: func() time.Time { return sampleNow }}
	if changed, err := sampler.sampleEndpointObservation(ctx, flow); err != nil || !changed {
		t.Fatalf("endpoint status sample failed: %v", err)
	}
	if err := kube.Get(ctx, flowKey, flow); err != nil {
		t.Fatal(err)
	}
	if flow.Status.LastEndpointObservation == nil || flow.Status.LastEndpointObservation.Result != "KubernetesEndpointsObserved" || flow.Status.LastEndpointObservation.ReadyByZone["a"] != 1 {
		t.Fatalf("Kubernetes endpoint sample not recorded: %#v", flow.Status.LastEndpointObservation)
	}
	expectAvailability := func(want metav1.ConditionStatus, reason string) {
		t.Helper()
		if got := meta.FindStatusCondition(flow.Status.Conditions, "Available"); got == nil || got.Status != want || got.Reason != reason || got.ObservedGeneration != flow.Generation {
			t.Fatalf("Available condition = %#v, want %s %s for generation %d", got, want, reason, flow.Generation)
		}
	}
	expectAvailability(metav1.ConditionTrue, "ReadyRoleEndpointsObserved")
	if changed, err := sampler.sampleEndpointObservation(ctx, flow); err != nil || changed {
		t.Fatalf("endpoint sample was not rate limited: %v", err)
	}
	endpointSlice.Endpoints[0].Conditions.Ready = &isTerminating
	if _, err := core.DiscoveryV1().EndpointSlices("default").Update(ctx, endpointSlice, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := observeRoleEndpoints(ctx, core.CoreV1(), core.DiscoveryV1(), flow, "search", "initial", flowService.UID, map[string]int{"a": 1}, qualified); err == nil {
		t.Fatal("unready endpoint satisfied the zone floor")
	}
	sampleNow = sampleNow.Add(time.Minute)
	if changed, err := sampler.sampleEndpointObservation(ctx, flow); err != nil || !changed {
		t.Fatalf("below-floor endpoint sample failed: %v", err)
	}
	if err := kube.Get(ctx, flowKey, flow); err != nil {
		t.Fatal(err)
	}
	if got := flow.Status.LastEndpointObservation; got == nil || got.Result != "KubernetesEndpointsBelowRegionalFloor" ||
		len(got.ReadyByZone) != 1 || got.ReadyByZone["a"] != 0 {
		t.Fatalf("verified zero endpoints were not persisted: %#v", got)
	}
	expectAvailability(metav1.ConditionFalse, "RoleEndpointFloorShortfall")
	endpointSlice, err = core.DiscoveryV1().EndpointSlices("default").Get(ctx, endpointSlice.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	endpointSlice.Endpoints[0].Conditions.Ready = &isReady
	if _, err := core.DiscoveryV1().EndpointSlices("default").Update(ctx, endpointSlice, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	sampleNow = sampleNow.Add(time.Minute)
	flowReconciler.Discovery = core.DiscoveryV1()
	flowReconciler.DevelopmentObserveEndpoints = true
	flowReconciler.Now = func() time.Time { return sampleNow }
	for i := 0; i < 6; i++ {
		reconcile()
		if flow.Status.LastEndpointObservation != nil && flow.Status.LastEndpointObservation.Result == "KubernetesEndpointsObserved" && flow.Status.LastEndpointObservation.ObservedAt.Time.Equal(sampleNow) {
			break
		}
	}
	if flow.Status.LastEndpointObservation == nil || flow.Status.LastEndpointObservation.Result != "KubernetesEndpointsObserved" || !flow.Status.LastEndpointObservation.ObservedAt.Time.Equal(sampleNow) {
		t.Fatal("normal reconciliation did not record the refreshed endpoint sample")
	}
	expectAvailability(metav1.ConditionTrue, "ReadyRoleEndpointsObserved")

	flowReconciler.DevelopmentServiceProbeImage = flow.Spec.Image
	flowReconciler.ServiceProbeClient = &http.Client{Transport: serviceRoundTripFunc(func(req *http.Request) (*http.Response, error) {
		body := fmt.Sprintf(`{"node":"java-node","name":%q,"roles":["search","ingest"],"lease":{"valid_now":true,"expires_in_millis":60000,"ttl_millis":60000}}`, bootstrapPod.Name)
		return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(body)), Request: req}, nil
	})}
	for i := 0; i < 8; i++ {
		reconcile()
		if flow.Status.LastServiceProbe != nil {
			break
		}
	}
	if flow.Status.LastServiceProbe == nil || flow.Status.LastServiceProbe.Result != "ServiceResponseObserved" || flow.Status.LastServiceProbe.PodUID != string(bootstrapPod.UID) || flow.Status.LastServiceProbe.ServiceUID != string(flowService.UID) {
		t.Fatalf("normal reconciliation did not record the Service sample: %#v", flow.Status.LastServiceProbe)
	}
	pods := core.CoreV1().Pods(flow.Namespace)
	if err := pods.Bind(ctx, &corev1.Binding{ObjectMeta: metav1.ObjectMeta{Name: strictPod.Name, UID: strictPod.UID}, Target: corev1.ObjectReference{Kind: "Node", Name: "node-b"}}, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	strictPod, err = pods.Get(ctx, strictPod.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	strictPod.Status = *bootstrapPod.Status.DeepCopy()
	strictPod.Status.PodIP = "10.0.0.2"
	if _, err := pods.UpdateStatus(ctx, strictPod, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	flowReconciler.ObserveWarmup = func(context.Context, *corev1.Pod) (java.Warmup, error) {
		return java.Warmup{ProcessIncarnation: "9aa977f3-4985-4cb5-9692-5ad1e3e243fe", Result: "completed", Ready: true}, nil
	}
	sampleNow = time.Now().UTC()
	zero := int64(0)
	if err := pods.Delete(ctx, bootstrapPod.Name, metav1.DeleteOptions{GracePeriodSeconds: &zero}); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 20 && flow.Status.PendingPodCreation == nil; i++ {
		reconcile()
	}
	if pending := flow.Status.PendingPodCreation; pending == nil || pending.WarmupMode != "Strict" || flow.Status.ColdBootstrap != nil {
		t.Fatalf("loss of the bootstrap Pod downgraded replacement despite a Ready search peer: %#v", pending)
	}

	serviceTemplate, err := workload.RoleService(instance, "search", "blue", 9200)
	if err != nil {
		t.Fatal(err)
	}
	service, err := core.CoreV1().Services("default").Create(ctx, serviceTemplate, metav1.CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	uid := service.UID
	// The planner observes the real Service but receives synthetic application
	// qualification. Changing even metadata after planning requires replanning.
	op := &v1alpha1.OpenSearchServerlessOperation{ObjectMeta: metav1.ObjectMeta{Name: "switch-plan", Namespace: "default"}, Spec: v1alpha1.OperationSpec{InstanceRef: v1alpha1.InstanceReference{Name: instance.Name, UID: string(instance.UID)}, Action: "Promote", TargetFleet: "green", ExpectedServingFleetByRole: map[string]string{"search": "blue", "ingest": "blue"}}}
	if err := kube.Create(ctx, op); err != nil {
		t.Fatal(err)
	}
	if err := kube.Get(ctx, key, instance); err != nil {
		t.Fatal(err)
	}
	instance.Status.ServingFleetIntentByRole = map[string]string{"search": "blue", "ingest": "blue"}
	instance.Status.ActiveOperation = &v1alpha1.OperationClaim{Name: op.Name, UID: string(op.UID), Action: "Promote", Generation: op.Generation}
	if err := kube.Status().Update(ctx, instance); err != nil {
		t.Fatal(err)
	}
	switchIntent := rollout.SwitchIntent{InstanceUID: string(instance.UID), OperationUID: string(op.UID), Role: "search", ServiceUID: string(uid), SourceFleet: "blue", TargetFleet: "green", CandidateRevision: "revision", Phase: rollout.Prepared}
	evidence := &rollout.CandidateEvidence{InstanceUID: string(instance.UID), OperationUID: string(op.UID), Role: "search", Fleet: "green", Revision: "revision", CapacityQualified: true, PeerQualified: true, SecurityQualified: true, RollbackCompatible: true, ObservedAt: time.Now()}
	adapter := &OperationReconciler{Client: kube, Core: core.CoreV1()}
	plan := func() rollout.SwitchAction {
		t.Helper()
		action, err := adapter.planObservedRoleSwitch(ctx, instance, op, switchIntent, evidence, nil, time.Minute)
		if err != nil {
			t.Fatal(err)
		}
		return action
	}
	action := plan()
	if action.Kind != rollout.PatchService || action.ResourceVersion == "" {
		t.Fatal("qualified plan did not bind Service version")
	}
	service.Annotations = map[string]string{"test": "changed"}
	service, err = core.CoreV1().Services("default").Update(ctx, service, metav1.UpdateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := adapter.applyPlannedRoleSwitch(ctx, instance, action, nil); err == nil {
		t.Fatal("stale plan changed Service")
	}
	if action = plan(); action.Kind != rollout.PatchService {
		t.Fatalf("fresh switch was not planned: %#v", action)
	}
	if err := adapter.applyPlannedRoleSwitch(ctx, instance, action, nil); err != nil {
		t.Fatal(err)
	}
	if action = plan(); action.Kind != rollout.RecordApplied {
		t.Fatalf("switch read-back did not recognize target: %#v", action)
	}
	// The HTTP response is synthetic; Kubernetes identities and versions are
	// real API-server objects. No proxy or client route is exercised here.
	owned := true
	probePod, err := core.CoreV1().Pods("default").Create(ctx, &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "service-probe-pod", Labels: map[string]string{workload.InstanceLabel: string(instance.UID), workload.RoleLabel: "search", workload.FleetLabel: "green", workload.ZoneLabel: "a"}, OwnerReferences: []metav1.OwnerReference{{APIVersion: v1alpha1.GroupVersion.String(), Kind: "OpenSearchServerless", Name: instance.Name, UID: instance.UID, Controller: &owned}}}, Spec: corev1.PodSpec{NodeName: "synthetic-node", Containers: []corev1.Container{{Name: "opensearch", Image: instance.Spec.Image}}}}, metav1.CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	probePod.Status = corev1.PodStatus{Phase: corev1.PodRunning, PodIP: "10.0.0.4", Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}}
	if _, err := core.CoreV1().Pods("default").UpdateStatus(ctx, probePod, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	probeClient := &http.Client{Transport: serviceRoundTripFunc(func(req *http.Request) (*http.Response, error) {
		body := `{"node":"java-node","name":"service-probe-pod","roles":["search","ingest"],"lease":{"valid_now":true,"expires_in_millis":60000,"ttl_millis":60000}}`
		return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(body)), Request: req}, nil
	})}
	probe, err := probeDevelopmentRoleService(ctx, core.CoreV1(), instance, instance.Spec.Image, "search", "green", uid, map[types.UID]struct{}{probePod.UID: {}}, probeClient, 5*time.Second)
	if err != nil || probe.PodUID != probePod.UID || probe.ServiceVersion == "" {
		t.Fatalf("Service probe did not bind Pod and Service: %#v %v", probe, err)
	}

	service, err = core.CoreV1().Services("default").Get(ctx, service.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	service.Spec.Selector[workload.FleetLabel] = "blue"
	service, err = core.CoreV1().Services("default").Update(ctx, service, metav1.UpdateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	service.Spec.PublishNotReadyAddresses = true
	service, err = core.CoreV1().Services("default").Update(ctx, service, metav1.UpdateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := switchRoleService(ctx, core.CoreV1(), instance, "search", uid, "blue", "green"); err == nil {
		t.Fatal("unsafe unready-address Service was switched")
	}
	service.Spec.PublishNotReadyAddresses = false
	if _, err := core.CoreV1().Services("default").Update(ctx, service, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	var injected bool
	var loseResponse bool
	var injectErr error
	config.Wrap(func(next http.RoundTripper) http.RoundTripper {
		return serviceRoundTripFunc(func(req *http.Request) (*http.Response, error) {
			if !injected && req.Method == http.MethodPatch && strings.HasSuffix(req.URL.Path, "/services/strict-search") {
				injected = true
				current, err := core.CoreV1().Services("default").Get(ctx, "strict-search", metav1.GetOptions{})
				if err == nil {
					current.Spec.Selector[workload.FleetLabel] = "other"
					_, err = core.CoreV1().Services("default").Update(ctx, current, metav1.UpdateOptions{})
				}
				injectErr = err
				if err != nil {
					return nil, err
				}
			}
			response, err := next.RoundTrip(req)
			if err == nil && loseResponse && req.Method == http.MethodPatch && strings.HasSuffix(req.URL.Path, "/services/strict-search") && response.StatusCode >= 200 && response.StatusCode < 300 {
				loseResponse = false
				response.Body.Close()
				return nil, io.ErrUnexpectedEOF
			}
			return response, err
		})
	})
	racing, err := kubernetes.NewForConfig(config)
	if err != nil {
		t.Fatal(err)
	}
	if err := switchRoleService(ctx, racing.CoreV1(), instance, "search", uid, "blue", "green"); err == nil || !injected || injectErr != nil {
		t.Fatalf("concurrent selector edit was not protected: %v, injection %v", err, injectErr)
	}
	service, err = core.CoreV1().Services("default").Get(ctx, "strict-search", metav1.GetOptions{})
	if err != nil || service.Spec.Selector[workload.FleetLabel] != "other" {
		t.Fatal("concurrent Service selector was overwritten")
	}
	service.Spec.Selector[workload.FleetLabel] = "blue"
	if _, err := core.CoreV1().Services("default").Update(ctx, service, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	loseResponse = true
	if err := switchRoleService(ctx, racing.CoreV1(), instance, "search", uid, "blue", "green"); err == nil || loseResponse {
		t.Fatalf("expected lost successful response: %v", err)
	}
	if err := switchRoleService(ctx, core.CoreV1(), instance, "search", uid, "blue", "green"); err != nil {
		t.Fatalf("successful switch could not be recovered: %v", err)
	}
	if err := switchRoleService(ctx, core.CoreV1(), instance, "search", "stale-uid", "blue", "green"); err == nil {
		t.Fatal("stale Service UID accepted")
	}
	if err := core.CoreV1().Services("default").Delete(ctx, service.Name, metav1.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	replacement, err := core.CoreV1().Services("default").Create(ctx, serviceTemplate, metav1.CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if replacement.UID == uid {
		t.Fatal("replacement reused Service UID")
	}
	if err := switchRoleService(ctx, core.CoreV1(), instance, "search", uid, "blue", "green"); err == nil {
		t.Fatal("old Service UID acted on same-name replacement")
	}
}
