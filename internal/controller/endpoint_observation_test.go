package controller

import (
	"context"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	clienttesting "k8s.io/client-go/testing"
	ctrl "sigs.k8s.io/controller-runtime"
	clientfake "sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/metrics"

	"github.com/huyz0/opensearch-serverless-operator/api/v1alpha1"
	"github.com/huyz0/opensearch-serverless-operator/internal/workload"
)

func TestAvailabilitySampleInvalidatesWhenEitherServingFleetChanges(t *testing.T) {
	instance := &v1alpha1.OpenSearchServerless{
		ObjectMeta: metav1.ObjectMeta{Generation: 2},
		Spec: v1alpha1.OpenSearchServerlessSpec{Pools: map[string]v1alpha1.Pool{
			"search": {Zones: map[string]v1alpha1.Zone{"a": {Replicas: 1}}},
			"ingest": {Zones: map[string]v1alpha1.Zone{"b": {WarmSpare: 1}}},
		}},
		Status: v1alpha1.OpenSearchServerlessStatus{ServingFleetIntentByRole: map[string]string{"search": "blue", "ingest": "blue"}},
	}
	instance.Status.LastEndpointObservation = &v1alpha1.EndpointObservationStatus{
		Role: "search", Fleet: "blue", ObservedGeneration: 2, ObservedServingFleets: endpointServingFleets(instance),
	}
	meta.SetStatusCondition(&instance.Status.Conditions, metav1.Condition{Type: "Available", Status: metav1.ConditionTrue,
		Reason: "ReadyRoleEndpointsObserved", ObservedGeneration: 2})
	if !endpointAvailabilitySampled(instance) {
		t.Fatal("current evidence did not qualify as an availability sample")
	}
	copy := instance.DeepCopyObject().(*v1alpha1.OpenSearchServerless)
	copy.Status.LastEndpointObservation.ObservedServingFleets["ingest"] = "other"
	if !endpointAvailabilitySampled(instance) {
		t.Fatal("copy shares the sampled fleet map with the original")
	}
	instance.Status.ServingFleetIntentByRole["ingest"] = "green"
	if endpointAvailabilitySampled(instance) {
		t.Fatal("old availability sample survived an index fleet change with the same generation")
	}
}

func TestReconcileExpiresStalePositiveAvailability(t *testing.T) {
	ctx := context.Background()
	now := time.Now()
	instance := &v1alpha1.OpenSearchServerless{
		ObjectMeta: metav1.ObjectMeta{Name: "catalog", Namespace: "n", Generation: 2},
		Spec: v1alpha1.OpenSearchServerlessSpec{Pools: map[string]v1alpha1.Pool{
			"search": {Zones: map[string]v1alpha1.Zone{"a": {Replicas: 1}}},
		}},
		Status: v1alpha1.OpenSearchServerlessStatus{ServingFleetIntentByRole: map[string]string{"search": "blue"}},
	}
	instance.Status.LastEndpointObservation = &v1alpha1.EndpointObservationStatus{
		Role: "search", Fleet: "blue", ObservedGeneration: 2, ObservedServingFleets: endpointServingFleets(instance),
		ObservedAt: metav1.NewTime(now.Add(-2 * time.Minute)),
	}
	meta.SetStatusCondition(&instance.Status.Conditions, metav1.Condition{Type: "Available", Status: metav1.ConditionTrue,
		Reason: "ReadyRoleEndpointsObserved", ObservedGeneration: 2})
	scheme := runtime.NewScheme()
	if err := v1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	kube := clientfake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(instance).WithObjects(instance).Build()
	r := &InstanceReconciler{Client: kube, DevelopmentObserveEndpoints: true, Now: func() time.Time { return now }}
	key := types.NamespacedName{Namespace: instance.Namespace, Name: instance.Name}
	if _, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: key}); err != nil {
		t.Fatal(err)
	}
	if err := kube.Get(ctx, key, instance); err != nil {
		t.Fatal(err)
	}
	if got := meta.FindStatusCondition(instance.Status.Conditions, "Available"); got == nil || got.Status != metav1.ConditionUnknown || got.Reason != "EndpointObservationPending" {
		t.Fatalf("stale positive evidence was retained: %#v", got)
	}
}

func TestRoleEndpointObservationRejectsStaleAndUnsafeEndpoints(t *testing.T) {
	instance := &v1alpha1.OpenSearchServerless{ObjectMeta: metav1.ObjectMeta{Name: "catalog", Namespace: "n", UID: "owner"}}
	service, err := workload.RoleService(instance, "search", "green", 9200)
	if err != nil {
		t.Fatal(err)
	}
	service.UID, service.ResourceVersion = "service-uid", "1"
	service.Spec.Type, service.Spec.ClusterIP = corev1.ServiceTypeClusterIP, "10.0.0.1"
	controller, ready, serving, terminating := true, true, true, false
	port, portName, protocol, nodeName := int32(9200), "http", corev1.ProtocolTCP, "node-a"
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "pod", Namespace: "n", UID: "pod-uid", OwnerReferences: []metav1.OwnerReference{{APIVersion: v1alpha1.GroupVersion.String(), Kind: "OpenSearchServerless", Name: instance.Name, UID: instance.UID, Controller: &controller}}, Labels: map[string]string{workload.InstanceLabel: "owner", workload.RoleLabel: "search", workload.FleetLabel: "green", workload.ZoneLabel: "a"}}, Spec: corev1.PodSpec{NodeName: nodeName}, Status: corev1.PodStatus{Phase: corev1.PodRunning, PodIP: "10.1.0.3", Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}}}
	pod.ResourceVersion = "1"
	slice := &discoveryv1.EndpointSlice{ObjectMeta: metav1.ObjectMeta{Name: "slice", Namespace: "n", UID: "slice-uid", Labels: map[string]string{discoveryv1.LabelServiceName: service.Name, discoveryv1.LabelManagedBy: "endpointslice-controller.k8s.io"}, OwnerReferences: []metav1.OwnerReference{{APIVersion: "v1", Kind: "Service", Name: service.Name, UID: service.UID, Controller: &controller}}}, AddressType: discoveryv1.AddressTypeIPv4, Ports: []discoveryv1.EndpointPort{{Name: &portName, Port: &port, Protocol: &protocol}}, Endpoints: []discoveryv1.Endpoint{{Addresses: []string{"10.1.0.3"}, NodeName: &nodeName, Conditions: discoveryv1.EndpointConditions{Ready: &ready, Serving: &serving, Terminating: &terminating}, TargetRef: &corev1.ObjectReference{APIVersion: "v1", Kind: "Pod", Namespace: "n", Name: "pod", UID: "pod-uid"}}}}
	slice.ResourceVersion = "1"
	check := func(service *corev1.Service, pod *corev1.Pod, slice *discoveryv1.EndpointSlice, zones map[string]int) (roleEndpointObservation, error) {
		t.Helper()
		objects := []runtime.Object{service, pod, slice}
		client := fake.NewSimpleClientset(objects...)
		return observeRoleEndpoints(context.Background(), client.CoreV1(), client.DiscoveryV1(), instance, "search", "green", "service-uid", zones, map[types.UID]struct{}{"pod-uid": {}})
	}
	zones := map[string]int{"a": 1}
	result, err := check(service, pod, slice, zones)
	if err != nil || result.ReadyByZone["a"] != 1 || result.ObservedAt.IsZero() {
		t.Fatalf("valid endpoints rejected: %#v %v", result, err)
	}
	t.Run("availability retries a changing endpoint snapshot", func(t *testing.T) {
		ctx := context.Background()
		sampled := instance.DeepCopyObject().(*v1alpha1.OpenSearchServerless)
		sampled.Generation = 1
		sampled.Status.ServingFleetIntentByRole = map[string]string{"search": "green"}
		sampled.Spec.Pools = map[string]v1alpha1.Pool{"search": {Zones: map[string]v1alpha1.Zone{
			"a": {Replicas: 1, MinServing: 1},
		}}}
		core := fake.NewSimpleClientset(service.DeepCopy(), pod.DeepCopy(), slice.DeepCopy())
		scheme := runtime.NewScheme()
		if err := v1alpha1.AddToScheme(scheme); err != nil {
			t.Fatal(err)
		}
		c := clientfake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(sampled).WithObjects(sampled).Build()
		now := time.Now().Truncate(time.Second)
		observer := &InstanceReconciler{Client: c, Core: core.CoreV1(), Discovery: core.DiscoveryV1(), Now: func() time.Time { return now }}
		beforeMetrics := endpointObservationCounts(t)
		gets := 0
		changesRemaining := 1
		core.PrependReactor("get", "pods", func(clienttesting.Action) (bool, runtime.Object, error) {
			gets++
			if gets%2 != 0 || changesRemaining == 0 {
				return false, nil, nil
			}
			changesRemaining--
			updated := pod.DeepCopy()
			updated.ResourceVersion = "2"
			return true, updated, nil
		})
		checkSample := func(want metav1.ConditionStatus, reason string, delay time.Duration, wantGets int) {
			t.Helper()
			changed, err := observer.sampleEndpointObservation(ctx, sampled)
			got := meta.FindStatusCondition(sampled.Status.Conditions, "Available")
			if err != nil || !changed || got == nil || got.Status != want || got.Reason != reason || gets != wantGets {
				t.Fatalf("available = %#v, patched = %t, Pod reads = %d, err = %v", got, changed, gets, err)
			}
			if gotDelay := endpointObservationDelay(sampled, now); gotDelay != delay {
				t.Fatalf("next sample in %v, want %v", gotDelay, delay)
			}
		}
		checkSample(metav1.ConditionTrue, "ReadyRoleEndpointsObserved", time.Minute, 4)

		now = now.Add(time.Minute)
		changesRemaining = 2
		checkSample(metav1.ConditionUnknown, "RoleEndpointsUnverified", 10*time.Second, 8)
		if got := sampled.Status.LastEndpointObservation.Result; got != "Unavailable" {
			t.Fatalf("unstable endpoint detail = %q, want Unavailable", got)
		}

		now = now.Add(9 * time.Second)
		if changed, err := observer.sampleEndpointObservation(ctx, sampled); err != nil || changed || gets != 8 {
			t.Fatalf("sample before retry deadline: patched = %t, Pod reads = %d, err = %v", changed, gets, err)
		}
		now = now.Add(time.Second)
		checkSample(metav1.ConditionTrue, "ReadyRoleEndpointsObserved", time.Minute, 10)

		if err := c.Delete(ctx, sampled); err != nil {
			t.Fatal(err)
		}
		now = now.Add(time.Minute)
		if changed, err := observer.sampleEndpointObservation(ctx, sampled); err == nil || changed {
			t.Fatalf("sample status write should fail for deleted instance: patched = %t, err = %v", changed, err)
		}
		afterMetrics := endpointObservationCounts(t)
		for outcome, want := range map[string]uint64{"ReadyRoleEndpointsObserved": 2, "RoleEndpointsUnverified": 1, "StatusPatchFailed": 1} {
			if got := afterMetrics[outcome] - beforeMetrics[outcome]; got != want {
				t.Errorf("%s recorded %d samples, want %d", outcome, got, want)
			}
		}
	})
	t.Run("explicit zero zone can remain in the endpoint scope", func(t *testing.T) {
		scoped := map[string]int{"a": 0, "b": 1}
		client := fake.NewSimpleClientset(service.DeepCopy(), pod.DeepCopy(), slice.DeepCopy())
		allowed := map[types.UID]struct{}{pod.UID: {}}
		raw, err := inspectServiceEndpoints(context.Background(), client.CoreV1(), client.DiscoveryV1(), instance,
			"search", "green", service, service.UID, scoped, allowed)
		if err != nil || raw.ReadyByZone["a"] != 1 || raw.ReadyByZone["b"] != 0 {
			t.Fatalf("the retiring endpoint must stay visible: %#v %v", raw, err)
		}
		if _, err := check(service, pod, slice, scoped); err == nil {
			t.Fatal("an endpoint in the zero zone satisfied a missing destination target")
		}
		if _, err := check(service, pod, slice, map[string]int{"a": 0}); err == nil {
			t.Fatal("an all-zero route qualified without an explicit positive destination")
		}
	})
	t.Run("Kubernetes Pod reference with observed zone", func(t *testing.T) {
		actual := slice.DeepCopy()
		zone := "a"
		actual.Endpoints[0].TargetRef.APIVersion = ""
		actual.Endpoints[0].Zone = &zone
		if _, err := check(service, pod, actual, zones); err != nil {
			t.Fatalf("Kubernetes endpoint rejected: %v", err)
		}
		zone = "b"
		if _, err := check(service, pod, actual, zones); err == nil {
			t.Fatal("EndpointSlice zone mismatch went unnoticed")
		}
		zone = "a"
		actual.Endpoints[0].TargetRef.APIVersion = "apps/v1"
		if _, err := check(service, pod, actual, zones); err == nil {
			t.Fatal("non-core Pod reference was accepted")
		}
	})
	instance.ResourceVersion = "1"
	instance.Status.ServingFleetIntentByRole = map[string]string{"search": "blue", "ingest": "blue"}
	instance.Status.ActiveOperation = &v1alpha1.OperationClaim{Name: "promote", UID: "operation", Action: "Promote", Generation: 1}
	op := &v1alpha1.OpenSearchServerlessOperation{ObjectMeta: metav1.ObjectMeta{Name: "promote", Namespace: "n", UID: "operation", ResourceVersion: "1", Generation: 1}, Spec: v1alpha1.OperationSpec{InstanceRef: v1alpha1.InstanceReference{Name: "catalog", UID: "owner"}, Action: "Promote", TargetFleet: "green", ExpectedServingFleetByRole: map[string]string{"search": "blue", "ingest": "blue"}}}
	appliedAt := metav1.NewMicroTime(time.Now().Add(-time.Second))
	op.Status.RolloutRoles = map[string]v1alpha1.RolloutRoleStatus{
		"search": {ServiceUID: "service-uid", SourceFleet: "blue", TargetFleet: "green", CandidateRevision: "revision", Phase: "Applied", AppliedAt: &appliedAt},
		"ingest": {ServiceUID: "ingest-service", SourceFleet: "blue", TargetFleet: "green", CandidateRevision: "revision", Phase: "Prepared"},
	}
	clientForRoute := fake.NewSimpleClientset(service.DeepCopy(), pod.DeepCopy(), slice.DeepCopy())
	r := &OperationReconciler{Core: clientForRoute.CoreV1(), Discovery: clientForRoute.DiscoveryV1()}
	edge := externalRouteObservation{InstanceUID: "owner", OperationUID: "operation", Role: "search", Fleet: "green", ServiceUID: "service-uid", ServiceVersion: "1", RouteID: "ingress/search", RespondingPodUID: "pod-uid", RespondingPodVersion: "1", ObservedAt: time.Now()}
	qualified := map[types.UID]struct{}{"pod-uid": {}, "other-qualified": {}}
	evidence, err := r.observeRolloutRoute(context.Background(), instance, op, "search", zones, qualified, edge, time.Minute)
	if err != nil || evidence.ServiceVersion != "1" || !evidence.ObservedAt.Equal(edge.ObservedAt) {
		t.Fatalf("valid route evidence rejected: %#v %v", evidence, err)
	}
	t.Run("regional floor blocks route with healthy zones", func(t *testing.T) {
		higherFloor := instance.DeepCopyObject().(*v1alpha1.OpenSearchServerless)
		higherFloor.Spec.Availability = &v1alpha1.Availability{MinServingTotal: map[string]int32{"search": 2}}
		if _, err := r.observeRolloutRoute(context.Background(), higherFloor, op, "search", zones, qualified, edge, time.Minute); err == nil {
			t.Fatal("route below the configured regional floor was accepted")
		}
	})
	t.Run("one of three zones has no ready endpoints", func(t *testing.T) {
		ctx := context.Background()
		sampled := instance.DeepCopyObject().(*v1alpha1.OpenSearchServerless)
		sampled.Status.ServingFleetIntentByRole = map[string]string{"search": "green"}
		sampled.Spec.Pools = map[string]v1alpha1.Pool{"search": {Zones: map[string]v1alpha1.Zone{
			"a": {Replicas: 1, MinServing: 1}, "b": {Replicas: 1, MinServing: 1}, "c": {Replicas: 1, MinServing: 1},
		}}}
		sampled.Spec.Availability = &v1alpha1.Availability{MinServingTotal: map[string]int32{"search": 2}}
		podB, sliceAB := pod.DeepCopy(), slice.DeepCopy()
		podB.Name, podB.UID, podB.Spec.NodeName, podB.Status.PodIP = "pod-b", "pod-b-uid", "node-b", "10.1.0.4"
		podB.Labels[workload.ZoneLabel] = "b"
		endpointB := *sliceAB.Endpoints[0].DeepCopy()
		endpointB.Addresses, endpointB.NodeName = []string{podB.Status.PodIP}, &podB.Spec.NodeName
		endpointB.TargetRef.Name, endpointB.TargetRef.UID = podB.Name, podB.UID
		sliceAB.Endpoints = append(sliceAB.Endpoints, endpointB)
		core := fake.NewSimpleClientset(service.DeepCopy(), pod.DeepCopy(), podB, sliceAB)
		scheme := runtime.NewScheme()
		if err := v1alpha1.AddToScheme(scheme); err != nil {
			t.Fatal(err)
		}
		c := clientfake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(sampled).WithObjects(sampled).Build()
		now := time.Now()
		observer := &InstanceReconciler{Client: c, Core: core.CoreV1(), Discovery: core.DiscoveryV1(), Now: func() time.Time { return now }}
		sample := func(want string) {
			t.Helper()
			changed, err := observer.sampleEndpointObservation(ctx, sampled)
			got := sampled.Status.LastEndpointObservation
			if err != nil || !changed || got == nil || got.Result != want || len(got.ReadyByZone) != 3 ||
				got.ReadyByZone["a"] != 1 || got.ReadyByZone["b"] != 1 || got.ReadyByZone["c"] != 0 {
				t.Fatalf("did not preserve the two verified survivor counts and the empty zone: %#v %v", got, err)
			}
		}
		beforeShortfalls := endpointObservationCounts(t)["RoleEndpointFloorShortfall"]
		sample("KubernetesEndpointsBelowZoneFloor")
		sampled.Spec.Availability.MinServingTotal["search"] = 3
		now = now.Add(time.Minute)
		sample("KubernetesEndpointsBelowRegionalFloor")
		if got := endpointObservationCounts(t)["RoleEndpointFloorShortfall"] - beforeShortfalls; got != 2 {
			t.Errorf("recorded %d shortfalls, want 2", got)
		}
		sampled.Spec.Availability.MinServingTotal["search"] = 2
		sampled.Spec.Pools["search"].Zones["c"] = v1alpha1.Zone{Replicas: 1}
		now = now.Add(time.Minute)
		sample("KubernetesEndpointsObserved")
		if got := meta.FindStatusCondition(sampled.Status.Conditions, "Available"); got == nil || got.Status != metav1.ConditionTrue {
			t.Fatalf("a zone without a required minimum should leave availability governed by the regional floor: %#v", got)
		}
	})
	for _, scenario := range []struct {
		name   string
		mutate func(*externalRouteObservation)
	}{
		{"different service version", func(e *externalRouteObservation) { e.ServiceVersion = "old" }},
		{"different responder version", func(e *externalRouteObservation) { e.RespondingPodVersion = "old" }},
		{"missing responder version", func(e *externalRouteObservation) { e.RespondingPodVersion = "" }},
		{"different operation", func(e *externalRouteObservation) { e.OperationUID = "old" }},
		{"unqualified responder", func(e *externalRouteObservation) { e.RespondingPodUID = "other" }},
		{"responder absent from endpoints", func(e *externalRouteObservation) { e.RespondingPodUID = "other-qualified" }},
		{"missing client path", func(e *externalRouteObservation) { e.RouteID = "" }},
		{"probe before switch", func(e *externalRouteObservation) { e.ObservedAt = appliedAt.Time.Add(-time.Second) }},
		{"probe in future", func(e *externalRouteObservation) { e.ObservedAt = time.Now().Add(time.Minute) }},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			bad := edge
			scenario.mutate(&bad)
			if _, err := r.observeRolloutRoute(context.Background(), instance, op, "search", zones, qualified, bad, time.Minute); err == nil {
				t.Fatal("invalid production route evidence accepted")
			}
		})
	}
	for _, scenario := range []struct {
		name   string
		mutate func(*corev1.Service, *corev1.Pod, *discoveryv1.EndpointSlice)
	}{
		{"wrong service uid", func(s *corev1.Service, _ *corev1.Pod, _ *discoveryv1.EndpointSlice) { s.UID = "other" }},
		{"role Service exposes an external IP", func(s *corev1.Service, _ *corev1.Pod, _ *discoveryv1.EndpointSlice) {
			s.Spec.ExternalIPs = []string{"192.0.2.11"}
		}},
		{"wrong fleet", func(_ *corev1.Service, p *corev1.Pod, _ *discoveryv1.EndpointSlice) {
			p.Labels[workload.FleetLabel] = "blue"
		}},
		{"stale pod uid", func(_ *corev1.Service, p *corev1.Pod, _ *discoveryv1.EndpointSlice) { p.UID = "new-pod" }},
		{"wrong pod ip", func(_ *corev1.Service, _ *corev1.Pod, s *discoveryv1.EndpointSlice) {
			s.Endpoints[0].Addresses = []string{"10.1.0.4"}
		}},
		{"unknown readiness", func(_ *corev1.Service, _ *corev1.Pod, s *discoveryv1.EndpointSlice) {
			s.Endpoints[0].Conditions.Ready = nil
		}},
		{"terminating but serving", func(_ *corev1.Service, _ *corev1.Pod, s *discoveryv1.EndpointSlice) {
			s.Endpoints[0].Conditions.Terminating = &ready
			s.Endpoints[0].Conditions.Ready = &terminating
		}},
		{"foreign slice", func(_ *corev1.Service, _ *corev1.Pod, s *discoveryv1.EndpointSlice) {
			s.OwnerReferences[0].UID = "other"
		}},
		{"unready pod", func(_ *corev1.Service, p *corev1.Pod, _ *discoveryv1.EndpointSlice) {
			p.Status.Conditions[0].Status = corev1.ConditionFalse
		}},
		{"conflicting Pod readiness", func(_ *corev1.Service, p *corev1.Pod, _ *discoveryv1.EndpointSlice) {
			p.Status.Conditions = append(p.Status.Conditions, corev1.PodCondition{Type: corev1.PodReady, Status: corev1.ConditionFalse})
		}},
		{"gated Pod", func(_ *corev1.Service, p *corev1.Pod, _ *discoveryv1.EndpointSlice) {
			p.Spec.SchedulingGates = []corev1.PodSchedulingGate{{Name: "serverless.opensearch.org/placement"}}
		}},
		{"host-network Pod", func(_ *corev1.Service, p *corev1.Pod, _ *discoveryv1.EndpointSlice) {
			p.Spec.HostNetwork = true
		}},
		{"ready endpoint already marked for disruption", func(_ *corev1.Service, p *corev1.Pod, _ *discoveryv1.EndpointSlice) {
			p.Status.Conditions = append(p.Status.Conditions, corev1.PodCondition{Type: corev1.DisruptionTarget, Status: corev1.ConditionTrue})
		}},
		{"wrong port", func(_ *corev1.Service, _ *corev1.Pod, s *discoveryv1.EndpointSlice) {
			wrong := int32(9300)
			s.Ports[0].Port = &wrong
		}},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			s, p, e := service.DeepCopy(), pod.DeepCopy(), slice.DeepCopy()
			scenario.mutate(s, p, e)
			if _, err := check(s, p, e, zones); err == nil {
				t.Fatal("unsafe routing observation accepted")
			}
		})
	}
	if _, err := check(service, pod, slice, map[string]int{"a": 2}); err == nil {
		t.Fatal("insufficient zone capacity accepted")
	}
	duplicate := slice.DeepCopy()
	duplicate.Endpoints = append(duplicate.Endpoints, duplicate.Endpoints[0])
	if _, err := check(service, pod, duplicate, map[string]int{"a": 2}); err == nil {
		t.Fatal("duplicate endpoint counted twice")
	}
	t.Run("dual stack endpoints count one Pod", func(t *testing.T) {
		dualStack := pod.DeepCopy()
		dualStack.Status.PodIPs = []corev1.PodIP{{IP: pod.Status.PodIP}, {IP: "2001:db8::3"}}
		ipv6 := slice.DeepCopy()
		ipv6.Name, ipv6.UID, ipv6.AddressType = "ipv6", "ipv6-slice-uid", discoveryv1.AddressTypeIPv6
		ipv6.Endpoints[0].Addresses = []string{"2001:db8:0:0:0:0:0:3"}
		client := fake.NewSimpleClientset(service.DeepCopy(), dualStack, slice.DeepCopy(), ipv6)
		observed, err := observeRoleEndpoints(context.Background(), client.CoreV1(), client.DiscoveryV1(), instance,
			"search", "green", service.UID, zones, map[types.UID]struct{}{pod.UID: {}})
		if err != nil || observed.ReadyByZone["a"] != 1 || len(observed.ReadyPodVersions) != 1 {
			t.Fatalf("dual-stack Pod counted incorrectly: %#v, %v", observed, err)
		}
	})
	t.Run("two ready Pod references share a backend address", func(t *testing.T) {
		other := pod.DeepCopy()
		other.Name, other.UID = "other", "other-uid"
		aliased := slice.DeepCopy()
		endpoint := *aliased.Endpoints[0].DeepCopy()
		endpoint.TargetRef.Name, endpoint.TargetRef.UID = other.Name, other.UID
		aliased.Endpoints = append(aliased.Endpoints, endpoint)
		client := fake.NewSimpleClientset(service.DeepCopy(), pod.DeepCopy(), other, aliased)
		if _, err := observeRoleEndpoints(context.Background(), client.CoreV1(), client.DiscoveryV1(), instance,
			"search", "green", service.UID, map[string]int{"a": 2}, map[types.UID]struct{}{pod.UID: {}, other.UID: {}}); err == nil ||
			!strings.Contains(err.Error(), "address refers to multiple Pods") {
			t.Fatalf("one address was permitted to back two qualified Pods: %v", err)
		}
	})
	tooMany := slice.DeepCopy()
	tooMany.Endpoints = make([]discoveryv1.Endpoint, 513)
	if _, err := check(service, pod, tooMany, zones); err == nil {
		t.Fatal("oversized endpoint set accepted")
	}
	unqualified := fake.NewSimpleClientset(service.DeepCopy(), pod.DeepCopy(), slice.DeepCopy())
	if _, err := observeRoleEndpoints(context.Background(), unqualified.CoreV1(), unqualified.DiscoveryV1(), instance, "search", "green", "service-uid", zones, map[types.UID]struct{}{"other-pod": {}}); err == nil {
		t.Fatal("unqualified ready endpoint accepted")
	}
	client := fake.NewSimpleClientset(service.DeepCopy(), pod.DeepCopy(), slice.DeepCopy())
	client.PrependReactor("list", "endpointslices", func(action clienttesting.Action) (bool, runtime.Object, error) {
		changed := service.DeepCopy()
		changed.ResourceVersion = "2"
		changed.Spec.Selector[workload.FleetLabel] = "blue"
		if err := client.Tracker().Update(corev1.SchemeGroupVersion.WithResource("services"), changed, "n"); err != nil {
			t.Fatal(err)
		}
		return false, nil, nil
	})
	if _, err := observeRoleEndpoints(context.Background(), client.CoreV1(), client.DiscoveryV1(), instance, "search", "green", "service-uid", zones, map[types.UID]struct{}{"pod-uid": {}}); err == nil {
		t.Fatal("Service change during endpoint observation accepted")
	}
	changedPodClient := fake.NewSimpleClientset(service.DeepCopy(), pod.DeepCopy(), slice.DeepCopy())
	gets := 0
	changedPodClient.PrependReactor("get", "pods", func(action clienttesting.Action) (bool, runtime.Object, error) {
		gets++
		if gets == 2 {
			changed := pod.DeepCopy()
			changed.ResourceVersion = "2"
			changed.Status.Conditions[0].Status = corev1.ConditionFalse
			return true, changed, nil
		}
		return false, nil, nil
	})
	if _, err := observeRoleEndpoints(context.Background(), changedPodClient.CoreV1(), changedPodClient.DiscoveryV1(), instance, "search", "green", "service-uid", zones, map[types.UID]struct{}{"pod-uid": {}}); err == nil {
		t.Fatal("Pod readiness change during endpoint observation accepted")
	}
	changedSliceClient := fake.NewSimpleClientset(service.DeepCopy(), pod.DeepCopy(), slice.DeepCopy())
	listCount := 0
	changedSliceClient.PrependReactor("list", "endpointslices", func(action clienttesting.Action) (bool, runtime.Object, error) {
		listCount++
		if listCount == 2 {
			changed := slice.DeepCopy()
			changed.ResourceVersion = "2"
			changed.Endpoints[0].Conditions.Ready = &terminating
			return true, &discoveryv1.EndpointSliceList{Items: []discoveryv1.EndpointSlice{*changed}}, nil
		}
		return false, nil, nil
	})
	if _, err := observeRoleEndpoints(context.Background(), changedSliceClient.CoreV1(), changedSliceClient.DiscoveryV1(), instance, "search", "green", "service-uid", zones, map[types.UID]struct{}{"pod-uid": {}}); err == nil {
		t.Fatal("EndpointSlice change during observation accepted")
	}
}

func endpointObservationCounts(t *testing.T) map[string]uint64 {
	t.Helper()
	families, err := metrics.Registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	counts := make(map[string]uint64)
	for _, family := range families {
		if family.GetName() != endpointObservationMetricName {
			continue
		}
		for _, sample := range family.GetMetric() {
			labels := sample.GetLabel()
			if len(labels) != 1 || labels[0].GetName() != "outcome" {
				t.Fatalf("endpoint metrics contain labels other than outcome: %v", labels)
			}
			counts[labels[0].GetValue()] = sample.GetHistogram().GetSampleCount()
		}
	}
	return counts
}
