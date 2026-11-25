//go:build envtest

package controller

import (
	"context"
	"errors"
	"maps"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	policyv1 "k8s.io/api/policy/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	kubernetesfake "k8s.io/client-go/kubernetes/fake"
	typedcore "k8s.io/client-go/kubernetes/typed/core/v1"
	typeddiscovery "k8s.io/client-go/kubernetes/typed/discovery/v1"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/huyz0/opensearch-serverless-operator/api/v1alpha1"
	"github.com/huyz0/opensearch-serverless-operator/internal/rollout"
	"github.com/huyz0/opensearch-serverless-operator/internal/workload"
)

var errLostWeightResponse = errors.New("lost weight response")

type lostWeightResponseClient struct {
	client.Client
}

func exerciseWeightedRollback(t *testing.T, c client.Client, r *OperationReconciler, instance *v1alpha1.OpenSearchServerless, promotion *v1alpha1.OpenSearchServerlessOperation, sourcePods map[string]*corev1.Pod, evidence weightStepEvidence, now *time.Time, interrupt bool) {
	t.Helper()
	ctx := context.Background()
	source := "blue"
	if !interrupt {
		source = "green"
		if _, err := r.setRolloutFinalizer(ctx, promotion, false); err != nil {
			t.Fatal(err)
		}
		if err := c.Delete(ctx, promotion); err != nil {
			t.Fatal(err)
		}
	}
	op := &v1alpha1.OpenSearchServerlessOperation{
		ObjectMeta: metav1.ObjectMeta{Namespace: instance.Namespace, Name: "recover-weighted", Finalizers: []string{rolloutOperationFinalizer}},
		Spec: v1alpha1.OperationSpec{
			InstanceRef: v1alpha1.InstanceReference{Name: instance.Name, UID: string(instance.UID)},
			Action:      "Rollback", TargetFleet: "blue", SupersedesOperationUID: string(promotion.UID),
			ExpectedServingFleetByRole: map[string]string{"search": source, "ingest": source},
		},
	}
	if err := c.Create(ctx, op); err != nil {
		t.Fatal(err)
	}
	if now.Before(op.CreationTimestamp.Time) {
		*now = op.CreationTimestamp.Time
	}
	key := client.ObjectKeyFromObject(op)
	approvedRoutes := map[string]string{"search": "journal-search", "ingest": "journal-ingest"}
	configureWeightedFixtureDispatch(r, c, approvedRoutes, evidence)
	if _, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: key}); err != nil {
		t.Fatalf("stage weighted rollback claim: %v", err)
	}
	if err := c.Get(ctx, key, op); err != nil {
		t.Fatal(err)
	}
	journal := op.Status.WeightedRollout
	if journal == nil {
		t.Fatal("rollback journal was not persisted before claim transfer")
	}
	search, ingest := journal.Roles["search"], journal.Roles["ingest"]
	if interrupt {
		if search.InitialWeights["green"] != 1 || len(search.Targets) != 1 || ingest.InitialWeights["blue"] != 100 || len(ingest.Targets) != 0 {
			t.Fatalf("rollback did not recover the mixed routes: %#v", journal.Roles)
		}
	} else if len(search.Targets) != 2 || len(ingest.Targets) != 2 || search.Targets[0].Weights["blue"] != 1 {
		t.Fatalf("rollback did not re-admit the zero-weight fleet: %#v", journal.Roles)
	}
	if _, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: key}); err != nil {
		t.Fatalf("transfer weighted rollback claim: %v", err)
	}
	if err := c.Get(ctx, client.ObjectKeyFromObject(instance), instance); err != nil ||
		instance.Status.ActiveOperation == nil || instance.Status.ActiveOperation.UID != string(op.UID) {
		t.Fatalf("manager did not transfer the rollback claim: %v", err)
	}
	done := false
	for i := 0; i < 16; i++ {
		*now = now.Add(time.Second)
		if _, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: key}); err != nil {
			t.Fatalf("execute reverse route step: %v", err)
		}
		if err := c.Get(ctx, key, op); err != nil {
			t.Fatal(err)
		}
		done = true
		for _, part := range op.Status.WeightedRollout.Roles {
			done = done && part.NextStep == len(part.Targets)
		}
		if done {
			break
		}
	}
	if !done {
		t.Fatal("reverse route schedule did not converge")
	}
	for _, role := range weightedRoleOrder {
		desired, err := workload.RoleService(instance, role, "blue", 9200)
		if err != nil {
			t.Fatal(err)
		}
		service, err := r.Core.Services(instance.Namespace).Get(ctx, desired.Name, metav1.GetOptions{})
		if err != nil {
			t.Fatal(err)
		}
		writeWeightedFixtureSlice(t, r.Discovery, service, sourcePods[role])
	}
	done = false
	for i := 0; i < 6; i++ {
		*now = now.Add(time.Second)
		if _, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: key}); err != nil {
			t.Fatal(err)
		}
		if err := c.Get(ctx, client.ObjectKeyFromObject(instance), instance); err != nil {
			t.Fatal(err)
		}
		if instance.Status.ActiveOperation == nil {
			done = true
			break
		}
	}
	if err := c.Get(ctx, client.ObjectKeyFromObject(instance), instance); err != nil {
		t.Fatal(err)
	}
	if !done || instance.Status.ActiveOperation != nil || instance.Status.RollbackTarget != nil ||
		instance.Status.LastRolloutCommit == nil || instance.Status.LastRolloutCommit.UID != string(op.UID) ||
		instance.Status.ServingFleetIntentByRole["search"] != "blue" || instance.Status.ServingFleetIntentByRole["ingest"] != "blue" {
		t.Fatalf("weighted rollback did not commit and release both serving roles: %#v", instance.Status)
	}
}

func configureWeightedFixtureDispatch(r *OperationReconciler, reader client.Reader, routes map[string]string, evidence weightStepEvidence) {
	r.RolloutReader, r.weightedRoutes, r.weightedEvidence = reader, routes, evidence
	r.RolloutPodDomain = func(*v1alpha1.OpenSearchServerless) (string, error) { return "", nil }
	r.RolloutEvidence = func(context.Context, *v1alpha1.OpenSearchServerless, *v1alpha1.OpenSearchServerlessOperation) (rolloutEvidence, error) {
		return rolloutEvidence{}, nil
	}
}

func writeWeightedFixtureSlice(t *testing.T, discovery typeddiscovery.DiscoveryV1Interface, service *corev1.Service, pod *corev1.Pod) {
	t.Helper()
	ctx := context.Background()
	slices := discovery.EndpointSlices(service.Namespace)
	name := service.Name + "-fixture"
	endpoints := []discoveryv1.Endpoint{{
		Addresses: []string{pod.Status.PodIP}, Conditions: discoveryv1.EndpointConditions{Ready: ptr.To(true)},
		TargetRef: &corev1.ObjectReference{Kind: "Pod", Name: pod.Name, Namespace: pod.Namespace, UID: pod.UID},
	}}
	existing, err := slices.Get(ctx, name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		_, err = slices.Create(ctx, &discoveryv1.EndpointSlice{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: service.Namespace,
				OwnerReferences: []metav1.OwnerReference{*metav1.NewControllerRef(service, corev1.SchemeGroupVersion.WithKind("Service"))},
				Labels:          map[string]string{discoveryv1.LabelServiceName: service.Name, discoveryv1.LabelManagedBy: "endpointslice-controller.k8s.io"},
			},
			AddressType: discoveryv1.AddressTypeIPv4,
			Ports:       []discoveryv1.EndpointPort{{Name: ptr.To("http"), Port: ptr.To(int32(9200)), Protocol: ptr.To(corev1.ProtocolTCP)}},
			Endpoints:   endpoints,
		}, metav1.CreateOptions{})
	} else if err == nil {
		existing.Endpoints = endpoints
		_, err = slices.Update(ctx, existing, metav1.UpdateOptions{})
	}
	if err != nil {
		t.Fatal(err)
	}
}

func (c lostWeightResponseClient) Patch(ctx context.Context, object client.Object, patch client.Patch, options ...client.PatchOption) error {
	if err := c.Client.Patch(ctx, object, patch, options...); err != nil {
		return err
	}
	return errLostWeightResponse
}

func exerciseWeightedJournal(t *testing.T, c client.Client, core typedcore.CoreV1Interface, discovery typeddiscovery.DiscoveryV1Interface, namespace string, interrupt bool) {
	ctx := context.Background()
	instance := &v1alpha1.OpenSearchServerless{
		ObjectMeta: metav1.ObjectMeta{Name: "journal", Namespace: namespace},
		Spec: v1alpha1.OpenSearchServerlessSpec{
			Image: "example/image@sha256:" + strings.Repeat("a", 64), ServiceAccountName: "runtime",
			Storage: v1alpha1.Storage{Type: "s3", Bucket: "bucket", Region: "region"},
			Pools: map[string]v1alpha1.Pool{
				"search": {Zones: map[string]v1alpha1.Zone{"az1": {Replicas: 1}}},
				"ingest": {Zones: map[string]v1alpha1.Zone{"az1": {Replicas: 1}}},
			},
			Rollout: &v1alpha1.Rollout{Strategy: "BlueGreen", GatewayBackends: true,
				Candidate: &v1alpha1.FleetCandidate{Fleet: "green", Image: "example/image@sha256:" + strings.Repeat("a", 64)}},
		},
	}
	if err := c.Create(ctx, instance); err != nil {
		t.Fatal(err)
	}
	op := &v1alpha1.OpenSearchServerlessOperation{
		ObjectMeta: metav1.ObjectMeta{Name: "weighted-journal", Namespace: namespace, Finalizers: []string{rolloutOperationFinalizer}},
		Spec: v1alpha1.OperationSpec{
			InstanceRef: v1alpha1.InstanceReference{Name: instance.Name, UID: string(instance.UID)},
			Action:      "Promote", TargetFleet: "green", RoutingMode: "WeightedGateway",
			ExpectedServingFleetByRole: map[string]string{"search": "blue", "ingest": "blue"},
		},
	}
	searchSchedule, ingestSchedule := []int{1, 100}, []int{1, 100}
	if !interrupt {
		op.Spec.WeightedTraffic = &v1alpha1.WeightedPromotionTraffic{Search: []int{10, 45}, Ingest: []int{20}}
		searchSchedule, ingestSchedule = []int{1, 10, 45, 100}, []int{1, 20, 100}
	}
	if err := c.Create(ctx, op); err != nil {
		t.Fatal(err)
	}
	instance.Status.GatewayBackends = make(map[string]v1alpha1.GatewayBackendStatus)
	instance.Status.ServingFleetIntentByRole = map[string]string{"search": "blue", "ingest": "blue"}
	if err := ensureRoleServices(ctx, core, instance); err != nil {
		t.Fatal(err)
	}
	greenRevision, err := podRevision(instance, "")
	if err != nil {
		t.Fatal(err)
	}
	previewUIDs, err := ensurePreviewServices(ctx, nil, core, instance, "green", nil)
	if err != nil {
		t.Fatal(err)
	}
	instance.Status.CandidateFleet = &v1alpha1.CandidateFleetStatus{
		Generation: instance.Generation, Fleet: "green", Revision: greenRevision, Image: instance.Spec.Image,
		SearchServiceUID: string(previewUIDs["search"]), IngestServiceUID: string(previewUIDs["ingest"]),
	}
	state := &v1alpha1.WeightedRolloutStatus{
		InstanceGeneration: instance.Generation, AdmissionWeight: 1,
		Roles: make(map[string]v1alpha1.WeightedRoleStatus),
	}
	sourcePods := make(map[string]*corev1.Pod)
	for i, role := range weightedRoleOrder {
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: "journal-source-" + role, Namespace: namespace,
				OwnerReferences: []metav1.OwnerReference{*metav1.NewControllerRef(instance, v1alpha1.GroupVersion.WithKind("OpenSearchServerless"))},
				Labels: map[string]string{workload.InstanceLabel: string(instance.UID), workload.RoleLabel: role,
					workload.FleetLabel: "blue", workload.ZoneLabel: "az1", podRevisionLabel: greenRevision},
			},
			Spec: corev1.PodSpec{NodeName: "fixture-node", Containers: []corev1.Container{{Name: "opensearch", Image: instance.Spec.Image}}},
		}
		created, err := core.Pods(namespace).Create(ctx, pod, metav1.CreateOptions{})
		if err != nil {
			t.Fatal(err)
		}
		created.Status.Phase = corev1.PodRunning
		created.Status.PodIP = []string{"10.0.0.20", "10.0.0.21"}[i]
		created.Status.PodIPs = []corev1.PodIP{{IP: created.Status.PodIP}}
		created.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}
		created, err = core.Pods(namespace).UpdateStatus(ctx, created, metav1.UpdateOptions{})
		if err != nil {
			t.Fatal(err)
		}
		sourcePods[role] = created
		route, binding := weightedHTTPRouteFixture(namespace)
		route.SetName("journal-" + role)
		if err := unstructured.SetNestedStringSlice(route.Object, []string{role + ".example.com"}, "spec", "hostnames"); err != nil {
			t.Fatal(err)
		}
		binding.Key, binding.InstanceUID, binding.Role = client.ObjectKeyFromObject(route), string(instance.UID), role
		part := v1alpha1.WeightedRoleStatus{
			RouteName: route.GetName(), BackendUIDs: make(map[string]string),
			Revisions: map[string]string{"blue": greenRevision, "green": greenRevision},
		}
		backends := []any{}
		for _, fleet := range []string{"blue", "green"} {
			service, err := workload.GatewayBackendService(instance, role, fleet)
			if err != nil {
				t.Fatal(err)
			}
			service, err = core.Services(namespace).Create(ctx, service, metav1.CreateOptions{})
			if err != nil {
				t.Fatal(err)
			}
			part.BackendUIDs[fleet], binding.ServiceNames[fleet] = string(service.UID), service.Name
			instance.Status.GatewayBackends[role+"/"+fleet] = v1alpha1.GatewayBackendStatus{
				Role: role, Fleet: fleet, Name: service.Name, ServiceUID: string(service.UID),
			}
			weight := int64(0)
			if fleet == "blue" {
				weight = 100
			}
			backends = append(backends, map[string]any{"name": service.Name, "port": int64(9200), "weight": weight})
		}
		if err := unstructured.SetNestedSlice(route.Object, []any{map[string]any{"backendRefs": backends}}, "spec", "rules"); err != nil {
			t.Fatal(err)
		}
		if err := c.Create(ctx, route); err != nil {
			t.Fatal(err)
		}
		binding.UID = route.GetUID()
		observed, digest, err := inspectWeightRoute(route, binding)
		if err != nil {
			t.Fatal(err)
		}
		part.RouteUID, part.RouteSpecDigest, part.InitialWeights = observed.RouteUID, digest, observed.Weights
		state.Roles[role] = part
	}
	if err := c.Status().Update(ctx, instance); err != nil {
		t.Fatal(err)
	}
	now := op.CreationTimestamp.Time
	r := &OperationReconciler{Client: c, Core: core, Now: func() time.Time { return now }}
	approvedRoutes := map[string]string{"search": "journal-search", "ingest": "journal-ingest"}
	projected := instance.DeepCopyObject().(*v1alpha1.OpenSearchServerless)
	claim := operationClaimFor(op)
	projected.Status.ActiveOperation = &claim
	liveState, err := r.buildWeightedPromotionJournal(ctx, c, projected, op, approvedRoutes)
	if err != nil {
		t.Fatal(err)
	}
	for _, role := range weightedRoleOrder {
		got, want := liveState.Roles[role], state.Roles[role]
		schedule := searchSchedule
		if role == "ingest" {
			schedule = ingestSchedule
		}
		if got.RouteUID != want.RouteUID || got.RouteSpecDigest != want.RouteSpecDigest ||
			!maps.Equal(got.Revisions, want.Revisions) || !maps.Equal(got.BackendUIDs, want.BackendUIDs) ||
			len(got.Targets) != len(schedule) {
			t.Fatalf("journal for %s does not bind approved route, revisions and schedule: %#v", role, got)
		}
		for i, weight := range schedule {
			if !maps.Equal(got.Targets[i].Weights, map[string]int{"blue": 100 - weight, "green": weight}) {
				t.Fatalf("%s step %d did not record candidate weight %d: %#v", role, i, weight, got.Targets[i])
			}
		}
	}
	state = liveState
	configureWeightedFixtureDispatch(r, c, approvedRoutes, nil)
	if _, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(op)}); err != nil {
		t.Fatal(err)
	}
	if err := c.Get(ctx, client.ObjectKeyFromObject(op), op); err != nil || op.Status.WeightedRollout != nil || op.Status.RolloutRoles != nil {
		t.Fatalf("weighted promotion fell back before route configuration: %v", err)
	}
	configureWeightedFixtureDispatch(r, c, approvedRoutes, func(context.Context, *v1alpha1.OpenSearchServerless, *v1alpha1.OpenSearchServerlessOperation, rollout.WeightIntent, observedWeightRoute) (weightedRolloutEvidence, error) {
		return weightedRolloutEvidence{}, nil
	})
	fakePolicy := kubernetesfake.NewSimpleClientset().PolicyV1()
	for _, role := range weightedRoleOrder {
		budget, err := workload.DisruptionBudget(instance, disruptionBudgetName(instance, "green", role, "az1"), "green", role, "az1", 0)
		if err != nil {
			t.Fatal(err)
		}
		budget.UID, budget.ResourceVersion, budget.Generation = types.UID("budget-"+role), "1", 1
		budget.Status.ObservedGeneration = 1
		budget.Status.Conditions = []metav1.Condition{{Type: policyv1.DisruptionAllowedCondition, Status: metav1.ConditionTrue, Reason: "SufficientPods"}}
		if _, err := fakePolicy.PodDisruptionBudgets(namespace).Create(ctx, budget, metav1.CreateOptions{}); err != nil {
			t.Fatal(err)
		}
	}
	r.Policy = fakePolicy
	r.RolloutEvidence = func(_ context.Context, instance *v1alpha1.OpenSearchServerless, operation *v1alpha1.OpenSearchServerlessOperation) (rolloutEvidence, error) {
		candidates := make(map[string]*rollout.CandidateEvidence, len(weightedRoleOrder))
		for _, role := range weightedRoleOrder {
			candidates[role] = &rollout.CandidateEvidence{
				InstanceUID: string(instance.UID), OperationUID: string(operation.UID), Role: role, Fleet: "green",
				Revision: greenRevision, ObservedAt: now, CapacityQualified: true, PeerQualified: true,
				SecurityQualified: true, RollbackCompatible: true,
			}
		}
		return rolloutEvidence{Candidates: candidates}, nil
	}
	if _, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(op)}); err != nil {
		t.Fatal(err)
	}
	if err := c.Get(ctx, client.ObjectKeyFromObject(instance), instance); err != nil || instance.Status.ActiveOperation == nil ||
		*instance.Status.ActiveOperation != claim {
		t.Fatalf("manager did not claim the weighted promotion: %v", err)
	}
	if _, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(op)}); err != nil {
		t.Fatal(err)
	}
	if err := c.Get(ctx, client.ObjectKeyFromObject(op), op); err != nil || op.Status.WeightedRollout == nil || op.Status.RolloutRoles != nil {
		t.Fatalf("manager failed to persist weighted journal: %v", err)
	}
	wantRole := "search"
	targetPodUIDs := make(map[string]string)
	evidence := func(_ context.Context, _ *v1alpha1.OpenSearchServerless, _ *v1alpha1.OpenSearchServerlessOperation, intent rollout.WeightIntent, route observedWeightRoute) (weightedRolloutEvidence, error) {
		if wantRole != "" && intent.Role != wantRole {
			t.Fatalf("operation attempted %s before %s completed", intent.Role, wantRole)
		}
		snapshot := rollout.WeightSnapshot{
			MaxAge: time.Minute, MinimumSamples: 100, ExpectedProxyUIDs: []string{"proxy"},
			Qualifications: make(map[string]rollout.FleetWeightQualification),
		}
		proxy := rollout.ProxyWeightObservation{
			ProxyUID: "proxy", InstanceUID: intent.InstanceUID, OperationUID: intent.OperationUID,
			Role: intent.Role, RouteUID: route.RouteUID, RouteVersion: route.ResourceVersion, ObservedAt: now,
			Healthy: make(map[string]map[string]string), Requests: maps.Clone(route.Weights),
			ConfiguredWeights: maps.Clone(route.Weights),
		}
		for fleet, weight := range intent.To {
			if weight == 0 {
				continue
			}
			podUID := fleet + "-uid"
			if fleet == "blue" {
				podUID = string(sourcePods[intent.Role].UID)
			}
			if fleet == "green" && targetPodUIDs[intent.Role] != "" {
				podUID = targetPodUIDs[intent.Role]
			}
			processes := map[string]string{podUID: fleet + "-process"}
			snapshot.Qualifications[fleet] = rollout.FleetWeightQualification{
				CandidateEvidence: rollout.CandidateEvidence{
					InstanceUID: intent.InstanceUID, OperationUID: intent.OperationUID,
					Role: intent.Role, Fleet: fleet, Revision: intent.Revisions[fleet], ObservedAt: now,
					CapacityQualified: true, PeerQualified: true, SecurityQualified: true, RollbackCompatible: true,
				}, Processes: processes,
			}
			proxy.Healthy[fleet] = processes
		}
		snapshot.Proxies = []rollout.ProxyWeightObservation{proxy}
		return weightedRolloutEvidence{WeightSnapshot: snapshot}, nil
	}
	step := func(readEvidence weightStepEvidence) (rollout.WeightAction, bool, error) {
		now = now.Add(time.Second)
		return r.executeWeightedRolloutStep(ctx, c, client.ObjectKeyFromObject(op), op.UID, readEvidence)
	}
	changedEvidence := func(ctx context.Context, instance *v1alpha1.OpenSearchServerless, op *v1alpha1.OpenSearchServerlessOperation, intent rollout.WeightIntent, route observedWeightRoute) (weightedRolloutEvidence, error) {
		snapshot, err := evidence(ctx, instance, op, intent, route)
		snapshot.unchanged = func(context.Context) bool { return false }
		return snapshot, err
	}
	if !interrupt {
		if action, _, err := step(changedEvidence); err != nil || action.Kind != rollout.Hold || action.Reason != "WeightedEvidenceChanged" {
			t.Fatalf("changed weighted evidence did not hold the HTTPRoute write: %#v %v", action, err)
		}
		secrets := core.Secrets(namespace)
		fixture := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "weighted-step-probe"}, Type: rollout.RouteCredentialSecretType}
		original, err := secrets.Create(ctx, fixture, metav1.CreateOptions{})
		if err != nil {
			t.Fatal(err)
		}
		stale := func(ctx context.Context, instance *v1alpha1.OpenSearchServerless, op *v1alpha1.OpenSearchServerlessOperation, intent rollout.WeightIntent, route observedWeightRoute) (weightedRolloutEvidence, error) {
			snapshot, err := evidence(ctx, instance, op, intent, route)
			if err != nil {
				return snapshot, err
			}
			snapshot.RouteAuthIdentity = rollout.RouteAuthIdentity{
				AuthSecretName: original.Name, AuthSecretUID: original.UID, AuthSecretVersion: original.ResourceVersion,
				AuthProbeURL: "https://search.example.com",
			}
			if err := secrets.Delete(ctx, original.Name, metav1.DeleteOptions{}); err != nil {
				return snapshot, err
			}
			_, err = secrets.Create(ctx, fixture, metav1.CreateOptions{})
			return snapshot, err
		}
		if action, _, err := step(stale); err != nil || action.Kind != rollout.Hold || action.Reason != "RouteCredentialChanged" {
			t.Fatalf("credential replacement did not hold the prepared weight write: %#v %v", action, err)
		}
	}
	r.Client = lostWeightResponseClient{c}
	if action, _, err := step(evidence); action.Kind != rollout.PatchRouteWeights || !errors.Is(err, errLostWeightResponse) {
		t.Fatalf("expected applied write with an ambiguous response, got %#v %v", action, err)
	}
	r = &OperationReconciler{Client: c, Core: core, Now: func() time.Time { return now }}
	if interrupt {
		wantRole = ""
		r.Discovery = discovery
		exerciseWeightedRollback(t, c, r, instance, op, sourcePods, evidence, &now, true)
		return
	}
	mustStep := func(reader weightStepEvidence, expected rollout.ActionKind) rollout.WeightAction {
		t.Helper()
		action, done, err := step(reader)
		if err != nil || done || action.Kind != expected {
			t.Fatalf("expected %s, got %#v done=%v err=%v", expected, action, done, err)
		}
		return action
	}
	mustStep(nil, rollout.RecordApplied)
	mustStep(nil, rollout.Hold)
	if action := mustStep(changedEvidence, rollout.Hold); action.Reason != "WeightedEvidenceChanged" {
		t.Fatalf("changed weighted observation influenced the convergence journal: %#v", action)
	}
	mustStep(func(ctx context.Context, instance *v1alpha1.OpenSearchServerless, op *v1alpha1.OpenSearchServerlessOperation, intent rollout.WeightIntent, route observedWeightRoute) (weightedRolloutEvidence, error) {
		snapshot, err := evidence(ctx, instance, op, intent, route)
		snapshot.Proxies[0].ObservedAt = intent.AppliedAt.Add(-time.Millisecond)
		return snapshot, err
	}, rollout.Hold)
	mustStep(evidence, rollout.RecordConverged)
	changeIngestRouteDuringRecheck := func(ctx context.Context, instance *v1alpha1.OpenSearchServerless, op *v1alpha1.OpenSearchServerlessOperation, intent rollout.WeightIntent, route observedWeightRoute) (weightedRolloutEvidence, error) {
		snapshot, err := evidence(ctx, instance, op, intent, route)
		if intent.Role == "search" {
			snapshot.unchanged = func(ctx context.Context) bool {
				// Keep the traffic weights unchanged while advancing the sibling version.
				other := &unstructured.Unstructured{}
				other.SetGroupVersionKind(httpRouteGVK)
				if err := c.Get(ctx, client.ObjectKey{Namespace: namespace, Name: "journal-ingest"}, other); err != nil {
					t.Fatal(err)
				}
				other.SetAnnotations(map[string]string{"fixture": other.GetResourceVersion()})
				if err := c.Update(ctx, other); err != nil {
					t.Fatal(err)
				}
				return true
			}
		}
		return snapshot, err
	}
	if _, _, err := step(changeIngestRouteDuringRecheck); err == nil || !strings.Contains(err.Error(), "ingest route changed while qualifying") {
		t.Fatalf("concurrent ingest route change did not hold search advancement: %v", err)
	}
	for _, weight := range searchSchedule[1:] {
		if action := mustStep(evidence, rollout.PatchRouteWeights); action.To["green"] != weight {
			t.Fatalf("search skipped candidate weight %d: %#v", weight, action)
		}
		mustStep(nil, rollout.RecordApplied)
		if weight == 10 {
			mustStep(func(ctx context.Context, instance *v1alpha1.OpenSearchServerless, op *v1alpha1.OpenSearchServerlessOperation, intent rollout.WeightIntent, route observedWeightRoute) (weightedRolloutEvidence, error) {
				snapshot, err := evidence(ctx, instance, op, intent, route)
				snapshot.Proxies[0].Requests = map[string]int{"blue": 99, "green": 1}
				return snapshot, err
			}, rollout.Hold)
			mustStep(func(ctx context.Context, instance *v1alpha1.OpenSearchServerless, op *v1alpha1.OpenSearchServerlessOperation, intent rollout.WeightIntent, route observedWeightRoute) (weightedRolloutEvidence, error) {
				snapshot, err := evidence(ctx, instance, op, intent, route)
				snapshot.Proxies[0].ConfiguredWeights = nil
				return snapshot, err
			}, rollout.Hold)
		}
		mustStep(evidence, rollout.RecordConverged)
	}
	wantRole = "ingest"
	for _, weight := range ingestSchedule {
		if action := mustStep(evidence, rollout.PatchRouteWeights); action.To["green"] != weight {
			t.Fatalf("ingest skipped candidate weight %d: %#v", weight, action)
		}
		mustStep(nil, rollout.RecordApplied)
		mustStep(evidence, rollout.RecordConverged)
	}
	if _, done, err := step(nil); err != nil || !done {
		t.Fatalf("completed journal was not recognized: done=%v err=%v", done, err)
	}
	wantRole = ""
	r.Discovery = discovery
	finalize := func(e weightStepEvidence) (bool, error) {
		now = now.Add(time.Second)
		return r.finalizeWeightedRollout(ctx, c, client.ObjectKeyFromObject(op), op.UID, "", e)
	}
	if _, err := finalize(nil); err == nil {
		t.Fatal("completed journal committed without fresh proxy evidence")
	}
	if _, err := finalize(changedEvidence); err == nil || !strings.Contains(err.Error(), "weighted observations changed") {
		t.Fatalf("stable Service handoff accepted changed weighted evidence: %v", err)
	}
	if _, err := finalize(changeIngestRouteDuringRecheck); err == nil || !strings.Contains(err.Error(), "ingest route changed while qualifying") {
		t.Fatalf("stable Service handoff accepted a changed ingest route: %v", err)
	}
	for _, role := range weightedRoleOrder {
		if done, err := finalize(evidence); err != nil || done {
			t.Fatalf("advance stable %s Service: done=%v err=%v", role, done, err)
		}
		desired, err := workload.RoleService(instance, role, "green", 9200)
		if err != nil {
			t.Fatal(err)
		}
		service, err := core.Services(namespace).Get(ctx, desired.Name, metav1.GetOptions{})
		if err != nil || !maps.Equal(service.Spec.Selector, desired.Spec.Selector) ||
			string(service.UID) != state.Roles[role].StableServiceUID {
			t.Fatalf("stable %s Service lost its identity or did not change fleet: %v", role, err)
		}
	}
	if _, err := finalize(evidence); err == nil {
		t.Fatal("serving intent committed before stable Service endpoints converged")
	}
	for i, role := range weightedRoleOrder {
		service, err := workload.RoleService(instance, role, "green", 9200)
		if err != nil {
			t.Fatal(err)
		}
		service, err = core.Services(namespace).Get(ctx, service.Name, metav1.GetOptions{})
		if err != nil {
			t.Fatal(err)
		}
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: "journal-target-" + role, Namespace: namespace,
				OwnerReferences: []metav1.OwnerReference{*metav1.NewControllerRef(instance, v1alpha1.GroupVersion.WithKind("OpenSearchServerless"))},
				Labels: map[string]string{workload.InstanceLabel: string(instance.UID), workload.RoleLabel: role,
					workload.FleetLabel: "green", workload.ZoneLabel: "az1", podRevisionLabel: greenRevision},
			},
			Spec: corev1.PodSpec{NodeName: "fixture-node", Containers: []corev1.Container{{Name: "opensearch", Image: instance.Spec.Image}}},
		}
		pod, err = core.Pods(namespace).Create(ctx, pod, metav1.CreateOptions{})
		if err != nil {
			t.Fatal(err)
		}
		ip := []string{"10.0.0.30", "10.0.0.31"}[i]
		pod.Status.Phase, pod.Status.PodIP, pod.Status.PodIPs = corev1.PodRunning, ip, []corev1.PodIP{{IP: ip}}
		pod.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}
		if _, err := core.Pods(namespace).UpdateStatus(ctx, pod, metav1.UpdateOptions{}); err != nil {
			t.Fatal(err)
		}
		targetPodUIDs[role] = string(pod.UID)
		writeWeightedFixtureSlice(t, discovery, service, pod)
	}
	if done, err := finalize(evidence); err != nil || done {
		t.Fatalf("commit weighted promotion: done=%v err=%v", done, err)
	}
	if err := c.Get(ctx, client.ObjectKeyFromObject(instance), instance); err != nil {
		t.Fatal(err)
	}
	if instance.Status.ActiveOperation == nil || instance.Status.LastRolloutCommit == nil ||
		instance.Status.RollbackTarget == nil || !instance.Status.RollbackTarget.Weighted ||
		instance.Status.ServingFleetIntentByRole["search"] != "green" || instance.Status.ServingFleetIntentByRole["ingest"] != "green" {
		t.Fatalf("unexpected weighted commit: %#v", instance.Status)
	}
	for _, role := range weightedRoleOrder {
		retained, part := instance.Status.RollbackTarget.Roles[role], state.Roles[role]
		if instance.Status.ServingRevisionByRole[role] != (v1alpha1.ServingRoleRevision{Fleet: "green", Revision: greenRevision}) {
			t.Fatalf("weighted promotion did not pin %s: %#v", role, instance.Status.ServingRevisionByRole)
		}
		if retained.Fleet != "blue" || retained.Revision != greenRevision || retained.ServingRevision != greenRevision ||
			retained.ServiceUID != part.StableServiceUID || retained.RouteName != part.RouteName ||
			retained.RouteUID != part.RouteUID || retained.RouteSpecDigest != part.RouteSpecDigest ||
			retained.RetainedBackendUID != part.BackendUIDs["blue"] || retained.ServingBackendUID != part.BackendUIDs["green"] {
			t.Fatalf("%s rollback cannot recover its route, Services and revisions from the instance: %#v", role, retained)
		}
	}
	if done, err := finalize(nil); err != nil || done {
		t.Fatalf("record weighted promotion success: done=%v err=%v", done, err)
	}
	if err := c.Get(ctx, client.ObjectKeyFromObject(op), op); err != nil || op.Status.Phase != "Succeeded" {
		t.Fatalf("operation success not persisted: %v", err)
	}
	if done, err := finalize(nil); err != nil || !done {
		t.Fatalf("release weighted promotion claim: done=%v err=%v", done, err)
	}
	if done, err := finalize(nil); err != nil || !done {
		t.Fatalf("resume after weighted claim release: done=%v err=%v", done, err)
	}
	if err := c.Get(ctx, client.ObjectKeyFromObject(instance), instance); err != nil || instance.Status.ActiveOperation != nil {
		t.Fatalf("persisted weighted promotion claim was not released: %v", err)
	}
	rollback := &v1alpha1.OpenSearchServerlessOperation{
		Spec: v1alpha1.OperationSpec{SupersedesOperationUID: string(op.UID)},
	}
	if _, err := r.prepareReleasedPromotionRollback(ctx, c, instance, rollback); err == nil ||
		!strings.Contains(err.Error(), "requires a weighted rollback") {
		t.Fatalf("selector rollback accepted a weighted rollback target: %v", err)
	}
	exerciseWeightedRollback(t, c, r, instance, op, sourcePods, evidence, &now, false)
}
