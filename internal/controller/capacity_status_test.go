package controller

import (
	"context"
	"fmt"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	kubernetesfake "k8s.io/client-go/kubernetes/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/huyz0/opensearch-serverless-operator/api/v1alpha1"
	"github.com/huyz0/opensearch-serverless-operator/internal/java"
	"github.com/huyz0/opensearch-serverless-operator/internal/workload"
)

func TestZoneCapacitySeparatesReadinessFromWarmth(t *testing.T) {
	instance := &v1alpha1.OpenSearchServerless{Spec: v1alpha1.OpenSearchServerlessSpec{Pools: map[string]v1alpha1.Pool{"search": {Zones: map[string]v1alpha1.Zone{"a": {Replicas: 2, WarmSpare: 1}}}}}, Status: v1alpha1.OpenSearchServerlessStatus{ServingFleetIntentByRole: map[string]string{"search": "blue"}}}
	labels := map[string]string{workload.RoleLabel: "search", workload.ZoneLabel: "a", workload.FleetLabel: "blue"}
	ready := corev1.Pod{ObjectMeta: metav1.ObjectMeta{Labels: labels}, Spec: corev1.PodSpec{NodeName: "node"}, Status: corev1.PodStatus{Phase: corev1.PodRunning, Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}}}
	terminal := *ready.DeepCopy()
	terminal.Status.Phase = corev1.PodFailed
	otherFleet := *ready.DeepCopy()
	otherFleet.Labels[workload.FleetLabel] = "green"
	result := summarizeZoneCapacity(instance, []corev1.Pod{ready, terminal, otherFleet})
	if len(result) != 1 || result[0].ObservedPods != 1 || result[0].KubernetesReady != 1 || result[0].WarmSpare != 1 || result[0].WarmReserveStatus != "InsufficientReadyPods" || result[0].WarmReserveReadyShortfall != 1 || result[0].TerminalPods != 1 || result[0].ReadyShortfall != 2 {
		t.Fatalf("misleading capacity status: %#v", result)
	}
}

func TestWarmReserveShortfallExcludesEmergencyAndDisruptingPods(t *testing.T) {
	instance := &v1alpha1.OpenSearchServerless{
		Spec: v1alpha1.OpenSearchServerlessSpec{Pools: map[string]v1alpha1.Pool{
			"search": {Zones: map[string]v1alpha1.Zone{"a": {Replicas: 1, WarmSpare: 2}}},
			"ingest": {Zones: map[string]v1alpha1.Zone{"a": {Replicas: 1, WarmSpare: 1}}},
		}},
		Status: v1alpha1.OpenSearchServerlessStatus{ServingFleetIntentByRole: map[string]string{"search": "blue", "ingest": "blue"}},
	}
	pod := func(role string, emergency, disrupting bool) corev1.Pod {
		p := corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{workload.RoleLabel: role, workload.ZoneLabel: "a", workload.FleetLabel: "blue"}},
			Spec:       corev1.PodSpec{NodeName: "node"},
			Status:     corev1.PodStatus{Phase: corev1.PodRunning, Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}},
		}
		if emergency {
			p.Annotations = map[string]string{emergencyCapacityAnnotation: "true"}
		}
		if disrupting {
			p.Status.Conditions = append(p.Status.Conditions, corev1.PodCondition{Type: corev1.DisruptionTarget, Status: corev1.ConditionTrue})
		}
		return p
	}
	pods := []corev1.Pod{
		pod("search", false, false), pod("search", false, false), pod("search", false, true), pod("search", true, true),
		pod("ingest", false, false), pod("ingest", true, false),
	}
	result := summarizeZoneCapacity(instance, pods)
	for _, group := range result {
		if group.WarmReserveReadyShortfall != 1 || group.WarmReserveStatus != "InsufficientReadyPods" {
			t.Fatalf("expected a one-Pod reserve shortfall: %#v", group)
		}
	}
	if result[1].ReadyShortfall != 0 || result[1].KubernetesReady != 4 {
		t.Fatalf("projected reserve shortfall changed the raw Ready counts: %#v", result[1])
	}
	pods[2].Status.Conditions[1].Status = corev1.ConditionFalse
	result = summarizeZoneCapacity(instance, pods)
	if result[1].WarmReserveReadyShortfall != 0 || result[1].WarmReserveStatus != "Unknown" {
		t.Fatalf("recovered Ready capacity does not prove cache warmth: %#v", result[1])
	}
}

func TestCandidateCapacityReportsPodsEvenWhenPolicyIsUnavailable(t *testing.T) {
	instance := &v1alpha1.OpenSearchServerless{ObjectMeta: metav1.ObjectMeta{UID: "owner", Generation: 1}, Spec: v1alpha1.OpenSearchServerlessSpec{Image: "image", Pools: map[string]v1alpha1.Pool{"search": {Zones: map[string]v1alpha1.Zone{"a": {Replicas: 2}}}}, Rollout: &v1alpha1.Rollout{Strategy: "BlueGreen", MaxPreviewReplicas: 2, Candidate: &v1alpha1.FleetCandidate{Fleet: "green", Image: "image"}}}, Status: v1alpha1.OpenSearchServerlessStatus{ServingFleetIntentByRole: map[string]string{"search": "blue"}, CandidateFleet: &v1alpha1.CandidateFleetStatus{Fleet: "green", Image: "image", Revision: "revision", Generation: 1, SearchServiceUID: "s", IngestServiceUID: "i"}, Conditions: []metav1.Condition{{Type: "PreviewServicesConfigured", Status: metav1.ConditionTrue, ObservedGeneration: 1}}}}
	labels := map[string]string{workload.RoleLabel: "search", workload.ZoneLabel: "a", workload.FleetLabel: "green"}
	ready := corev1.Pod{ObjectMeta: metav1.ObjectMeta{Labels: labels}, Spec: corev1.PodSpec{NodeName: "node"}, Status: corev1.PodStatus{Phase: corev1.PodRunning, Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}}}
	pending := *ready.DeepCopy()
	pending.Spec.NodeName = ""
	terminating := *ready.DeepCopy()
	stamp := metav1.Now()
	terminating.DeletionTimestamp = &stamp
	capacity := summarizeCandidateCapacity(instance, []corev1.Pod{ready, pending, terminating})
	if len(capacity) != 1 || !capacity[0].DesiredKnown || capacity[0].DesiredPods != 2 || capacity[0].ObservedPods != 2 || capacity[0].UnboundPods != 1 || capacity[0].KubernetesReady != 1 || capacity[0].TerminatingPods != 1 || capacity[0].Warmth != "Unknown" {
		t.Fatalf("candidate counts are incorrect: %#v", capacity)
	}
	instance.Status.Conditions[0].Status = metav1.ConditionFalse
	capacity = summarizeCandidateCapacity(instance, []corev1.Pod{ready})
	if len(capacity) != 1 || capacity[0].DesiredKnown || capacity[0].KubernetesReady != 1 || capacity[0].Warmth != "Unknown" {
		t.Fatalf("held candidate Pod was hidden: %#v", capacity)
	}
	instance.Status.CandidateFleet.Capacity = capacity
	copy := instance.DeepCopyObject().(*v1alpha1.OpenSearchServerless)
	copy.Status.CandidateFleet.Capacity[0].KubernetesReady = 99
	if instance.Status.CandidateFleet.Capacity[0].KubernetesReady == 99 {
		t.Fatal("candidate capacity was not deep copied")
	}
}

func TestDevelopmentQualificationSamplesAndRotatesWithoutClaimingWarmth(t *testing.T) {
	ctx := context.Background()
	scheme := runtime.NewScheme()
	if err := v1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	instance := &v1alpha1.OpenSearchServerless{ObjectMeta: metav1.ObjectMeta{Name: "instance", Namespace: "n", UID: "owner", Generation: 1}, Spec: v1alpha1.OpenSearchServerlessSpec{Image: "image", Pools: map[string]v1alpha1.Pool{"search": {WarmOnJoin: true}}}, Status: v1alpha1.OpenSearchServerlessStatus{ServingFleetIntentByRole: map[string]string{"search": "blue"}}}
	instance.Spec.Pools["search"] = v1alpha1.Pool{WarmOnJoin: true, Zones: map[string]v1alpha1.Zone{"a": {Replicas: 1}}}
	instance.Spec.Rollout = &v1alpha1.Rollout{Strategy: "BlueGreen", MaxPreviewReplicas: 1, Candidate: &v1alpha1.FleetCandidate{Fleet: "green", Image: "image"}}
	instance.Status.CandidateFleet = &v1alpha1.CandidateFleetStatus{Fleet: "green", Image: "image", Revision: "revision", Generation: 1, SearchServiceUID: "s", IngestServiceUID: "i"}
	instance.Status.Conditions = []metav1.Condition{{Type: "PreviewServicesConfigured", Status: metav1.ConditionTrue, ObservedGeneration: 1}}
	controller := true
	makePod := func(name string) *corev1.Pod {
		return &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "n", UID: types.UID(name), ResourceVersion: "1", OwnerReferences: []metav1.OwnerReference{{APIVersion: v1alpha1.GroupVersion.String(), Kind: "OpenSearchServerless", Name: "instance", UID: "owner", Controller: &controller}}, Labels: map[string]string{workload.InstanceLabel: "owner", workload.RoleLabel: "search", workload.FleetLabel: "blue", workload.ZoneLabel: "a"}}, Spec: corev1.PodSpec{NodeName: "node"}, Status: corev1.PodStatus{Phase: corev1.PodRunning, PodIP: "10.0.0.1", Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}, ContainerStatuses: []corev1.ContainerStatus{{Name: "opensearch", ContainerID: "container", Ready: true, State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}}}}}}
	}
	candidatePod := makePod("c")
	candidatePod.Labels[workload.FleetLabel] = "green"
	core := kubernetesfake.NewSimpleClientset(makePod("a"), makePod("b"), candidatePod)
	kube := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(instance).WithObjects(instance).Build()
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	probes := 0
	failed := false
	r := &InstanceReconciler{Client: kube, Core: core.CoreV1(), DevelopmentWarmupImage: "image", Now: func() time.Time { return now }, ObserveSearchQualification: func(context.Context, *corev1.Pod) (java.SearchQualification, error) {
		probes++
		if failed {
			return java.SearchQualification{}, fmt.Errorf("unavailable")
		}
		return java.SearchQualification{ProcessIncarnation: "9aa977f3-4985-4cb5-9692-5ad1e3e243fe",
			StatsProcessVerified: probes == 1, LocalSearchActivity: &java.LocalSearchActivity{BlockCacheHits: 7}}, nil
	}}
	key := types.NamespacedName{Namespace: "n", Name: "instance"}
	step := func() bool {
		t.Helper()
		if err := kube.Get(ctx, key, instance); err != nil {
			t.Fatal(err)
		}
		changed, err := r.sampleSearchQualification(ctx, instance)
		if err != nil {
			t.Fatal(err)
		}
		return changed
	}
	if !step() || instance.Status.LastSearchQualification.PodUID != "a" || instance.Status.LastSearchQualification.Result != "WarmupCompleted" {
		t.Fatal("first qualified observation missing")
	}
	if activity := instance.Status.LastSearchQualification.LocalSearchActivity; activity == nil || activity.ShardReaders != 0 || activity.BlockCacheHits != 7 {
		t.Fatalf("zero readers or cache counters lost from the verified sample: %#v", activity)
	}
	copy := instance.DeepCopyObject().(*v1alpha1.OpenSearchServerless)
	copy.Status.LastSearchQualification.LocalSearchActivity.BlockCacheHits = 99
	if instance.Status.LastSearchQualification.LocalSearchActivity.BlockCacheHits != 7 {
		t.Fatal("the local activity snapshot was not deep copied")
	}
	if step() || probes != 1 {
		t.Fatal("probe was not rate limited")
	}
	now = now.Add(time.Minute)
	if !step() || instance.Status.LastSearchQualification.PodUID != "b" || probes != 2 || instance.Status.LastSearchQualification.LocalSearchActivity != nil {
		t.Fatal("sample did not rotate")
	}
	now = now.Add(time.Minute)
	if !step() || instance.Status.LastSearchQualification.PodUID != "c" || instance.Status.LastSearchQualification.Fleet != "green" || probes != 3 {
		t.Fatal("candidate process was not sampled")
	}
	failed = true
	now = now.Add(time.Minute)
	if !step() || instance.Status.LastSearchQualification.Result != "Unavailable" || instance.Status.LastSearchQualification.ProcessIncarnation != "" || instance.Status.LastSearchQualification.LocalSearchActivity != nil {
		t.Fatal("failed probe retained success evidence")
	}
	instance.Spec.TLS = &v1alpha1.TLSConfig{HTTP: &v1alpha1.TLSChannel{}}
	if err := kube.Update(ctx, instance); err != nil {
		t.Fatal(err)
	}
	now = now.Add(time.Minute)
	if step() || probes != 4 {
		t.Fatal("probe ran with HTTP TLS configured")
	}
}

func TestZoneCapacityShowsShortfallWithoutClaimingWarmth(t *testing.T) {
	instance := &v1alpha1.OpenSearchServerless{Spec: v1alpha1.OpenSearchServerlessSpec{Pools: map[string]v1alpha1.Pool{"search": {Zones: map[string]v1alpha1.Zone{"a": {Replicas: 2, WarmSpare: 1, MinServing: 2}, "b": {Replicas: 1, MinServing: 1}}}}}, Status: v1alpha1.OpenSearchServerlessStatus{ServingFleetIntentByRole: map[string]string{"search": "blue"}}}
	labels := func(zone string) map[string]string {
		return map[string]string{workload.RoleLabel: "search", workload.ZoneLabel: zone, workload.FleetLabel: "blue"}
	}
	ready := corev1.Pod{ObjectMeta: metav1.ObjectMeta{Labels: labels("a")}, Spec: corev1.PodSpec{NodeName: "node"}, Status: corev1.PodStatus{Phase: corev1.PodRunning, Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}}}
	unbound := corev1.Pod{ObjectMeta: metav1.ObjectMeta{Labels: labels("a")}, Status: corev1.PodStatus{Phase: corev1.PodPending, Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}}}
	stale := *ready.DeepCopy()
	stale.Status.Phase = corev1.PodPending
	terminal := *ready.DeepCopy()
	terminal.Status.Phase = corev1.PodFailed
	terminating := *ready.DeepCopy()
	stamp := metav1.NewTime(time.Now())
	terminating.DeletionTimestamp = &stamp
	b := *ready.DeepCopy()
	b.Labels = labels("b")
	result := summarizeZoneCapacity(instance, []corev1.Pod{ready, unbound, stale, terminal, terminating, b})
	if len(result) != 2 || result[0].Zone != "a" || result[0].ObservedPods != 3 || result[0].UnboundPods != 1 || result[0].TerminatingPods != 1 || result[0].TerminalPods != 1 || result[0].KubernetesReady != 1 || result[0].ReadyShortfall != 2 || !result[0].BelowMinServing || result[0].WarmReserveStatus != "InsufficientReadyPods" || result[0].WarmReserveReadyShortfall != 1 {
		t.Fatalf("incorrect impaired zone: %#v", result)
	}
	if result[1].ReadyShortfall != 0 || result[1].BelowMinServing || result[1].WarmReserveStatus != "NotRequested" {
		t.Fatalf("incorrect healthy zone: %#v", result[1])
	}
}

func TestZoneCapacitySampleRefreshHasBoundedFreshness(t *testing.T) {
	ctx := context.Background()
	scheme := runtime.NewScheme()
	if err := v1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	instance := &v1alpha1.OpenSearchServerless{ObjectMeta: metav1.ObjectMeta{Name: "instance", Namespace: "n", UID: "owner", Generation: 1}, Spec: v1alpha1.OpenSearchServerlessSpec{Pools: map[string]v1alpha1.Pool{"search": {Zones: map[string]v1alpha1.Zone{"a": {Replicas: 1}}}}}, Status: v1alpha1.OpenSearchServerlessStatus{ServingFleetIntentByRole: map[string]string{"search": "blue"}}}
	core := kubernetesfake.NewSimpleClientset()
	kube := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(instance).WithObjects(instance).Build()
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	r := &InstanceReconciler{Client: kube, Core: core.CoreV1(), Now: func() time.Time { return now }}
	key := types.NamespacedName{Namespace: "n", Name: "instance"}
	step := func() bool {
		t.Helper()
		if err := kube.Get(ctx, key, instance); err != nil {
			t.Fatal(err)
		}
		changed, err := r.refreshZoneCapacity(ctx, instance)
		if err != nil {
			t.Fatal(err)
		}
		return changed
	}
	if !step() || instance.Status.ZoneCapacityObservedAt == nil || instance.Status.ZoneCapacityObservedGeneration != 1 {
		t.Fatal("first observation missing")
	}
	if step() {
		t.Fatal("unchanged observation updated too soon")
	}
	now = now.Add(time.Minute)
	if !step() || !instance.Status.ZoneCapacityObservedAt.Time.Equal(now) {
		t.Fatal("unchanged observation did not refresh")
	}
	now = now.Add(-2 * time.Minute)
	if !step() || !instance.Status.ZoneCapacityObservedAt.Time.Equal(now) {
		t.Fatal("clock reversal did not refresh observation")
	}
	owner := true
	conflict := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "conflict", Namespace: "n", UID: "pod", Labels: map[string]string{workload.InstanceLabel: "owner"}, OwnerReferences: []metav1.OwnerReference{{UID: "different-owner", Controller: &owner}}}}
	if _, err := core.CoreV1().Pods("n").Create(ctx, conflict, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	now = now.Add(time.Minute)
	if _, err := r.refreshZoneCapacity(ctx, instance); err == nil {
		t.Fatal("conflicting owner accepted")
	}
	if err := kube.Get(ctx, key, instance); err != nil || instance.Status.ZoneCapacityObservedAt.Time.Equal(now) {
		t.Fatal("failed observation refreshed timestamp")
	}
}
