package controller

import (
	"context"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	clientfake "sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/huyz0/opensearch-serverless-operator/api/v1alpha1"
	"github.com/huyz0/opensearch-serverless-operator/internal/workload"
)

func TestEmergencyProposalExpiresWithoutBlockingOrdinaryRecovery(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 1, 1, 0, 1, 0, 0, time.UTC)
	instance := &v1alpha1.OpenSearchServerless{
		ObjectMeta: metav1.ObjectMeta{Name: "catalog", Namespace: "n", UID: "instance", Generation: 3},
		Spec: v1alpha1.OpenSearchServerlessSpec{
			Pools: map[string]v1alpha1.Pool{"search": {CapacityProfileRef: "profile", Zones: map[string]v1alpha1.Zone{
				"a": {Replicas: 1}, "b": {Replicas: 1},
			}}},
			Availability: &v1alpha1.Availability{MinServingTotal: map[string]int32{"search": 2}, MaxPendingProvisionablePods: 1,
				EmergencyCapacity: &v1alpha1.EmergencyCapacityPolicy{EvidenceRef: v1alpha1.LocalReference{Name: "outage"},
					MaxTemporaryByRole: map[string]int32{"search": 1}, MaxTemporaryPerZone: 1}},
		},
		Status: v1alpha1.OpenSearchServerlessStatus{ServingFleetIntentByRole: map[string]string{"search": "initial"}},
	}
	outage := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: "outage", Namespace: "n", UID: "evidence", ResourceVersion: "4"},
		Data:       map[string]string{"evidence.json": `{"instanceUID":"instance","generation":3,"observedAt":"2026-01-01T00:00:00Z","expiresAt":"2026-01-01T00:02:00Z","unavailableZones":["a"],"fitByRole":{"search":{"b":{"additionalPods":1,"profileUID":"profile","profileVersion":"2"}}}}`},
	}
	immutable := true
	profile := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: "profile", Namespace: "n", UID: "profile", ResourceVersion: "2"}, Immutable: &immutable,
		Data: map[string]string{"profile.json": `{"revision":"v1","role":"search","zones":["b"],"nodeSelector":{"platform.example/profile":"search"},"taint":{"key":"platform.example/dedicated","value":"search","effect":"NoSchedule"},"observedAt":"2026-01-01T00:00:00Z","expiresAt":"2026-01-01T00:05:00Z","allocatable":{"cpu":"8","memory":"16Gi","ephemeral-storage":"40Gi","pods":"20"},"reserved":{"cpu":"1","memory":"2Gi","ephemeral-storage":"5Gi","pods":"4"},"headroom":{"cpu":"1","memory":"2Gi","ephemeral-storage":"5Gi","pods":"1"}}`},
	}
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "ready", Namespace: "n", UID: "pod",
			OwnerReferences: []metav1.OwnerReference{*metav1.NewControllerRef(instance, v1alpha1.GroupVersion.WithKind("OpenSearchServerless"))},
			Labels:          map[string]string{workload.InstanceLabel: "instance", workload.RoleLabel: "search", workload.ZoneLabel: "b", workload.FleetLabel: "initial"}},
		Spec:   corev1.PodSpec{NodeName: "node-b"},
		Status: corev1.PodStatus{Phase: corev1.PodRunning, Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}},
	}
	scheme := runtime.NewScheme()
	if err := v1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	c := clientfake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(instance).WithObjects(instance).Build()
	core := fake.NewSimpleClientset(outage, profile, pod)
	r := &InstanceReconciler{Client: c, Core: core.CoreV1(), Now: func() time.Time { return now }}
	if changed, err := r.sampleEmergencyCapacity(ctx, instance); err != nil || !changed {
		t.Fatalf("emergency evidence was not evaluated: %v", err)
	}
	got := instance.Status.EmergencyCapacityProposal
	if got == nil || got.Zone != "b" || got.Role != "search" || got.Fleet != "initial" || got.EvidenceVersion != "4" {
		t.Fatalf("expected a proposal bound to the surviving zone and platform evidence: %#v", got)
	}
	t.Run("zone cap includes other roles", func(t *testing.T) {
		committed := pod.DeepCopy()
		committed.Name, committed.UID = "temporary-ingest", "temporary"
		committed.Labels[workload.RoleLabel] = "ingest"
		committed.Annotations = map[string]string{emergencyCapacityAnnotation: "true"}
		termination := metav1.NewTime(now)
		committed.DeletionTimestamp = &termination
		committed.Finalizers = []string{"example.org/termination"}
		if _, err := core.CoreV1().Pods("n").Create(ctx, committed, metav1.CreateOptions{}); err != nil {
			t.Fatal(err)
		}
		if proposal, err := r.planEmergencyCapacity(ctx, instance); err != nil || proposal != nil {
			t.Fatalf("terminating ingest Pod must reserve the shared zone slot: %#v, %v", proposal, err)
		}
		if err := core.CoreV1().Pods("n").Delete(ctx, committed.Name, metav1.DeleteOptions{}); err != nil {
			t.Fatal(err)
		}
		if proposal, err := r.planEmergencyCapacity(ctx, instance); err != nil || proposal == nil || proposal.Zone != "b" {
			t.Fatalf("removed commitment should release the zone slot: %#v, %v", proposal, err)
		}
	})
	now = got.ExpiresAt.Time
	if changed, err := r.sampleEmergencyCapacity(ctx, instance); err != nil || !changed || instance.Status.EmergencyCapacityProposal != nil {
		t.Fatalf("expired evidence was not cleared: %v", err)
	}
	condition := meta.FindStatusCondition(instance.Status.Conditions, "EmergencyCapacityEvaluated")
	if condition == nil || condition.Status != metav1.ConditionUnknown {
		t.Fatalf("expired evidence was not reported as unavailable: %#v", condition)
	}
}

func TestEmergencyEvidenceRejectsAmbiguousReport(t *testing.T) {
	instance := &v1alpha1.OpenSearchServerless{ObjectMeta: metav1.ObjectMeta{Namespace: "n", UID: "instance", Generation: 3}}
	const header = `{"instanceUID":"instance","generation":3,"observedAt":"2026-01-01T00:00:00Z","expiresAt":"2026-01-01T00:02:00Z",`
	for _, tc := range []struct {
		name, body string
		accepted   bool
	}{
		{name: "explicit clearance", body: `"unavailableZones":[]}`, accepted: true},
		{name: "clearance overrides outage", body: `"unavailableZones":["a"],"unavailableZones":[]}`},
		{name: "duplicate zone fit", body: `"unavailableZones":["a"],"fitByRole":{"search":{"b":{"additionalPods":0},"b":{"additionalPods":1}}}}`},
		{name: "unknown field", body: `"unavailableZones":[],"UnavailableZones":["a"]}`},
		{name: "second document", body: `"unavailableZones":[]} {}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			report := &corev1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{Name: "outage", Namespace: instance.Namespace, UID: "report", ResourceVersion: "1"},
				Data:       map[string]string{"evidence.json": header + tc.body},
			}
			r := &InstanceReconciler{Core: fake.NewSimpleClientset(report).CoreV1(), Now: func() time.Time {
				return time.Date(2026, 1, 1, 0, 1, 0, 0, time.UTC)
			}}
			_, _, err := r.readEmergencyCapacityEvidence(context.Background(), instance, report.Name)
			if (err == nil) != tc.accepted {
				t.Fatalf("report accepted=%t, want %t: %v", err == nil, tc.accepted, err)
			}
		})
	}
}
