package controller

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/huyz0/opensearch-serverless-operator/api/v1alpha1"
	"github.com/huyz0/opensearch-serverless-operator/internal/workload"
	"github.com/huyz0/opensearch-serverless-operator/pkg/podlifecycle"
)

func TestDevelopmentCandidateCapacityBoundedAndQualified(t *testing.T) {
	instance := &v1alpha1.OpenSearchServerless{ObjectMeta: metav1.ObjectMeta{UID: "owner", Generation: 2}, Spec: v1alpha1.OpenSearchServerlessSpec{Image: "same", Rollout: &v1alpha1.Rollout{Strategy: "BlueGreen", MaxPreviewReplicas: 3, Candidate: &v1alpha1.FleetCandidate{Fleet: "green", Image: "same"}}, Pools: map[string]v1alpha1.Pool{"search": {Zones: map[string]v1alpha1.Zone{"a": {Replicas: 2}, "b": {Replicas: 2}}}, "ingest": {Zones: map[string]v1alpha1.Zone{"a": {Replicas: 2}, "b": {Replicas: 2}}}}}, Status: v1alpha1.OpenSearchServerlessStatus{ServingFleetIntentByRole: map[string]string{"search": "blue", "ingest": "blue"}, CandidateFleet: &v1alpha1.CandidateFleetStatus{Fleet: "green", Image: "same", Revision: "revision", Generation: 2, SearchServiceUID: "s", IngestServiceUID: "i"}, Conditions: []metav1.Condition{{Type: "PreviewServicesConfigured", Status: metav1.ConditionTrue, ObservedGeneration: 2}}}}
	policies := developmentCandidatePolicies(instance)
	if len(policies) != 3 {
		t.Fatalf("expected three role/zone groups: %#v", policies)
	}
	for _, policy := range policies {
		if policy.Desired != 1 || policy.Group.Fleet != "green" || !fleetAllowedForPod(instance, policy.Group.Role, policy.Group.Zone, "green") {
			t.Fatalf("unexpected candidate allocation: %#v", policy)
		}
	}
	if fleetAllowedForPod(instance, "search", "b", "green") {
		t.Fatal("unallocated zone accepted")
	}
	instance.Spec.Rollout.MaxPreviewReplicas = 2147483647
	policies = developmentCandidatePolicies(instance)
	total := 0
	for _, policy := range policies {
		total += policy.Desired
	}
	if total != 8 {
		t.Fatalf("candidate exceeds desired capacity: %d", total)
	}
	instance.Spec.Rollout.Candidate.Image = "different"
	if len(developmentCandidatePolicies(instance)) != 0 || fleetAllowedForPod(instance, "search", "a", "green") {
		t.Fatal("different image accepted without compatibility qualification")
	}
	instance.Spec.Rollout.Candidate.Image = "same"
	instance.Status.Conditions[0].ObservedGeneration--
	if len(developmentCandidatePolicies(instance)) != 0 {
		t.Fatal("stale preview Services accepted")
	}
	if !fleetAllowedForPod(instance, "search", "a", "blue") {
		t.Fatal("serving fleet rejected")
	}
}

func TestDisruptingPodOccupiesPlacementWithoutServingEvidence(t *testing.T) {
	instance := &v1alpha1.OpenSearchServerless{ObjectMeta: metav1.ObjectMeta{Name: "catalog", UID: "owner"},
		Status: v1alpha1.OpenSearchServerlessStatus{ServingFleetIntentByRole: map[string]string{"search": "blue"}}}
	pod := corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "search-a", UID: "pod", OwnerReferences: []metav1.OwnerReference{*metav1.NewControllerRef(instance, v1alpha1.GroupVersion.WithKind("OpenSearchServerless"))},
			Labels: map[string]string{workload.RoleLabel: "search", workload.ZoneLabel: "a", workload.FleetLabel: "blue"}},
		Spec: corev1.PodSpec{NodeName: "node"},
		Status: corev1.PodStatus{Phase: corev1.PodRunning, Conditions: []corev1.PodCondition{
			{Type: corev1.PodReady, Status: corev1.ConditionTrue}, {Type: corev1.DisruptionTarget, Status: corev1.ConditionTrue},
		}},
	}
	r := &InstanceReconciler{}
	inventory, err := r.observeGatedPodInventory(context.Background(), instance, []corev1.Pod{pod}, "")
	if err != nil {
		t.Fatal(err)
	}
	group := podlifecycle.Group{Workload: "owner", Role: "search", Zone: "a", Fleet: "blue"}
	if inventory.readyByGroup[group] != 0 || inventory.servingReadyByGroup[group] != 0 || !inventory.servingSearchStarted {
		t.Fatalf("disruption did not distinguish serving evidence from an existing process: %+v", inventory)
	}
	availability, err := podlifecycle.Assess(inventory.snapshot, podlifecycle.GroupPolicy{Group: group, Desired: 1})
	if err != nil || availability.Active != 1 || availability.Deficit != 0 {
		t.Fatalf("disrupted Pod did not occupy its capacity slot: %+v, %v", availability, err)
	}
}
