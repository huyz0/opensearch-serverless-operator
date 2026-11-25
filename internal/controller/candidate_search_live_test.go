package controller

import (
	"context"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/huyz0/opensearch-serverless-operator/api/v1alpha1"
	"github.com/huyz0/opensearch-serverless-operator/internal/java"
	"github.com/huyz0/opensearch-serverless-operator/internal/workload"
)

// The shell is real; Pod metadata is a fixture and does not prove Kubernetes
// networking, AZ placement, preview routes or takeover.
func TestLiveCandidateSearchObservation(t *testing.T) {
	endpoint := os.Getenv("SERVERLESS_TEST_CANDIDATE_SEARCH")
	if endpoint == "" {
		t.Skip("SERVERLESS_TEST_CANDIDATE_SEARCH is not set")
	}
	image := "fixture.invalid/runtime@sha256:" + strings.Repeat("a", 64)
	instance := &v1alpha1.OpenSearchServerless{ObjectMeta: metav1.ObjectMeta{Name: "cluster", Namespace: "n", UID: "owner", Generation: 1}, Spec: v1alpha1.OpenSearchServerlessSpec{Image: image, Pools: map[string]v1alpha1.Pool{"search": {WarmOnJoin: true, WarmReadiness: "Strict", Zones: map[string]v1alpha1.Zone{"a": {Replicas: 1}}}}, Rollout: &v1alpha1.Rollout{Strategy: "BlueGreen", MaxPreviewReplicas: 1, Candidate: &v1alpha1.FleetCandidate{Fleet: "green", Image: image}}}, Status: v1alpha1.OpenSearchServerlessStatus{ServingFleetIntentByRole: map[string]string{"search": "blue"}, CandidateFleet: &v1alpha1.CandidateFleetStatus{Fleet: "green", Image: image, Revision: "revision", Generation: 1, SearchServiceUID: "s", IngestServiceUID: "i"}, Conditions: []metav1.Condition{{Type: "PreviewServicesConfigured", Status: metav1.ConditionTrue, ObservedGeneration: 1}}}}
	owner := true
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "search-warm", Namespace: "n", UID: "pod", ResourceVersion: "1", OwnerReferences: []metav1.OwnerReference{{APIVersion: v1alpha1.GroupVersion.String(), Kind: "OpenSearchServerless", Name: "cluster", UID: "owner", Controller: &owner}}, Labels: map[string]string{workload.InstanceLabel: "owner", workload.RoleLabel: "search", workload.FleetLabel: "green", workload.ZoneLabel: "a", "serverless.opensearch.org/revision": "revision"}, Annotations: map[string]string{warmupModeAnnotation: "Strict"}}, Spec: corev1.PodSpec{NodeName: "fixture"}, Status: corev1.PodStatus{Phase: corev1.PodRunning, PodIP: "127.0.0.1", Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}, ContainerStatuses: []corev1.ContainerStatus{{Name: "opensearch", ContainerID: "fixture", Ready: true, State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}}}}}}
	r := &InstanceReconciler{Core: fake.NewSimpleClientset(pod).CoreV1(), DevelopmentWarmupImage: image, ObserveSearchQualification: func(ctx context.Context, pod *corev1.Pod) (java.SearchQualification, error) {
		return java.QualifySearch(ctx, &http.Client{Timeout: 3 * time.Second}, endpoint, pod.Name, time.Second)
	}}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	observed, err := r.observeCandidateSearchWarmth(ctx, instance, 10*time.Second)
	if err != nil || len(observed.Pods) != 1 || observed.Pods[0].ProcessIncarnation == "" {
		t.Fatalf("candidate search qualification: %#v %v", observed, err)
	}
}
