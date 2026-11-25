package controller

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/huyz0/opensearch-serverless-operator/api/v1alpha1"
	"github.com/huyz0/opensearch-serverless-operator/internal/java"
	"github.com/huyz0/opensearch-serverless-operator/internal/workload"
)

func TestSearchQualificationBindsEvidenceToUnchangedPod(t *testing.T) {
	ctx := context.Background()
	controller := true
	instance := &v1alpha1.OpenSearchServerless{ObjectMeta: metav1.ObjectMeta{Name: "instance", Namespace: "n", UID: "owner"}}
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "search-a", Namespace: "n", UID: "pod-uid", ResourceVersion: "1", OwnerReferences: []metav1.OwnerReference{{APIVersion: v1alpha1.GroupVersion.String(), Kind: "OpenSearchServerless", Name: "instance", UID: "owner", Controller: &controller}}, Labels: map[string]string{workload.InstanceLabel: "owner", workload.RoleLabel: "search", workload.FleetLabel: "blue", workload.ZoneLabel: "a"}}, Spec: corev1.PodSpec{NodeName: "node"}, Status: corev1.PodStatus{Phase: corev1.PodRunning, PodIP: "10.0.0.1", Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}, ContainerStatuses: []corev1.ContainerStatus{{Name: "opensearch", ContainerID: "container-1", Ready: true, State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}}}}}}
	core := fake.NewSimpleClientset(pod)
	probe := func(context.Context, *corev1.Pod) (java.SearchQualification, error) {
		return java.SearchQualification{ProcessIncarnation: "9aa977f3-4985-4cb5-9692-5ad1e3e243fe"}, nil
	}
	check := func(probe func(context.Context, *corev1.Pod) (java.SearchQualification, error)) (searchPodQualification, error) {
		return qualifySearchPod(ctx, core.CoreV1(), instance, "search-a", "pod-uid", "blue", "a", probe)
	}
	result, err := check(probe)
	if err != nil || result.PodUID != "pod-uid" || result.ContainerID != "container-1" || result.ObservedAt.IsZero() {
		t.Fatalf("valid observation rejected: %#v %v", result, err)
	}
	if _, err := qualifySearchPod(ctx, core.CoreV1(), instance, "search-a", "pod-uid", "green", "a", probe); err == nil {
		t.Fatal("different fleet accepted")
	}
	_, err = check(func(ctx context.Context, observed *corev1.Pod) (java.SearchQualification, error) {
		updated, err := core.CoreV1().Pods("n").Get(ctx, observed.Name, metav1.GetOptions{})
		if err != nil {
			return java.SearchQualification{}, err
		}
		updated.ResourceVersion = "2"
		updated.Status.ContainerStatuses[0].ContainerID = "container-2"
		updated.Status.ContainerStatuses[0].RestartCount++
		if _, err := core.CoreV1().Pods("n").Update(ctx, updated, metav1.UpdateOptions{}); err != nil {
			return java.SearchQualification{}, err
		}
		return probe(ctx, observed)
	})
	if err == nil {
		t.Fatal("container restart during probe was accepted")
	}
	if _, err := check(func(context.Context, *corev1.Pod) (java.SearchQualification, error) {
		return java.SearchQualification{ProcessIncarnation: "invalid"}, nil
	}); err == nil {
		t.Fatal("invalid process incarnation accepted")
	}
}

func TestDevelopmentSearchProbeUsesPodIPAndRejectsSecurity(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/_serverless/stats" {
			_, _ = fmt.Fprint(w, `{"node":"id","name":"search-a","roles":["search","ingest"],"lease":{"valid_now":true,"expires_in_millis":30000,"ttl_millis":30000}}`)
			return
		}
		_, _ = fmt.Fprint(w, `{"ready":true,"state":"ready","serves_searches":true,"process_incarnation":"9aa977f3-4985-4cb5-9692-5ad1e3e243fe","warmup_result":"completed","warmup_timed_out":false}`)
	}))
	defer server.Close()
	host, portText, err := net.SplitHostPort(server.Listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	port, err := strconv.Atoi(portText)
	if err != nil {
		t.Fatal(err)
	}
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "search-a"}, Status: corev1.PodStatus{PodIP: host}}
	if _, err := (&InstanceReconciler{}).fleetQualification().developmentSearchProbe(&v1alpha1.OpenSearchServerless{}, port, time.Second)(context.Background(), pod); err != nil {
		t.Fatalf("direct probe failed: %v", err)
	}
	r := &InstanceReconciler{DevelopmentWarmupImage: "qualified"}
	instance := &v1alpha1.OpenSearchServerless{Spec: v1alpha1.OpenSearchServerlessSpec{Image: "qualified", TLS: &v1alpha1.TLSConfig{HTTP: &v1alpha1.TLSChannel{}}}}
	if _, err := r.fleetQualification().qualifyDevelopmentSearchPod(context.Background(), instance, "p", "uid", "fleet", "zone", 0); err == nil {
		t.Fatal("probe accepted HTTP TLS configuration without its pins")
	}
	instance.Spec.TLS = nil
	instance.Spec.Security = &v1alpha1.SecurityConfig{}
	if _, err := r.fleetQualification().qualifyDevelopmentSearchPod(context.Background(), instance, "p", "uid", "fleet", "zone", 0); err == nil {
		t.Fatal("plaintext probe accepted security configuration")
	}
}
