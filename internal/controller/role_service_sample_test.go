package controller

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	kubernetesfake "k8s.io/client-go/kubernetes/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/huyz0/opensearch-serverless-operator/api/v1alpha1"
	"github.com/huyz0/opensearch-serverless-operator/internal/workload"
)

func TestServiceProbeSampleRotatesAndClearsFailedEvidence(t *testing.T) {
	ctx := context.Background()
	image := "registry/runtime@sha256:" + strings.Repeat("a", 64)
	scheme := runtime.NewScheme()
	if err := v1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	instance := &v1alpha1.OpenSearchServerless{ObjectMeta: metav1.ObjectMeta{Name: "catalog", Namespace: "n", UID: "owner", Generation: 1}, Spec: v1alpha1.OpenSearchServerlessSpec{Image: image, Pools: map[string]v1alpha1.Pool{"search": {Zones: map[string]v1alpha1.Zone{"a": {Replicas: 1}}}, "ingest": {Zones: map[string]v1alpha1.Zone{"a": {Replicas: 1}}}}}, Status: v1alpha1.OpenSearchServerlessStatus{ServingFleetIntentByRole: map[string]string{"search": "blue", "ingest": "blue"}}}
	owner := true
	objects := []runtime.Object{}
	for _, role := range []string{"ingest", "search"} {
		service, err := workload.RoleService(instance, role, "blue", 9200)
		if err != nil {
			t.Fatal(err)
		}
		service.UID = types.UID("svc-" + role)
		service.ResourceVersion = "10"
		service.Spec.Type = corev1.ServiceTypeClusterIP
		service.Spec.ClusterIP = "10.0.0.1"
		pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: role + "-pod", Namespace: "n", UID: types.UID("pod-" + role), ResourceVersion: "20", Labels: map[string]string{workload.InstanceLabel: "owner", workload.RoleLabel: role, workload.FleetLabel: "blue", workload.ZoneLabel: "a"}, OwnerReferences: []metav1.OwnerReference{{APIVersion: v1alpha1.GroupVersion.String(), Kind: "OpenSearchServerless", Name: "catalog", UID: "owner", Controller: &owner}}}, Spec: corev1.PodSpec{NodeName: "node"}, Status: corev1.PodStatus{Phase: corev1.PodRunning, PodIP: "10.1.0.2", Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}}}
		objects = append(objects, service, pod)
	}
	core := kubernetesfake.NewSimpleClientset(objects...)
	kube := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(instance).WithObjects(instance).Build()
	fail := false
	calls := 0
	httpClient := &http.Client{Transport: serviceProbeTransport(func(req *http.Request) (*http.Response, error) {
		calls++
		if fail {
			return nil, fmt.Errorf("unavailable")
		}
		role := "search"
		if strings.HasPrefix(req.URL.Host, "catalog-index.") {
			role = "ingest"
		}
		body := fmt.Sprintf(`{"node":"id","name":"%s-pod","roles":["search","ingest"],"lease":{"valid_now":true,"expires_in_millis":60000,"ttl_millis":60000}}`, role)
		return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader(body)), Header: make(http.Header), Request: req}, nil
	})}
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	r := &InstanceReconciler{Client: kube, Core: core.CoreV1(), DevelopmentServiceProbeImage: image, ServiceProbeClient: httpClient, Now: func() time.Time { return now }}
	key := types.NamespacedName{Namespace: "n", Name: "catalog"}
	step := func() bool {
		t.Helper()
		if err := kube.Get(ctx, key, instance); err != nil {
			t.Fatal(err)
		}
		changed, err := r.sampleRoleServiceProbe(ctx, instance)
		if err != nil {
			t.Fatal(err)
		}
		return changed
	}
	if !step() || instance.Status.LastServiceProbe.Role != "ingest" || instance.Status.LastServiceProbe.PodUID != "pod-ingest" || instance.Status.LastServiceProbe.Result != "ServiceResponseObserved" {
		t.Fatal("ingest sample missing")
	}
	if step() || calls != 1 {
		t.Fatal("sample was not rate limited")
	}
	now = now.Add(time.Minute)
	if !step() || instance.Status.LastServiceProbe.Role != "search" || instance.Status.LastServiceProbe.PodUID != "pod-search" {
		t.Fatal("sample did not rotate")
	}
	fail = true
	now = now.Add(time.Minute)
	if !step() || instance.Status.LastServiceProbe.Result != "Unavailable" || instance.Status.LastServiceProbe.PodUID != "" || instance.Status.LastServiceProbe.ServiceUID != "" {
		t.Fatal("failed sample retained previous success")
	}
	instance.Spec.Security = &v1alpha1.SecurityConfig{Provider: "security"}
	if err := kube.Update(ctx, instance); err != nil {
		t.Fatal(err)
	}
	now = now.Add(time.Minute)
	if step() || calls != 3 {
		t.Fatal("unqualified security configuration was probed")
	}
}
