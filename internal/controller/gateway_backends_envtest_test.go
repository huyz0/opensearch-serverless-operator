//go:build envtest

package controller

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/huyz0/opensearch-serverless-operator/api/v1alpha1"
	"github.com/huyz0/opensearch-serverless-operator/internal/workload"
)

func testGatewayBackendLifecycle(t *testing.T, config *rest.Config, c client.Client, core kubernetes.Interface) {
	ctx := context.Background()
	instance := &v1alpha1.OpenSearchServerless{
		ObjectMeta: metav1.ObjectMeta{Name: "weighted", Namespace: "rollout-test"},
		Spec: v1alpha1.OpenSearchServerlessSpec{
			Image: "example/image@sha256:" + strings.Repeat("a", 64), ServiceAccountName: "runtime",
			Storage: v1alpha1.Storage{Type: "s3", Bucket: "bucket", Region: "region"},
			Pools:   map[string]v1alpha1.Pool{"search": {Zones: map[string]v1alpha1.Zone{"a": {Replicas: 1}}}},
			Rollout: &v1alpha1.Rollout{
				Strategy: "BlueGreen", GatewayBackends: true,
				Candidate: &v1alpha1.FleetCandidate{Fleet: "green", Image: "example/image@sha256:" + strings.Repeat("a", 64)},
			},
		},
	}
	if err := c.Create(ctx, instance); err != nil {
		t.Fatal(err)
	}
	key := client.ObjectKeyFromObject(instance)
	services := core.CoreV1().Services(instance.Namespace)
	r := &InstanceReconciler{Client: c, APIReader: c, Core: core.CoreV1()}
	reconcile := func() {
		t.Helper()
		if _, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: key}); err != nil {
			t.Fatal(err)
		}
		if err := c.Get(ctx, key, instance); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 20 && len(instance.Status.GatewayBackends) == 0; i++ {
		reconcile()
	}
	if len(instance.Status.GatewayBackends) != 4 {
		t.Fatalf("expected four per-role backend intents, got %#v", instance.Status.GatewayBackends)
	}
	for _, intent := range instance.Status.GatewayBackends {
		if intent.ServiceUID != "" {
			t.Fatalf("unexpected backend UID before create: %#v", intent)
		}
		if _, err := services.Get(ctx, intent.Name, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
			t.Fatalf("backend existed before its intent was observed: %v", err)
		}
	}
	stable, err := services.Get(ctx, "weighted-search", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}

	// Lose one successful create response and recover by reading the Service
	// through a new reconciler.
	missingResponse := instance.Status.GatewayBackends["ingest/green"]
	faultConfig := rest.CopyConfig(config)
	lost := false
	faultConfig.Wrap(func(next http.RoundTripper) http.RoundTripper {
		return candidateRaceTransport(func(req *http.Request) (*http.Response, error) {
			if !lost && req.Method == http.MethodPost && strings.HasSuffix(req.URL.Path, "/services") {
				body, err := io.ReadAll(req.Body)
				if err != nil {
					return nil, err
				}
				req.Body = io.NopCloser(bytes.NewReader(body))
				response, err := next.RoundTrip(req)
				if err == nil && response.StatusCode == http.StatusCreated && bytes.Contains(body, []byte(missingResponse.Name)) {
					lost = true
					response.Body.Close()
					return nil, fmt.Errorf("simulated lost backend create response")
				}
				return response, err
			}
			return next.RoundTrip(req)
		})
	})
	faultedCore, err := kubernetes.NewForConfig(faultConfig)
	if err != nil {
		t.Fatal(err)
	}
	r.Core = faultedCore.CoreV1()
	reconcile()
	created, err := services.Get(ctx, missingResponse.Name, metav1.GetOptions{})
	if err != nil || !lost || instance.Status.GatewayBackends["ingest/green"].ServiceUID != "" {
		t.Fatalf("lost response was not exercised: lost=%v, err=%v", lost, err)
	}
	r = &InstanceReconciler{Client: c, APIReader: c, Core: core.CoreV1()}
	for i := 0; i < 20 && !meta.IsStatusConditionTrue(instance.Status.Conditions, gatewayBackendsCondition); i++ {
		reconcile()
	}
	if !meta.IsStatusConditionTrue(instance.Status.Conditions, gatewayBackendsCondition) ||
		instance.Status.GatewayBackends["ingest/green"].ServiceUID != string(created.UID) {
		t.Fatalf("backend identities were not recovered: %#v", instance.Status)
	}
	for _, intent := range instance.Status.GatewayBackends {
		service, err := readGatewayBackend(ctx, core.CoreV1(), instance, intent)
		if err != nil || service.Spec.Selector[workload.RoleLabel] != intent.Role || service.Spec.Selector[workload.FleetLabel] != intent.Fleet {
			t.Fatalf("unexpected backend for %s/%s: %v", intent.Role, intent.Fleet, err)
		}
	}

	// Disabling backend provisioning preserves the old addresses for any
	// platform-managed HTTPRoute that still references them.
	instance.Spec.Rollout.GatewayBackends = false
	if err := c.Update(ctx, instance); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 10; i++ {
		reconcile()
		condition := meta.FindStatusCondition(instance.Status.Conditions, gatewayBackendsCondition)
		if condition != nil && condition.Reason == "BackendsDisabled" {
			break
		}
	}
	retained, err := services.Get(ctx, missingResponse.Name, metav1.GetOptions{})
	if err != nil || retained.UID != created.UID || retained.Spec.ClusterIP != created.Spec.ClusterIP {
		t.Fatalf("a disabled backend lost its identity: %v", err)
	}
	if err := services.Delete(ctx, retained.Name, metav1.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	instance.Spec.Rollout.GatewayBackends = true
	if err := c.Update(ctx, instance); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 15; i++ {
		reconcile()
	}
	if condition := meta.FindStatusCondition(instance.Status.Conditions, gatewayBackendsCondition); condition == nil || condition.Reason != "BackendUnavailable" {
		t.Fatalf("missing pinned Service did not fail closed: %#v", condition)
	}
	if _, err := services.Get(ctx, retained.Name, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Fatalf("deleted pinned Service was recreated: %v", err)
	}
	replacement, err := workload.GatewayBackendService(instance, "ingest", "green")
	if err != nil {
		t.Fatal(err)
	}
	replacement, err = services.Create(ctx, replacement, metav1.CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	reconcile()
	if condition := meta.FindStatusCondition(instance.Status.Conditions, gatewayBackendsCondition); condition == nil ||
		condition.Reason != "BackendUnavailable" || replacement.UID == created.UID ||
		instance.Status.GatewayBackends["ingest/green"].ServiceUID != string(created.UID) {
		t.Fatalf("a replacement took over the original pin: condition=%#v, backends=%#v", condition, instance.Status.GatewayBackends)
	}
	after, err := services.Get(ctx, stable.Name, metav1.GetOptions{})
	if err != nil || after.UID != stable.UID || after.Spec.ClusterIP != stable.Spec.ClusterIP || after.Spec.Selector[workload.FleetLabel] != "initial" {
		t.Fatalf("weighted backend reconciliation changed stable client routing: %v", err)
	}
}
