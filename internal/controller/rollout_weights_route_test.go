package controller

import (
	"context"
	"errors"
	"maps"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/huyz0/opensearch-serverless-operator/internal/rollout"
)

func weightedHTTPRouteFixture(namespace string) (*unstructured.Unstructured, weightRouteBinding) {
	route := &unstructured.Unstructured{Object: map[string]any{
		"metadata": map[string]any{"name": "catalog-search", "namespace": namespace},
		"spec": map[string]any{
			"parentRefs": []any{map[string]any{"name": "gateway", "sectionName": "https"}},
			"hostnames":  []any{"search.example.com"},
			"rules": []any{map[string]any{"backendRefs": []any{
				map[string]any{"name": "search-blue", "port": int64(9200), "weight": int64(100)},
				map[string]any{"name": "search-green", "port": int64(9200), "weight": int64(0)},
			}}},
		},
	}}
	route.SetGroupVersionKind(httpRouteGVK)
	return route, weightRouteBinding{
		Key: client.ObjectKeyFromObject(route), InstanceUID: "instance", Role: "search", ServicePort: 9200,
		ServiceNames: map[string]string{"blue": "search-blue", "green": "search-green"},
	}
}

type concurrentWeightRouteWriter struct {
	client.Writer
	beforePatch func()
}

func (w concurrentWeightRouteWriter) Patch(ctx context.Context, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
	w.beforePatch()
	return w.Writer.Patch(ctx, obj, patch, opts...)
}

func exerciseWeightRoutePatch(t *testing.T, c client.Client, namespace string) {
	t.Helper()
	ctx := context.Background()
	route, binding := weightedHTTPRouteFixture(namespace)
	if err := c.Create(ctx, route); err != nil {
		t.Fatal(err)
	}
	if route.GetUID() == "" {
		route.SetUID("route-uid") // The fake client does not allocate UIDs.
		if err := c.Update(ctx, route); err != nil {
			t.Fatal(err)
		}
	}
	binding.UID = route.GetUID()
	observed, digest, err := inspectWeightRoute(route, binding)
	if err != nil {
		t.Fatal(err)
	}
	binding.SpecWithoutWeights = digest
	action := rollout.WeightAction{
		Kind: rollout.PatchRouteWeights, RouteUID: observed.RouteUID, ResourceVersion: observed.ResourceVersion,
		From: observed.Weights, To: map[string]int{"blue": 99, "green": 1},
	}
	if err := applyWeightRoute(ctx, c, c, binding, action, time.Now, func() error { return errRolloutPlanChanged }); !errors.Is(err, errRolloutPlanChanged) {
		t.Fatalf("expired authorization did not reject weighted patch: %v", err)
	}
	if live, err := readWeightRoute(ctx, c, binding, time.Now); err != nil || live.ResourceVersion != action.ResourceVersion {
		t.Fatalf("guarded refusal changed the HTTPRoute: %v", err)
	}
	if err := applyWeightRoute(ctx, c, c, binding, action, time.Now, nil); err != nil {
		t.Fatalf("apply weighted route: %v", err)
	}
	live, err := readWeightRoute(ctx, c, binding, time.Now)
	if err != nil || !maps.Equal(live.Weights, action.To) {
		t.Fatalf("live weights = %v, err = %v", live.Weights, err)
	}
	action.ResourceVersion, action.From = live.ResourceVersion, live.Weights
	action.To = map[string]int{"blue": 0, "green": 100}
	writer := concurrentWeightRouteWriter{Writer: c, beforePatch: func() {
		modified := live.Object.DeepCopy()
		if err := unstructured.SetNestedStringSlice(modified.Object, []string{"different.example.com"}, "spec", "hostnames"); err != nil {
			t.Fatal(err)
		}
		if err := c.Update(ctx, modified); err != nil {
			t.Fatal(err)
		}
	}}
	if err := applyWeightRoute(ctx, c, writer, binding, action, time.Now, nil); err == nil {
		t.Fatal("conditional patch overwrote a concurrent route edit")
	}
	if _, err := readWeightRoute(ctx, c, binding, time.Now); err == nil {
		t.Fatal("route with changed hostname was still considered bound")
	}
	if err := c.Get(ctx, binding.Key, route); err != nil {
		t.Fatal(err)
	}
	observed, _, err = inspectWeightRoute(route, binding)
	if err != nil || !maps.Equal(observed.Weights, action.From) {
		t.Fatalf("concurrent edit did not preserve prior weights: %v, %v", observed.Weights, err)
	}
}

func TestWeightRouteConditionalPatch(t *testing.T) {
	exerciseWeightRoutePatch(t, fake.NewClientBuilder().Build(), "test")
}
