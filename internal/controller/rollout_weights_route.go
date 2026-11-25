package controller

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"maps"
	"slices"
	"time"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/huyz0/opensearch-serverless-operator/internal/rollout"
)

var httpRouteGVK = schema.GroupVersionKind{Group: "gateway.networking.k8s.io", Version: "v1", Kind: "HTTPRoute"}

// weightRouteBinding limits a weight patch to one platform-approved role
// route. The platform retains control of the other route fields.
type weightRouteBinding struct {
	Key                                   client.ObjectKey
	UID                                   types.UID
	InstanceUID, Role, SpecWithoutWeights string
	ServiceNames                          map[string]string
	ServicePort                           int64
}

type observedWeightRoute struct {
	rollout.WeightRouteObservation
	Object       *unstructured.Unstructured
	BackendIndex map[string]int
}

func readWeightRoute(ctx context.Context, reader client.Reader, binding weightRouteBinding, now func() time.Time) (observedWeightRoute, error) {
	route := &unstructured.Unstructured{}
	route.SetGroupVersionKind(httpRouteGVK)
	if err := reader.Get(ctx, binding.Key, route); err != nil {
		return observedWeightRoute{}, fmt.Errorf("read weighted HTTPRoute: %w", err)
	}
	observed, digest, err := inspectWeightRoute(route, binding)
	if err != nil {
		return observedWeightRoute{}, err
	}
	if binding.SpecWithoutWeights == "" || binding.SpecWithoutWeights != digest {
		return observedWeightRoute{}, fmt.Errorf("weighted HTTPRoute changed outside the authorized weight fields")
	}
	observed.ScopeVerified = true
	observed.ObservedAt = now()
	return observed, nil
}

func inspectWeightRoute(route *unstructured.Unstructured, binding weightRouteBinding) (observedWeightRoute, string, error) {
	if binding.Key.Namespace == "" || binding.Key.Name == "" || binding.InstanceUID == "" ||
		binding.UID == "" || route.GetUID() != binding.UID || route.GetResourceVersion() == "" ||
		client.ObjectKeyFromObject(route) != binding.Key || route.GetDeletionTimestamp() != nil ||
		(binding.Role != "search" && binding.Role != "ingest") ||
		binding.ServicePort <= 0 || binding.ServicePort > 65535 || len(binding.ServiceNames) < 2 {
		return observedWeightRoute{}, "", fmt.Errorf("live weighted route binding is incomplete or changed")
	}
	fleetByService := make(map[string]string, len(binding.ServiceNames))
	for fleet, service := range binding.ServiceNames {
		if fleet == "" || service == "" || fleetByService[service] != "" {
			return observedWeightRoute{}, "", fmt.Errorf("weighted HTTPRoute needs distinct per-fleet Services")
		}
		fleetByService[service] = fleet
	}
	// NestedMap returns a copy, leaving the observed route untouched.
	spec, found, err := unstructured.NestedMap(route.Object, "spec")
	if err != nil || !found {
		return observedWeightRoute{}, "", fmt.Errorf("weighted HTTPRoute needs a spec")
	}
	rules, _ := spec["rules"].([]any)
	if len(rules) != 1 {
		return observedWeightRoute{}, "", fmt.Errorf("weighted HTTPRoute must have one dedicated role rule")
	}
	rule, _ := rules[0].(map[string]any)
	backends, _ := rule["backendRefs"].([]any)
	if len(backends) != len(binding.ServiceNames) {
		return observedWeightRoute{}, "", fmt.Errorf("weighted HTTPRoute backend count changed")
	}
	observed := observedWeightRoute{
		WeightRouteObservation: rollout.WeightRouteObservation{
			InstanceUID: binding.InstanceUID, Role: binding.Role, RouteUID: string(route.GetUID()),
			ResourceVersion: route.GetResourceVersion(), Weights: make(map[string]int, len(backends)),
		},
		Object: route, BackendIndex: make(map[string]int, len(backends)),
	}
	total := int64(0)
	for index, item := range backends {
		backend, _ := item.(map[string]any)
		name, _ := backend["name"].(string)
		fleet := fleetByService[name]
		_, duplicate := observed.Weights[fleet]
		group, kind, namespace := backend["group"], backend["kind"], backend["namespace"]
		port, _ := backend["port"].(int64)
		weight, explicitWeight := backend["weight"].(int64)
		if fleet == "" || duplicate || (group != nil && group != "") || (kind != nil && kind != "Service") ||
			(namespace != nil && namespace != binding.Key.Namespace) || port != binding.ServicePort ||
			!explicitWeight || weight < 0 || weight > 100 {
			return observedWeightRoute{}, "", fmt.Errorf("weighted HTTPRoute has an invalid backend reference at index %d", index)
		}
		observed.BackendIndex[fleet] = index
		observed.Weights[fleet] = int(weight)
		total += weight
		delete(backend, "weight")
	}
	if total != 100 {
		return observedWeightRoute{}, "", fmt.Errorf("weighted HTTPRoute backend weights must total 100")
	}
	encoded, err := json.Marshal(spec)
	if err != nil {
		return observedWeightRoute{}, "", fmt.Errorf("encode weighted HTTPRoute scope: %w", err)
	}
	return observed, fmt.Sprintf("%x", sha256.Sum256(encoded)), nil
}

// applyWeightRoute reads the API directly before patching. The caller must
// also recheck the operation claim and planner evidence before the write.
func applyWeightRoute(ctx context.Context, reader client.Reader, writer client.Writer, binding weightRouteBinding, action rollout.WeightAction, now func() time.Time, authorize func() error) error {
	if action.Kind != rollout.PatchRouteWeights || action.RouteUID != string(binding.UID) || action.ResourceVersion == "" {
		return fmt.Errorf("a planned weight patch with a bound route version is required")
	}
	live, err := readWeightRoute(ctx, reader, binding, now)
	if err != nil {
		return err
	}
	if live.ResourceVersion != action.ResourceVersion || !maps.Equal(live.Weights, action.From) ||
		len(action.To) != len(live.BackendIndex) || maps.Equal(action.From, action.To) {
		return fmt.Errorf("weighted HTTPRoute changed after the plan")
	}
	type patchOperation struct {
		Op    string `json:"op"`
		Path  string `json:"path"`
		Value any    `json:"value"`
	}
	patch := []patchOperation{
		{Op: "test", Path: "/metadata/uid", Value: live.RouteUID},
		{Op: "test", Path: "/metadata/resourceVersion", Value: live.ResourceVersion},
	}
	total := 0
	for _, fleet := range slices.Sorted(maps.Keys(live.BackendIndex)) {
		weight, found := action.To[fleet]
		if !found || weight < 0 || weight > 100 {
			return fmt.Errorf("planned weighted HTTPRoute backend is invalid")
		}
		total += weight
		if weight != live.Weights[fleet] {
			patch = append(patch, patchOperation{Op: "replace", Path: fmt.Sprintf("/spec/rules/0/backendRefs/%d/weight", live.BackendIndex[fleet]), Value: weight})
		}
	}
	if total != 100 {
		return fmt.Errorf("planned weighted HTTPRoute weights must total 100")
	}
	body, err := json.Marshal(patch)
	if err != nil {
		return err
	}
	if authorize != nil {
		if err := authorize(); err != nil {
			return err
		}
	}
	return writer.Patch(ctx, live.Object, client.RawPatch(types.JSONPatchType, body))
}
