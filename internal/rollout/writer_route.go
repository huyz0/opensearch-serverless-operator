package rollout

import "k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

// WriterRouteUsesService verifies the catch-all stable index route. The
// Gateway API materializes an omitted match as a root PathPrefix.
func WriterRouteUsesService(route *unstructured.Unstructured, service string) bool {
	rules, _, err := unstructured.NestedSlice(route.Object, "spec", "rules")
	if err != nil || len(rules) != 1 {
		return false
	}
	rule, _ := rules[0].(map[string]any)
	refs, _, refsErr := unstructured.NestedSlice(rule, "backendRefs")
	filters, _, filtersErr := unstructured.NestedSlice(rule, "filters")
	matches, _, matchesErr := unstructured.NestedSlice(rule, "matches")
	if refsErr != nil || filtersErr != nil || matchesErr != nil || len(refs) != 1 || len(filters) != 0 || len(matches) > 1 {
		return false
	}
	if len(matches) == 1 {
		match, _ := matches[0].(map[string]any)
		path, _ := match["path"].(map[string]any)
		if len(match) != 1 || len(path) != 2 || path["type"] != "PathPrefix" || path["value"] != "/" {
			return false
		}
	}
	backend, _ := refs[0].(map[string]any)
	weight, hasWeight, err := unstructured.NestedInt64(backend, "weight")
	return err == nil && (!hasWeight || weight > 0) && backend["name"] == service && backend["port"] == int64(9200) &&
		(backend["group"] == nil || backend["group"] == "") && (backend["kind"] == nil || backend["kind"] == "Service") &&
		(backend["namespace"] == nil || backend["namespace"] == route.GetNamespace()) && backend["filters"] == nil
}
