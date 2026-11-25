package rollout

import (
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

func TestBindAuthenticatedProbeURL(t *testing.T) {
	route := &unstructured.Unstructured{Object: map[string]any{"spec": map[string]any{
		"hostnames": []any{"search.example.com"},
		"rules": []any{map[string]any{"matches": []any{map[string]any{
			"method": "GET", "path": map[string]any{"type": "PathPrefix", "value": "/search"},
		}}}},
	}}}
	base, err := BindAuthenticatedProbeURL("https://SEARCH.example.com/search/", route)
	if err != nil || base != "https://search.example.com/search" {
		t.Fatalf("probe base = %q, %v", base, err)
	}
	for _, raw := range []string{
		"https://index.example.com/search", "https://search.example.com/search-other",
		"https://search.example.com/search/../index", "https://search.example.com/search?alternate", "http://search.example.com/search",
	} {
		if _, err := BindAuthenticatedProbeURL(raw, route); err == nil {
			t.Errorf("accepted unbound probe URL %q", raw)
		}
	}
}
