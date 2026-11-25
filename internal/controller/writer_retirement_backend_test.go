package controller

import (
	"encoding/base64"
	"strings"
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

func TestDevelopmentWriterRetirementRequiresAnExactRootHTTPSBinding(t *testing.T) {
	config := DevelopmentWriterRetirementConfig{
		Instance: "catalog", HTTPRoute: "catalog-index", GatewayNamespace: "runtime", GatewayName: "envoy",
		GatewayListener: "https", FrontendNamespace: "envoy-system", FrontendService: "catalog-envoy",
		EdgeCredential: "edge", ReaderCredential: "reader", PreparerCredential: "preparer",
		PlatformKey: base64.StdEncoding.EncodeToString(make([]byte, 32)),
	}
	runtime := &InstanceReconciler{DevelopmentObserveEndpoints: true, DevelopmentServiceProbeImage: "example/java@sha256:" + strings.Repeat("a", 64)}
	for _, tc := range []struct {
		url string
		ok  bool
	}{
		{"https://index.example.com", true},
		{"https://index.example.com:443/", true},
		{"https://index.example.com:8443", false},
		{"https://index.example.com/path", false},
		{"http://index.example.com", false},
		{"https://index.example.com?unsafe", false},
	} {
		config.IndexURL = tc.url
		var reconciler OperationReconciler
		err := reconciler.ConfigureDevelopmentWriterRetirement(runtime, config)
		if (err == nil) != tc.ok || (reconciler.writerRetirement != nil) != tc.ok {
			t.Errorf("configure %s = %v", tc.url, err)
		}
	}
}

func TestWriterRouteRequiresCurrentEnvoyListenerAndParentStatus(t *testing.T) {
	conditions := func(names ...string) []any {
		result := make([]any, 0, len(names))
		for _, name := range names {
			result = append(result, map[string]any{"type": name, "observedGeneration": int64(2), "status": "True"})
		}
		return result
	}
	gateway := &unstructured.Unstructured{Object: map[string]any{"status": map[string]any{
		"conditions": conditions("Accepted", "Programmed"),
		"listeners":  []any{map[string]any{"name": "https", "conditions": conditions("Accepted", "Programmed", "ResolvedRefs")}},
	}}}
	gateway.SetName("gateway")
	gateway.SetNamespace("app")
	gateway.SetGeneration(2)
	parent := map[string]any{
		"conditions":     conditions("Accepted", "ResolvedRefs"),
		"controllerName": "gateway.envoyproxy.io/gatewayclass-controller",
		"parentRef":      map[string]any{"name": "gateway", "sectionName": "https"},
	}
	route := &unstructured.Unstructured{Object: map[string]any{"status": map[string]any{"parents": []any{parent}}}}
	route.SetNamespace("app")
	route.SetGeneration(2)
	if !writerRouteAccepted(route, gateway, "https") {
		t.Fatal("current Envoy route and listener should qualify")
	}
	route.SetGeneration(3)
	if writerRouteAccepted(route, gateway, "https") {
		t.Fatal("stale route status qualified a new generation")
	}
	route.SetGeneration(2)
	gateway.SetGeneration(3)
	if writerRouteAccepted(route, gateway, "https") {
		t.Fatal("stale Gateway status qualified a new generation")
	}
	gateway.SetGeneration(2)
	parent["controllerName"] = "other"
	if writerRouteAccepted(route, gateway, "https") {
		t.Fatal("another controller qualified the Envoy route")
	}
}
