package java_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/huyz0/opensearch-serverless-operator/api/v1alpha1"
	"github.com/huyz0/opensearch-serverless-operator/internal/java"
)

func TestApplyClusterSettings(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPut || r.URL.Path != "/_cluster/settings" {
			t.Errorf("unexpected request %s %s", r.Method, r.URL.Path)
		}
		var payload struct {
			Persistent map[string]*string `json:"persistent"`
		}
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil || payload.Persistent["set"] == nil || *payload.Persistent["set"] != "value" {
			t.Errorf("unexpected request payload: %+v, %v", payload, err)
		}
		if value, exists := payload.Persistent["remove"]; !exists || value != nil {
			t.Error("removed key must be encoded as null")
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()
	patch := v1alpha1.ClusterSettingsPatch{Set: map[string]string{"set": "value"}, Remove: []string{"remove"}}
	if err := java.ApplyClusterSettings(context.Background(), server.Client(), server.URL, patch, map[string]bool{"set": true, "remove": true}); err != nil {
		t.Fatal(err)
	}
	if err := java.ApplyClusterSettings(context.Background(), server.Client(), server.URL, patch, map[string]bool{"set": true}); err == nil {
		t.Fatal("unqualified key must be rejected")
	}
}

func TestGetPersistentSettings(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Query().Get("flat_settings") != "true" {
			t.Errorf("expected flat settings read, got %s", r.URL.String())
		}
		_, _ = w.Write([]byte(`{"persistent":{"serverless.example":"on"},"transient":{}}`))
	}))
	defer server.Close()
	settings, err := java.GetPersistentSettings(context.Background(), server.Client(), server.URL)
	if err != nil || settings["serverless.example"] != "on" {
		t.Fatalf("unexpected settings %v: %v", settings, err)
	}
}

func TestGetPersistentSettingsRejectsAmbiguousReadBack(t *testing.T) {
	for _, test := range []struct {
		name, body string
	}{
		{"null value", `{"persistent":{"serverless.bucket":null}}`},
		{"duplicate key", `{"persistent":{"serverless.bucket":"auto","serverless.bucket":"off"}}`},
		{"duplicate projection", `{"persistent":{"serverless.bucket":"auto"},"persistent":{}}`},
		{"missing projection", `{"transient":{}}`},
		{"trailing response", `{"persistent":{}} {}`},
	} {
		t.Run(test.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				_, _ = w.Write([]byte(test.body))
			}))
			defer server.Close()
			if settings, err := java.GetPersistentSettings(context.Background(), server.Client(), server.URL); settings != nil || err == nil {
				t.Fatalf("ambiguous settings accepted: %v, %v", settings, err)
			}
		})
	}
}

func TestSettingsPatchApplied(t *testing.T) {
	patch := v1alpha1.ClusterSettingsPatch{Set: map[string]string{"a": "new"}, Remove: []string{"b"}}
	if !java.SettingsPatchApplied(map[string]string{"a": "new", "unrelated": "kept"}, patch) {
		t.Fatal("matching patch should be confirmed")
	}
	if java.SettingsPatchApplied(map[string]string{"a": "new", "b": "old"}, patch) {
		t.Fatal("a key still present after removal must not be confirmed")
	}
	if java.SettingsPatchApplied(map[string]string{"a": "old"}, patch) {
		t.Fatal("a different value must not be confirmed")
	}
}
