package plugins_test

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/huyz0/opensearch-serverless-operator/api/v1alpha1"
	"github.com/huyz0/opensearch-serverless-operator/internal/plugins"
)

func TestResolveRolePinsApprovedRoleSpecificPlugins(t *testing.T) {
	a := "registry.example/team/a@sha256:" + strings.Repeat("a", 64)
	b := "registry.example/team/b@sha256:" + strings.Repeat("b", 64)
	policy, err := json.Marshal(plugins.Policy{ApprovedArtifacts: []string{a, b}, OpenSearchVersion: "3.0", JavaVersion: "21", MaxBytes: 1024, MaxFiles: 10})
	if err != nil {
		t.Fatal(err)
	}
	selected := []v1alpha1.Plugin{{Name: "b", Artifact: b, Roles: []string{"search"}}, {Name: "a", Artifact: a, RegistryCredentialsRef: &v1alpha1.LocalReference{Name: "pull"}}}
	search, err := plugins.ResolveRole(selected, "search", policy)
	if err != nil || search == nil || search.CredentialsSecret != "pull" || string(search.RequestJSON) != `[{"name":"a","artifact":"`+a+`"},{"name":"b","artifact":"`+b+`"}]` {
		t.Fatalf("unexpected search selection: %#v %v", search, err)
	}
	selected[0], selected[1] = selected[1], selected[0]
	reordered, err := plugins.ResolveRole(selected, "search", policy)
	if err != nil || reordered.RequestSHA256 != search.RequestSHA256 || reordered.PolicySHA256 != search.PolicySHA256 {
		t.Fatalf("resolution is not deterministic: %#v %v", reordered, err)
	}
	ingest, err := plugins.ResolveRole(selected, "ingest", policy)
	if err != nil || strings.Contains(string(ingest.RequestJSON), `"name":"b"`) {
		t.Fatalf("role-specific plugin was included: %#v %v", ingest, err)
	}
	selected[1].Artifact = "registry.example/team/b:latest"
	if _, err := plugins.ResolveRole(selected, "search", policy); err == nil {
		t.Fatal("tagged artifact accepted")
	}
	selected[1].Artifact = b
	selected[1].RegistryCredentialsRef = &v1alpha1.LocalReference{Name: "other"}
	if _, err := plugins.ResolveRole(selected, "search", policy); err == nil {
		t.Fatal("conflicting credential sources accepted")
	}
	selected[1].RegistryCredentialsRef = nil
	selected[1].Name = "repository-s3"
	if _, err := plugins.ResolveRole(selected, "search", policy); err == nil {
		t.Fatal("bundled repository-s3 accepted")
	}
	if _, err := plugins.ResolveRole(nil, "search", []byte(`{"unknown":true}`)); err == nil {
		t.Fatal("invalid policy accepted")
	}
}
