package runtimeconfig_test

import (
	"encoding/json"
	"reflect"
	"testing"

	"github.com/huyz0/opensearch-serverless-operator/api/v1alpha1"
	"github.com/huyz0/opensearch-serverless-operator/internal/runtimeconfig"
)

func TestResolveLiteralArgumentsAndEmptyOverride(t *testing.T) {
	commonOptions := []string{"-XX:+ExitOnOutOfMemoryError"}
	emptyOptions := []string{}
	common := &v1alpha1.RuntimeConfig{Settings: map[string]string{"cluster.name": "literal $(whoami)"}, JVM: &v1alpha1.JVMConfig{Options: &commonOptions}}
	role := &v1alpha1.RuntimeConfig{JVM: &v1alpha1.JVMConfig{Options: &emptyOptions}}
	catalog := runtimeconfig.Catalog{Settings: map[string]bool{"cluster.name": false}, JVMOptions: map[string]bool{"-XX:+ExitOnOutOfMemoryError": true}}
	args, err := runtimeconfig.Resolve(common, role, catalog)
	if err != nil || !reflect.DeepEqual(args, []string{"-Dcluster.name=literal $(whoami)"}) {
		t.Fatalf("unexpected arguments %q: %v", args, err)
	}
	role.Settings = map[string]string{"cluster.name": "other"}
	if _, err := runtimeconfig.Resolve(common, role, catalog); err == nil {
		t.Fatal("role override of a common-only setting must fail")
	}
}

func TestEmptyOptionsSurviveJSONRoundTrip(t *testing.T) {
	options := []string{}
	original := v1alpha1.RuntimeConfig{JVM: &v1alpha1.JVMConfig{Options: &options}}
	encoded, err := json.Marshal(original)
	if err != nil {
		t.Fatal(err)
	}
	var decoded v1alpha1.RuntimeConfig
	if err := json.Unmarshal(encoded, &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded.JVM == nil || decoded.JVM.Options == nil || len(*decoded.JVM.Options) != 0 {
		t.Fatalf("empty override was lost: %s", encoded)
	}
}
