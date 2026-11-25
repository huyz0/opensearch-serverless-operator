package plugins_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/huyz0/opensearch-serverless-operator/internal/plugins"
)

func TestCheckDescriptor(t *testing.T) {
	directory := t.TempDir()
	descriptor := "name=example\ndescription=test\nversion=1\nopensearch.version=3.9.0\njava.version=21\nclassname=example.Plugin\n"
	if err := os.WriteFile(filepath.Join(directory, "plugin-descriptor.properties"), []byte(descriptor), 0600); err != nil {
		t.Fatal(err)
	}
	if err := plugins.CheckDescriptor(directory, "example"); err != nil {
		t.Fatal(err)
	}
	if err := plugins.CheckDescriptor(directory, "other"); err == nil {
		t.Fatal("name mismatch must fail")
	}
}

func TestCheckExactCompatibilityReadsLiteralJavaProperties(t *testing.T) {
	t.Setenv("PLUGIN_TEST_JAVA_VERSION", "21")
	descriptor := "name=example\ndescription=Example ${catalog}\nversion=1\nopensearch.version=3.9.0\njava.version=21\nclassname=example.Plugin\n"
	for _, test := range []struct {
		name       string
		descriptor string
		wantError  string
	}{
		{name: "literal description", descriptor: descriptor},
		{name: "literal version", descriptor: strings.Replace(descriptor, "java.version=21", "java.version=${PLUGIN_TEST_JAVA_VERSION}", 1), wantError: "qualified versions"},
		{name: "unknown key", descriptor: descriptor + "java.versoin=21\n", wantError: "unsupported plugin descriptor property"},
		{name: "extended plugin", descriptor: descriptor + "extended.plugins=analysis\n", wantError: "unsupported dependencies"},
		{name: "qualified layout", descriptor: descriptor + "custom.foldername=example\nhas.native.controller=false\n"},
	} {
		t.Run(test.name, func(t *testing.T) {
			directory := t.TempDir()
			if err := os.WriteFile(filepath.Join(directory, "plugin-descriptor.properties"), []byte(test.descriptor), 0600); err != nil {
				t.Fatal(err)
			}
			err := plugins.CheckExactCompatibility(directory, "example", "3.9.0", "21")
			if test.wantError == "" {
				if err != nil {
					t.Fatal(err)
				}
			} else if err == nil || !strings.Contains(err.Error(), test.wantError) {
				t.Fatalf("want error containing %q, got %v", test.wantError, err)
			}
		})
	}
}
