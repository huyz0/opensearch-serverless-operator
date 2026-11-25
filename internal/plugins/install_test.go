package plugins_test

import (
	"archive/zip"
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	digest "github.com/opencontainers/go-digest"
	"github.com/opencontainers/image-spec/specs-go"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"oras.land/oras-go/v2/registry"
	"oras.land/oras-go/v2/registry/remote"

	"github.com/huyz0/opensearch-serverless-operator/internal/plugins"
)

func TestInstallSetPublishesOnlyAfterAllBundlesValidate(t *testing.T) {
	var zipped bytes.Buffer
	writer := zip.NewWriter(&zipped)
	entry, err := writer.Create("plugin-descriptor.properties")
	if err != nil {
		t.Fatal(err)
	}
	_, _ = entry.Write([]byte("name=example\ndescription=test\nversion=1\nopensearch.version=3.9.0\njava.version=21\nclassname=example.Plugin\n"))
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	blob := zipped.Bytes()
	manifest, err := json.Marshal(ocispec.Manifest{Versioned: specs.Versioned{SchemaVersion: 2}, MediaType: ocispec.MediaTypeImageManifest, ArtifactType: plugins.PluginArtifactType, Layers: []ocispec.Descriptor{{MediaType: "application/zip", Digest: digest.FromBytes(blob), Size: int64(len(blob))}}})
	if err != nil {
		t.Fatal(err)
	}
	manifestDigest := digest.FromBytes(manifest)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.URL.Path, "/manifests/") {
			w.Header().Set("Content-Type", ocispec.MediaTypeImageManifest)
			w.Header().Set("Docker-Content-Digest", manifestDigest.String())
			_, _ = w.Write(manifest)
			return
		}
		_, _ = w.Write(blob)
	}))
	defer server.Close()
	ref := strings.TrimPrefix(server.URL, "http://") + "/plugins/example@" + manifestDigest.String()
	factory := func(ref registry.Reference) (*remote.Repository, error) {
		repo, err := remote.NewRepository(ref.Registry + "/" + ref.Repository)
		if err == nil {
			repo.PlainHTTP = true
		}
		return repo, err
	}
	parent := t.TempDir()
	output := filepath.Join(parent, "plugins")
	requested := []plugins.Installation{{Name: "example", Artifact: ref}}
	stale := filepath.Join(parent, ".plugin-set-12345")
	if err := os.MkdirAll(stale, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(stale, "partial"), []byte("partial"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := plugins.InstallSet(context.Background(), output, requested, map[string]bool{ref: true}, "3.9.0", "21", 1<<20, 10, factory); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(stale); !os.IsNotExist(err) {
		t.Fatalf("abandoned stage was retained: %v", err)
	}
	if err := os.Mkdir(stale, 0700); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(output, "example", "plugin-descriptor.properties")); err != nil {
		t.Fatal(err)
	}
	if err := plugins.InstallSet(context.Background(), output, requested, map[string]bool{ref: true}, "3.9.0", "21", 1<<20, 10, factory); err != nil {
		t.Fatalf("completed set must survive an init-container retry: %v", err)
	}
	if _, err := os.Stat(stale); !os.IsNotExist(err) {
		t.Fatalf("completed retry retained abandoned stage: %v", err)
	}
	if err := os.WriteFile(stale, []byte("unexpected"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := plugins.InstallSet(context.Background(), output, requested, map[string]bool{ref: true}, "3.9.0", "21", 1<<20, 10, factory); err == nil {
		t.Fatal("unexpected reserved stage file accepted")
	}
	if err := os.Remove(stale); err != nil {
		t.Fatal(err)
	}
	if err := plugins.InstallSet(context.Background(), output, requested, map[string]bool{ref: true}, "3.9.0", "22", 1<<20, 10, factory); err == nil {
		t.Fatal("completed set must not satisfy a changed compatibility policy")
	}
	failedOutput := filepath.Join(parent, "failed")
	requested = append(requested, plugins.Installation{Name: "different", Artifact: ref})
	if err := plugins.InstallSet(context.Background(), failedOutput, requested, map[string]bool{ref: true}, "3.9.0", "21", 1<<20, 10, factory); err == nil {
		t.Fatal("descriptor mismatch must fail the complete installation")
	}
	if _, err := os.Stat(failedOutput); !os.IsNotExist(err) {
		t.Fatalf("failed installation published output: %v", err)
	}
	entries, err := os.ReadDir(parent)
	if err != nil || len(entries) != 1 || entries[0].Name() != "plugins" {
		t.Fatalf("failed installation left staging files: %v, %v", entries, err)
	}
	if err := plugins.InstallSet(context.Background(), failedOutput, requested[:1], nil, "3.9.0", "21", 1<<20, 10, factory); err == nil {
		t.Fatal("unapproved artifact must fail")
	}
}
