package plugins_test

import (
	"archive/zip"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	digest "github.com/opencontainers/go-digest"
	"github.com/opencontainers/image-spec/specs-go"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"oras.land/oras-go/v2/registry/remote"

	"github.com/huyz0/opensearch-serverless-operator/internal/plugins"
)

func TestFetchPluginVerifiesManifestAndLayer(t *testing.T) {
	blob := []byte("zip bytes")
	layer := ocispec.Descriptor{MediaType: "application/zip", Digest: digest.FromBytes(blob), Size: int64(len(blob))}
	manifest := ocispec.Manifest{Versioned: specs.Versioned{SchemaVersion: 2}, MediaType: ocispec.MediaTypeImageManifest, ArtifactType: plugins.PluginArtifactType, Layers: []ocispec.Descriptor{layer}}
	manifestBytes, err := json.Marshal(manifest)
	if err != nil {
		t.Fatal(err)
	}
	manifestDigest := digest.FromBytes(manifestBytes)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.URL.Path, "/manifests/") {
			w.Header().Set("Content-Type", ocispec.MediaTypeImageManifest)
			w.Header().Set("Docker-Content-Digest", manifestDigest.String())
			_, _ = w.Write(manifestBytes)
			return
		}
		if strings.Contains(r.URL.Path, "/blobs/") {
			_, _ = w.Write(blob)
			return
		}
		http.NotFound(w, r)
	}))
	defer server.Close()
	repo, err := remote.NewRepository(strings.TrimPrefix(server.URL, "http://") + "/plugins/example")
	if err != nil {
		t.Fatal(err)
	}
	repo.PlainHTTP = true
	var output bytes.Buffer
	if err := plugins.FetchPlugin(context.Background(), repo, manifestDigest.String(), &output, 1024); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(output.Bytes(), blob) {
		t.Fatal("downloaded layer differs")
	}
}

func TestStagePluginDownloadsAndQualifies(t *testing.T) {
	var zipped bytes.Buffer
	zipWriter := zip.NewWriter(&zipped)
	entry, err := zipWriter.Create("plugin-descriptor.properties")
	if err != nil {
		t.Fatal(err)
	}
	_, _ = entry.Write([]byte("name=example"))
	if err := zipWriter.Close(); err != nil {
		t.Fatal(err)
	}
	blob := zipped.Bytes()
	layer := ocispec.Descriptor{MediaType: "application/zip", Digest: digest.FromBytes(blob), Size: int64(len(blob))}
	manifestBytes, err := json.Marshal(ocispec.Manifest{Versioned: specs.Versioned{SchemaVersion: 2}, MediaType: ocispec.MediaTypeImageManifest, ArtifactType: plugins.PluginArtifactType, Layers: []ocispec.Descriptor{layer}})
	if err != nil {
		t.Fatal(err)
	}
	manifestDigest := digest.FromBytes(manifestBytes)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.URL.Path, "/manifests/") {
			w.Header().Set("Content-Type", ocispec.MediaTypeImageManifest)
			w.Header().Set("Docker-Content-Digest", manifestDigest.String())
			_, _ = w.Write(manifestBytes)
			return
		}
		_, _ = w.Write(blob)
	}))
	defer server.Close()
	repo, err := remote.NewRepository(strings.TrimPrefix(server.URL, "http://") + "/plugins/example")
	if err != nil {
		t.Fatal(err)
	}
	repo.PlainHTTP = true
	parent := t.TempDir()
	directory, err := plugins.StagePlugin(context.Background(), repo, manifestDigest.String(), parent, 1<<20, 10, func(directory string) error {
		contents, err := os.ReadFile(filepath.Join(directory, "plugin-descriptor.properties"))
		if err != nil || string(contents) != "name=example" {
			t.Fatalf("unexpected descriptor %q: %v", contents, err)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(directory); err != nil {
		t.Fatal(err)
	}
	entries, err := os.ReadDir(parent)
	if err != nil || len(entries) != 1 {
		t.Fatalf("temporary download not removed: %v, %v", entries, err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	canceledParent := t.TempDir()
	directory, err = plugins.StagePlugin(ctx, repo, manifestDigest.String(), canceledParent, 1<<20, 10, func(string) error {
		cancel()
		return nil
	})
	if directory != "" || !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled qualification returned directory %q and error %v", directory, err)
	}
	entries, err = os.ReadDir(canceledParent)
	if err != nil || len(entries) != 0 {
		t.Fatalf("canceled qualification left staging content: %v, %v", entries, err)
	}
}
