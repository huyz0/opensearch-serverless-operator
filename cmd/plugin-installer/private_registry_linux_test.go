//go:build linux

package main

import (
	"archive/zip"
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"encoding/pem"
	"flag"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/huyz0/opensearch-serverless-operator/internal/plugins"
	digest "github.com/opencontainers/go-digest"
	"github.com/opencontainers/image-spec/specs-go"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
)

// The child gets a fresh system certificate pool and exercises the actual CLI
// parsing and HTTPS transport, including the normal registry credential lookup.
func TestInstallerCLIChild(t *testing.T) {
	encoded := os.Getenv("PLUGIN_TEST_ARGS")
	if encoded == "" {
		return
	}
	var args []string
	if err := json.Unmarshal([]byte(encoded), &args); err != nil {
		t.Fatal(err)
	}
	flag.CommandLine = flag.NewFlagSet("plugin-installer", flag.ContinueOnError)
	os.Args = append([]string{"plugin-installer"}, args...)
	if err := run(); err != nil {
		t.Fatal(err)
	}
}

func TestInstallerCLIPrivateHTTPSRegistry(t *testing.T) {
	var archive bytes.Buffer
	zw := zip.NewWriter(&archive)
	entry, err := zw.Create("plugin-descriptor.properties")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := entry.Write([]byte("name=example\ndescription=test\nversion=1\nopensearch.version=3.9.0\njava.version=21\nclassname=example.Plugin\n")); err != nil {
		t.Fatal(err)
	}
	if err := zw.Close(); err != nil {
		t.Fatal(err)
	}
	blob := archive.Bytes()
	manifest, err := json.Marshal(ocispec.Manifest{Versioned: specs.Versioned{SchemaVersion: 2}, MediaType: ocispec.MediaTypeImageManifest, ArtifactType: plugins.PluginArtifactType, Layers: []ocispec.Descriptor{{MediaType: "application/zip", Digest: digest.FromBytes(blob), Size: int64(len(blob))}}})
	if err != nil {
		t.Fatal(err)
	}
	manifestDigest := digest.FromBytes(manifest)
	var authorized, denied atomic.Int64
	var corruptBlob, blockBlob atomic.Bool
	blocked := make(chan struct{}, 1)
	registry := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		user, pass, ok := r.BasicAuth()
		if !ok || user != "pull-user" || pass != "pull-password" {
			denied.Add(1)
			w.Header().Set("WWW-Authenticate", `Basic realm="test"`)
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		authorized.Add(1)
		switch {
		case strings.Contains(r.URL.Path, "/manifests/"):
			w.Header().Set("Content-Type", ocispec.MediaTypeImageManifest)
			w.Header().Set("Docker-Content-Digest", manifestDigest.String())
			_, _ = w.Write(manifest)
		case strings.Contains(r.URL.Path, "/blobs/"):
			if blockBlob.Load() {
				blocked <- struct{}{}
				<-r.Context().Done()
				return
			}
			if corruptBlob.Load() {
				changed := append([]byte(nil), blob...)
				changed[0] ^= 1
				_, _ = w.Write(changed)
			} else {
				_, _ = w.Write(blob)
			}
		default:
			http.NotFound(w, r)
		}
	}))
	defer registry.Close()
	host := strings.TrimPrefix(registry.URL, "https://")
	ref := host + "/plugins/example@" + manifestDigest.String()
	parent := t.TempDir()
	write := func(name string, data []byte) string {
		t.Helper()
		path := filepath.Join(parent, name)
		if err := os.WriteFile(path, data, 0600); err != nil {
			t.Fatal(err)
		}
		return path
	}
	emptyCertDir := filepath.Join(parent, "empty-certs")
	if err := os.Mkdir(emptyCertDir, 0700); err != nil {
		t.Fatal(err)
	}
	ca := write("ca.pem", pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: registry.Certificate().Raw}))
	request, _ := json.Marshal([]plugins.Installation{{Name: "example", Artifact: ref}})
	approval, _ := json.Marshal(policy{ApprovedArtifacts: []string{ref}, OpenSearchVersion: "3.9.0", JavaVersion: "21", MaxBytes: 1 << 20, MaxFiles: 10})
	requestPath, policyPath := write("request.json", request), write("policy.json", approval)
	credentials, _ := json.Marshal(map[string]credential{host: {Username: "pull-user", Password: "pull-password"}})
	credentialsPath := write("credentials.json", credentials)
	output := filepath.Join(parent, "plugins")
	args := []string{"-request", requestPath, "-policy", policyPath, "-credentials", credentialsPath, "-output", output, "-request-sha256", fmt.Sprintf("%x", sha256.Sum256(request)), "-policy-sha256", fmt.Sprintf("%x", sha256.Sum256(approval))}
	invoke := func(args []string, trusted bool) ([]byte, error) {
		t.Helper()
		encoded, err := json.Marshal(args)
		if err != nil {
			t.Fatal(err)
		}
		cmd := exec.Command(os.Args[0], "-test.run=^TestInstallerCLIChild$")
		cert := ca
		if !trusted {
			cert = filepath.Join(parent, "missing.pem")
		}
		cmd.Env = append(os.Environ(), "PLUGIN_TEST_ARGS="+string(encoded), "SSL_CERT_FILE="+cert, "SSL_CERT_DIR="+emptyCertDir)
		return cmd.CombinedOutput()
	}
	// A failed TLS connection must not publish a partial plugin directory.
	if out, err := invoke(args, false); err == nil {
		t.Fatalf("untrusted registry accepted: %s", out)
	}
	if _, err := os.Stat(output); !os.IsNotExist(err) {
		t.Fatalf("failed install published output: %v", err)
	}
	customCAArgs := append(append([]string{}, args...), "-registry-ca", ca, "-output", filepath.Join(parent, "with-custom-ca"))
	if out, err := invoke(customCAArgs, false); err != nil {
		t.Fatalf("explicit registry CA rejected: %s %v", out, err)
	}
	// A wrong password fails without publishing and must not appear in errors.
	bad, _ := json.Marshal(map[string]credential{host: {Username: "pull-user", Password: "wrong-password"}})
	if err := os.WriteFile(credentialsPath, bad, 0600); err != nil {
		t.Fatal(err)
	}
	if out, err := invoke(args, true); err == nil || bytes.Contains(out, []byte("wrong-password")) {
		t.Fatalf("bad credentials result: %s, %v", out, err)
	}
	if _, err := os.Stat(output); !os.IsNotExist(err) {
		t.Fatalf("failed authentication published output: %v", err)
	}
	if err := os.WriteFile(credentialsPath, credentials, 0600); err != nil {
		t.Fatal(err)
	}
	corruptBlob.Store(true)
	if out, err := invoke(args, true); err == nil {
		t.Fatalf("corrupted blob accepted: %s", out)
	}
	if _, err := os.Stat(output); !os.IsNotExist(err) {
		t.Fatalf("corrupted blob published output: %v", err)
	}
	corruptBlob.Store(false)
	// Kill an installer while the registry holds its blob response. The next
	// invocation must remove its abandoned staging tree and finish normally.
	blockBlob.Store(true)
	encoded, err := json.Marshal(args)
	if err != nil {
		t.Fatal(err)
	}
	child := exec.Command(os.Args[0], "-test.run=^TestInstallerCLIChild$")
	child.Env = append(os.Environ(), "PLUGIN_TEST_ARGS="+string(encoded), "SSL_CERT_FILE="+ca, "SSL_CERT_DIR="+emptyCertDir)
	if err := child.Start(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-blocked:
	case <-time.After(10 * time.Second):
		_ = child.Process.Kill()
		_ = child.Wait()
		t.Fatal("installer did not request blob")
	}
	if err := child.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	_ = child.Wait()
	blockBlob.Store(false)
	stages, err := filepath.Glob(filepath.Join(parent, ".plugin-set-*"))
	if err != nil || len(stages) != 1 {
		t.Fatalf("killed installer staging missing: %v %v", stages, err)
	}
	if out, err := invoke(args, true); err != nil {
		t.Fatalf("install failed: %s, %v", out, err)
	}
	stages, err = filepath.Glob(filepath.Join(parent, ".plugin-set-*"))
	if err != nil || len(stages) != 0 {
		t.Fatalf("abandoned staging retained: %v %v", stages, err)
	}
	if authorized.Load() < 2 || denied.Load() == 0 {
		t.Fatalf("registry authentication or manifest/blob fetch was not exercised")
	}
	if _, err := os.Stat(filepath.Join(output, "example", "plugin-descriptor.properties")); err != nil {
		t.Fatal(err)
	}
	// A completed set is recovered without contacting the registry again.
	registry.Close()
	if out, err := invoke(args, true); err != nil {
		t.Fatalf("offline retry failed: %s, %v", out, err)
	}
	if err := os.WriteFile(requestPath, append(request, ' '), 0600); err != nil {
		t.Fatal(err)
	}
	if out, err := invoke(args, true); err == nil {
		t.Fatalf("changed request accepted: %s", out)
	}
}
