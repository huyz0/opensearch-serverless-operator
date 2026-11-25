//go:build registry_integration

package plugins_test

import (
	"archive/zip"
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"math/big"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	digest "github.com/opencontainers/go-digest"
	"github.com/opencontainers/image-spec/specs-go"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"oras.land/oras-go/v2/registry"
	"oras.land/oras-go/v2/registry/remote"
	"oras.land/oras-go/v2/registry/remote/auth"

	"github.com/huyz0/opensearch-serverless-operator/internal/plugins"
)

// A real standalone Distribution registry with disposable credentials and TLS.
// It does not exercise kubelet or a production plugin.
func TestDistributionRegistryPluginInstall(t *testing.T) {
	binary := os.Getenv("OPERATOR_REGISTRY_BINARY")
	if binary == "" {
		t.Skip("set OPERATOR_REGISTRY_BINARY to a Distribution registry binary")
	}
	lib := os.Getenv("OPERATOR_JAVA_RUNTIME_LIB")
	if lib == "" {
		t.Skip("set OPERATOR_JAVA_RUNTIME_LIB to the patched Java runtime")
	}
	dir := t.TempDir()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	template := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "localhost"}, DNSNames: []string{"localhost"}, IPAddresses: []net.IP{net.ParseIP("127.0.0.1")}, NotBefore: now.Add(-time.Minute), NotAfter: now.Add(time.Hour), KeyUsage: x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}, BasicConstraintsValid: true, IsCA: true}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	write := func(name string, data []byte) string {
		t.Helper()
		path := filepath.Join(dir, name)
		if err := os.WriteFile(path, data, 0600); err != nil {
			t.Fatal(err)
		}
		return path
	}
	certFile := write("cert.pem", certPEM)
	keyFile := write("key.pem", pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER}))
	// Fixed, public bcrypt fixture for the non-secret password fixture-password.
	passwd := write("htpasswd", []byte("fixture:$2b$10$abcdefghijklmnopqrstuu8vlHjBPcS/rvADQ/wyGPn4IRDgLYpvW\n"))
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := listener.Addr().(*net.TCPAddr).Port
	_ = listener.Close()
	config := fmt.Sprintf("version: 0.1\nlog:\n  level: error\nstorage:\n  filesystem:\n    rootdirectory: %s\nhttp:\n  addr: 127.0.0.1:%d\n  tls:\n    certificate: %s\n    key: %s\nauth:\n  htpasswd:\n    realm: test-realm\n    path: %s\n", filepath.Join(dir, "data"), port, certFile, keyFile, passwd)
	configFile := write("registry.yml", []byte(config))
	process := exec.Command(binary, "serve", configFile)
	var logs bytes.Buffer
	process.Stdout, process.Stderr = &logs, &logs
	if err := process.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if process.Process != nil {
			_ = process.Process.Kill()
		}
		_ = process.Wait()
	})
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(certPEM) {
		t.Fatal("invalid test certificate")
	}
	httpClient := &http.Client{Timeout: 3 * time.Second, Transport: &http.Transport{TLSClientConfig: &tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS12}}}
	endpoint := fmt.Sprintf("127.0.0.1:%d", port)
	deadline := time.Now().Add(15 * time.Second)
	for {
		resp, err := httpClient.Get("https://" + endpoint + "/v2/")
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode == http.StatusUnauthorized {
				break
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("registry did not start: %v; %s", err, logs.String())
		}
		time.Sleep(100 * time.Millisecond)
	}
	var zipData bytes.Buffer
	zw := zip.NewWriter(&zipData)
	descriptor, err := zw.Create("plugin-descriptor.properties")
	if err != nil {
		t.Fatal(err)
	}
	_, _ = descriptor.Write([]byte("name=fixture\ndescription=test\nversion=1\nopensearch.version=3.10.0\njava.version=25\nclassname=fixture.Plugin\n"))
	classes := filepath.Join(dir, "classes")
	if err := os.Mkdir(classes, 0700); err != nil {
		t.Fatal(err)
	}
	source := write("Plugin.java", []byte(`package fixture; public class Plugin extends org.opensearch.plugins.Plugin { public Plugin() { System.err.println("REGISTRY_FIXTURE_PLUGIN_ACTIVE"); } }`))
	if out, err := exec.Command("javac", "-cp", filepath.Join(lib, "*"), "-d", classes, source).CombinedOutput(); err != nil {
		t.Fatalf("compile fixture plugin: %v %s", err, out)
	}
	jarPath := filepath.Join(dir, "fixture.jar")
	if out, err := exec.Command("jar", "cf", jarPath, "-C", classes, ".").CombinedOutput(); err != nil {
		t.Fatalf("package fixture plugin: %v %s", err, out)
	}
	jarBytes, err := os.ReadFile(jarPath)
	if err != nil {
		t.Fatal(err)
	}
	jarEntry, err := zw.Create("fixture.jar")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := jarEntry.Write(jarBytes); err != nil {
		t.Fatal(err)
	}
	if err := zw.Close(); err != nil {
		t.Fatal(err)
	}
	blob := zipData.Bytes()
	layer := ocispec.Descriptor{MediaType: "application/zip", Digest: digest.FromBytes(blob), Size: int64(len(blob))}
	configBlob := []byte("{}")
	configDescriptor := ocispec.Descriptor{MediaType: "application/vnd.oci.empty.v1+json", Digest: digest.FromBytes(configBlob), Size: int64(len(configBlob))}
	manifest, err := json.Marshal(ocispec.Manifest{Versioned: specs.Versioned{SchemaVersion: 2}, MediaType: ocispec.MediaTypeImageManifest, ArtifactType: plugins.PluginArtifactType, Config: configDescriptor, Layers: []ocispec.Descriptor{layer}})
	if err != nil {
		t.Fatal(err)
	}
	manifestDescriptor := ocispec.Descriptor{MediaType: ocispec.MediaTypeImageManifest, Digest: digest.FromBytes(manifest), Size: int64(len(manifest))}
	newRepo := func(password string, client *http.Client) func(registry.Reference) (*remote.Repository, error) {
		return func(ref registry.Reference) (*remote.Repository, error) {
			repo, err := remote.NewRepository(ref.Registry + "/" + ref.Repository)
			if err == nil {
				repo.Client = &auth.Client{Client: client, Credential: auth.StaticCredential(ref.Registry, auth.Credential{Username: "fixture", Password: password}), Cache: auth.NewCache()}
			}
			return repo, err
		}
	}
	reference := endpoint + "/plugins/fixture@" + manifestDescriptor.Digest.String()
	parsed, err := registry.ParseReference(reference)
	if err != nil {
		t.Fatal(err)
	}
	factory := newRepo("fixture-password", httpClient)
	repo, err := factory(parsed)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	if err := repo.Blobs().Push(ctx, layer, bytes.NewReader(blob)); err != nil {
		t.Fatalf("push plugin blob: %v", err)
	}
	if err := repo.Blobs().Push(ctx, configDescriptor, bytes.NewReader(configBlob)); err != nil {
		t.Fatalf("push plugin config: %v", err)
	}
	if err := repo.Manifests().Push(ctx, manifestDescriptor, bytes.NewReader(manifest)); err != nil {
		t.Fatalf("push plugin manifest: %v", err)
	}
	requested := []plugins.Installation{{Name: "fixture", Artifact: reference}}
	approved := map[string]bool{reference: true}
	install := func(name, password string, client *http.Client) error {
		return plugins.InstallSet(ctx, filepath.Join(dir, name), requested, approved, "3.10.0", "25", 1<<20, 10, newRepo(password, client))
	}
	if err := install("bad-credentials", "wrong", httpClient); err == nil {
		t.Fatal("wrong registry credentials accepted")
	}
	if err := install("untrusted", "fixture-password", &http.Client{Timeout: 3 * time.Second}); err == nil {
		t.Fatal("untrusted registry certificate accepted")
	}
	installerPath := filepath.Join(dir, "plugin-installer")
	if output, err := exec.CommandContext(ctx, "go", "build", "-o", installerPath, "../../cmd/plugin-installer").CombinedOutput(); err != nil {
		t.Fatalf("build installer: %v %s", err, output)
	}
	requestJSON, err := json.Marshal(requested)
	if err != nil {
		t.Fatal(err)
	}
	policyJSON, err := json.Marshal(plugins.Policy{ApprovedArtifacts: []string{reference}, OpenSearchVersion: "3.10.0", JavaVersion: "25", MaxBytes: 1 << 20, MaxFiles: 10})
	if err != nil {
		t.Fatal(err)
	}
	credentialsJSON, err := json.Marshal(map[string]map[string]string{endpoint: {"username": "fixture", "password": "fixture-password"}})
	if err != nil {
		t.Fatal(err)
	}
	requestPath, policyPath, credentialsPath := write("request.json", requestJSON), write("policy.json", policyJSON), write("credentials.json", credentialsJSON)
	invokeInstaller := func(output string, includeCA bool) error {
		args := []string{"-request", requestPath, "-policy", policyPath, "-credentials", credentialsPath, "-output", filepath.Join(dir, output)}
		if includeCA {
			args = append(args, "-registry-ca", certFile)
		}
		command := exec.CommandContext(ctx, installerPath, args...)
		result, err := command.CombinedOutput()
		if err != nil {
			return fmt.Errorf("installer failed: %w: %s", err, result)
		}
		return nil
	}
	if err := invokeInstaller("cli-untrusted", false); err == nil {
		t.Fatal("installer accepted untrusted registry without configured CA")
	}
	badCredentials, err := json.Marshal(map[string]map[string]string{endpoint: {"username": "fixture", "password": "wrong-password"}})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(credentialsPath, badCredentials, 0600); err != nil {
		t.Fatal(err)
	}
	if err := invokeInstaller("cli-bad-credentials", true); err == nil || strings.Contains(err.Error(), "wrong-password") {
		t.Fatalf("installer authentication failure was unsafe: %v", err)
	}
	if err := os.WriteFile(credentialsPath, credentialsJSON, 0600); err != nil {
		t.Fatal(err)
	}
	if err := invokeInstaller("plugins", true); err != nil {
		t.Fatalf("installer against Distribution: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, "plugins", "fixture", "plugin-descriptor.properties")); err != nil {
		t.Fatal(err)
	}
	_ = process.Process.Kill()
	if err := invokeInstaller("plugins", true); err != nil {
		t.Fatalf("completed set did not survive registry outage: %v", err)
	}
	if err := install("fresh", "fixture-password", httpClient); err == nil {
		t.Fatal("fresh installation succeeded during registry outage")
	}
	if err := os.MkdirAll(filepath.Join(dir, "config"), 0700); err != nil {
		t.Fatal(err)
	}
	logPath := filepath.Join(dir, "java.log")
	logFile, err := os.Create(logPath)
	if err != nil {
		t.Fatal(err)
	}
	defer logFile.Close()
	javaListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	javaPort := javaListener.Addr().(*net.TCPAddr).Port
	_ = javaListener.Close()
	args := []string{"--add-opens=java.base/java.lang=ALL-UNNAMED", "--add-opens=java.base/java.nio=ALL-UNNAMED", "--add-opens=java.base/java.util=ALL-UNNAMED", "-Dio.netty.noUnsafe=true", "-Dio.netty.noKeySetOptimization=true", "-Dio.netty.recycler.maxCapacityPerThread=0", "-Dopensearch.set.netty.runtime.available.processors=false", "-Dopensearch.path.conf=" + filepath.Join(dir, "config"), "-Dpath.home=" + dir, "-Dnode.name=registry-fixture", "-Dcluster.name=registry-fixture", "-Dnetwork.host=127.0.0.1", fmt.Sprintf("-Dhttp.port=%d", javaPort), "-Dtransport.port=0", "-Dserverless.roles=ingest", "-Dserverless.store.path=" + filepath.Join(dir, "store"), "-cp", filepath.Join(lib, "*"), "org.opensearch.serverless.shell.ServerlessBootstrap"}
	javaProcess := exec.Command("java", args...)
	javaProcess.Stdout, javaProcess.Stderr = logFile, logFile
	if err := javaProcess.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = javaProcess.Process.Kill(); _ = javaProcess.Wait() }()
	javaDeadline := time.Now().Add(45 * time.Second)
	for {
		log, _ := os.ReadFile(logPath)
		if bytes.Contains(log, []byte("REGISTRY_FIXTURE_PLUGIN_ACTIVE")) {
			response, err := http.Get(fmt.Sprintf("http://127.0.0.1:%d/_serverless/ready", javaPort))
			if err == nil {
				response.Body.Close()
				if response.StatusCode == http.StatusOK {
					break
				}
			}
		}
		if time.Now().After(javaDeadline) {
			t.Fatalf("Java did not load registry plugin: %s", log)
		}
		time.Sleep(100 * time.Millisecond)
	}
}
