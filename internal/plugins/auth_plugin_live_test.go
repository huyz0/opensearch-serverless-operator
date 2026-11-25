//go:build java_integration

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
	"io"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
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

	"github.com/huyz0/opensearch-serverless-operator/internal/plugins"
	"github.com/huyz0/opensearch-serverless-operator/internal/security"
)

// Uses the pinned shell's real auth plugin, an OCI protocol fixture and a
// local filesystem store. An optional mode enables experimental HTTPS; this
// is not a Kubernetes test.
func TestInstalledShellAuthPluginManagesAccounts(t *testing.T) {
	lib, jarPath := os.Getenv("OPERATOR_JAVA_RUNTIME_LIB"), os.Getenv("OPERATOR_AUTH_PLUGIN_JAR")
	if lib == "" || jarPath == "" {
		t.Skip("set OPERATOR_JAVA_RUNTIME_LIB and OPERATOR_AUTH_PLUGIN_JAR")
	}
	s3Endpoint, s3Bucket := os.Getenv("OPERATOR_AUTH_S3_ENDPOINT"), os.Getenv("OPERATOR_AUTH_S3_BUCKET")
	if s3Endpoint != "" || s3Bucket != "" {
		endpoint, err := url.Parse(s3Endpoint)
		if err != nil || endpoint.Scheme != "http" || endpoint.Hostname() != "127.0.0.1" || endpoint.Port() == "" ||
			endpoint.User != nil || endpoint.Path != "" || endpoint.RawQuery != "" || endpoint.ForceQuery || endpoint.Fragment != "" ||
			s3Bucket == "" {
			t.Fatal("S3 auth fixture requires a disposable bucket and an explicit loopback HTTP endpoint")
		}
	}
	jar, err := os.ReadFile(jarPath)
	if err != nil {
		t.Fatal(err)
	}
	var buffer bytes.Buffer
	zw := zip.NewWriter(&buffer)
	for name, data := range map[string][]byte{"plugin-descriptor.properties": []byte("name=serverless-auth\ndescription=auth\nversion=1\nopensearch.version=3.10.0\njava.version=25\nclassname=org.opensearch.serverless.auth.ServerlessAuthPlugin\n"), "auth.jar": jar} {
		entry, err := zw.Create(name)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := entry.Write(data); err != nil {
			t.Fatal(err)
		}
	}
	if err := zw.Close(); err != nil {
		t.Fatal(err)
	}
	blob := buffer.Bytes()
	manifest, err := json.Marshal(ocispec.Manifest{Versioned: specs.Versioned{SchemaVersion: 2}, MediaType: ocispec.MediaTypeImageManifest, ArtifactType: plugins.PluginArtifactType, Layers: []ocispec.Descriptor{{MediaType: "application/zip", Digest: digest.FromBytes(blob), Size: int64(len(blob))}}})
	if err != nil {
		t.Fatal(err)
	}
	manifestDigest := digest.FromBytes(manifest)
	registryServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if strings.Contains(req.URL.Path, "/manifests/") {
			w.Header().Set("Content-Type", ocispec.MediaTypeImageManifest)
			w.Header().Set("Docker-Content-Digest", manifestDigest.String())
			_, _ = w.Write(manifest)
			return
		}
		_, _ = w.Write(blob)
	}))
	defer registryServer.Close()
	reference := strings.TrimPrefix(registryServer.URL, "http://") + "/plugins/auth@" + manifestDigest.String()
	factory := func(ref registry.Reference) (*remote.Repository, error) {
		repo, err := remote.NewRepository(ref.Registry + "/" + ref.Repository)
		if err == nil {
			repo.PlainHTTP = true
		}
		return repo, err
	}
	dir := t.TempDir()
	home, classes := filepath.Join(dir, "home"), filepath.Join(dir, "classes")
	if err := os.MkdirAll(filepath.Join(home, "config"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(classes, 0700); err != nil {
		t.Fatal(err)
	}
	if err := plugins.InstallSet(context.Background(), filepath.Join(home, "plugins"), []plugins.Installation{{Name: "serverless-auth", Artifact: reference}}, map[string]bool{reference: true}, "3.10.0", "25", 1<<20, 10, factory); err != nil {
		t.Fatal(err)
	}
	helper := filepath.Join(dir, "AuthKeystore.java")
	if err := os.WriteFile(helper, []byte(`
import java.nio.file.Path;
import org.opensearch.common.settings.KeyStoreWrapper;

public class AuthKeystore {
    public static void main(String[] args) throws Exception {
        try (KeyStoreWrapper keystore = KeyStoreWrapper.create()) {
            keystore.setString("serverless.auth.bootstrap.password", "fixture-admin-password".toCharArray());
            if (args.length == 2) {
                keystore.setString("s3.client.default.access_key", "test-access".toCharArray());
                keystore.setString("s3.client.default.secret_key", "test-secret".toCharArray());
            }
            keystore.save(Path.of(args[0]), new char[0]);
        }
    }
}
`), 0600); err != nil {
		t.Fatal(err)
	}
	if out, err := exec.Command("javac", "-cp", filepath.Join(lib, "*"), "-d", classes, helper).CombinedOutput(); err != nil {
		t.Fatalf("compile keystore fixture: %v %s", err, out)
	}
	keystoreArgs := []string{"-cp", classes + string(os.PathListSeparator) + filepath.Join(lib, "*"), "AuthKeystore", filepath.Join(home, "config")}
	if s3Endpoint != "" {
		keystoreArgs = append(keystoreArgs, "s3")
	}
	if out, err := exec.Command("java", keystoreArgs...).CombinedOutput(); err != nil {
		t.Fatalf("create keystore: %v %s", err, out)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := listener.Addr().(*net.TCPAddr).Port
	_ = listener.Close()
	logFile, err := os.Create(filepath.Join(dir, "java.log"))
	if err != nil {
		t.Fatal(err)
	}
	defer logFile.Close()
	args := []string{"-Xms128m", "-Xmx512m", "-XX:ActiveProcessorCount=2", "--add-opens=java.base/java.lang=ALL-UNNAMED",
		"--add-opens=java.base/java.nio=ALL-UNNAMED", "--add-opens=java.base/java.util=ALL-UNNAMED", "-Dio.netty.noUnsafe=true",
		"-Dio.netty.noKeySetOptimization=true", "-Dio.netty.recycler.maxCapacityPerThread=0", "-Dopensearch.set.netty.runtime.available.processors=false",
		"-Dopensearch.path.conf=" + filepath.Join(home, "config"), "-Dpath.home=" + home, "-Dnode.name=auth-fixture",
		"-Dcluster.name=auth-fixture", "-Dnetwork.host=127.0.0.1", fmt.Sprintf("-Dhttp.port=%d", port), "-Dtransport.port=0", "-Dserverless.roles=ingest",
		"-Dserverless.auth.cache.ttl=5m"}
	if s3Endpoint == "" {
		args = append(args, "-Dserverless.store.path="+filepath.Join(dir, "store"))
	} else {
		args = append(args, "-Dserverless.store.type=s3", "-Dserverless.store.bucket="+s3Bucket, "-Dserverless.store.region=us-east-1",
			"-Dserverless.store.endpoint="+s3Endpoint, "-Dserverless.store.path_style_access=true")
	}
	args = append(args, "-cp", filepath.Join(lib, "*"), "org.opensearch.serverless.shell.ServerlessBootstrap")
	client := &http.Client{Timeout: 3 * time.Second}
	base := fmt.Sprintf("http://127.0.0.1:%d", port)
	if os.Getenv("OPERATOR_TEST_AUTH_HTTPS") == "1" {
		key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		if err != nil {
			t.Fatal(err)
		}
		now := time.Now()
		cert := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "auth-fixture"}, IPAddresses: []net.IP{net.ParseIP("127.0.0.1")}, NotBefore: now.Add(-time.Minute), NotAfter: now.Add(time.Hour), KeyUsage: x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}, BasicConstraintsValid: true, IsCA: true}
		der, err := x509.CreateCertificate(rand.Reader, cert, cert, &key.PublicKey, key)
		if err != nil {
			t.Fatal(err)
		}
		keyDER, err := x509.MarshalPKCS8PrivateKey(key)
		if err != nil {
			t.Fatal(err)
		}
		certPath, keyPath := filepath.Join(dir, "http.crt"), filepath.Join(dir, "http.key")
		if err := os.WriteFile(certPath, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0600); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(keyPath, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER}), 0600); err != nil {
			t.Fatal(err)
		}
		parsed, err := x509.ParseCertificate(der)
		if err != nil {
			t.Fatal(err)
		}
		roots := x509.NewCertPool()
		roots.AddCert(parsed)
		client = &http.Client{Timeout: 3 * time.Second, Transport: &http.Transport{TLSClientConfig: &tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS12}}}
		base = fmt.Sprintf("https://127.0.0.1:%d", port)
		args = append([]string{"-Dserverless.plugins=org.opensearch.serverless.operator.HttpTlsPlugin", "-Dserverless.http_tls.certificate=" + certPath, "-Dserverless.http_tls.private_key=" + keyPath}, args...)
	}
	process := exec.Command("java", args...)
	process.Stdout, process.Stderr = logFile, logFile
	if err := process.Start(); err != nil {
		t.Fatal(err)
	}
	processDone := make(chan struct{})
	var processErr error
	go func() {
		processErr = process.Wait()
		close(processDone)
	}()
	defer func() {
		_ = process.Process.Kill()
		<-processDone
	}()
	request := func(method, path, user, password, body string) (int, error) {
		req, err := http.NewRequest(method, base+path, strings.NewReader(body))
		if err != nil {
			return 0, err
		}
		if user != "" {
			req.SetBasicAuth(user, password)
		}
		if body != "" {
			req.Header.Set("Content-Type", "application/json")
		}
		resp, err := client.Do(req)
		if err != nil {
			return 0, err
		}
		_, _ = io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		return resp.StatusCode, nil
	}
	deadline := time.Now().Add(70 * time.Second)
	for {
		status, err := request("GET", "/", "admin", "fixture-admin-password", "")
		if err == nil && status == 200 {
			break
		}
		if time.Now().After(deadline) {
			log, _ := os.ReadFile(filepath.Join(dir, "java.log"))
			t.Fatalf("auth shell did not start: %v %d %s", err, status, log)
		}
		select {
		case <-processDone:
			log, _ := os.ReadFile(filepath.Join(dir, "java.log"))
			t.Fatalf("auth shell exited before readiness: %v\n%s", processErr, log)
		case <-time.After(200 * time.Millisecond):
		}
	}
	if status, err := request("GET", "/", "", "", ""); err != nil || status != 401 {
		t.Fatalf("anonymous request status: %d %v", status, err)
	}
	for {
		status, err := request("PUT", "/_serverless/security/users/client", "admin", "fixture-admin-password", `{"password":"fixture-client-password"}`)
		if err == nil && status == 200 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("account creation failed: %d %v", status, err)
		}
		time.Sleep(200 * time.Millisecond)
	}
	if status, err := request("GET", "/", "client", "fixture-client-password", ""); err != nil || status != 200 {
		t.Fatalf("new account status: %d %v", status, err)
	}
	if status, err := request("PUT", "/_serverless/security/users/client", "admin", "fixture-admin-password", `{"password":"fixture-rotated-password"}`); err != nil || status != 200 {
		t.Fatalf("password rotation status: %d %v", status, err)
	}
	if status, err := request("GET", "/", "client", "fixture-client-password", ""); err != nil || status != 401 {
		t.Fatalf("old credential after rotation: %d %v", status, err)
	}
	if status, err := request("GET", "/", "client", "fixture-rotated-password", ""); err != nil || status != 200 {
		t.Fatalf("new credential after rotation: %d %v", status, err)
	}
	if status, err := request("DELETE", "/_serverless/security/users/client", "admin", "fixture-admin-password", ""); err != nil || status != 200 {
		t.Fatalf("account removal status: %d %v", status, err)
	}
	if status, err := request("GET", "/", "client", "fixture-rotated-password", ""); err != nil || status != 401 {
		t.Fatalf("removed account status: %d %v", status, err)
	}
	if os.Getenv("OPERATOR_TEST_AUTH_HTTPS") == "1" {
		if probe := os.Getenv("OPERATOR_AUTH_HEALTH_PROBE"); probe != "" {
			t.Run("authenticated_ingest_health_probe", func(t *testing.T) {
				testShellAuthHealthProbe(t, probe, dir, "auth-fixture", "ingest", fmt.Sprint(port))
			})
		}
		admin := security.UserAdmin{BaseURL: base, Username: "admin", Password: "fixture-admin-password", Client: client}
		ctx := context.Background()
		if err := admin.Put(ctx, "operator-client", "first-password"); err != nil {
			t.Fatalf("HTTPS account creation: %v", err)
		}
		if exists, err := admin.Exists(ctx, "operator-client"); err != nil || !exists {
			t.Fatalf("HTTPS account lookup: %v %v", exists, err)
		}
		if err := admin.Put(ctx, "operator-client", "second-password"); err != nil {
			t.Fatalf("HTTPS account rotation: %v", err)
		}
		if status, err := request("GET", "/", "operator-client", "first-password", ""); err != nil || status != 401 {
			t.Fatalf("HTTPS old account credential: %d %v", status, err)
		}
		if status, err := request("GET", "/", "operator-client", "second-password", ""); err != nil || status != 200 {
			t.Fatalf("HTTPS rotated account credential: %d %v", status, err)
		}
		if found, err := admin.Delete(ctx, "operator-client"); err != nil || !found {
			t.Fatalf("HTTPS account removal: %v %v", found, err)
		}
		if status, err := request("GET", "/", "operator-client", "second-password", ""); err != nil || status != 401 {
			t.Fatalf("HTTPS deleted account credential: %d %v", status, err)
		}
		untrusted := admin
		untrusted.Client = &http.Client{Timeout: 3 * time.Second, Transport: &http.Transport{TLSClientConfig: &tls.Config{RootCAs: x509.NewCertPool(), MinVersion: tls.VersionTLS12}}}
		if _, err := untrusted.Exists(ctx, "operator-client"); err == nil {
			t.Fatal("untrusted HTTP TLS certificate accepted")
		}
		if os.Getenv("OPERATOR_TEST_AUTH_COHORT") == "1" {
			t.Run("hot_reload_ingest_search", func(t *testing.T) {
				testShellAuthCohort(t, dir, home, args, admin)
			})
		}
	} else if os.Getenv("OPERATOR_TEST_AUTH_COHORT") == "1" || os.Getenv("OPERATOR_AUTH_HEALTH_PROBE") != "" {
		t.Fatal("auth cohort and health-probe checks require OPERATOR_TEST_AUTH_HTTPS=1")
	}
}

func testShellAuthHealthProbe(t *testing.T, binary, dir, name, role, port string) {
	t.Helper()
	passwordPath := filepath.Join(t.TempDir(), "password")
	probe := func(password string) ([]byte, error) {
		if err := os.WriteFile(passwordPath, []byte(password), 0600); err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 6*time.Second)
		defer cancel()
		cmd := exec.CommandContext(ctx, binary, "-role", role, "-port", port, "-tls-ca", filepath.Join(dir, "http.crt"),
			"-tls-server-name", "127.0.0.1", "-basic-auth-username", "admin", "-basic-auth-password-file", passwordPath)
		cmd.Env = append(os.Environ(), "POD_NAME="+name, "POD_IP=127.0.0.1")
		return cmd.CombinedOutput()
	}
	if out, err := probe("fixture-wrong-password"); err == nil || !bytes.Contains(out, []byte("401")) {
		t.Fatalf("wrong fixture credential must fail with HTTP 401: %v %s", err, out)
	}
	deadline := time.Now().Add(30 * time.Second)
	for {
		out, err := probe("fixture-admin-password")
		if err == nil {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("authenticated probe did not observe Java readiness and a live lease: %v %s", err, out)
		}
		time.Sleep(time.Second)
	}
}
