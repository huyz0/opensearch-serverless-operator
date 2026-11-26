//go:build java_integration

package plugins_test

import (
	"archive/zip"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/huyz0/opensearch-serverless-operator/internal/plugins"
	digest "github.com/opencontainers/go-digest"
	"github.com/opencontainers/image-spec/specs-go"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"oras.land/oras-go/v2/registry"
	"oras.land/oras-go/v2/registry/remote"
)

// This test needs the explicitly selected pinned Java runtime bundle and JDK.
// It uses a registry protocol fixture and runs a real local serverless JVM.
func TestInstalledPluginLoadsInJavaShell(t *testing.T) {
	lib := os.Getenv("OPERATOR_JAVA_RUNTIME_LIB")
	if lib == "" {
		t.Skip("set OPERATOR_JAVA_RUNTIME_LIB to the qualified runtime lib directory")
	}
	lib, err := filepath.Abs(lib)
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	classes := filepath.Join(dir, "classes")
	if err := os.Mkdir(classes, 0700); err != nil {
		t.Fatal(err)
	}
	source := filepath.Join(dir, "OperatorFixturePlugin.java")
	code := `package fixture;
import java.util.List;
import java.util.function.Supplier;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.IndexScopedSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.settings.SettingsFilter;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.plugins.ActionPlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestController;
import org.opensearch.rest.RestHandler;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.RestChannel;
import org.opensearch.transport.client.node.NodeClient;
public class OperatorFixturePlugin extends Plugin implements ActionPlugin {
    public OperatorFixturePlugin() { System.err.println("OPERATOR_FIXTURE_PLUGIN_LOADED"); }
    @Override public List<RestHandler> getRestHandlers(Settings settings, RestController controller,
        ClusterSettings clusterSettings, IndexScopedSettings indexSettings, SettingsFilter filter,
        IndexNameExpressionResolver resolver, Supplier<DiscoveryNodes> nodes) {
        return List.of(new RestHandler() {
            @Override public List<Route> routes() { return List.of(new Route(RestRequest.Method.GET, "/_operator_fixture")); }
            @Override public void handleRequest(RestRequest request, RestChannel channel, NodeClient client) {
                channel.sendResponse(new BytesRestResponse(RestStatus.OK, BytesRestResponse.TEXT_CONTENT_TYPE, "fixture plugin active"));
            }
        });
    }
}`
	if err := os.WriteFile(source, []byte(code), 0600); err != nil {
		t.Fatal(err)
	}
	command := func(name string, args ...string) {
		t.Helper()
		output, err := exec.Command(name, args...).CombinedOutput()
		if err != nil {
			t.Fatalf("%s failed: %v\n%s", name, err, output)
		}
	}
	command("javac", "-cp", filepath.Join(lib, "*"), "-d", classes, source)
	jar := filepath.Join(dir, "fixture.jar")
	command("jar", "cf", jar, "-C", classes, ".")
	jarBytes, err := os.ReadFile(jar)
	if err != nil {
		t.Fatal(err)
	}
	var buffer bytes.Buffer
	zw := zip.NewWriter(&buffer)
	descriptor := "name=operator-fixture\ndescription=local integration fixture for ${catalog}\nversion=1\nopensearch.version=3.10.0\njava.version=25\nclassname=fixture.OperatorFixturePlugin\n"
	for name, data := range map[string][]byte{"plugin-descriptor.properties": []byte(descriptor), "fixture.jar": jarBytes} {
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
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.URL.Path, "/manifests/") {
			w.Header().Set("Content-Type", ocispec.MediaTypeImageManifest)
			w.Header().Set("Docker-Content-Digest", manifestDigest.String())
			_, _ = w.Write(manifest)
		} else if strings.Contains(r.URL.Path, "/blobs/") {
			_, _ = w.Write(blob)
		} else {
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	ref := strings.TrimPrefix(server.URL, "http://") + "/plugins/fixture@" + manifestDigest.String()
	home := filepath.Join(dir, "home")
	if err := os.MkdirAll(filepath.Join(home, "config"), 0700); err != nil {
		t.Fatal(err)
	}
	factory := func(ref registry.Reference) (*remote.Repository, error) {
		repo, err := remote.NewRepository(ref.Registry + "/" + ref.Repository)
		if err == nil {
			repo.PlainHTTP = true
		}
		return repo, err
	}
	if err := plugins.InstallSet(context.Background(), filepath.Join(home, "plugins"), []plugins.Installation{{Name: "operator-fixture", Artifact: ref}}, map[string]bool{ref: true}, "3.10.0", "25", 1<<20, 10, factory); err != nil {
		t.Fatal(err)
	}
	server.Close()
	if err := plugins.InstallSet(context.Background(), filepath.Join(home, "plugins"), []plugins.Installation{{Name: "operator-fixture", Artifact: ref}}, map[string]bool{ref: true}, "3.10.0", "25", 1<<20, 10, factory); err != nil {
		t.Fatalf("completed set did not survive registry outage: %v", err)
	}
	// Reserve an ephemeral port for this isolated smoke test. The loading marker
	// proves the installed JAR's constructor ran; the REST request below also
	// exercises a plugin-provided handler. Another process could claim the port.
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := listener.Addr().(*net.TCPAddr).Port
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	logPath := filepath.Join(dir, "java.log")
	logFile, err := os.Create(logPath)
	if err != nil {
		t.Fatal(err)
	}
	defer logFile.Close()
	args := []string{"--add-opens=java.base/java.lang=ALL-UNNAMED", "--add-opens=java.base/java.nio=ALL-UNNAMED", "--add-opens=java.base/java.util=ALL-UNNAMED", "-Dio.netty.noUnsafe=true", "-Dio.netty.noKeySetOptimization=true", "-Dio.netty.recycler.maxCapacityPerThread=0", "-Dopensearch.set.netty.runtime.available.processors=false", "-Dopensearch.path.conf=" + filepath.Join(home, "config"), "-Dnode.name=plugin-fixture", "-Dcluster.name=plugin-fixture", "-Dpath.home=" + home, "-Dnetwork.host=127.0.0.1", fmt.Sprintf("-Dhttp.port=%d", port), "-Dtransport.port=0", "-Dserverless.roles=ingest", "-Dserverless.store.path=" + filepath.Join(dir, "store"), "-cp", filepath.Join(lib, "*"), "org.opensearch.serverless.shell.ServerlessBootstrap"}
	java := exec.Command("java", args...)
	java.Stdout, java.Stderr = logFile, logFile
	if err := java.Start(); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- java.Wait() }()
	defer func() {
		if java.Process != nil {
			_ = java.Process.Kill()
		}
		<-done
	}()
	httpClient := &http.Client{Timeout: time.Second}
	endpoint := fmt.Sprintf("http://127.0.0.1:%d/_serverless/ready", port)
	deadline := time.After(45 * time.Second)
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			log, _ := os.ReadFile(logPath)
			if bytes.Contains(log, []byte("OPERATOR_FIXTURE_PLUGIN_LOADED")) {
				response, err := httpClient.Get(endpoint)
				if err == nil {
					var body struct {
						Ready bool `json:"ready"`
					}
					decodeErr := json.NewDecoder(response.Body).Decode(&body)
					response.Body.Close()
					if response.StatusCode == http.StatusOK && decodeErr == nil && body.Ready {
						pluginResponse, err := httpClient.Get(fmt.Sprintf("http://127.0.0.1:%d/_operator_fixture", port))
						if err == nil {
							data, readErr := io.ReadAll(io.LimitReader(pluginResponse.Body, 1024))
							pluginResponse.Body.Close()
							if readErr == nil && pluginResponse.StatusCode == http.StatusOK && string(data) == "fixture plugin active" {
								return
							}
						}
					}
				}
			}
		case err := <-done:
			done <- err
			log, _ := os.ReadFile(logPath)
			t.Fatalf("Java exited before plugin load: %v\n%s", err, log)
		case <-deadline:
			log, _ := os.ReadFile(logPath)
			t.Fatal(fmt.Sprintf("plugin load timed out\n%s", log))
		}
	}
}
