//go:build envtest

package controller

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/huyz0/opensearch-serverless-operator/internal/java"
)

// launchJavaRollout starts a real ingest process and a warm source search
// process, seeds the shared store, then waits for each strict target search.
// The API server fixture still supplies Pod status and EndpointSlices.
func launchJavaRollout(t *testing.T, sourceSearch, sourceIngest string, sourceSearchNames, searchNames []string) (map[string]string, func() error) {
	t.Helper()
	runtimeLib := os.Getenv("OPERATOR_JAVA_RUNTIME_LIB")
	gateway := os.Getenv("OPERATOR_ROLLOUT_S3_BINARY")
	if runtimeLib == "" {
		if gateway != "" {
			t.Fatal("OPERATOR_ROLLOUT_S3_BINARY requires OPERATOR_JAVA_RUNTIME_LIB")
		}
		return nil, nil
	}
	logs := t.TempDir()
	store := filepath.Join(logs, "store")
	var s3 rolloutS3Fixture
	if gateway != "" {
		s3 = launchRolloutS3(t, gateway, runtimeLib)
	} else if err := os.Mkdir(store, 0700); err != nil {
		t.Fatal(err)
	}
	httpClient := &http.Client{Timeout: 2 * time.Second}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	endpoints := make(map[string]string, len(sourceSearchNames)+len(searchNames)+3)
	start := func(name, role string, strict bool) func() error {
		t.Helper()
		home := filepath.Join(logs, name)
		config := filepath.Join(home, "config")
		if err := os.MkdirAll(config, 0700); err != nil {
			t.Fatal(err)
		}
		ports := rolloutFixturePorts(t, 2)
		httpPort, peerPort := ports[0], ports[1]
		args := []string{
			"-Xms128m", "-Xmx512m", "-XX:ActiveProcessorCount=2",
			"--add-opens=java.base/java.lang=ALL-UNNAMED", "--add-opens=java.base/java.nio=ALL-UNNAMED",
			"--add-opens=java.base/java.util=ALL-UNNAMED",
			"-Dio.netty.noUnsafe=true", "-Dio.netty.noKeySetOptimization=true", "-Dio.netty.recycler.maxCapacityPerThread=0",
			"-Dopensearch.set.netty.runtime.available.processors=false", "-Dopensearch.path.conf=" + config,
			"-Dnode.name=" + name, "-Dcluster.name=operator-rollout-fixture", "-Dpath.home=" + home,
			"-Dnetwork.host=127.0.0.1", "-Dhttp.port=" + httpPort, "-Dtransport.port=" + peerPort,
			"-Dserverless.roles=" + role,
		}
		if gateway != "" {
			storeArgs, err := s3.arguments(config)
			if err != nil {
				t.Fatal(err)
			}
			args = append(args, storeArgs...)
		} else {
			args = append(args, "-Dserverless.store.path="+store)
		}
		if role == "search" {
			args = append(args, "-Dserverless.search.warm_on_join=true", "-Dserverless.search.warm_on_join.max_wait=12s")
		}
		if strict {
			args = append(args, "-Dserverless.search.require_warmup_completion=true")
		}
		cmd := exec.Command("java", append(args, "-cp", filepath.Join(runtimeLib, "*"), "org.opensearch.serverless.shell.ServerlessBootstrap")...)
		logPath := filepath.Join(home, "java.log")
		log, err := os.Create(logPath)
		if err != nil {
			t.Fatal(err)
		}
		cmd.Stdout, cmd.Stderr = log, log
		err = cmd.Start()
		_ = log.Close()
		if err != nil {
			t.Fatal(err)
		}
		stop := sync.OnceValue(func() error {
			err := cmd.Process.Kill()
			_ = cmd.Wait()
			return err
		})
		t.Cleanup(func() {
			_ = stop()
			if t.Failed() {
				if body, err := os.ReadFile(logPath); err == nil {
					t.Logf("%s Java log tail: %s", name, body[max(0, len(body)-4500):])
				}
			}
		})
		endpoint := "http://127.0.0.1:" + httpPort
		endpoints[name] = endpoint
		ticker := time.NewTicker(150 * time.Millisecond)
		defer ticker.Stop()
		for {
			var err error
			if strict {
				_, err = java.QualifySearch(ctx, httpClient, endpoint, name, 5*time.Second)
			} else {
				err = java.CheckServing(ctx, httpClient, endpoint, name, role, 5*time.Second)
			}
			if err == nil {
				return stop
			}
			select {
			case <-ctx.Done():
				t.Fatalf("Java %s failed to start: %v", name, err)
			case <-ticker.C:
			}
		}
	}
	start(sourceIngest, "ingest", false)
	start(sourceSearch, "search", false)
	sendRolloutJavaFixtureRequest(t, httpClient, http.MethodPut, endpoints[sourceIngest]+"/rollout-proof", "{}")
	sendRolloutJavaFixtureRequest(t, httpClient, http.MethodPut, endpoints[sourceIngest]+"/rollout-proof/_doc/1?refresh=true", `{"message":"before promotion"}`)
	for _, name := range sourceSearchNames {
		start(name, "search", false)
	}
	stopGreenIngest := start("publisher-ingest-a", "ingest", false)
	for _, name := range searchNames {
		start(name, "search", true)
	}
	return endpoints, stopGreenIngest
}

func rolloutFixturePorts(t *testing.T, count int) []string {
	t.Helper()
	ports := make([]string, 0, count)
	for range count {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		// Keep each port reserved until the full set is allocated so the
		// kernel cannot give two listeners the same number.
		defer listener.Close()
		_, port, err := net.SplitHostPort(listener.Addr().String())
		if err != nil {
			t.Fatal(err)
		}
		ports = append(ports, port)
	}
	return ports
}

func sendRolloutJavaFixtureRequest(t *testing.T, client *http.Client, method, address, payload string) string {
	t.Helper()
	request, err := http.NewRequest(method, address, strings.NewReader(payload))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Content-Type", "application/json")
	response, err := client.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	body, err := io.ReadAll(io.LimitReader(response.Body, 64<<10))
	if err != nil || response.StatusCode < 200 || response.StatusCode >= 300 {
		t.Fatal(fmt.Errorf("fixture request %s returned %d: %s (%v)", address, response.StatusCode, body, err))
	}
	return string(body)
}
