//go:build envtest

package controller

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/huyz0/opensearch-serverless-operator/internal/java"
)

type rolloutS3Fixture struct {
	endpoint string
	bucket   string
	keystore string
}

type rolloutShardHead struct {
	State string `json:"state"`
	Owner string `json:"owner_node_id"`
	Term  int64  `json:"term"`
}

// writeCandidateOwnedRolloutShard brackets a routed write with authoritative
// shard-head observations from the original ingest process.
func writeCandidateOwnedRolloutShard(t *testing.T, ctx context.Context, directClient, edgeClient *http.Client, source, candidate, edge string) string {
	t.Helper()
	lease, err := java.GetLease(ctx, directClient, candidate)
	if err != nil || !lease.Valid || !lease.HasRole("ingest") {
		t.Fatalf("candidate ingest lease is unavailable: %v", err)
	}
	for n := 1; n <= 12; n++ {
		index := fmt.Sprintf("rollout-owner-%d", n)
		base := edge + "/ingest/" + index
		sendRolloutJavaFixtureRequest(t, edgeClient, http.MethodPut, base, `{"settings":{"number_of_shards":1}}`)
		sendRolloutJavaFixtureRequest(t, edgeClient, http.MethodPut, base+"/_doc/1?refresh=true", `{"message":"probe shard owner"}`)
		before := readRolloutShardHead(t, directClient, source, index)
		if before.State != "owned" || before.Owner != lease.NodeID || before.Term <= 0 {
			continue
		}
		writeResult := sendRolloutJavaFixtureRequest(t, edgeClient, http.MethodPut, base+"/_doc/2?refresh=true", `{"message":"candidate owned write"}`)
		var result struct {
			Result string `json:"result"`
		}
		if err := json.Unmarshal([]byte(writeResult), &result); err != nil || result.Result != "created" {
			t.Fatalf("candidate-owned write did not create the document: %s (%v)", writeResult, err)
		}
		after := readRolloutShardHead(t, directClient, source, index)
		if after.State != "owned" || after.Owner != before.Owner || after.Term != before.Term {
			t.Fatalf("shard ownership changed across the write: before=%+v after=%+v", before, after)
		}
		t.Logf("Green ingest owned %s at term %d before and after the acknowledged write", index, after.Term)
		return index
	}
	t.Fatal("no fixture shard was assigned to green ingest")
	return ""
}

func readRolloutShardHead(t *testing.T, client *http.Client, source, index string) rolloutShardHead {
	t.Helper()
	body := sendRolloutJavaFixtureRequest(t, client, http.MethodGet, source+"/_serverless/shards/"+index, "")
	var report struct {
		Index  string             `json:"index"`
		Shards []rolloutShardHead `json:"shards"`
	}
	if err := json.Unmarshal([]byte(body), &report); err != nil || report.Index != index || len(report.Shards) != 1 {
		t.Fatalf("invalid report for single-shard index %s: %s (%v)", index, body, err)
	}
	return report.Shards[0]
}

func verifyRolloutShardTakeover(t *testing.T, directClient, edgeClient *http.Client, source, candidate, edge, index string, stopGreen func() error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	blue, err := java.GetLease(ctx, directClient, source)
	if err != nil || !blue.Valid || !blue.HasRole("ingest") {
		t.Fatalf("blue ingest lease is unavailable: %v", err)
	}
	green, err := java.GetLease(ctx, directClient, candidate)
	if err != nil || !green.Valid || !green.HasRole("ingest") || green.NodeID == blue.NodeID {
		t.Fatalf("green ingest identity is unavailable: %v", err)
	}
	before := readRolloutShardHead(t, directClient, source, index)
	if before.State != "owned" || before.Owner != green.NodeID {
		t.Fatalf("green ingest did not own the shard before process loss: %+v", before)
	}
	if err := stopGreen(); err != nil {
		t.Fatalf("stop green ingest: %v", err)
	}
	started := time.Now()
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	for {
		request, err := http.NewRequestWithContext(ctx, http.MethodPut, edge+"/ingest/"+index+"/_doc/3?refresh=true",
			strings.NewReader(`{"message":"blue takeover write"}`))
		if err != nil {
			t.Fatal(err)
		}
		request.Header.Set("Content-Type", "application/json")
		response, err := edgeClient.Do(request)
		if err == nil {
			var written struct {
				Result string `json:"result"`
			}
			decodeErr := json.NewDecoder(io.LimitReader(response.Body, 64<<10)).Decode(&written)
			_ = response.Body.Close()
			if response.StatusCode == http.StatusOK || response.StatusCode == http.StatusCreated {
				if decodeErr != nil || (written.Result != "created" && written.Result != "updated") {
					t.Fatalf("unexpected write acknowledgement: %#v %v", written, decodeErr)
				}
				break
			}
			if response.StatusCode < 500 && response.StatusCode != http.StatusTooManyRequests {
				t.Fatalf("unexpected response while waiting for takeover: HTTP %d", response.StatusCode)
			}
		}
		select {
		case <-ctx.Done():
			t.Fatal("blue ingest did not acknowledge a write after green process loss")
		case <-ticker.C:
		}
	}
	after := readRolloutShardHead(t, directClient, source, index)
	if after.State != "owned" || after.Owner != blue.NodeID || after.Term <= before.Term {
		t.Fatalf("shard did not transfer to blue in a newer term: before=%+v after=%+v", before, after)
	}
	t.Logf("Blue ingest acknowledged a write %s after green process loss, with shard term %d -> %d", time.Since(started).Round(time.Millisecond), before.Term, after.Term)
	verifyCandidateOwnedRolloutDocument(t, edgeClient, edge, index, "candidate owned write")
	verifyCandidateOwnedRolloutDocument(t, edgeClient, edge, index, "blue takeover write")
}

func verifyCandidateOwnedRolloutDocument(t *testing.T, client *http.Client, edge, index, message string) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		result := sendRolloutJavaFixtureRequest(t, client, http.MethodGet, edge+"/search/"+index+"/_search?q=message:write", "")
		if strings.Contains(result, message) {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("restored route did not read %q: %s", message, result)
		}
		time.Sleep(150 * time.Millisecond)
	}
}

// launchRolloutS3 runs an isolated gateway and creates the bucket outside the
// operator. The shell reads disposable credentials from its usual keystore.
func launchRolloutS3(t *testing.T, executable, runtimeLib string) rolloutS3Fixture {
	t.Helper()
	dir := t.TempDir()
	ports := rolloutFixturePorts(t, 8)
	port := ports[0]
	fixture := rolloutS3Fixture{
		endpoint: "http://127.0.0.1:" + port,
		bucket:   "operator-rollout-fixture",
		keystore: filepath.Join(dir, "keys", "opensearch.keystore"),
	}
	data := filepath.Join(dir, "data")
	if err := os.Mkdir(data, 0700); err != nil {
		t.Fatal(err)
	}
	logPath := filepath.Join(dir, "s3.log")
	log, err := os.Create(logPath)
	if err != nil {
		t.Fatal(err)
	}
	gateway := exec.Command(executable, "server", "-s3", "-ip=127.0.0.1", "-ip.bind=127.0.0.1", "-dir="+data, "-volume.max=2", "-master.telemetry=false",
		"-master.port="+ports[1], "-master.port.grpc="+ports[2], "-volume.port="+ports[3], "-volume.port.grpc="+ports[4],
		"-filer.port="+ports[5], "-filer.port.grpc="+ports[6], "-s3.port="+port, "-s3.port.grpc="+ports[7])
	gateway.Dir, gateway.Stdout, gateway.Stderr = dir, log, log
	err = gateway.Start()
	_ = log.Close()
	if err != nil {
		t.Fatal(err)
	}
	exited := make(chan struct{})
	var gatewayErr error
	go func() {
		gatewayErr = gateway.Wait()
		close(exited)
	}()
	t.Cleanup(func() {
		_ = gateway.Process.Kill()
		<-exited
		if t.Failed() {
			if body, err := os.ReadFile(logPath); err == nil {
				if len(body) > 4500 {
					t.Logf("S3 gateway log start: %s", body[:min(500, len(body))])
				}
				t.Logf("S3 gateway log tail: %s", body[max(0, len(body)-4500):])
			}
		}
	})
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	client := &http.Client{Timeout: 2 * time.Second, Transport: &http.Transport{}}
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()
	for {
		request, err := http.NewRequestWithContext(ctx, http.MethodPut, fixture.endpoint+"/"+fixture.bucket, nil)
		if err != nil {
			t.Fatal(err)
		}
		response, err := client.Do(request)
		if err == nil {
			_ = response.Body.Close()
			if response.StatusCode >= 200 && response.StatusCode < 300 {
				break
			}
		}
		select {
		case <-exited:
			t.Fatalf("S3 fixture gateway exited before the bucket was available: %v", gatewayErr)
		case <-ctx.Done():
			t.Fatalf("S3 fixture bucket did not become available: %v", err)
		case <-ticker.C:
		}
	}
	classpath := filepath.Join(runtimeLib, "*")
	javac := exec.CommandContext(ctx, "javac", "-cp", classpath, "-d", dir, filepath.Join("..", "..", "tools", "SmokeKeystore.java"))
	if output, err := javac.CombinedOutput(); err != nil {
		t.Fatalf("compile test keystore helper: %v\n%s", err, output)
	}
	java := exec.CommandContext(ctx, "java", "-cp", dir+string(os.PathListSeparator)+classpath, "SmokeKeystore", filepath.Dir(fixture.keystore))
	java.Env = append(os.Environ(), "SMOKE_S3_ACCESS_KEY=rollout-fixture-access", "SMOKE_S3_SECRET_KEY=rollout-fixture-secret")
	if output, err := java.CombinedOutput(); err != nil {
		t.Fatalf("create test keystore: %v\n%s", err, output)
	}
	t.Logf("Java rollout will use the S3 fixture bucket %s", fixture.bucket)
	return fixture
}

func (s rolloutS3Fixture) arguments(config string) ([]string, error) {
	data, err := os.ReadFile(s.keystore)
	if err != nil {
		return nil, fmt.Errorf("read test S3 keystore: %w", err)
	}
	if err := os.WriteFile(filepath.Join(config, "opensearch.keystore"), data, 0600); err != nil {
		return nil, err
	}
	return []string{
		"-Dserverless.store.type=s3", "-Dserverless.store.bucket=" + s.bucket,
		"-Dserverless.store.region=us-east-1", "-Dserverless.store.endpoint=" + s.endpoint,
		"-Dserverless.store.path_style_access=true",
	}, nil
}
