package java_test

import (
	"context"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/huyz0/opensearch-serverless-operator/api/v1alpha1"
	"github.com/huyz0/opensearch-serverless-operator/internal/java"
)

// Set SERVERLESS_TEST_ENDPOINT to an isolated shell process for this test.
func TestLiveReadinessAndSettings(t *testing.T) {
	endpoint := os.Getenv("SERVERLESS_TEST_ENDPOINT")
	if endpoint == "" {
		t.Skip("SERVERLESS_TEST_ENDPOINT is not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	client := &http.Client{Timeout: 5 * time.Second}
	ready, err := java.GetReadiness(ctx, client, endpoint)
	if err != nil || !ready.Ready {
		t.Fatalf("shell was not ready: %+v, %v", ready, err)
	}
	if os.Getenv("SERVERLESS_TEST_EXPECT_WARMUP") == "1" {
		warmup, err := java.GetWarmup(ctx, client, endpoint)
		if err != nil || warmup.Result != "not_requested" || warmup.Finished() {
			t.Fatalf("unexpected patched warm-up status: %+v, %v", warmup, err)
		}
	}
	if name := os.Getenv("SERVERLESS_TEST_NODE_NAME"); name != "" {
		ticker := time.NewTicker(100 * time.Millisecond)
		defer ticker.Stop()
		for {
			if err = java.CheckServing(ctx, client, endpoint, name, "ingest", time.Second); err == nil {
				break
			}
			select {
			case <-ctx.Done():
				t.Fatalf("real shell lease did not become eligible: %v", err)
			case <-ticker.C:
			}
		}
		lease, err := java.GetLease(ctx, client, endpoint)
		if err != nil || !lease.HasRole("ingest") || lease.HasRole("search") {
			t.Fatalf("real ingest process reported unexpected local roles: %+v, %v", lease.Roles, err)
		}
		if os.Getenv("SERVERLESS_TEST_EXPECT_WARMUP") == "1" {
			qualification, err := java.QualifyIngest(ctx, client, endpoint, name, time.Second)
			if err != nil || qualification.ProcessIncarnation == "" {
				t.Fatalf("real ingest process qualification failed: %+v, %v", qualification, err)
			}
		}
	}
	if _, err := java.GetPersistentSettings(ctx, client, endpoint); err != nil {
		t.Fatalf("read persistent settings: %v", err)
	}
	const key = "serverless.bucket"
	allowed := map[string]bool{key: true}
	patch := v1alpha1.ClusterSettingsPatch{Set: map[string]string{key: "off"}}
	if err := java.ApplyClusterSettings(ctx, client, endpoint, patch, allowed); err != nil {
		t.Fatalf("apply setting: %v", err)
	}
	current, err := java.GetPersistentSettings(ctx, client, endpoint)
	if err != nil || !java.SettingsPatchApplied(current, patch) {
		t.Fatalf("setting did not persist: %v, %v", current, err)
	}
	removal := v1alpha1.ClusterSettingsPatch{Remove: []string{key}}
	if err := java.ApplyClusterSettings(ctx, client, endpoint, removal, allowed); err != nil {
		t.Fatalf("remove setting: %v", err)
	}
	current, err = java.GetPersistentSettings(ctx, client, endpoint)
	if err != nil || !java.SettingsPatchApplied(current, removal) {
		t.Fatalf("setting removal did not persist: %v, %v", current, err)
	}
}

func TestLiveWarmupStatus(t *testing.T) {
	endpoint := os.Getenv("SERVERLESS_TEST_WARM_ENDPOINT")
	if endpoint == "" {
		t.Skip("SERVERLESS_TEST_WARM_ENDPOINT is not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	client := &http.Client{Timeout: 3 * time.Second}
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		status, err := java.GetWarmup(ctx, client, endpoint)
		if err == nil && status.Finished() {
			if status.Result != "degraded" {
				t.Fatalf("a node without search peers must report degraded warm-up: %+v", status)
			}
			if _, err := java.QualifySearch(ctx, client, endpoint, "search", time.Second); err == nil {
				t.Fatal("degraded warm-up unexpectedly passed search qualification")
			}
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("warm-up did not finish: status=%+v error=%v", status, err)
		case <-ticker.C:
		}
	}
}

func TestLiveQualifiedSearch(t *testing.T) {
	endpoint := os.Getenv("SERVERLESS_TEST_QUALIFIED_ENDPOINT")
	if endpoint == "" {
		t.Skip("SERVERLESS_TEST_QUALIFIED_ENDPOINT is not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	client := &http.Client{Timeout: 3 * time.Second}
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()
	var lastErr error
	for {
		result, err := java.QualifySearch(ctx, client, endpoint, "search-warm", time.Second)
		if err == nil {
			if result.ProcessIncarnation == "" {
				t.Fatal("qualified process identity missing")
			}
			if previous := os.Getenv("SERVERLESS_TEST_PREVIOUS_INCARNATION"); previous != "" && result.ProcessIncarnation == previous {
				t.Fatal("restarted Java process reused its previous incarnation")
			}
			return
		}
		if ctx.Err() == nil {
			lastErr = err
		}
		select {
		case <-ctx.Done():
			t.Fatalf("real warmed search did not qualify: %v", lastErr)
		case <-ticker.C:
		}
	}
}

func TestLiveRestartedSearch(t *testing.T) {
	endpoint, previous := os.Getenv("SERVERLESS_TEST_QUALIFIED_ENDPOINT"), os.Getenv("SERVERLESS_TEST_PREVIOUS_INCARNATION")
	if endpoint == "" || previous == "" {
		t.Skip("restart endpoint and previous incarnation are required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	client := &http.Client{Timeout: 3 * time.Second}
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()
	seenRunning := false
	for {
		status, err := java.GetWarmup(ctx, client, endpoint)
		if err == nil && status.Result == "running" && status.ProcessIncarnation != previous {
			seenRunning = true
		}
		if err == nil && status.Finished() {
			if status.ProcessIncarnation == previous {
				t.Fatal("restarted Java process reused its previous incarnation")
			}
			if os.Getenv("SERVERLESS_TEST_EXPECT_COMPLETED") == "1" && (status.Result != "completed" || status.TimedOut) {
				t.Fatalf("restart did not complete warm-up after old lease expiry: %+v", status)
			}
			if os.Getenv("SERVERLESS_TEST_EXPECT_RETRY") == "1" && !seenRunning {
				t.Fatal("retry was not observed as running")
			}
			if os.Getenv("SERVERLESS_TEST_EXPECT_DEGRADED") == "1" && status.Result != "degraded" {
				t.Fatalf("expected bounded retries to end degraded: %+v", status)
			}
			if status.Result != "completed" || status.TimedOut {
				if _, err := java.QualifySearch(ctx, client, endpoint, "search-warm", time.Second); err == nil {
					t.Fatal("degraded restart qualified")
				}
			} else {
				for {
					result, err := java.QualifySearch(ctx, client, endpoint, "search-warm", time.Second)
					if err == nil {
						if result.ProcessIncarnation != status.ProcessIncarnation {
							t.Fatal("qualified process changed during restart check")
						}
						break
					}
					select {
					case <-ctx.Done():
						t.Fatalf("completed restart did not qualify: %v", err)
					case <-ticker.C:
					}
				}
			}
			t.Logf("restarted process warm-up result: %s, timed out: %t", status.Result, status.TimedOut)
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("restarted process did not report finished warm-up: %v", err)
		case <-ticker.C:
		}
	}
}
