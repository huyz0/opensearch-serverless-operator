package java_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/huyz0/opensearch-serverless-operator/internal/java"
)

func TestWarmupSeparatesReadinessFromWorkerCompletion(t *testing.T) {
	body := `{"ready":true,"process_incarnation":"9aa977f3-4985-4cb5-9692-5ad1e3e243fe","warmup_result":"running","warmup_timed_out":true}`
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write([]byte(body)) }))
	defer server.Close()
	observation, err := java.GetWarmup(context.Background(), server.Client(), server.URL)
	if err != nil || !observation.Ready || !observation.TimedOut || observation.Finished() {
		t.Fatalf("timeout must not imply finished warm-up: %+v, %v", observation, err)
	}
	body = `{"ready":true,"process_incarnation":"9aa977f3-4985-4cb5-9692-5ad1e3e243fe","warmup_result":"degraded","warmup_timed_out":true}`
	observation, err = java.GetWarmup(context.Background(), server.Client(), server.URL)
	if err != nil || !observation.Finished() || observation.Result != "degraded" {
		t.Fatalf("degraded worker completion was lost: %+v, %v", observation, err)
	}
	body = `{"ready":true,"state":"ready","serves_searches":true}`
	if _, err := java.GetWarmup(context.Background(), server.Client(), server.URL); err == nil {
		t.Fatal("unpatched shell must remain unknown")
	}
}
