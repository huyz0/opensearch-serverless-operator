package java_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/huyz0/opensearch-serverless-operator/internal/java"
)

func TestReadinessResponse(t *testing.T) {
	status := http.StatusOK
	body := `{"ready":true,"state":"ready","serves_searches":true}`
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/_serverless/ready" {
			t.Errorf("unexpected path %q", r.URL.Path)
		}
		w.WriteHeader(status)
		_, _ = w.Write([]byte(body))
	}))
	defer server.Close()
	ready, err := java.GetReadiness(context.Background(), server.Client(), server.URL)
	if err != nil || !ready.Ready || !ready.ServesSearches {
		t.Fatalf("unexpected readiness: %+v %v", ready, err)
	}
	status = http.StatusServiceUnavailable
	body = `{"ready":false,"state":"warming","serves_searches":true}`
	ready, err = java.GetReadiness(context.Background(), server.Client(), server.URL)
	if err != nil || ready.Ready || ready.State != "warming" {
		t.Fatalf("unexpected warming result: %+v %v", ready, err)
	}
	body = `{"ready":true,"state":"ready","serves_searches":true}`
	if _, err = java.GetReadiness(context.Background(), server.Client(), server.URL); err == nil {
		t.Fatal("inconsistent status and response must fail")
	}
	status = http.StatusOK
	body = `{"ready":true,"state":"ready","serves_searches":true}` + strings.Repeat(" ", 64<<10)
	if _, err = java.GetReadiness(context.Background(), server.Client(), server.URL); err == nil {
		t.Fatal("oversized response must fail")
	}
	body = `{"ready":true,"state":"ready","serves_searches":true} {}`
	if _, err = java.GetReadiness(context.Background(), server.Client(), server.URL); err == nil {
		t.Fatal("trailing JSON must fail")
	}
}
