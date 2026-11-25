package java_test

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/huyz0/opensearch-serverless-operator/internal/java"
)

func TestServingRequiresReadyAndValidLocalLease(t *testing.T) {
	valid := true
	expires := int64(10000)
	name := "node-a"
	leasePresent := true
	roles := `["search","ingest"]`
	readyCalls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/_serverless/ready":
			readyCalls++
			_, _ = w.Write([]byte(`{"ready":true,"state":"ready","serves_searches":true}`))
		case "/_serverless/stats":
			if !leasePresent {
				_, _ = w.Write([]byte(`{"node":"id","name":"node-a"}`))
				return
			}
			_, _ = fmt.Fprintf(w, `{"node":"id","name":%q,"roles":%s,"lease":{"valid_now":%t,"expires_in_millis":%d,"ttl_millis":30000}}`, name, roles, valid, expires)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	check := func() error {
		return java.CheckServing(context.Background(), server.Client(), server.URL, "node-a", "search", 5*time.Second)
	}
	if err := check(); err != nil || readyCalls != 2 {
		t.Fatalf("eligible process rejected: %v", err)
	}
	roles = `["search"]`
	if err := java.CheckServing(context.Background(), server.Client(), server.URL, "node-a", "ingest", 5*time.Second); err == nil {
		t.Fatal("search-only process accepted for ingest")
	}
	roles = `[]`
	if err := check(); err == nil {
		t.Fatal("missing local search role accepted")
	}
	roles = `["search","ingest"]`
	if err := java.CheckServing(context.Background(), server.Client(), server.URL, "node-a", "ingest", 5*time.Second); err != nil {
		t.Fatalf("ingest role rejected: %v", err)
	}
	valid = false
	if err := check(); err == nil {
		t.Fatal("invalid lease accepted")
	}
	valid = true
	expires = 5000
	if err := check(); err == nil {
		t.Fatal("expiring lease accepted")
	}
	expires = 10000
	name = "other"
	if err := check(); err == nil {
		t.Fatal("other node accepted")
	}
	name = "node-a"
	leasePresent = false
	if err := check(); err == nil {
		t.Fatal("missing lease accepted")
	}
}
