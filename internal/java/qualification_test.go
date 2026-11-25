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

func TestSearchQualificationRejectsTimeoutDegradationAndRestart(t *testing.T) {
	const first = "9aa977f3-4985-4cb5-9692-5ad1e3e243fe"
	const second = "59c0c3f1-6db8-4a58-a335-683cf0429c02"
	result, timedOut, incarnation, changed, valid := "completed", false, first, false, true
	statsProcess := ""
	activity := `,"shards":{"readers":0},"block_cache":{"hits":12,"misses":3,"bytes_fetched":4096}`
	ttl := 30000
	readyCalls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/_serverless/ready":
			readyCalls++
			id := incarnation
			if changed && readyCalls >= 4 {
				id = second
			}
			_, _ = fmt.Fprintf(w, `{"ready":true,"state":"ready","serves_searches":true,"process_incarnation":%q,"warmup_result":%q,"warmup_timed_out":%t}`, id, result, timedOut)
		case "/_serverless/stats":
			_, _ = fmt.Fprintf(w, `{"node":"id","name":"node-a","process_incarnation":%q,"roles":["search","ingest"],"lease":{"valid_now":%t,"expires_in_millis":30000,"ttl_millis":%d}%s}`, statsProcess, valid, ttl, activity)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	check := func() (java.SearchQualification, error) {
		readyCalls = 0
		return java.QualifySearch(context.Background(), server.Client(), server.URL, "node-a", time.Second)
	}
	qualification, err := check()
	if err != nil || qualification.ProcessIncarnation != first || qualification.StatsProcessVerified || qualification.LocalSearchActivity != nil {
		t.Fatalf("legacy stats should qualify for ordinary search only: %#v %v", qualification, err)
	}
	statsProcess = second
	if _, err := check(); err == nil {
		t.Fatal("lease from a different search JVM qualified")
	}
	statsProcess = first
	ttl = 320000
	started := time.Now()
	if qualification, err := check(); err != nil || !qualification.StatsProcessVerified ||
		!qualification.LeaseValidUntil.After(started) || qualification.LeaseValidUntil.After(started.Add(21*time.Second)) ||
		qualification.LocalSearchActivity == nil || *qualification.LocalSearchActivity != (java.LocalSearchActivity{BlockCacheHits: 12, BlockCacheMisses: 3, BlockBytesFetched: 4096}) {
		t.Fatalf("matching lease identity was not verified: %#v %v", qualification, err)
	}
	for _, missing := range []string{"", `,"shards":{"readers":-1},"block_cache":{"hits":1,"misses":0,"bytes_fetched":2}`, `,"shards":{"readers":"invalid"}`, `,"shards":{"readers":0},"block_cache":{"hits":1,"bytes_fetched":2}`} {
		activity = missing
		if qualification, err := check(); err != nil || qualification.LocalSearchActivity != nil {
			t.Fatalf("optional stats changed the readiness decision or supplied invalid activity: %#v %v", qualification, err)
		}
	}
	for _, value := range []string{"running", "degraded", "failed", "not_requested"} {
		result = value
		if _, err := check(); err == nil {
			t.Fatalf("warm-up result %q qualified", value)
		}
	}
	result, timedOut = "completed", true
	if _, err := check(); err == nil {
		t.Fatal("timed-out warm-up qualified")
	}
	timedOut, changed = false, true
	if _, err := check(); err == nil {
		t.Fatal("changed process qualified")
	}
	changed, valid = false, false
	if _, err := check(); err == nil {
		t.Fatal("invalid lease qualified")
	}
}

func TestSearchQualificationRejectsLeaseExpiredDuringFinalRead(t *testing.T) {
	readyCalls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/_serverless/stats" {
			_, _ = fmt.Fprint(w, `{"node":"id","name":"node-a","roles":["search","ingest"],"lease":{"valid_now":true,"expires_in_millis":25,"ttl_millis":1000}}`)
			return
		}
		readyCalls++
		if readyCalls == 4 {
			time.Sleep(50 * time.Millisecond)
		}
		_, _ = fmt.Fprint(w, `{"ready":true,"state":"ready","serves_searches":true,"process_incarnation":"9aa977f3-4985-4cb5-9692-5ad1e3e243fe","warmup_result":"completed","warmup_timed_out":false}`)
	}))
	defer server.Close()
	if _, err := java.QualifySearch(context.Background(), server.Client(), server.URL, "node-a", 0); err == nil {
		t.Fatal("expired lease qualified after slow final read")
	}
}

func TestIngestQualificationRequiresStableProcessAndIngestRole(t *testing.T) {
	const first = "9aa977f3-4985-4cb5-9692-5ad1e3e243fe"
	const second = "59c0c3f1-6db8-4a58-a335-683cf0429c02"
	role, restart, reads := "ingest", false, 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if req.URL.Path == "/_serverless/stats" {
			_, _ = fmt.Fprintf(w, `{"node":"id","name":"writer","process_incarnation":%q,"roles":[%q],"lease":{"valid_now":true,"expires_in_millis":30000,"ttl_millis":30000}}`, first, role)
			return
		}
		reads++
		incarnation := first
		if restart && reads >= 4 {
			incarnation = second
		}
		_, _ = fmt.Fprintf(w, `{"ready":true,"state":"ready","serves_searches":false,"process_incarnation":%q,"warmup_result":"not_requested","warmup_timed_out":false}`, incarnation)
	}))
	defer server.Close()
	check := func() (java.IngestQualification, error) {
		reads = 0
		return java.QualifyIngest(context.Background(), server.Client(), server.URL, "writer", time.Second)
	}
	result, err := check()
	if err != nil || result.ProcessIncarnation != first || !result.StatsProcessVerified {
		t.Fatalf("valid ingest rejected: %#v %v", result, err)
	}
	role = "search"
	if _, err := check(); err == nil {
		t.Fatal("search-only process accepted")
	}
	role, restart = "ingest", true
	if _, err := check(); err == nil {
		t.Fatal("same-Pod JVM restart accepted")
	}
}
