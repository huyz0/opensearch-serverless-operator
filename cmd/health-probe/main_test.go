package main

import (
	"context"
	"crypto/sha256"
	"encoding/pem"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"
)

func TestReadinessRequiresPublishedLeaseExpiry(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/_serverless/ready":
			_, _ = fmt.Fprint(w, `{"ready":true,"state":"ready","serves_searches":false}`)
		case "/_serverless/stats":
			_, _ = fmt.Fprint(w, `{"node":"id","name":"ingest-a","roles":["ingest"],"lease":{"valid_now":true,"ttl_millis":30000,"known_nodes":0}}`)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	err := checkReadiness(context.Background(), server.Client(), server.URL, "ingest-a", "ingest", time.Second, false)
	if err == nil || err.Error() != "local lease expiry is missing from the membership snapshot" {
		t.Fatalf("missing expiry did not hold readiness with the expected diagnosis: %v", err)
	}
}

func TestReadinessCanRequireWarmupCompletion(t *testing.T) {
	result := "running"
	timedOut := true
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/_serverless/stats" {
			_, _ = fmt.Fprint(w, `{"node":"id","name":"search-a","roles":["search","ingest"],"lease":{"valid_now":true,"expires_in_millis":30000,"ttl_millis":30000}}`)
			return
		}
		_, _ = fmt.Fprintf(w, `{"ready":true,"state":"ready","serves_searches":true,"process_incarnation":"9aa977f3-4985-4cb5-9692-5ad1e3e243fe","warmup_result":%q,"warmup_timed_out":%t}`, result, timedOut)
	}))
	defer server.Close()
	check := func(require bool) error {
		return checkReadiness(context.Background(), server.Client(), server.URL, "search-a", "search", time.Second, require)
	}
	if err := check(false); err != nil {
		t.Fatalf("serving-ready mode rejected ready process: %v", err)
	}
	if err := check(true); err == nil {
		t.Fatal("timed-out running warm-up was admitted")
	}
	result, timedOut = "degraded", false
	if err := check(true); err == nil {
		t.Fatal("degraded warm-up was admitted")
	}
	result = "completed"
	if err := check(true); err != nil {
		t.Fatalf("completed warm-up rejected: %v", err)
	}
	if err := checkReadiness(context.Background(), server.Client(), server.URL, "search-a", "ingest", time.Second, true); err == nil {
		t.Fatal("warm-up mode accepted for ingest role")
	}
}

func TestStrictHTTPSReadinessRequiresStatsIncarnation(t *testing.T) {
	const incarnation = "9aa977f3-4985-4cb5-9692-5ad1e3e243fe"
	var statsHasIncarnation atomic.Bool
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/_serverless/stats" {
			process := ""
			if statsHasIncarnation.Load() {
				process = incarnation
			}
			fmt.Fprintf(w, `{"node":"id","name":"search-a","roles":["search"],"process_incarnation":%q,"lease":{"valid_now":true,"expires_in_millis":30000,"ttl_millis":30000}}`, process)
			return
		}
		fmt.Fprintf(w, `{"ready":true,"state":"ready","serves_searches":true,"process_incarnation":%q,"warmup_result":"completed","warmup_timed_out":false}`, incarnation)
	}))
	defer server.Close()
	check := func() error {
		return checkReadiness(context.Background(), server.Client(), server.URL, "search-a", "search", time.Second, true)
	}
	if err := check(); err == nil {
		t.Fatal("strict HTTPS readiness accepted stats without an incarnation")
	}
	statsHasIncarnation.Store(true)
	if err := check(); err != nil {
		t.Fatalf("matching HTTPS stats did not qualify: %v", err)
	}
}

func TestPeerDNSRequiresOnlyThePodAddress(t *testing.T) {
	tests := []struct {
		name      string
		addresses []netip.Addr
		succeeds  bool
	}{
		{"matching", []netip.Addr{netip.MustParseAddr("10.1.2.3")}, true},
		{"duplicate matching records", []netip.Addr{netip.MustParseAddr("10.1.2.3"), netip.MustParseAddr("10.1.2.3")}, true},
		{"wrong", []netip.Addr{netip.MustParseAddr("10.1.2.4")}, false},
		{"multiple", []netip.Addr{netip.MustParseAddr("10.1.2.3"), netip.MustParseAddr("10.1.2.4")}, false},
		{"dual stack", []netip.Addr{netip.MustParseAddr("10.1.2.3"), netip.MustParseAddr("fd00::1")}, false},
		{"empty", nil, false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
			defer cancel()
			lookup := func(_ context.Context, network, host string) ([]netip.Addr, error) {
				if network != "ip" || host != "pod.peer.ns.svc.cluster.local." {
					t.Fatalf("unexpected lookup %s %s", network, host)
				}
				return test.addresses, nil
			}
			err := waitForPeerDNS(ctx, "pod.peer.ns.svc.cluster.local", "10.1.2.3", lookup)
			if (err == nil) != test.succeeds {
				t.Fatalf("unexpected result: %v", err)
			}
		})
	}
}

func TestHTTPSClientVerifiesIdentityAndRequiresCompleteInputs(t *testing.T) {
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) }))
	defer server.Close()
	ca := filepath.Join(t.TempDir(), "ca.pem")
	bundle := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw})
	if err := os.WriteFile(ca, bundle, 0600); err != nil {
		t.Fatal(err)
	}
	client, scheme, err := newClient(ca, "wrong.invalid", "", "", "")
	if err != nil || scheme != "https" {
		t.Fatalf("HTTPS configuration: %s %v", scheme, err)
	}
	if _, err := client.Get(server.URL); err == nil {
		t.Fatal("wrong server identity must fail")
	}
	client, _, err = newClient(ca, "127.0.0.1", "", "", fmt.Sprintf("%x", sha256.Sum256(bundle)))
	if err != nil {
		t.Fatal(err)
	}
	response, err := client.Get(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if _, _, err := newClient(ca, "127.0.0.1", "", "", fmt.Sprintf("%x", sha256.Sum256(append(bundle, '\n')))); err == nil {
		t.Fatal("a changed trust bundle was accepted")
	}
	for _, input := range [][4]string{{ca, "", "", ""}, {"", "localhost", "", ""}, {ca, "localhost", "cert", ""}} {
		if _, _, err := newClient(input[0], input[1], input[2], input[3], ""); err == nil {
			t.Fatal("incomplete HTTPS inputs accepted")
		}
	}
}
