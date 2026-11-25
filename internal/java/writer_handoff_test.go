package java

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestReadWriterHandoffReceipt(t *testing.T) {
	target := WriterProcess{NodeID: "node-a", EphemeralID: "previous-process"}
	const shard = `{"index":"logs","index_uuid":"logs-1","shard":0,"term":5,"hibernated":false,"outcome":"RELEASED_WITH_WAL","local_cleanup_complete":true}`
	const receipt = `{"version":1,"node_id":"node-a","ephemeral_id":"previous-process","shards":[` + shard + `]}`
	for _, tc := range []struct {
		name    string
		status  int
		body    string
		missing bool
		pass    bool
	}{
		{"exact receipt", http.StatusOK, receipt, false, true},
		{"wrong process", http.StatusOK, strings.Replace(receipt, "previous-process", "new-process", 1), false, false},
		{"unknown confirmed term", http.StatusOK, strings.Replace(receipt, `"term":5`, `"term":-1`, 1), false, false},
		{"duplicate shard", http.StatusOK, strings.Replace(receipt, shard, shard+","+shard, 1), false, false},
		{"missing cleanup flag", http.StatusOK, strings.Replace(receipt, `,"local_cleanup_complete":true`, "", 1), false, false},
		{"reported absence", http.StatusNotFound, `{"error":{"type":"writer_handoff_receipt_missing"}}`, true, false},
		{"unknown route", http.StatusNotFound, `{"error":"No handler"}`, false, false},
		{"forbidden", http.StatusForbidden, "", false, false},
		{"redirect", http.StatusTemporaryRedirect, "", false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != http.MethodGet || r.URL.EscapedPath() != "/_serverless/writer_handoffs/node-a/previous-process" {
					t.Errorf("unexpected request %s %s", r.Method, r.URL.EscapedPath())
					w.WriteHeader(http.StatusBadRequest)
					return
				}
				if user, password, ok := r.BasicAuth(); !ok || user != "reader" || password != "credential" {
					t.Error("request omitted the configured credential")
				}
				if tc.status == http.StatusTemporaryRedirect {
					w.Header().Set("Location", "/another-endpoint")
				}
				w.WriteHeader(tc.status)
				_, _ = w.Write([]byte(tc.body))
			}))
			defer server.Close()
			got, err := GetWriterHandoffReceipt(context.Background(), server.Client(), server.URL, target, "reader", []byte("credential"))
			if errors.Is(err, ErrWriterHandoffReceiptMissing) != tc.missing {
				t.Fatalf("unexpected receipt absence: %v", err)
			}
			if !tc.pass {
				if err == nil || len(got.Shards) != 0 {
					t.Fatalf("unexpected receipt: %+v, %v", got, err)
				}
				return
			}
			if err != nil || got.WriterProcess != target || len(got.Shards) != 1 ||
				got.Shards[0].Outcome != WriterHeadReleasedWithWAL || got.Shards[0].Term != 5 {
				t.Fatalf("incorrect receipt: %+v, %v", got, err)
			}
		})
	}
}

func TestWriterHandoffReceiptRefusesPlainHTTP(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Error("receipt request used plaintext")
	}))
	defer server.Close()
	target := WriterProcess{NodeID: "node-a", EphemeralID: "previous-process"}
	if _, err := GetWriterHandoffReceipt(context.Background(), server.Client(), server.URL, target, "reader", []byte("credential")); err == nil {
		t.Fatal("receipt read accepted plain HTTP")
	}
}
