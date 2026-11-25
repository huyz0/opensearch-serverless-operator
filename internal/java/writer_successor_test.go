package java

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestObserveWriterSuccessor(t *testing.T) {
	for _, tc := range []struct {
		name           string
		status         int
		body           string
		term           int64
		pending        bool
		requireSettled bool
	}{
		{"acknowledged", 200, `{"version":1,"node_id":"node","ephemeral_id":"process","index":"logs","index_uuid":"uuid","shard":0,"term":3}`, 3, false, false},
		{"previous term", 200, `{"version":1,"node_id":"node","ephemeral_id":"process","index":"logs","index_uuid":"uuid","shard":0,"term":2}`, 0, false, false},
		{"wrong process", 200, `{"version":1,"node_id":"node","ephemeral_id":"previous","index":"logs","index_uuid":"uuid","shard":0,"term":3}`, 0, false, false},
		{"recreated index", 200, `{"version":1,"node_id":"node","ephemeral_id":"process","index":"logs","index_uuid":"other","shard":0,"term":3}`, 0, false, false},
		{"pending", 404, `{"error":{"type":"writer_successor_pending"}}`, 0, true, false},
		{"unsupported image", 404, `{}`, 0, false, false},
		{"old evidence format", 200, `{"version":1,"node_id":"node","ephemeral_id":"process","index":"logs","index_uuid":"uuid","shard":0,"term":3}`, 0, true, true},
		{"predecessor WAL unsettled", 200, `{"version":1,"node_id":"node","ephemeral_id":"process","index":"logs","index_uuid":"uuid","shard":0,"term":3,"predecessor_wal_settled":false}`, 0, true, true},
		{"predecessor WAL settled", 200, `{"version":1,"node_id":"node","ephemeral_id":"process","index":"logs","index_uuid":"uuid","shard":0,"term":3,"predecessor_wal_settled":true}`, 3, false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				user, password, ok := r.BasicAuth()
				q := r.URL.Query()
				if !ok || user != "reader" || password != "credential" || r.Method != "GET" ||
					r.URL.Path != "/_serverless/writer_successor/node/process" ||
					q.Get("index") != "logs" || q.Get("index_uuid") != "uuid" || q.Get("shard") != "0" || q.Get("previous_term") != "2" {
					t.Error("request did not bind the exact reader, process and shard")
				}
				w.WriteHeader(tc.status)
				fmt.Fprint(w, tc.body)
			}))
			defer s.Close()
			term, err := ObserveWriterSuccessor(context.Background(), s.Client(), s.URL, WriterProcess{"node", "process"},
				WriterSuccessorShard{Index: "logs", IndexUUID: "uuid", Shard: 0, PreviousTerm: 2, RequireSettledWAL: tc.requireSettled},
				"reader", []byte("credential"))
			if term != tc.term || (err == nil) != (tc.term > 0) || errors.Is(err, ErrWriterSuccessorPending) != tc.pending {
				t.Fatalf("got term %d and error %v", term, err)
			}
		})
	}
}
