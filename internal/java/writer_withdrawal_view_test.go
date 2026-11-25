package java

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestWriterWithdrawalViewBindsBothProcessesAndExpiry(t *testing.T) {
	observer := WriterProcess{NodeID: "observer", EphemeralID: "observer-process"}
	writer := WriterProcess{NodeID: "writer", EphemeralID: "writer-process"}
	for _, scenario := range []struct {
		name, observerEpoch, writerEpoch, state string
		want                                    time.Duration
		fail                                    bool
	}{
		{name: "live marker", state: `"withdrawn":true,"lease_remaining_millis":6500`, want: 6500 * time.Millisecond},
		{name: "pending", state: `"withdrawn":false,"lease_remaining_millis":0`},
		{name: "stale observer", observerEpoch: "old-observer", state: `"withdrawn":true,"lease_remaining_millis":6500`, fail: true},
		{name: "stale writer", writerEpoch: "old-writer", state: `"withdrawn":true,"lease_remaining_millis":6500`, fail: true},
		{name: "missing state", state: `"lease_remaining_millis":0`, fail: true},
		{name: "missing expiry", state: `"withdrawn":false`, fail: true},
		{name: "expired marker", state: `"withdrawn":true,"lease_remaining_millis":0`, fail: true},
		{name: "contradictory state", state: `"withdrawn":false,"lease_remaining_millis":6500`, fail: true},
		{name: "expiry overflow", state: `"withdrawn":true,"lease_remaining_millis":9223372036854775807`, fail: true},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			observerEpoch, writerEpoch := observer.EphemeralID, writer.EphemeralID
			if scenario.observerEpoch != "" {
				observerEpoch = scenario.observerEpoch
			}
			if scenario.writerEpoch != "" {
				writerEpoch = scenario.writerEpoch
			}
			server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				user, pass, auth := r.BasicAuth()
				if r.Method != http.MethodGet || r.URL.Path != "/_serverless/writer_withdrawal_view/observer/observer-process/writer/writer-process" ||
					!auth || user != "reader" || pass != "password" {
					t.Error("expected an authenticated GET to the exact observer and writer")
					w.WriteHeader(http.StatusForbidden)
					return
				}
				fmt.Fprintf(w, `{"version":1,"observer":{"node_id":"observer","ephemeral_id":%q},"writer":{"node_id":"writer","ephemeral_id":%q},%s}`, observerEpoch, writerEpoch, scenario.state)
			}))
			defer server.Close()
			remaining, err := ObserveWriterWithdrawalView(context.Background(), server.Client(), server.URL, observer, writer, "reader", []byte("password"))
			if remaining != scenario.want || (err != nil) != scenario.fail {
				t.Fatalf("observed %v, %v, want %v, failure=%v", remaining, err, scenario.want, scenario.fail)
			}
		})
	}
}
