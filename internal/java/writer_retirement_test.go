package java

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestTimedWriterPreparationBindsDeadlineAndNeverFallsBackToUntimedPath(t *testing.T) {
	deadline := time.Now().Add(time.Minute).Truncate(time.Millisecond)
	target := WriterProcess{NodeID: "node", EphemeralID: "process"}
	wantPath := fmt.Sprintf("/_serverless/writer_retirement/node/process/operation/prepare_before/%d", deadline.UnixMilli())
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != wantPath {
			t.Errorf("prepare used an unexpected path or method: %s %s", r.Method, r.URL.Path)
		}
		http.NotFound(w, r)
	}))
	defer server.Close()
	if state, err := PrepareWriterRetirementBefore(context.Background(), server.Client(), server.URL, target, "operation", "retirer", []byte("password"), deadline); state != "" || err == nil {
		t.Fatalf("missing timed handler returned %q, %v", state, err)
	}
}

func TestWriterRetirementRequiresExactProcessOperationAndState(t *testing.T) {
	ctx := context.Background()
	process := WriterProcess{NodeID: "node", EphemeralID: "process"}
	for _, scenario := range []struct {
		name, operation, state string
		status                 int
		prepare, conflict      bool
		want                   WriterRetirementState
	}{
		{"observe open", "operation", "open", 200, false, false, WriterAdmissionOpen},
		{"observe withdrawn", "operation", "withdrawn", 200, false, false, WriterTrafficWithdrawn},
		{"prepare closing", "operation", "closing", 200, true, false, WriterAdmissionClosing},
		{"prepare must close admission", "operation", "withdrawn", 200, true, false, ""},
		{"observe closed", "operation", "closed", 200, false, false, WriterAdmissionClosed},
		{"wrong token", "another", "closed", 200, false, false, ""},
		{"open after mutation is invalid", "operation", "open", 200, true, false, ""},
		{"conflicting reservation", "operation", "", 409, true, true, ""},
		{"unknown proxy conflict", "operation", "", 409, false, false, ""},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				method := http.MethodGet
				if scenario.prepare {
					method = http.MethodPost
				}
				user, password, authenticated := r.BasicAuth()
				if r.Method != method || r.URL.Path != "/_serverless/writer_retirement/node/process/operation" ||
					!authenticated || user != "prepare" || password != "credential" {
					t.Error("incorrect retirement request")
					w.WriteHeader(http.StatusBadRequest)
					return
				}
				w.WriteHeader(scenario.status)
				if scenario.conflict {
					fmt.Fprint(w, `{"error":{"type":"writer_admission_already_reserved"}}`)
					return
				}
				fmt.Fprintf(w, `{"version":1,"node_id":"node","ephemeral_id":"process","operation_id":%q,"state":%q}`, scenario.operation, scenario.state)
			}))
			defer server.Close()
			call := GetWriterRetirementState
			if scenario.prepare {
				call = PrepareWriterRetirement
			}
			got, err := call(ctx, server.Client(), server.URL, process, "operation", "prepare", []byte("credential"))
			if got != scenario.want || (err != nil) != (scenario.want == "") || errors.Is(err, ErrWriterRetirementConflict) != scenario.conflict {
				t.Fatalf("unexpected retirement result %q, %v", got, err)
			}
		})
	}
}

func TestWithdrawWriterTrafficUsesTheExactOperation(t *testing.T) {
	target := WriterProcess{NodeID: "node", EphemeralID: "process"}
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/_serverless/writer_withdrawal/node/process/operation" {
			t.Error("incorrect withdrawal request")
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		fmt.Fprint(w, `{"version":1,"node_id":"node","ephemeral_id":"process","operation_id":"operation","state":"withdrawn"}`)
	}))
	defer server.Close()
	got, err := WithdrawWriterTraffic(context.Background(), server.Client(), server.URL, target, "operation", "user", []byte("password"))
	if err != nil || got != WriterTrafficWithdrawn {
		t.Fatalf("withdrawal response = %q, %v", got, err)
	}
}

func TestTimedWriterWithdrawalDoesNotFallBackToUntimedPath(t *testing.T) {
	deadline := time.Now().Add(time.Minute).Truncate(time.Millisecond)
	target := WriterProcess{NodeID: "node", EphemeralID: "process"}
	wantPath := fmt.Sprintf("/_serverless/writer_withdrawal/node/process/operation/withdraw_before/%d", deadline.UnixMilli())
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		user, password, authenticated := r.BasicAuth()
		if r.Method != http.MethodPost || r.URL.Path != wantPath || !authenticated || user != "retirer" || password != "password" {
			t.Errorf("unexpected authenticated withdrawal request: %s %s", r.Method, r.URL.Path)
		}
		http.NotFound(w, r)
	}))
	defer server.Close()
	if state, err := WithdrawWriterTrafficBefore(context.Background(), server.Client(), server.URL, target, "operation", "retirer", []byte("password"), deadline); state != "" || err == nil {
		t.Fatalf("missing timed withdrawal handler returned %q, %v", state, err)
	}
	if state, err := WithdrawWriterTrafficBefore(context.Background(), server.Client(), server.URL, target, "operation", "retirer", []byte("password"), time.Now().Add(-time.Second)); state != "" || err == nil {
		t.Fatalf("expired withdrawal returned %q, %v", state, err)
	}
}
