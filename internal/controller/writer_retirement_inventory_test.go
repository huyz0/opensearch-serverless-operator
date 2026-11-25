package controller

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"k8s.io/apimachinery/pkg/types"

	"github.com/huyz0/opensearch-serverless-operator/api/v1alpha1"
	"github.com/huyz0/opensearch-serverless-operator/internal/java"
)

func TestWriterInventoryObserverRotatesOnFailure(t *testing.T) {
	ready := []types.UID{"a", "b", "c"}
	var journal v1alpha1.WriterRetirementStatus
	for _, want := range []types.UID{"a", "b", "c", "a"} {
		uid, next, err := writerInventoryObserver(ready, journal.InventoryObserverOffset)
		if err != nil || uid != want {
			t.Fatalf("next inventory observer = %q, want %q: %v", uid, want, err)
		}
		failure := &writerInventoryObserverError{cause: context.DeadlineExceeded, next: next}
		if !errors.Is(failure, context.DeadlineExceeded) {
			t.Fatal("observer failure must preserve timeout classification")
		}
		rememberInventoryObserverFailure(&journal, failure)
	}
	rememberInventoryObserverFailure(&journal, java.ErrWriterHeadInventoryLimit)
	if journal.InventoryObserverOffset != 1 {
		t.Fatal("an oversized inventory must not be treated as observer failure")
	}
}

func TestRetirementInventoryRecordsPhysicalHeadsAndRejectsAmbiguity(t *testing.T) {
	victim := java.WriterProcess{NodeID: "writer", EphemeralID: "process"}
	shard := java.WriterHead{Key: "z", Index: "docs", IndexUUID: "uuid", Shard: 2, Term: 7, OwnerEphemeralID: victim.EphemeralID}
	bucket := java.WriterHead{Key: "a", BucketID: "bucket", Index: "#bucket#bucket", Term: 9, OwnerEphemeralID: victim.EphemeralID}
	heads, err := retirementInventoryEntries([]java.WriterHead{shard, bucket}, victim)
	if err != nil || len(heads) != 2 || heads[0].BucketID != "bucket" || heads[0].Term != 9 ||
		heads[1].IndexUUID != "uuid" || heads[1].Shard != 2 || heads[1].Term != 7 {
		t.Fatalf("unexpected journal inventory %#v, %v", heads, err)
	}
	if empty, err := retirementInventoryEntries([]java.WriterHead{}, victim); err != nil || empty == nil || len(empty) != 0 {
		t.Fatalf("complete inventory with no owned heads must be representable: %#v, %v", empty, err)
	}
	legacy := shard
	legacy.OwnerEphemeralID = ""
	unidentifiedIndex := shard
	unidentifiedIndex.IndexUUID = ""
	for _, invalid := range [][]java.WriterHead{nil, {legacy}, {unidentifiedIndex}, {shard, shard}} {
		if got, err := retirementInventoryEntries(invalid, victim); got != nil || err == nil {
			t.Fatalf("ambiguous inventory was retained: %#v, %v", got, err)
		}
	}
}

func TestOpenWriterHeadScanRejectsBucketAndIncompleteInventory(t *testing.T) {
	for _, tc := range []struct {
		name     string
		response string
		want     error
	}{
		{"independent", `{"version":1,"node_id":"writer","ephemeral_id":"jvm","after":null,"next_after":null,"heads":[
			{"key":"head","index":"docs","index_uuid":"uuid","shard":0,"term":1,"owner_ephemeral_id":"jvm"}]}`, nil},
		{"shared bucket", `{"version":1,"node_id":"writer","ephemeral_id":"jvm","after":null,"next_after":null,"heads":[
			{"key":"bucket","index":"#bucket#b1","bucket_id":"b1","shard":0,"term":1,"owner_ephemeral_id":"jvm"}]}`, errWriterEvictionUnsupportedHeads},
		{"incomplete", `{"version":1,"node_id":"writer","ephemeral_id":"jvm","after":null}`, errWriterPreflightInventory},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				user, password, ok := r.BasicAuth()
				if r.Method != http.MethodGet || r.URL.Path != "/_serverless/writer_heads/writer/jvm" ||
					!ok || user != "reader" || password != "secret" {
					t.Errorf("unexpected preflight request: %s %s", r.Method, r.URL.Path)
					w.WriteHeader(http.StatusBadRequest)
					return
				}
				fmt.Fprint(w, tc.response)
			}))
			defer server.Close()
			err := checkOpenWriterHeads(context.Background(), server.Client(), server.URL, java.WriterProcess{NodeID: "writer", EphemeralID: "jvm"}, "reader", []byte("secret"))
			if !errors.Is(err, tc.want) {
				t.Fatalf("preflight error = %v, want %v", err, tc.want)
			}
		})
	}
}
