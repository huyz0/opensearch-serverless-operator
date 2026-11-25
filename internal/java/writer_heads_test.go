package java

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestWriterHeadInventoryContinuesAcrossAnEmptyPage(t *testing.T) {
	target := WriterProcess{NodeID: "node-a", EphemeralID: "old-process"}
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		user, password, authenticated := r.BasicAuth()
		if !authenticated || user != "reader" || password != "credential" || r.URL.Path != "/_serverless/writer_heads/node-a/old-process" {
			t.Error("incorrect writer inventory endpoint or credential")
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		switch r.URL.Query().Get("after") {
		case "":
			fmt.Fprint(w, `{"version":1,"node_id":"node-a","ephemeral_id":"old-process","after":null,"next_after":"40-other#1","heads":[]}`)
		case "40-other#1":
			fmt.Fprint(w, `{"version":1,"node_id":"node-a","ephemeral_id":"old-process","after":"40-other#1","next_after":null,"heads":[
				{"key":"62-#bucket#streams#0","bucket_id":"streams","index":"#bucket#streams","index_uuid":null,"shard":0,"term":3,"owner_ephemeral_id":"old-process"},
				{"key":"fc-legacy#1","bucket_id":null,"index":"legacy","index_uuid":null,"shard":1,"term":7,"owner_ephemeral_id":null}]}`)
		default:
			t.Error("wrong continuation cursor")
			w.WriteHeader(http.StatusBadRequest)
		}
	}))
	defer server.Close()

	heads, err := GetWriterHeadInventory(context.Background(), server.Client(), server.URL, target, "reader", []byte("credential"))
	if err != nil {
		t.Fatal(err)
	}
	if len(heads) != 2 || heads[0].BucketID != "streams" || heads[0].OwnerEphemeralID != target.EphemeralID || heads[1].OwnerEphemeralID != "" {
		t.Fatalf("wrong bucket or legacy ownership: %+v", heads)
	}
	if bounded, err := GetBoundedWriterHeadInventory(context.Background(), server.Client(), server.URL, target, "reader", []byte("credential"), 1); err == nil || bounded != nil {
		t.Fatalf("oversized scan returned partial evidence: %+v, %v", bounded, err)
	}
}

func TestWriterHeadInventoryDiscardsPartialResults(t *testing.T) {
	target := WriterProcess{NodeID: "node-a", EphemeralID: "old-process"}
	for name, followup := range map[string]string{
		"missing end marker": `{"version":1,"node_id":"node-a","ephemeral_id":"old-process","after":"a-head#0","heads":[]}`,
		"empty end marker":   `{"version":1,"node_id":"node-a","ephemeral_id":"old-process","after":"a-head#0","next_after":"","heads":[]}`,
		"wrong cursor":       `{"version":1,"node_id":"node-a","ephemeral_id":"old-process","after":null,"next_after":null,"heads":[]}`,
		"cursor repeats":     `{"version":1,"node_id":"node-a","ephemeral_id":"old-process","after":"a-head#0","next_after":"a-head#0","heads":[]}`,
		"wrong process":      `{"version":1,"node_id":"node-a","ephemeral_id":"replacement","after":"a-head#0","next_after":null,"heads":[]}`,
	} {
		t.Run(name, func(t *testing.T) {
			server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Query().Get("after") == "" {
					fmt.Fprint(w, `{"version":1,"node_id":"node-a","ephemeral_id":"old-process","after":null,"next_after":"a-head#0","heads":[
						{"key":"a-head#0","index":"head","index_uuid":"head-1","shard":0,"term":1,"owner_ephemeral_id":"old-process"}]}`)
					return
				}
				fmt.Fprint(w, followup)
			}))
			defer server.Close()
			got, err := GetWriterHeadInventory(context.Background(), server.Client(), server.URL, target, "reader", []byte("credential"))
			if err == nil || got != nil {
				t.Fatalf("incomplete inventory accepted: %+v, %v", got, err)
			}
		})
	}
}
