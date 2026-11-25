package java

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"unicode"
)

var ErrWriterHeadInventoryLimit = errors.New("writer head inventory exceeds the journal limit")

// WriterHead is a physical shard or bucket ownership register.
type WriterHead struct {
	Key              string
	BucketID         string
	Index            string
	IndexUUID        string
	Shard            int32
	Term             int64
	OwnerEphemeralID string // Empty means the head has a legacy node-ID-only owner.
}

// writerHeadPage describes one bounded page of a non-snapshot scan.
// An empty page can still have a continuation cursor.
type writerHeadPage struct {
	Heads     []WriterHead
	NextAfter string
}

// GetWriterHeadInventory walks the durable registers through a survivor on the same store.
// Close the target's writer admission first. A failed walk returns no partial inventory.
func GetWriterHeadInventory(ctx context.Context, client *http.Client, baseURL string, target WriterProcess, username string, password []byte) ([]WriterHead, error) {
	return getWriterHeadInventory(ctx, client, baseURL, target, username, password, 0)
}

// GetBoundedWriterHeadInventory performs the same complete scan with an output
// limit. Reaching the limit never returns an incomplete inventory as evidence.
func GetBoundedWriterHeadInventory(ctx context.Context, client *http.Client, baseURL string, target WriterProcess, username string, password []byte, limit int) ([]WriterHead, error) {
	if limit <= 0 {
		return nil, fmt.Errorf("a positive writer head inventory limit is required")
	}
	return getWriterHeadInventory(ctx, client, baseURL, target, username, password, limit)
}

func getWriterHeadInventory(ctx context.Context, client *http.Client, baseURL string, target WriterProcess, username string, password []byte, limit int) ([]WriterHead, error) {
	heads := make([]WriterHead, 0)
	cursors := make(map[string]struct{})
	keys := make(map[string]struct{})
	for after := ""; ; {
		page, err := getWriterHeadPage(ctx, client, baseURL, target, after, username, password)
		if err != nil {
			return nil, err
		}
		if limit > 0 && len(page.Heads) > limit-len(heads) {
			return nil, fmt.Errorf("%w of %d heads", ErrWriterHeadInventoryLimit, limit)
		}
		for _, head := range page.Heads {
			if _, seen := keys[head.Key]; seen {
				return nil, fmt.Errorf("writer head inventory repeated a register across pages")
			}
			keys[head.Key] = struct{}{}
			heads = append(heads, head)
		}
		if page.NextAfter == "" {
			return heads, nil
		}
		if _, seen := cursors[page.NextAfter]; seen {
			return nil, fmt.Errorf("writer head inventory repeated a cursor")
		}
		cursors[page.NextAfter] = struct{}{}
		after = page.NextAfter
	}
}

func getWriterHeadPage(ctx context.Context, client *http.Client, baseURL string, target WriterProcess, after, username string, password []byte) (writerHeadPage, error) {
	query := url.Values{}
	if after != "" {
		if !validWriterHeadCursor(after) {
			return writerHeadPage{}, fmt.Errorf("invalid writer head cursor")
		}
		query.Set("after", after)
	}
	status, body, err := getWriterEvidence(ctx, client, baseURL, target, username, password, "/_serverless/writer_heads/", query)
	if err != nil {
		return writerHeadPage{}, err
	}
	if status != http.StatusOK {
		return writerHeadPage{}, fmt.Errorf("writer head inventory returned HTTP %d", status)
	}
	return decodeWriterHeadPage(body, target, after)
}

func decodeWriterHeadPage(body []byte, target WriterProcess, after string) (writerHeadPage, error) {
	var wire struct {
		Version int `json:"version"`
		WriterProcess
		After     json.RawMessage `json:"after"`
		NextAfter json.RawMessage `json:"next_after"`
		Heads     []struct {
			Key              string `json:"key"`
			BucketID         string `json:"bucket_id"`
			Index            string `json:"index"`
			IndexUUID        string `json:"index_uuid"`
			Shard            *int32 `json:"shard"`
			Term             *int64 `json:"term"`
			OwnerEphemeralID string `json:"owner_ephemeral_id"`
		} `json:"heads"`
	}
	if err := json.Unmarshal(body, &wire); err != nil {
		return writerHeadPage{}, fmt.Errorf("decode writer head inventory: %w", err)
	}
	var observedAfter, nextAfter *string
	if wire.Version != 1 || wire.WriterProcess != target || wire.Heads == nil || len(wire.Heads) > 32 ||
		json.Unmarshal(wire.After, &observedAfter) != nil ||
		(observedAfter == nil && after != "") || (observedAfter != nil && (*observedAfter == "" || *observedAfter != after)) ||
		json.Unmarshal(wire.NextAfter, &nextAfter) != nil ||
		(nextAfter != nil && (*nextAfter == after || !validWriterHeadCursor(*nextAfter))) {
		return writerHeadPage{}, fmt.Errorf("writer head inventory version, process, cursor or page is invalid")
	}
	page := writerHeadPage{Heads: make([]WriterHead, 0, len(wire.Heads))}
	if nextAfter != nil {
		page.NextAfter = *nextAfter
	}
	for _, head := range wire.Heads {
		if !validWriterHeadCursor(head.Key) || head.Key == after || head.Index == "" ||
			head.Shard == nil || *head.Shard < 0 || head.Term == nil || *head.Term < 1 ||
			(head.OwnerEphemeralID != "" && head.OwnerEphemeralID != target.EphemeralID) {
			return writerHeadPage{}, fmt.Errorf("writer head inventory contains an invalid head")
		}
		if head.BucketID != "" {
			if head.Index != "#bucket#"+head.BucketID || *head.Shard != 0 || head.IndexUUID != "" {
				return writerHeadPage{}, fmt.Errorf("writer head inventory contains an invalid bucket head")
			}
		} else if strings.HasPrefix(head.Index, "#bucket#") {
			return writerHeadPage{}, fmt.Errorf("writer head inventory omits the bucket identity")
		}
		page.Heads = append(page.Heads, WriterHead{
			Key: head.Key, BucketID: head.BucketID, Index: head.Index, IndexUUID: head.IndexUUID,
			Shard: *head.Shard, Term: *head.Term, OwnerEphemeralID: head.OwnerEphemeralID,
		})
	}
	return page, nil
}

func validWriterHeadCursor(cursor string) bool {
	return cursor != "" && len(cursor) <= 1024 && strings.IndexFunc(cursor, unicode.IsControl) < 0
}
