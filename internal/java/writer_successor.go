package java

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
)

var ErrWriterSuccessorPending = errors.New("the exact successor has no newer-term write evidence")

// WriterSuccessorShard describes the retired owner's independent shard and term.
type WriterSuccessorShard struct {
	Index, IndexUUID  string
	Shard             int32
	PreviousTerm      int64
	RequireSettledWAL bool
}

// ObserveWriterSuccessor returns the active newer term that has accepted a
// foreground write. It never triggers shard activation or writes a document.
func ObserveWriterSuccessor(ctx context.Context, client *http.Client, baseURL string, successor WriterProcess, shard WriterSuccessorShard, username string, password []byte) (int64, error) {
	if shard.Index == "" || len(shard.Index) > 1024 || strings.HasPrefix(shard.Index, "#bucket#") ||
		!writerProcessID.MatchString(shard.IndexUUID) || shard.Shard < 0 || shard.PreviousTerm < 1 {
		return 0, fmt.Errorf("an exact independent shard and previous term are required")
	}
	query := url.Values{
		"index": {shard.Index}, "index_uuid": {shard.IndexUUID},
		"shard": {strconv.FormatInt(int64(shard.Shard), 10)}, "previous_term": {strconv.FormatInt(shard.PreviousTerm, 10)},
	}
	status, body, err := getWriterEvidence(ctx, client, baseURL, successor, username, password, "/_serverless/writer_successor/", query)
	if err != nil {
		return 0, err
	}
	if status == http.StatusNotFound {
		var failure struct {
			Error struct {
				Type string `json:"type"`
			} `json:"error"`
		}
		if json.Unmarshal(body, &failure) == nil && failure.Error.Type == "writer_successor_pending" {
			return 0, ErrWriterSuccessorPending
		}
	}
	if status != http.StatusOK {
		return 0, fmt.Errorf("writer successor endpoint returned HTTP %d", status)
	}
	var evidence struct {
		Version int `json:"version"`
		WriterProcess
		Index                 string `json:"index"`
		IndexUUID             string `json:"index_uuid"`
		Shard                 *int32 `json:"shard"`
		Term                  int64  `json:"term"`
		PredecessorWALSettled bool   `json:"predecessor_wal_settled"`
	}
	if err := json.Unmarshal(body, &evidence); err != nil {
		return 0, fmt.Errorf("decode writer successor evidence: %w", err)
	}
	if evidence.Version != 1 || evidence.WriterProcess != successor || evidence.Index != shard.Index ||
		evidence.IndexUUID != shard.IndexUUID || evidence.Shard == nil || *evidence.Shard != shard.Shard || evidence.Term <= shard.PreviousTerm {
		return 0, fmt.Errorf("writer successor evidence does not match the expected process, shard or term")
	}
	if shard.RequireSettledWAL && !evidence.PredecessorWALSettled {
		return 0, ErrWriterSuccessorPending
	}
	return evidence.Term, nil
}
