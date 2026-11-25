package java

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
)

const (
	maxReceiptBytes  = 16 << 20
	maxReceiptShards = 16_384
)

var (
	writerProcessID = regexp.MustCompile(`^[A-Za-z0-9_-]{1,128}$`)
	// ErrWriterHandoffReceiptMissing means the authenticated endpoint confirmed absence for this process.
	ErrWriterHandoffReceiptMissing = errors.New("writer handoff receipt is missing")
)

// WriterProcess identifies the writer's Java node incarnation. The ephemeral ID comes from stats, independently of the readiness incarnation.
type WriterProcess struct {
	NodeID      string `json:"node_id"`
	EphemeralID string `json:"ephemeral_id"`
}

type WriterHandoffOutcome string

const (
	WriterHeadReleased        WriterHandoffOutcome = "RELEASED"
	WriterHeadReleasedWithWAL WriterHandoffOutcome = "RELEASED_WITH_WAL"
	WriterMemberReleased      WriterHandoffOutcome = "MEMBER_RELEASED"
	WriterReleaseUnconfirmed  WriterHandoffOutcome = "UNCONFIRMED"
)

// WriterShardHandoff is the outgoing writer's observation.
type WriterShardHandoff struct {
	Index, IndexUUID     string
	Shard                int32
	Term                 int64
	Hibernated           bool
	Outcome              WriterHandoffOutcome
	LocalCleanupComplete bool
}

// WriterHandoffReceipt records the exact target's shutdown observations.
type WriterHandoffReceipt struct {
	WriterProcess
	Shards []WriterShardHandoff
}

// GetWriterHandoffReceipt reads a receipt from a surviving Java process over direct HTTPS.
func GetWriterHandoffReceipt(ctx context.Context, client *http.Client, baseURL string, target WriterProcess, username string, password []byte) (WriterHandoffReceipt, error) {
	status, body, err := getWriterEvidence(ctx, client, baseURL, target, username, password, "/_serverless/writer_handoffs/", nil)
	if err != nil {
		return WriterHandoffReceipt{}, err
	}
	if status == http.StatusNotFound {
		var failure struct {
			Error struct {
				Type string `json:"type"`
			} `json:"error"`
		}
		if json.Unmarshal(body, &failure) == nil && failure.Error.Type == "writer_handoff_receipt_missing" {
			return WriterHandoffReceipt{}, ErrWriterHandoffReceiptMissing
		}
		return WriterHandoffReceipt{}, fmt.Errorf("writer handoff endpoint returned an unrecognized HTTP 404")
	}
	if status != http.StatusOK {
		return WriterHandoffReceipt{}, fmt.Errorf("writer handoff endpoint returned HTTP %d", status)
	}
	return decodeWriterHandoffReceipt(body, target)
}

func getWriterEvidence(ctx context.Context, client *http.Client, baseURL string, target WriterProcess, username string, password []byte, path string, query url.Values) (int, []byte, error) {
	path, err := writerProcessPath(path, target)
	if err != nil {
		return 0, nil, err
	}
	return requestWriterEvidence(ctx, client, baseURL, username, password, http.MethodGet, path, query)
}

func writerProcessPath(prefix string, target WriterProcess) (string, error) {
	if !writerProcessID.MatchString(target.NodeID) || !writerProcessID.MatchString(target.EphemeralID) {
		return "", fmt.Errorf("exact writer node and ephemeral IDs are required")
	}
	return prefix + target.NodeID + "/" + target.EphemeralID, nil
}

func requestWriterEvidence(ctx context.Context, client *http.Client, baseURL, username string, password []byte, method, path string, query url.Values) (int, []byte, error) {
	endpoint, err := url.Parse(baseURL)
	if err != nil || endpoint.Scheme != "https" || endpoint.Hostname() == "" || endpoint.User != nil ||
		endpoint.RawQuery != "" || endpoint.ForceQuery || endpoint.Fragment != "" || endpoint.RawPath != "" ||
		(endpoint.Path != "" && endpoint.Path != "/") {
		return 0, nil, fmt.Errorf("a direct HTTPS Java endpoint is required")
	}
	probe, err := exactBasicAuthClient(client, username, password)
	if err != nil {
		return 0, nil, err
	}
	endpoint.Path = path
	endpoint.RawQuery = query.Encode()
	request, err := http.NewRequestWithContext(ctx, method, endpoint.String(), nil)
	if err != nil {
		return 0, nil, fmt.Errorf("create writer evidence request: %w", err)
	}
	request.SetBasicAuth(username, string(password))
	request.Header.Set("Accept", "application/json")
	request.Header.Set("Cache-Control", "no-cache, no-store")
	response, err := probe.Do(request)
	if err != nil {
		return 0, nil, fmt.Errorf("read writer evidence: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK && response.StatusCode != http.StatusNotFound && response.StatusCode != http.StatusConflict {
		return 0, nil, fmt.Errorf("writer evidence endpoint returned HTTP %d", response.StatusCode)
	}
	body, err := io.ReadAll(io.LimitReader(response.Body, maxReceiptBytes+1))
	if err != nil || len(body) > maxReceiptBytes {
		return 0, nil, fmt.Errorf("read writer evidence failed or exceeded size limit")
	}
	return response.StatusCode, body, nil
}

func decodeWriterHandoffReceipt(body []byte, target WriterProcess) (WriterHandoffReceipt, error) {
	var encoded struct {
		Version int `json:"version"`
		WriterProcess
		Shards []struct {
			Index                string               `json:"index"`
			IndexUUID            string               `json:"index_uuid"`
			Shard                *int32               `json:"shard"`
			Term                 *int64               `json:"term"`
			Hibernated           *bool                `json:"hibernated"`
			Outcome              WriterHandoffOutcome `json:"outcome"`
			LocalCleanupComplete *bool                `json:"local_cleanup_complete"`
		} `json:"shards"`
	}
	if err := json.Unmarshal(body, &encoded); err != nil {
		return WriterHandoffReceipt{}, fmt.Errorf("decode writer handoff receipt: %w", err)
	}
	if encoded.Version != 1 || encoded.WriterProcess != target || encoded.Shards == nil || len(encoded.Shards) > maxReceiptShards {
		return WriterHandoffReceipt{}, fmt.Errorf("writer handoff receipt version, process or shard list is invalid")
	}
	type shardIdentity struct {
		index, uuid string
		shard       int32
	}
	seen := make(map[shardIdentity]struct{}, len(encoded.Shards))
	receipt := WriterHandoffReceipt{WriterProcess: target, Shards: make([]WriterShardHandoff, 0, len(encoded.Shards))}
	for _, shard := range encoded.Shards {
		if shard.Index == "" || shard.IndexUUID == "" || shard.Shard == nil || *shard.Shard < 0 ||
			shard.Term == nil || *shard.Term < -1 || shard.Hibernated == nil || shard.LocalCleanupComplete == nil {
			return WriterHandoffReceipt{}, fmt.Errorf("writer handoff receipt contains an incomplete or invalid shard")
		}
		switch shard.Outcome {
		case WriterHeadReleased, WriterHeadReleasedWithWAL, WriterMemberReleased:
			if *shard.Term < 0 {
				return WriterHandoffReceipt{}, fmt.Errorf("confirmed writer handoff requires a known term")
			}
		case WriterReleaseUnconfirmed:
		default:
			return WriterHandoffReceipt{}, fmt.Errorf("writer handoff receipt contains an unknown outcome")
		}
		identity := shardIdentity{shard.Index, shard.IndexUUID, *shard.Shard}
		if _, duplicate := seen[identity]; duplicate {
			return WriterHandoffReceipt{}, fmt.Errorf("writer handoff receipt contains a duplicate shard")
		}
		seen[identity] = struct{}{}
		receipt.Shards = append(receipt.Shards, WriterShardHandoff{
			Index: shard.Index, IndexUUID: shard.IndexUUID, Shard: *shard.Shard, Term: *shard.Term,
			Hibernated: *shard.Hibernated, Outcome: shard.Outcome, LocalCleanupComplete: *shard.LocalCleanupComplete,
		})
	}
	return receipt, nil
}
