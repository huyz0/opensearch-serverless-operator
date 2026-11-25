package java

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// Lease is a process-local observation from the shell's stats endpoint.
// It does not prove peer reachability or successful search cache warming.
type Lease struct {
	NodeID              string
	NodeEphemeralID     string
	NodeName            string
	ProcessIncarnation  string
	Valid               bool
	ExpiresInMillis     int64
	TTLMillis           int64
	Roles               []string
	LocalSearchActivity *LocalSearchActivity
}

// LocalSearchActivity contains the local reader gauge and cumulative process
// cache counters. A nil observation represents an incomplete stats payload.
type LocalSearchActivity struct {
	ShardReaders      int64
	BlockCacheHits    int64
	BlockCacheMisses  int64
	BlockBytesFetched int64
}

// HasRole reports the local roles included in the stats response. Missing
// roles are unknown, not proof that the process serves a role.
func (lease Lease) HasRole(role string) bool {
	for _, observed := range lease.Roles {
		if observed == role {
			return true
		}
	}
	return false
}

// EvidenceValidUntil bounds a sample from the start of its request. The
// pinned shell stops trusting a local lease TTL/32 before its stamped expiry.
func (lease Lease) EvidenceValidUntil(start time.Time, minimumMargin time.Duration) time.Time {
	const maxMillis = math.MaxInt64 / int64(time.Millisecond)
	skewMillis := lease.TTLMillis / 32
	if !lease.Valid || lease.TTLMillis <= 0 || skewMillis > maxMillis || lease.ExpiresInMillis <= 0 || lease.ExpiresInMillis > maxMillis {
		return time.Time{}
	}
	remaining := time.Duration(lease.ExpiresInMillis)*time.Millisecond - max(minimumMargin, time.Duration(skewMillis)*time.Millisecond)
	if remaining <= 0 {
		return time.Time{}
	}
	return start.Add(remaining)
}

// GetLease fails closed when the metadata plane or a complete local lease
// observation is unavailable. The caller supplies transport and credentials.
func GetLease(ctx context.Context, client *http.Client, baseURL string) (Lease, error) {
	return getLease(ctx, client, baseURL, "", nil)
}

// GetLeaseWithBasicAuth attaches a credential only to this exact HTTPS stats
// request. The caller supplies the transport and CA used to verify the route.
func GetLeaseWithBasicAuth(ctx context.Context, client *http.Client, baseURL, username string, password []byte) (Lease, error) {
	probe, err := exactBasicAuthClient(client, username, password)
	if err != nil {
		return Lease{}, err
	}
	return getLease(ctx, probe, baseURL, username, password)
}

func getLease(ctx context.Context, client *http.Client, baseURL, username string, password []byte) (Lease, error) {
	var result Lease
	endpoint, err := url.Parse(baseURL)
	if err != nil || endpoint == nil || (endpoint.Scheme != "http" && endpoint.Scheme != "https") || endpoint.Host == "" || endpoint.User != nil || endpoint.RawQuery != "" || endpoint.Fragment != "" || client == nil {
		return result, fmt.Errorf("valid Java endpoint and HTTP client are required")
	}
	if username != "" && endpoint.Scheme != "https" {
		return result, fmt.Errorf("lease credential requires HTTPS")
	}
	endpoint.Path = strings.TrimRight(endpoint.Path, "/") + "/_serverless/stats"
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint.String(), nil)
	if err != nil {
		return result, fmt.Errorf("create lease request: %w", err)
	}
	request.Header.Set("Cache-Control", "no-cache, no-store")
	if username != "" {
		request.SetBasicAuth(username, string(password))
	}
	response, err := client.Do(request)
	if err != nil {
		return result, fmt.Errorf("read lease: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return result, fmt.Errorf("stats returned HTTP %d", response.StatusCode)
	}
	body, err := io.ReadAll(io.LimitReader(response.Body, (2<<20)+1))
	if err != nil || len(body) > 2<<20 {
		return result, fmt.Errorf("read stats failed or exceeded size limit")
	}
	var payload struct {
		Node               string          `json:"node"`
		NodeEphemeralID    string          `json:"node_ephemeral_id"`
		Name               string          `json:"name"`
		ProcessIncarnation string          `json:"process_incarnation"`
		Roles              []string        `json:"roles"`
		Shards             json.RawMessage `json:"shards"`
		BlockCache         json.RawMessage `json:"block_cache"`
		Lease              *struct {
			Valid     *bool  `json:"valid_now"`
			ExpiresIn *int64 `json:"expires_in_millis"`
			TTL       *int64 `json:"ttl_millis"`
		} `json:"lease"`
	}
	if err := json.Unmarshal(body, &payload); err != nil {
		return result, fmt.Errorf("decode stats: %w", err)
	}
	if payload.Node == "" || payload.Name == "" || payload.Lease == nil || payload.Lease.Valid == nil || payload.Lease.TTL == nil || *payload.Lease.TTL <= 0 {
		return result, fmt.Errorf("local lease observation is incomplete")
	}
	if payload.Lease.ExpiresIn == nil {
		// Java can report validity before the first renewal. Readiness still
		// requires an expiry from the node's observed membership record.
		return result, fmt.Errorf("local lease expiry is missing from the membership snapshot")
	}
	return Lease{
		NodeID: payload.Node, NodeEphemeralID: payload.NodeEphemeralID, NodeName: payload.Name, ProcessIncarnation: payload.ProcessIncarnation,
		Valid: *payload.Lease.Valid, ExpiresInMillis: *payload.Lease.ExpiresIn, TTLMillis: *payload.Lease.TTL, Roles: payload.Roles,
		LocalSearchActivity: readLocalSearchActivity(payload.Shards, payload.BlockCache),
	}, nil
}

func readLocalSearchActivity(shards, blockCache json.RawMessage) *LocalSearchActivity {
	var local struct {
		Readers *int64 `json:"readers"`
	}
	var cache struct {
		Hits         *int64 `json:"hits"`
		Misses       *int64 `json:"misses"`
		BytesFetched *int64 `json:"bytes_fetched"`
	}
	if json.Unmarshal(shards, &local) != nil || json.Unmarshal(blockCache, &cache) != nil ||
		local.Readers == nil || cache.Hits == nil || cache.Misses == nil || cache.BytesFetched == nil ||
		*local.Readers < 0 || *cache.Hits < 0 || *cache.Misses < 0 || *cache.BytesFetched < 0 {
		return nil
	}
	return &LocalSearchActivity{
		ShardReaders: *local.Readers, BlockCacheHits: *cache.Hits,
		BlockCacheMisses: *cache.Misses, BlockBytesFetched: *cache.BytesFetched,
	}
}
