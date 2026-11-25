package java

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"

	"github.com/google/uuid"
)

// Warmup reports the patched shell's local background-work state. Completed
// means no detected warm-up error, not proof that cache blocks remain resident.
type Warmup struct {
	ProcessIncarnation string
	Result             string
	TimedOut           bool
	Ready              bool
}

// Finished reports that the background worker has exited for this incarnation.
func (w Warmup) Finished() bool {
	return w.Result == "completed" || w.Result == "degraded" || w.Result == "failed"
}

// GetWarmup requires the explicit signal; older images remain unknown.
func GetWarmup(ctx context.Context, client *http.Client, baseURL string) (Warmup, error) {
	var result Warmup
	endpoint, err := url.Parse(baseURL)
	if err != nil || endpoint == nil || (endpoint.Scheme != "http" && endpoint.Scheme != "https") || endpoint.Host == "" || endpoint.User != nil || endpoint.RawQuery != "" || endpoint.Fragment != "" || client == nil {
		return result, fmt.Errorf("valid Java endpoint and HTTP client are required")
	}
	endpoint.Path = strings.TrimRight(endpoint.Path, "/") + "/_serverless/ready"
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint.String(), nil)
	if err != nil {
		return result, err
	}
	response, err := client.Do(request)
	if err != nil {
		return result, fmt.Errorf("read warm-up status: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK && response.StatusCode != http.StatusServiceUnavailable {
		return result, fmt.Errorf("readiness returned HTTP %d", response.StatusCode)
	}
	body, err := io.ReadAll(io.LimitReader(response.Body, (64<<10)+1))
	if err != nil || len(body) > 64<<10 {
		return result, fmt.Errorf("read warm-up status failed or exceeded size limit")
	}
	var payload struct {
		Ready       *bool  `json:"ready"`
		Incarnation string `json:"process_incarnation"`
		Result      string `json:"warmup_result"`
		TimedOut    *bool  `json:"warmup_timed_out"`
	}
	if err := json.Unmarshal(body, &payload); err != nil {
		return result, fmt.Errorf("decode warm-up status: %w", err)
	}
	if payload.Ready == nil || *payload.Ready != (response.StatusCode == http.StatusOK) || payload.TimedOut == nil {
		return result, fmt.Errorf("warm-up status is incomplete or inconsistent")
	}
	incarnation, err := uuid.Parse(payload.Incarnation)
	if err != nil || incarnation == uuid.Nil || incarnation.Version() != 4 {
		return result, fmt.Errorf("warm-up process incarnation is missing or invalid")
	}
	switch payload.Result {
	case "not_requested", "running", "completed", "degraded", "failed":
	default:
		return result, fmt.Errorf("unsupported warm-up result")
	}
	return Warmup{ProcessIncarnation: payload.Incarnation, Result: payload.Result, TimedOut: *payload.TimedOut, Ready: *payload.Ready}, nil
}
