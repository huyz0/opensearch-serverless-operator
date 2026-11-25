// Package java contains adapters for the serverless Java process.
package java

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
)

// Readiness is the shell's local readiness response. Ready does not establish
// strict cache warmth when the shell's warm-up timeout has elapsed.
type Readiness struct {
	Ready          bool   `json:"ready"`
	State          string `json:"state"`
	ServesSearches bool   `json:"serves_searches"`
}

// GetReadiness reads one node's readiness over the caller-configured client.
// The caller supplies authentication, TLS policy and a bounded context.
func GetReadiness(ctx context.Context, client *http.Client, baseURL string) (Readiness, error) {
	var result Readiness
	endpoint, err := url.Parse(baseURL)
	if err != nil || endpoint == nil || (endpoint.Scheme != "http" && endpoint.Scheme != "https") || endpoint.Host == "" || endpoint.User != nil || endpoint.RawQuery != "" || endpoint.Fragment != "" {
		return result, fmt.Errorf("invalid Java endpoint")
	}
	if client == nil {
		return result, fmt.Errorf("HTTP client is required")
	}
	endpoint.Path = strings.TrimRight(endpoint.Path, "/") + "/_serverless/ready"
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint.String(), nil)
	if err != nil {
		return result, fmt.Errorf("create readiness request: %w", err)
	}
	response, err := client.Do(request)
	if err != nil {
		return result, fmt.Errorf("read readiness: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK && response.StatusCode != http.StatusServiceUnavailable {
		return result, fmt.Errorf("unexpected readiness status %d", response.StatusCode)
	}
	body, err := io.ReadAll(io.LimitReader(response.Body, (64<<10)+1))
	if err != nil {
		return result, fmt.Errorf("read readiness response: %w", err)
	}
	if len(body) > 64<<10 {
		return result, fmt.Errorf("readiness response exceeds size limit")
	}
	var payload struct {
		Ready          *bool  `json:"ready"`
		State          string `json:"state"`
		ServesSearches *bool  `json:"serves_searches"`
	}
	if err := json.Unmarshal(body, &payload); err != nil {
		return result, fmt.Errorf("decode readiness: %w", err)
	}
	if payload.Ready == nil || payload.ServesSearches == nil || payload.State == "" || *payload.Ready != (response.StatusCode == http.StatusOK) {
		return result, fmt.Errorf("inconsistent readiness response")
	}
	return Readiness{Ready: *payload.Ready, State: payload.State, ServesSearches: *payload.ServesSearches}, nil
}
