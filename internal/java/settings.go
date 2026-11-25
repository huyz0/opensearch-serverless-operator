package java

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"

	"github.com/huyz0/opensearch-serverless-operator/api/v1alpha1"
)

// ApplyClusterSettings sends a qualified persistent patch. A successful HTTP
// response confirms the server accepted the request; callers must read back
// persistence and separately verify runtime effect. On any request error the
// caller must inspect current settings before retrying.
func ApplyClusterSettings(ctx context.Context, client *http.Client, baseURL string, patch v1alpha1.ClusterSettingsPatch, allowed map[string]bool) error {
	endpoint, err := clusterSettingsEndpoint(client, baseURL)
	if err != nil {
		return err
	}
	if len(patch.Set) == 0 && len(patch.Remove) == 0 {
		return fmt.Errorf("settings patch must not be empty")
	}
	values := make(map[string]*string, len(patch.Set)+len(patch.Remove))
	for key, value := range patch.Set {
		if !allowed[key] {
			return fmt.Errorf("setting %q is not qualified", key)
		}
		valueCopy := value
		values[key] = &valueCopy
	}
	for _, key := range patch.Remove {
		if !allowed[key] {
			return fmt.Errorf("setting %q is not qualified", key)
		}
		if _, exists := values[key]; exists {
			return fmt.Errorf("setting %q is repeated or both set and removed", key)
		}
		values[key] = nil
	}
	body, err := json.Marshal(map[string]any{"persistent": values})
	if err != nil {
		return fmt.Errorf("encode settings patch: %w", err)
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPut, endpoint.String(), bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("create settings request: %w", err)
	}
	request.Header.Set("Content-Type", "application/json")
	response, err := client.Do(request)
	if err != nil {
		return fmt.Errorf("apply settings: %w", err)
	}
	defer response.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 64<<10))
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return fmt.Errorf("settings request returned HTTP %d", response.StatusCode)
	}
	return nil
}

// GetPersistentSettings reads the flat persisted settings projection. It does
// not prove that every process has applied a setting.
func GetPersistentSettings(ctx context.Context, client *http.Client, baseURL string) (map[string]string, error) {
	endpoint, err := clusterSettingsEndpoint(client, baseURL)
	if err != nil {
		return nil, err
	}
	endpoint.RawQuery = "flat_settings=true"
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint.String(), nil)
	if err != nil {
		return nil, fmt.Errorf("create settings read: %w", err)
	}
	response, err := client.Do(request)
	if err != nil {
		return nil, fmt.Errorf("read settings: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("settings read returned HTTP %d", response.StatusCode)
	}
	body, err := io.ReadAll(io.LimitReader(response.Body, (1<<20)+1))
	if err != nil || len(body) > 1<<20 {
		return nil, fmt.Errorf("read settings response failed or exceeded size limit")
	}
	responseFields, err := settingsObject(body)
	if err != nil {
		return nil, fmt.Errorf("invalid cluster settings response: %w", err)
	}
	persistent, err := settingsObject(responseFields["persistent"])
	if err != nil {
		return nil, fmt.Errorf("invalid persistent settings response: %w", err)
	}
	values := make(map[string]string, len(persistent))
	for key, field := range persistent {
		var value *string
		if key == "" || json.Unmarshal(field, &value) != nil || value == nil {
			return nil, fmt.Errorf("persistent settings must contain named string values")
		}
		values[key] = *value
	}
	return values, nil
}

func clusterSettingsEndpoint(client *http.Client, baseURL string) (*url.URL, error) {
	endpoint, err := url.Parse(baseURL)
	if err != nil || (endpoint.Scheme != "http" && endpoint.Scheme != "https") || endpoint.Hostname() == "" || endpoint.User != nil || endpoint.RawQuery != "" || endpoint.ForceQuery || endpoint.Fragment != "" || client == nil {
		return nil, fmt.Errorf("valid Java endpoint and HTTP client are required")
	}
	endpoint.Path = strings.TrimRight(endpoint.Path, "/") + "/_cluster/settings"
	return endpoint, nil
}

// settingsObject keeps read-back evidence unambiguous. encoding/json would
// otherwise let a later duplicate key silently replace an earlier value.
func settingsObject(data []byte) (map[string]json.RawMessage, error) {
	decoder := json.NewDecoder(bytes.NewReader(data))
	if token, err := decoder.Token(); err != nil || token != json.Delim('{') {
		return nil, fmt.Errorf("expected JSON object")
	}
	fields := make(map[string]json.RawMessage)
	for decoder.More() {
		token, err := decoder.Token()
		if err != nil {
			return nil, err
		}
		key, ok := token.(string)
		if !ok || fields[key] != nil {
			return nil, fmt.Errorf("invalid or duplicate JSON field")
		}
		var value json.RawMessage
		if err := decoder.Decode(&value); err != nil {
			return nil, err
		}
		fields[key] = value
	}
	if token, err := decoder.Token(); err != nil || token != json.Delim('}') {
		return nil, fmt.Errorf("expected end of JSON object")
	}
	if _, err := decoder.Token(); err != io.EOF {
		return nil, fmt.Errorf("unexpected data after JSON object")
	}
	return fields, nil
}

// SettingsPatchApplied checks only the keys in a patch against a read-back.
// It reports persistence, not whether each Java process applied the setting.
func SettingsPatchApplied(current map[string]string, patch v1alpha1.ClusterSettingsPatch) bool {
	for key, value := range patch.Set {
		actual, exists := current[key]
		if !exists || actual != value {
			return false
		}
	}
	for _, key := range patch.Remove {
		if _, exists := current[key]; exists {
			return false
		}
	}
	return len(patch.Set) > 0 || len(patch.Remove) > 0
}
