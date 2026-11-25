package security

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptrace"
	"slices"
	"strings"
	"sync"
	"time"
)

// AccountProcess identifies a shell incarnation whose HTTPS endpoint has
// been resolved independently by the caller.
type AccountProcess struct {
	Name        string
	Role        string
	Incarnation string
}

// VerifyAccount observes one shell process through a direct HTTPS endpoint.
// A nonempty previous password must be rejected and a nonempty desired
// password must be accepted. Pass only the desired password to observe
// acceptance or only the previous password to observe rejection. Rejection
// cannot establish that the indexed account record is absent. The endpoint
// must reach the named process directly so all responses describe that JVM.
func (admin UserAdmin) VerifyAccount(ctx context.Context, process AccountProcess, user, previous, desired string) (bool, error) {
	if process.Incarnation == "" {
		return false, fmt.Errorf("expected process incarnation is required")
	}
	_, verified, err := admin.ObserveAccount(ctx, process, user, previous, desired)
	return verified, err
}

// ObserveAccount can learn the incarnation from the first authenticated stats
// response. Subsequent responses must agree before the identity is returned.
func (admin UserAdmin) ObserveAccount(ctx context.Context, process AccountProcess, user, previous, desired string) (AccountProcess, bool, error) {
	verified, err := admin.observeAccount(ctx, &process, user, previous, desired)
	if err != nil || !verified {
		return AccountProcess{}, false, err
	}
	return process, true, nil
}

func (admin UserAdmin) observeAccount(ctx context.Context, process *AccountProcess, user, previous, desired string) (bool, error) {
	if ctx == nil || process.Name == "" || (process.Role != "search" && process.Role != "ingest") ||
		!ValidAccountName(user) || user == admin.Username || previous == desired ||
		(previous != "" && !validPassword(previous)) || (desired != "" && !validPassword(desired)) {
		return false, fmt.Errorf("process identity and distinct account credentials are required")
	}
	endpoint, client, err := admin.verifiedClient()
	if err != nil {
		return false, err
	}
	endpoint.Path = strings.TrimRight(endpoint.Path, "/") + "/_serverless/stats"
	endpoint.RawPath = ""
	transport := client.Transport.(*http.Transport).Clone()
	defer transport.CloseIdleConnections()
	client.Transport = transport
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	var connection net.Conn
	probe := func(username, password string) (bool, error) {
		var mu sync.Mutex
		var observedConnection net.Conn
		trace := &httptrace.ClientTrace{GotConn: func(info httptrace.GotConnInfo) {
			mu.Lock()
			observedConnection = info.Conn
			mu.Unlock()
		}}
		request, err := http.NewRequestWithContext(httptrace.WithClientTrace(ctx, trace), http.MethodGet, endpoint.String(), nil)
		if err != nil {
			return false, fmt.Errorf("account observation request could not be constructed")
		}
		refusalType := "no_credentials"
		if username != "" {
			request.SetBasicAuth(username, password)
			refusalType = "authentication_failed"
		}
		request.Header.Set("Cache-Control", "no-cache, no-store")
		response, err := client.Do(request)
		if err != nil {
			return false, fmt.Errorf("account observation request failed")
		}
		defer response.Body.Close()
		if response.TLS == nil || len(response.TLS.VerifiedChains) == 0 {
			return false, fmt.Errorf("account observation requires a verified HTTPS response")
		}
		mu.Lock()
		current := observedConnection
		mu.Unlock()
		if current == nil || (connection != nil && connection != current) {
			return false, fmt.Errorf("account observation connection changed")
		}
		connection = current
		return accountResponse(response, process, refusalType)
	}
	control := func() error {
		accepted, err := probe(admin.Username, admin.Password)
		if err == nil && !accepted {
			return fmt.Errorf("bootstrap credential was rejected during account observation")
		}
		return err
	}
	if err := control(); err != nil {
		return false, err
	}
	if accepted, err := probe("", ""); err != nil || accepted {
		if err == nil {
			err = fmt.Errorf("shell accepted an anonymous account observation")
		}
		return false, err
	}
	if previous != "" {
		accepted, err := probe(user, previous)
		if err != nil || accepted {
			return false, err
		}
	}
	if desired != "" {
		accepted, err := probe(user, desired)
		if err != nil || !accepted {
			return false, err
		}
	}
	if err := control(); err != nil {
		return false, err
	}
	return true, nil
}

func accountResponse(response *http.Response, process *AccountProcess, refusalType string) (bool, error) {
	const limit = 2 << 20
	data, err := io.ReadAll(io.LimitReader(response.Body, limit+1))
	if err != nil || len(data) > limit {
		return false, fmt.Errorf("account observation response is unavailable")
	}
	switch response.StatusCode {
	case http.StatusOK:
		var stats struct {
			Name        string   `json:"name"`
			Roles       []string `json:"roles"`
			Incarnation string   `json:"process_incarnation"`
		}
		if json.Unmarshal(data, &stats) != nil || stats.Name != process.Name || stats.Incarnation == "" ||
			(process.Incarnation != "" && stats.Incarnation != process.Incarnation) || !slices.Contains(stats.Roles, process.Role) {
			return false, fmt.Errorf("account observation did not identify the expected shell process")
		}
		process.Incarnation = stats.Incarnation
		return true, nil
	case http.StatusUnauthorized:
		var refusal struct {
			Status int `json:"status"`
			Error  struct {
				Type string `json:"type"`
			} `json:"error"`
		}
		if response.Header.Get("WWW-Authenticate") != `Basic realm="opensearch-serverless", charset="UTF-8"` ||
			json.Unmarshal(data, &refusal) != nil || refusal.Status != http.StatusUnauthorized || refusal.Error.Type != refusalType {
			return false, fmt.Errorf("account observation received an unrecognized HTTP 401 response")
		}
		return false, nil
	default:
		return false, fmt.Errorf("account observation returned HTTP %d", response.StatusCode)
	}
}
