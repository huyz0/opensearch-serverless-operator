package java

import (
	"context"
	"fmt"
	"net/http"
	"strings"
)

const MaxHealthPasswordBytes = 4096

var healthPaths = [...]string{"/_serverless/ready", "/_serverless/stats"}

func exactBasicAuthClient(client *http.Client, username string, password []byte) (*http.Client, error) {
	if username == "" || strings.Contains(username, ":") || len(password) == 0 || len(password) > MaxHealthPasswordBytes || client == nil {
		return nil, fmt.Errorf("valid HTTPS Basic Auth credential and client are required")
	}
	transport, ok := client.Transport.(*http.Transport)
	if !ok || transport == nil || transport.Proxy != nil || transport.DialTLS != nil || transport.DialTLSContext != nil ||
		(transport.TLSClientConfig != nil && transport.TLSClientConfig.InsecureSkipVerify) {
		return nil, fmt.Errorf("Basic Auth requires a direct certificate-verifying transport")
	}
	scoped := *client
	scoped.Jar = nil
	scoped.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	return &scoped, nil
}

// RequireHealthAuthentication checks the unauthenticated path before trusting
// a positive health response as evidence that the auth plugin is active.
func RequireHealthAuthentication(ctx context.Context, client *http.Client, endpoint string) error {
	for _, path := range healthPaths {
		if err := requireUnauthorized(ctx, client, endpoint+path); err != nil {
			return err
		}
	}
	return nil
}

// RequireRouteStatsAuthentication checks that the client route rejects an anonymous stats read.
func RequireRouteStatsAuthentication(ctx context.Context, client *http.Client, endpoint string) error {
	return requireUnauthorized(ctx, client, strings.TrimRight(endpoint, "/")+healthPaths[1])
}

func requireUnauthorized(ctx context.Context, client *http.Client, endpoint string) error {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return err
	}
	request.Header.Set("Cache-Control", "no-cache, no-store")
	response, err := client.Do(request)
	if err != nil {
		return err
	}
	response.Body.Close()
	if response.StatusCode != http.StatusUnauthorized {
		return fmt.Errorf("anonymous health read returned HTTP %d, expected HTTP 401", response.StatusCode)
	}
	return nil
}

// ConfigureHealthBasicAuth scopes a credential to one verified endpoint's health reads.
func ConfigureHealthBasicAuth(client *http.Client, host, username string, password []byte) error {
	if username == "" || strings.Contains(username, ":") || host == "" || len(password) == 0 || len(password) > MaxHealthPasswordBytes {
		return fmt.Errorf("invalid probe Basic Auth inputs")
	}
	transport, ok := client.Transport.(*http.Transport)
	if !ok || transport.Proxy != nil || transport.TLSClientConfig == nil || transport.TLSClientConfig.InsecureSkipVerify ||
		transport.TLSClientConfig.ServerName == "" || transport.TLSClientConfig.RootCAs == nil {
		return fmt.Errorf("probe Basic Auth requires direct HTTPS with explicit trust and server identity")
	}
	client.Transport = &healthBasicAuth{transport: transport, host: host, username: username, password: string(password)}
	return nil
}

type healthBasicAuth struct {
	transport *http.Transport
	host      string
	username  string
	password  string
}

func (auth *healthBasicAuth) RoundTrip(request *http.Request) (*http.Response, error) {
	path := request.URL.EscapedPath()
	if request.URL.Scheme != "https" || request.URL.Host != auth.host || request.URL.User != nil ||
		request.Method != http.MethodGet || (path != healthPaths[0] && path != healthPaths[1]) {
		return nil, fmt.Errorf("probe Basic Auth requires a health read on the configured local HTTPS endpoint")
	}
	request = request.Clone(request.Context())
	request.SetBasicAuth(auth.username, auth.password)
	return auth.transport.RoundTrip(request)
}

func (auth *healthBasicAuth) CloseIdleConnections() {
	auth.transport.CloseIdleConnections()
}
