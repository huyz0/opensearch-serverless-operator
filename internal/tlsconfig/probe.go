package tlsconfig

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"
)

// HTTPSObservation reports the server certificate observed on a fresh HTTPS
// connection and the HTTP response. It does not establish peer-transport TLS.
type HTTPSObservation struct {
	CertificateFingerprint string
	NotAfter               time.Time
	TLSVersion             uint16
	HTTPStatus             int
}

// ProbeHTTPS verifies server identity and trust on a fresh connection and can
// present a client certificate. A response proves that this HTTP endpoint
// accepted the connection, but whether it requires and validates client
// certificates must be qualified independently with negative tests.
func ProbeHTTPS(ctx context.Context, endpoint, identity string, trustPEM, clientCertificatePEM, clientKeyPEM []byte) (HTTPSObservation, error) {
	var result HTTPSObservation
	parsed, err := url.Parse(endpoint)
	if err != nil || parsed == nil || parsed.Scheme != "https" || parsed.Host == "" || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" || identity == "" {
		return result, fmt.Errorf("valid HTTPS endpoint and expected identity are required")
	}
	roots, err := parseTrust(trustPEM)
	if err != nil {
		return result, err
	}
	tlsConfig := &tls.Config{MinVersion: tls.VersionTLS12, ServerName: identity, RootCAs: roots}
	if len(clientCertificatePEM) != 0 || len(clientKeyPEM) != 0 {
		pair, err := tls.X509KeyPair(clientCertificatePEM, clientKeyPEM)
		if err != nil {
			return result, fmt.Errorf("invalid client certificate and key pair")
		}
		tlsConfig.Certificates = []tls.Certificate{pair}
	}
	transport := &http.Transport{Proxy: nil, TLSClientConfig: tlsConfig, DisableKeepAlives: true, TLSHandshakeTimeout: 5 * time.Second}
	defer transport.CloseIdleConnections()
	client := &http.Client{Transport: transport, Timeout: 10 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, parsed.String(), nil)
	if err != nil {
		return result, fmt.Errorf("create HTTPS probe: %w", err)
	}
	response, err := client.Do(request)
	if err != nil {
		return result, fmt.Errorf("HTTPS probe failed: %w", err)
	}
	defer response.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 64<<10))
	if response.TLS == nil || len(response.TLS.PeerCertificates) == 0 || !response.TLS.HandshakeComplete {
		return result, fmt.Errorf("HTTPS response lacks a completed TLS handshake")
	}
	leaf := response.TLS.PeerCertificates[0]
	fingerprint := sha256.Sum256(leaf.Raw)
	return HTTPSObservation{CertificateFingerprint: hex.EncodeToString(fingerprint[:]), NotAfter: leaf.NotAfter, TLSVersion: response.TLS.Version, HTTPStatus: response.StatusCode}, nil
}
