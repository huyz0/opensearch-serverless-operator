// health-probe checks local Java readiness and its membership lease.
package main

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"flag"
	"fmt"
	"net"
	"net/http"
	"net/netip"
	"os"
	"strings"
	"time"

	"github.com/huyz0/opensearch-serverless-operator/internal/java"
)

func main() {
	role := flag.String("role", "", "Java node role: search or ingest")
	port := flag.String("port", "9200", "Local Java HTTP port")
	margin := flag.Duration("minimum-lease", 5*time.Second, "Minimum remaining lease time")
	requireWarmup := flag.Bool("require-warmup-completion", false, "Require completed, non-timeout search warm-up for this Java process")
	ca := flag.String("tls-ca", "", "PEM CA bundle for local HTTPS")
	caSHA := flag.String("tls-ca-sha256", "", "Expected SHA-256 of the mounted CA bundle")
	serverName := flag.String("tls-server-name", "", "Expected HTTPS server DNS name or IP")
	serverNameFromPodIP := flag.Bool("tls-server-name-from-pod-ip", false, "Verify HTTPS against the Pod IP")
	clientCert := flag.String("tls-client-cert", "", "PEM client certificate for local HTTPS")
	clientKey := flag.String("tls-client-key", "", "PEM client private key for local HTTPS")
	authUser := flag.String("basic-auth-username", "", "Username for the local HTTPS probe")
	authPassword := flag.String("basic-auth-password-file", "", "File containing the exact Basic Auth password bytes")
	waitDNS := flag.String("wait-peer-dns", "", "Wait for this peer DNS name to resolve only to the Pod IP")
	dnsTimeout := flag.Duration("peer-dns-timeout", 90*time.Second, "Maximum wait for peer DNS to resolve")
	credentialSource := flag.String("stage-credentials-from", "", "Mounted startup credential directory")
	credentialDestination := flag.String("stage-credentials-to", "", "Private runtime credential directory")
	credentialSHA := flag.String("startup-bundle-sha256", "", "Expected checksum of the startup credential bundle")
	flag.Parse()
	if *credentialSource != "" || *credentialDestination != "" || *credentialSHA != "" {
		if err := stageCredentials(*credentialSource, *credentialDestination, *credentialSHA); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		return
	}
	ip := os.Getenv("POD_IP")
	name := os.Getenv("POD_NAME")
	if net.ParseIP(ip) == nil || name == "" {
		fmt.Fprintln(os.Stderr, "Pod identity or IP is unavailable")
		os.Exit(1)
	}
	if *waitDNS != "" {
		if *dnsTimeout <= 0 || *dnsTimeout > 10*time.Minute {
			fmt.Fprintln(os.Stderr, "peer DNS timeout must be positive and at most ten minutes")
			os.Exit(1)
		}
		ctx, cancel := context.WithTimeout(context.Background(), *dnsTimeout)
		defer cancel()
		if err := waitForPeerDNS(ctx, *waitDNS, ip, net.DefaultResolver.LookupNetIP); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		return
	}
	if *serverNameFromPodIP {
		if *serverName != "" {
			fmt.Fprintln(os.Stderr, "choose one HTTPS server identity source")
			os.Exit(1)
		}
		*serverName = ip
	}
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel()
	client, scheme, err := newClient(*ca, *serverName, *clientCert, *clientKey, *caSHA)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	host := net.JoinHostPort(ip, *port)
	endpoint := scheme + "://" + host
	anonymous := *client
	if err := configureBasicAuth(client, host, *authUser, *authPassword); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if *authUser != "" {
		if err := java.RequireHealthAuthentication(ctx, &anonymous, endpoint); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
	}
	if err := checkReadiness(ctx, client, endpoint, name, *role, *margin, *requireWarmup); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

// checkReadiness can require completed background warm-up in addition to
// serving readiness. Completion is not proof that cached blocks remain warm.
func checkReadiness(ctx context.Context, client *http.Client, endpoint, name, role string, margin time.Duration, requireWarmup bool) error {
	if requireWarmup {
		if role != "search" {
			return fmt.Errorf("warm-up completion is only supported for the search role")
		}
		qualification, err := java.QualifySearch(ctx, client, endpoint, name, margin)
		if err != nil {
			return err
		}
		if strings.HasPrefix(endpoint, "https://") && !qualification.StatsProcessVerified {
			return fmt.Errorf("strict HTTPS readiness requires the local stats process identity")
		}
		return nil
	}
	return java.CheckServing(ctx, client, endpoint, name, role, margin)
}

type lookupIP func(context.Context, string, string) ([]netip.Addr, error)

// waitForPeerDNS requires one distinct address, matching the current Pod IP.
// It proves only the observed local resolution at startup, not future DNS state.
func waitForPeerDNS(ctx context.Context, host, podIP string, lookup lookupIP) error {
	expected, err := netip.ParseAddr(podIP)
	if err != nil || host == "" || strings.HasSuffix(host, ".") || lookup == nil {
		return fmt.Errorf("valid peer hostname, Pod IP and resolver are required")
	}
	expected = expected.Unmap()
	var last error
	for {
		addresses, err := lookup(ctx, "ip", host+".")
		if err == nil {
			unique := make(map[netip.Addr]struct{}, len(addresses))
			for _, address := range addresses {
				unique[address.Unmap()] = struct{}{}
			}
			if len(unique) == 1 {
				if _, ok := unique[expected]; ok {
					return nil
				}
			}
			last = fmt.Errorf("peer DNS did not resolve exclusively to the Pod IP")
		} else {
			last = fmt.Errorf("peer DNS lookup failed: %w", err)
		}
		timer := time.NewTimer(time.Second)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			return fmt.Errorf("peer DNS validation did not converge: %w (%v)", ctx.Err(), last)
		case <-timer.C:
		}
	}
}

// newClient enables HTTPS only with an explicit trust bundle and expected
// identity; it never falls back to HTTP after a TLS failure.
func newClient(caPath, identity, certPath, keyPath, caSHA string) (*http.Client, string, error) {
	transport := &http.Transport{Proxy: nil, DisableKeepAlives: true}
	scheme := "http"
	if caPath != "" || identity != "" || certPath != "" || keyPath != "" || caSHA != "" {
		if caPath == "" || identity == "" || (certPath == "") != (keyPath == "") {
			return nil, "", fmt.Errorf("HTTPS requires a CA, server identity, and a complete optional client key pair")
		}
		pemBytes, err := os.ReadFile(caPath)
		if err != nil {
			return nil, "", fmt.Errorf("read HTTPS trust bundle: %w", err)
		}
		if caSHA != "" && fmt.Sprintf("%x", sha256.Sum256(pemBytes)) != caSHA {
			return nil, "", fmt.Errorf("mounted HTTPS trust bundle differs from expected public material")
		}
		roots := x509.NewCertPool()
		if !roots.AppendCertsFromPEM(pemBytes) {
			return nil, "", fmt.Errorf("invalid HTTPS trust bundle")
		}
		config := &tls.Config{MinVersion: tls.VersionTLS12, ServerName: identity, RootCAs: roots}
		if certPath != "" {
			pair, err := tls.LoadX509KeyPair(certPath, keyPath)
			if err != nil {
				return nil, "", fmt.Errorf("invalid HTTPS client certificate or key: %w", err)
			}
			config.Certificates = []tls.Certificate{pair}
		}
		transport.TLSClientConfig = config
		scheme = "https"
	}
	return &http.Client{Transport: transport, Timeout: 2 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}, scheme, nil
}
