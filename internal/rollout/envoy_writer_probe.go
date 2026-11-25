package rollout

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"maps"
	"net"
	"net/http"
	"net/netip"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation"

	"github.com/huyz0/opensearch-serverless-operator/api/v1alpha1"
	"github.com/huyz0/opensearch-serverless-operator/internal/java"
	"github.com/huyz0/opensearch-serverless-operator/internal/workload"
)

// NewWriterLeaseProbe authenticates a Java stats read using
// target Pod DNS identity and the configured runtime CA roots. A tunnel is an
// optional verified per-Pod port-forward and must listen on loopback.
func NewWriterLeaseProbe(roots *x509.CertPool, clusterDomain string, podTunnels map[types.UID]string) (func(context.Context, *v1alpha1.OpenSearchServerless, *corev1.Pod, *RouteCredential) (java.Lease, error), error) {
	if roots == nil || len(validation.IsDNS1123Subdomain(clusterDomain)) != 0 {
		return nil, fmt.Errorf("runtime CA roots and an explicit cluster DNS domain are required")
	}
	roots = roots.Clone()
	tunnels := maps.Clone(podTunnels)
	seen := make(map[netip.AddrPort]bool, len(tunnels))
	for uid, value := range tunnels {
		address, err := netip.ParseAddrPort(value)
		if uid == "" || err != nil || !address.Addr().IsLoopback() || address.Port() == 0 ||
			address.Addr().Zone() != "" {
			return nil, fmt.Errorf("writer probes require distinct per-Pod loopback tunnels")
		}
		address = netip.AddrPortFrom(address.Addr().Unmap(), address.Port())
		if seen[address] {
			return nil, fmt.Errorf("writer probes require distinct per-Pod loopback tunnels")
		}
		seen[address], tunnels[uid] = true, address.String()
	}
	return func(ctx context.Context, instance *v1alpha1.OpenSearchServerless, pod *corev1.Pod, credential *RouteCredential) (java.Lease, error) {
		if pod == nil || pod.UID == "" || instance == nil || pod.Namespace != instance.Namespace ||
			!writerInventoryOwnedBy(pod, instance) || credential == nil {
			return java.Lease{}, fmt.Errorf("owned local Pod and scoped Java reader credential are required")
		}
		service, err := workload.PeerDNSService(instance)
		if err != nil {
			return java.Lease{}, err
		}
		host, err := workload.PeerHostname(pod.Name)
		if err != nil {
			return java.Lease{}, err
		}
		name := strings.Join([]string{host, service.Name, service.Namespace, "svc", clusterDomain}, ".")
		if len(validation.IsDNS1123Subdomain(name)) != 0 {
			return java.Lease{}, fmt.Errorf("invalid exact Pod TLS DNS identity")
		}
		ip, err := netip.ParseAddr(pod.Status.PodIP)
		if err != nil || !ip.IsGlobalUnicast() || ip.Zone() != "" {
			return java.Lease{}, fmt.Errorf("valid direct Pod IP is required")
		}
		address := netip.AddrPortFrom(ip.Unmap(), 9200).String()
		if tunnel, exists := tunnels[pod.UID]; exists {
			address = tunnel
		}
		dialer := &net.Dialer{Timeout: 2 * time.Second}
		base := instance.Status.HTTPTLSBaseline
		fleet := pod.Labels[workload.FleetLabel]
		if fleet == "" || base == nil || base.Domain != clusterDomain {
			return java.Lease{}, fmt.Errorf("Pod TLS domain or fleet does not match the recorded HTTPS baseline")
		}
		fingerprint := base.LeafSHA256
		if leaf, found := instance.Status.HTTPFleetCertificates[fleet]; found {
			fingerprint = leaf.LeafSHA256
			if fingerprint == "" {
				return java.Lease{}, fmt.Errorf("recorded fleet has no HTTP TLS leaf fingerprint")
			}
		}
		tlsConfig := &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: roots, ServerName: name}
		if fingerprint != "" {
			tlsConfig.VerifyConnection = func(state tls.ConnectionState) error {
				if len(state.PeerCertificates) == 0 || fmt.Sprintf("%x", sha256.Sum256(state.PeerCertificates[0].Raw)) != fingerprint {
					return fmt.Errorf("Java certificate differs from the fleet's pinned leaf")
				}
				return nil
			}
		}
		transport := &http.Transport{
			TLSClientConfig: tlsConfig,
			DialContext: func(ctx context.Context, network, _ string) (net.Conn, error) {
				return dialer.DialContext(ctx, network, address)
			},
		}
		defer transport.CloseIdleConnections()
		httpClient := &http.Client{Transport: transport, Timeout: 5 * time.Second}
		return java.GetLeaseWithBasicAuth(ctx, httpClient, "https://"+name+":9200", credential.Username, credential.Password)
	}, nil
}
