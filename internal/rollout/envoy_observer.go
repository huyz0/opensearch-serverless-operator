package rollout

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"io"
	"maps"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"reflect"
	"regexp"
	"slices"
	"sort"
	"strings"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/huyz0/opensearch-serverless-operator/internal/java"
)

// EnvoyObserver reads the active EDS assignment and health state of each
// serving replica. ReadInventory must independently re-read the route,
// serving proxy inventory and qualified Pod identities on every call.
type EnvoyObserver struct {
	RouteURL      string
	Roots         *x509.CertPool
	Samples       int
	ReadInventory func(context.Context) (EnvoyInventory, error)
}

type EnvoyInventory struct {
	WeightEvidenceScope
	Weights map[string]int
	// Proxies maps serving replica UIDs to platform-verified per-Pod tunnels.
	Proxies map[string]EnvoyTunnel
	// Pods maps Java process names to the current approved Pod identity.
	Pods map[string]EnvoyBackend
	// GatewayVersions makes changes to the serving Service or its slices
	// visible even when the set of ready Pod UIDs is unchanged.
	GatewayVersions map[string]string
	routeCredential *RouteCredential
	writerVictims   map[netip.Addr]bool
	rdsName         string
	literalHost     string
}

// snapshot separates the first read from any buffers reused by the inventory reader.
func (inventory EnvoyInventory) snapshot() EnvoyInventory {
	inventory.Weights = maps.Clone(inventory.Weights)
	inventory.Proxies = maps.Clone(inventory.Proxies)
	inventory.Pods = snapshotEnvoyBackends(inventory.Pods)
	inventory.GatewayVersions = maps.Clone(inventory.GatewayVersions)
	inventory.writerVictims = maps.Clone(inventory.writerVictims)
	inventory.routeCredential = snapshotRouteCredential(inventory.routeCredential)
	return inventory
}

func snapshotEnvoyBackends(pods map[string]EnvoyBackend) map[string]EnvoyBackend {
	snapshot := maps.Clone(pods)
	for name, pod := range snapshot {
		pod.Addresses = slices.Clone(pod.Addresses)
		snapshot[name] = pod
	}
	return snapshot
}

func snapshotRouteCredential(credential *RouteCredential) *RouteCredential {
	if credential == nil {
		return nil
	}
	result := *credential
	result.Password = slices.Clone(credential.Password)
	return &result
}

type EnvoyTunnel struct {
	AdminAddress  string `json:"adminAddress"`
	ClientAddress string `json:"clientAddress"`
}

type EnvoyBackend struct {
	UID, ResourceVersion, Fleet, ProcessIncarnation string
	Addresses                                       []string
}

func (o EnvoyObserver) Observe(ctx context.Context) (WeightEvidence, error) {
	return o.observe(ctx, 500)
}

func (o EnvoyObserver) observe(ctx context.Context, minimumSamples int) (WeightEvidence, error) {
	base, err := url.Parse(o.RouteURL)
	if err != nil || base.Scheme != "https" || base.Hostname() == "" || base.User != nil ||
		base.ForceQuery || base.RawQuery != "" || base.Fragment != "" || o.Samples < minimumSamples || o.Samples > 10_000 || o.ReadInventory == nil {
		return WeightEvidence{}, fmt.Errorf("HTTPS route, complete inventory reader and %d to 10000 probes per replica are required", minimumSamples)
	}
	ctx, cancel := context.WithTimeout(ctx, PlatformEvidenceMaxAge-2*time.Second)
	defer cancel()
	started := time.Now()
	before, err := o.ReadInventory(ctx)
	if err != nil {
		return WeightEvidence{}, err
	}
	before = before.snapshot()
	if !validWeights(before.Weights) || len(before.Proxies) == 0 || len(before.Pods) == 0 {
		return WeightEvidence{}, fmt.Errorf("weighted route, replica tunnels and qualified Pods are required")
	}
	if (before.routeCredential == nil) != (before.RouteAuthIdentity == (RouteAuthIdentity{})) ||
		(before.routeCredential != nil && before.routeCredential.RouteAuthIdentity != before.RouteAuthIdentity) {
		return WeightEvidence{}, fmt.Errorf("route credential differs from the observed authentication scope")
	}
	if before.routeCredential != nil {
		canonical, err := canonicalAuthProbeURL(o.RouteURL)
		if err != nil || canonical != before.AuthProbeURL {
			return WeightEvidence{}, fmt.Errorf("authenticated probe URL differs from the bound weighted route")
		}
		o.RouteURL = canonical
	}
	result := WeightEvidence{WeightEvidenceScope: before.WeightEvidenceScope, Weights: before.Weights, InventoryObservedAt: started}
	addresses := make(map[netip.AddrPort]struct{})
	for uid, tunnel := range before.Proxies {
		for _, value := range []string{tunnel.AdminAddress, tunnel.ClientAddress} {
			address, err := netip.ParseAddrPort(value)
			if err != nil || !address.Addr().IsLoopback() || address.Addr().Zone() != "" || address.Port() == 0 {
				return WeightEvidence{}, fmt.Errorf("proxy %q requires explicit loopback tunnels", uid)
			}
			address = netip.AddrPortFrom(address.Addr().Unmap(), address.Port())
			if _, reused := addresses[address]; reused {
				return WeightEvidence{}, fmt.Errorf("each proxy tunnel must have a distinct address")
			}
			addresses[address] = struct{}{}
		}
		result.ServingProxyUIDs = append(result.ServingProxyUIDs, uid)
	}
	sort.Strings(result.ServingProxyUIDs)
	podsByAddress := make(map[netip.Addr]EnvoyBackend)
	for name, pod := range before.Pods {
		if name == "" || pod.UID == "" || pod.ResourceVersion == "" || pod.ProcessIncarnation == "" ||
			before.Weights[pod.Fleet] == 0 || len(pod.Addresses) == 0 {
			return WeightEvidence{}, fmt.Errorf("incomplete active Pod identity")
		}
		for _, value := range pod.Addresses {
			address, err := netip.ParseAddr(value)
			if err != nil || !address.IsValid() || podsByAddress[address.Unmap()].UID != "" {
				return WeightEvidence{}, fmt.Errorf("invalid or duplicated active Pod IP")
			}
			podsByAddress[address.Unmap()] = pod
		}
	}
	result.Proxies = make([]WeightProxyReport, len(result.ServingProxyUIDs))
	group, work := errgroup.WithContext(ctx)
	group.SetLimit(4)
	for i, uid := range result.ServingProxyUIDs {
		group.Go(func() error {
			report, err := o.observeReplica(work, before, uid, podsByAddress)
			result.Proxies[i] = report
			return err
		})
	}
	if err := group.Wait(); err != nil {
		return WeightEvidence{}, err
	}
	after, err := o.ReadInventory(ctx)
	if err != nil {
		return WeightEvidence{}, err
	}
	if !reflect.DeepEqual(before, after) {
		return WeightEvidence{}, fmt.Errorf("route, serving proxies or qualified Pods changed while sampling")
	}
	if err := result.validate(time.Now()); err != nil {
		return WeightEvidence{}, err
	}
	return result, nil
}

func (o EnvoyObserver) observeReplica(ctx context.Context, inventory EnvoyInventory, uid string, pods map[netip.Addr]EnvoyBackend) (WeightProxyReport, error) {
	tunnel := inventory.Proxies[uid]
	transport := &http.Transport{TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: o.Roots}}
	defer transport.CloseIdleConnections()
	const probeConcurrency = 8
	edgeTransport := transport.Clone()
	edgeTransport.MaxIdleConnsPerHost = probeConcurrency
	edgeTransport.MaxConnsPerHost = probeConcurrency
	defer edgeTransport.CloseIdleConnections()
	dialer := &net.Dialer{Timeout: 2 * time.Second}
	edgeTransport.DialContext = func(ctx context.Context, network, _ string) (net.Conn, error) {
		return dialer.DialContext(ctx, network, tunnel.ClientAddress)
	}
	noRedirect := func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	admin := &http.Client{Transport: transport, Timeout: 3 * time.Second, CheckRedirect: noRedirect}
	edge := &http.Client{Transport: edgeTransport, Timeout: 3 * time.Second, CheckRedirect: noRedirect}
	report := WeightProxyReport{
		ProxyUID: uid, RouteVersion: inventory.RouteVersion, ObservedAt: time.Now(), Requests: make(map[string]int),
	}
	cluster := "httproute/" + inventory.Namespace + "/" + inventory.RouteName + "/rule/0"
	routeHash, err := observeEnvoyLiteralRoute(ctx, admin, tunnel.AdminAddress, inventory.rdsName, inventory.literalHost, cluster)
	if err != nil {
		return WeightProxyReport{}, fmt.Errorf("observe proxy %q route: %w", uid, err)
	}
	initial, err := inspectEnvoyReplica(ctx, admin, tunnel.AdminAddress, cluster, inventory.Weights, pods, inventory.writerVictims)
	if err != nil {
		return WeightProxyReport{}, fmt.Errorf("observe proxy %q: %w", uid, err)
	}
	getLease := func(ctx context.Context) (java.Lease, error) {
		return java.GetLease(ctx, edge, o.RouteURL)
	}
	if credential := inventory.routeCredential; credential != nil {
		if err := java.RequireRouteStatsAuthentication(ctx, edge, o.RouteURL); err != nil {
			return WeightProxyReport{}, fmt.Errorf("verify anonymous denial from proxy %q: %w", uid, err)
		}
		getLease = func(ctx context.Context) (java.Lease, error) {
			return java.GetLeaseWithBasicAuth(ctx, edge, o.RouteURL, credential.Username, credential.Password)
		}
	}
	counts := make(chan string, o.Samples)
	workers, sampleCtx := errgroup.WithContext(ctx)
	workers.SetLimit(probeConcurrency)
	for range o.Samples {
		if sampleCtx.Err() != nil {
			break
		}
		workers.Go(func() error {
			lease, err := getLease(sampleCtx)
			pod, known := inventory.Pods[lease.NodeName]
			if err != nil || !known || !lease.Valid || lease.ExpiresInMillis <= 0 ||
				!lease.HasRole(inventory.Role) || lease.ProcessIncarnation != pod.ProcessIncarnation {
				counts <- ""
			} else {
				counts <- pod.Fleet
			}
			return nil
		})
	}
	_ = workers.Wait()
	if err := ctx.Err(); err != nil {
		return WeightProxyReport{}, err
	}
	close(counts)
	for fleet := range counts {
		if fleet == "" {
			report.Failures++
		} else {
			report.Requests[fleet]++
		}
	}
	final, err := inspectEnvoyReplica(ctx, admin, tunnel.AdminAddress, cluster, inventory.Weights, pods, inventory.writerVictims)
	if err != nil {
		return WeightProxyReport{}, fmt.Errorf("recheck proxy %q: %w", uid, err)
	}
	if !reflect.DeepEqual(initial, final) {
		return WeightProxyReport{}, fmt.Errorf("proxy %q changed its active assignment or health while sampling", uid)
	}
	finalRouteHash, err := observeEnvoyLiteralRoute(ctx, admin, tunnel.AdminAddress, inventory.rdsName, inventory.literalHost, cluster)
	if err != nil || routeHash != finalRouteHash {
		return WeightProxyReport{}, fmt.Errorf("proxy %q did not retain its active HTTPRoute during sampling", uid)
	}
	report.ConfiguredWeights, report.Healthy = final.weights, final.healthy
	return report, nil
}

func observeEnvoyLiteralRoute(ctx context.Context, admin *http.Client, address, name, host, cluster string) ([sha256.Size]byte, error) {
	var zero [sha256.Size]byte
	if name == "" {
		return zero, nil
	}
	var dump struct {
		Configs []struct {
			Route json.RawMessage `json:"route_config"`
		} `json:"configs"`
	}
	query := url.Values{"resource": {"dynamic_route_configs"}, "name_regex": {"^" + regexp.QuoteMeta(name) + "$"}}
	if err := readEnvoyJSON(ctx, admin, "http://"+address+"/config_dump?"+query.Encode(), &dump); err != nil {
		return zero, err
	}
	if len(dump.Configs) != 1 {
		return zero, fmt.Errorf("expected one active listener route configuration")
	}
	var route struct {
		Name  string `json:"name"`
		Hosts []struct {
			Domains []string                     `json:"domains"`
			Rules   []map[string]json.RawMessage `json:"routes"`
		} `json:"virtual_hosts"`
	}
	raw := dump.Configs[0].Route
	if err := json.Unmarshal(raw, &route); err != nil || route.Name != name {
		return zero, fmt.Errorf("active listener route configuration differs from the expected listener")
	}
	matches := 0
	for _, virtualHost := range route.Hosts {
		for _, domain := range virtualHost.Domains {
			if !strings.EqualFold(domain, host) {
				continue
			}
			matches++
			if len(virtualHost.Domains) != 1 || len(virtualHost.Rules) != 1 {
				return zero, fmt.Errorf("index virtual host must have one literal domain and one active rule")
			}
			rule := virtualHost.Rules[0]
			var match map[string]string
			var action struct {
				Cluster string `json:"cluster"`
			}
			if json.Unmarshal(rule["match"], &match) != nil || len(match) != 1 || match["prefix"] != "/" ||
				json.Unmarshal(rule["route"], &action) != nil || action.Cluster != cluster {
				return zero, fmt.Errorf("index methods must all route to the inspected EDS cluster")
			}
		}
	}
	if matches != 1 {
		return zero, fmt.Errorf("active route configuration lacks one matching index virtual host")
	}
	return sha256.Sum256(raw), nil
}

type envoyReplicaState struct {
	weights        map[string]int
	healthy        map[string]map[string]string
	xdsID          string
	assignmentHash [sha256.Size]byte
}

type envoySocketAddress struct {
	Socket struct {
		Address string `json:"address"`
		Port    uint16 `json:"port_value"`
	} `json:"socket_address"`
}

func (a envoySocketAddress) backend(pods map[netip.Addr]EnvoyBackend) (EnvoyBackend, error) {
	address, err := netip.ParseAddr(a.Socket.Address)
	pod := pods[address.Unmap()]
	if err != nil || a.Socket.Port != 9200 || pod.UID == "" {
		return EnvoyBackend{}, fmt.Errorf("Envoy reported an unknown Pod endpoint")
	}
	return pod, nil
}

// inspectEnvoyReplica supports the single EDS cluster generated for a
// one-rule HTTPRoute by Envoy Gateway 1.8.6. Other layouts fail closed.
func inspectEnvoyReplica(ctx context.Context, admin *http.Client, address, cluster string, weights map[string]int, pods map[netip.Addr]EnvoyBackend, writerVictims map[netip.Addr]bool) (envoyReplicaState, error) {
	var dump struct {
		Configs []struct {
			Version     string          `json:"version_info"`
			LastUpdated string          `json:"last_updated"`
			Assignment  json.RawMessage `json:"endpoint_config"`
		} `json:"configs"`
	}
	var assignment struct {
		ClusterName string `json:"cluster_name"`
		Localities  []struct {
			Weight   uint32 `json:"load_balancing_weight"`
			Priority uint32 `json:"priority"`
			Backends []struct {
				Health   string `json:"health_status"`
				Endpoint struct {
					Address envoySocketAddress `json:"address"`
				} `json:"endpoint"`
			} `json:"lb_endpoints"`
		} `json:"endpoints"`
	}
	query := url.Values{"include_eds": {""}, "resource": {"dynamic_endpoint_configs"}, "name_regex": {"^" + regexp.QuoteMeta(cluster) + "$"}}
	origin := "http://" + address
	if err := readEnvoyJSON(ctx, admin, origin+"/config_dump?"+query.Encode(), &dump); err != nil {
		return envoyReplicaState{}, err
	}
	if len(dump.Configs) != 1 {
		return envoyReplicaState{}, fmt.Errorf("Envoy did not report one current EDS assignment")
	}
	config := dump.Configs[0]
	if err := json.Unmarshal(config.Assignment, &assignment); err != nil || assignment.ClusterName != cluster {
		return envoyReplicaState{}, fmt.Errorf("Envoy did not report the requested EDS assignment")
	}
	byFleet, expectedPods := make(map[string]uint64), make(map[string]bool)
	drained := make(map[netip.Addr]bool)
	total := uint64(0)
	for _, locality := range assignment.Localities {
		fleet := ""
		markedDrained := false
		for _, backend := range locality.Backends {
			addr := backend.Endpoint.Address
			if ip, ok := addr.withdrawnWriter(writerVictims); ok {
				if drained[ip] || (backend.Health != "UNHEALTHY" && backend.Health != "DRAINING") {
					return envoyReplicaState{}, fmt.Errorf("withdrawn writer is duplicated or remains eligible in EDS")
				}
				drained[ip] = true
				markedDrained = true
				continue
			}
			pod, err := addr.backend(pods)
			if err != nil || (fleet != "" && fleet != pod.Fleet) {
				return envoyReplicaState{}, fmt.Errorf("Envoy locality does not identify one qualified fleet")
			}
			fleet = pod.Fleet
			expectedPods[pod.UID] = true
		}
		if fleet == "" && markedDrained {
			continue
		}
		if fleet == "" || locality.Weight == 0 || locality.Priority != 0 {
			return envoyReplicaState{}, fmt.Errorf("Envoy locality lacks an active weight or uses a fallback priority")
		}
		byFleet[fleet] += uint64(locality.Weight)
		total += uint64(locality.Weight)
	}
	actual := make(map[string]int, len(weights))
	for fleet, weight := range weights {
		if total == 0 || byFleet[fleet]*100 != uint64(weight)*total {
			return envoyReplicaState{}, fmt.Errorf("Envoy's configured weights differ from the HTTPRoute distribution")
		}
		actual[fleet] = int(byFleet[fleet] * 100 / total)
	}
	qualifiedUIDs := make(map[string]bool)
	for _, pod := range pods {
		qualifiedUIDs[pod.UID] = true
	}
	if !maps.Equal(expectedPods, qualifiedUIDs) {
		return envoyReplicaState{}, fmt.Errorf("Envoy assignment differs from the active qualified Pod inventory")
	}
	healthy, err := envoyHealthyPods(ctx, admin, origin, cluster, pods, writerVictims, drained)
	if err == nil && len(drained) != 0 {
		err = requireEnvoyPanicDisabled(ctx, admin, origin, cluster)
	}
	// Some supported Envoy responses omit optional xDS metadata. Compare the
	// full assignment across the sampling window even when metadata is present.
	return envoyReplicaState{
		weights: actual, healthy: healthy, xdsID: config.Version + "/" + config.LastUpdated,
		assignmentHash: sha256.Sum256(config.Assignment),
	}, err
}

func (a envoySocketAddress) withdrawnWriter(victims map[netip.Addr]bool) (netip.Addr, bool) {
	ip, err := netip.ParseAddr(a.Socket.Address)
	return ip.Unmap(), err == nil && ip.Zone() == "" && a.Socket.Port == 9200 && victims[ip.Unmap()]
}

func envoyHealthyPods(ctx context.Context, admin *http.Client, origin, cluster string, pods map[netip.Addr]EnvoyBackend, writerVictims, drained map[netip.Addr]bool) (map[string]map[string]string, error) {
	var status struct {
		Clusters []struct {
			Name  string `json:"name"`
			Hosts []struct {
				Address envoySocketAddress         `json:"address"`
				Health  map[string]json.RawMessage `json:"health_status"`
			} `json:"host_statuses"`
		} `json:"cluster_statuses"`
	}
	query := url.Values{"format": {"json"}, "filter": {"^" + regexp.QuoteMeta(cluster) + "$"}}
	if err := readEnvoyJSON(ctx, admin, origin+"/clusters?"+query.Encode(), &status); err != nil {
		return nil, err
	}
	if len(status.Clusters) != 1 || status.Clusters[0].Name != cluster {
		return nil, fmt.Errorf("Envoy did not report one matching active cluster")
	}
	healthy := make(map[string]map[string]string)
	liveDrained := make(map[netip.Addr]bool, len(drained))
	for _, host := range status.Clusters[0].Hosts {
		if ip, ok := host.Address.withdrawnWriter(writerVictims); ok {
			if !drained[ip] || liveDrained[ip] || string(host.Health["eds_health_status"]) != `"DRAINING"` {
				return nil, fmt.Errorf("withdrawn writer differs from the drained EDS host inventory")
			}
			liveDrained[ip] = true
			continue
		}
		pod, err := host.Address.backend(pods)
		if err != nil {
			return nil, err
		}
		if string(host.Health["eds_health_status"]) != `"HEALTHY"` {
			continue
		}
		failed := false
		for name, flag := range host.Health {
			if name != "eds_health_status" && string(flag) != "false" {
				failed = true
				break
			}
		}
		if failed {
			continue
		}
		if healthy[pod.Fleet] == nil {
			healthy[pod.Fleet] = make(map[string]string)
		}
		healthy[pod.Fleet][pod.UID] = pod.ProcessIncarnation
	}
	if !maps.Equal(drained, liveDrained) {
		return nil, fmt.Errorf("active cluster lacks a drained writer reported by EDS")
	}
	return healthy, nil
}

func requireEnvoyPanicDisabled(ctx context.Context, admin *http.Client, origin, cluster string) error {
	var dump struct {
		Configs []struct {
			Cluster struct {
				Name   string `json:"name"`
				Common struct {
					Threshold *struct {
						Value float64 `json:"value"`
					} `json:"healthy_panic_threshold"`
				} `json:"common_lb_config"`
			} `json:"cluster"`
		} `json:"configs"`
	}
	query := url.Values{"resource": {"dynamic_active_clusters"}, "name_regex": {"^" + regexp.QuoteMeta(cluster) + "$"}}
	if err := readEnvoyJSON(ctx, admin, origin+"/config_dump?"+query.Encode(), &dump); err != nil {
		return err
	}
	if len(dump.Configs) != 1 || dump.Configs[0].Cluster.Name != cluster ||
		dump.Configs[0].Cluster.Common.Threshold == nil || dump.Configs[0].Cluster.Common.Threshold.Value != 0 {
		return fmt.Errorf("Envoy panic routing must be explicitly disabled while the withdrawn writer is in EDS")
	}
	return nil
}
func readEnvoyJSON(ctx context.Context, client *http.Client, address string, result any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, address, nil)
	if err != nil {
		return err
	}
	response, err := client.Do(req)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return fmt.Errorf("Envoy admin returned HTTP %d", response.StatusCode)
	}
	const limit = 4 << 20
	raw, err := io.ReadAll(io.LimitReader(response.Body, limit+1))
	if err != nil || len(raw) > limit {
		return fmt.Errorf("Envoy admin response could not be read or exceeded the size limit")
	}
	return json.Unmarshal(raw, result)
}
