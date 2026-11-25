package rollout

import (
	"context"
	"crypto/x509"
	"fmt"
	"maps"
	"net/netip"
	"net/url"
	"reflect"
	"slices"
	"time"

	"k8s.io/apimachinery/pkg/types"
)

// EnvoyWriterWithdrawalInventory is the platform's independent observation
// of the bound index HTTPRoute, stable Service and serving proxy cohort.
type EnvoyWriterWithdrawalInventory struct {
	Scope            WriterWithdrawalScope
	WithdrawnAt      time.Time
	Fleet            string
	VictimAddresses  []string
	EndpointVersions map[types.UID]string
	Pods             map[string]EnvoyBackend
	Proxies          map[string]EnvoyTunnel
	GatewayVersions  map[string]string
	Credential       *RouteCredential
}

// ObserveEnvoyWriterWithdrawal checks active EDS and cluster health, probes the
// authenticated HTTPS index route on every replica, and rereads the inventory.
// The platform inventory reader must bind the one-rule route to the stable
// Service and independently enumerate every serving replica.
func ObserveEnvoyWriterWithdrawal(ctx context.Context, routeURL string, roots *x509.CertPool, read func(context.Context) (EnvoyWriterWithdrawalInventory, error)) (WriterWithdrawalEvidence, error) {
	if read == nil {
		return WriterWithdrawalEvidence{}, fmt.Errorf("a bound Envoy index route and inventory reader are required")
	}
	var initial *EnvoyWriterWithdrawalInventory
	observer := EnvoyObserver{RouteURL: routeURL, Roots: roots, Samples: 4}
	observer.ReadInventory = func(ctx context.Context) (EnvoyInventory, error) {
		live, err := read(ctx)
		if err != nil {
			return EnvoyInventory{}, err
		}
		if live.Fleet == "" || live.Credential == nil || live.Scope.RouteAuthIdentity != live.Credential.RouteAuthIdentity ||
			live.WithdrawnAt.IsZero() || len(live.VictimAddresses) == 0 || len(live.GatewayVersions) == 0 {
			return EnvoyInventory{}, fmt.Errorf("serving fleet, authenticated route and independent gateway and victim inventory are required")
		}
		if initial == nil {
			snapshot := live
			snapshot.VictimAddresses = slices.Clone(live.VictimAddresses)
			snapshot.EndpointVersions = maps.Clone(live.EndpointVersions)
			snapshot.Pods = snapshotEnvoyBackends(live.Pods)
			snapshot.Proxies = maps.Clone(live.Proxies)
			snapshot.GatewayVersions = maps.Clone(live.GatewayVersions)
			snapshot.Credential = snapshotRouteCredential(live.Credential)
			initial = &snapshot
		} else if !reflect.DeepEqual(*initial, live) {
			return EnvoyInventory{}, fmt.Errorf("writer withdrawal routing inventory changed during observation")
		}
		victimIPs := make(map[netip.Addr]bool, len(live.VictimAddresses))
		for _, address := range live.VictimAddresses {
			ip, err := netip.ParseAddr(address)
			if err != nil || ip.Zone() != "" || !ip.IsGlobalUnicast() {
				return EnvoyInventory{}, fmt.Errorf("victim has an invalid address")
			}
			victimIPs[ip.Unmap()] = true
		}
		for _, pod := range live.Pods {
			if types.UID(pod.UID) == live.Scope.VictimPodUID || pod.Fleet != live.Fleet {
				return EnvoyInventory{}, fmt.Errorf("writer withdrawal survivor is the victim or outside the index fleet")
			}
			for _, address := range pod.Addresses {
				ip, err := netip.ParseAddr(address)
				if err != nil || victimIPs[ip.Unmap()] {
					return EnvoyInventory{}, fmt.Errorf("writer withdrawal survivor address is invalid or belongs to the victim")
				}
			}
		}
		scope := live.Scope
		base, err := url.Parse(scope.AuthProbeURL)
		if err != nil || base.Path != "" {
			return EnvoyInventory{}, fmt.Errorf("writer withdrawal requires an index hostname routed at the root")
		}
		return EnvoyInventory{
			WeightEvidenceScope: WeightEvidenceScope{Namespace: scope.Namespace, InstanceUID: scope.InstanceUID, InstanceGeneration: scope.InstanceGeneration,
				OperationUID: scope.OperationUID, Role: "ingest", RouteName: scope.RouteName, RouteUID: scope.RouteUID,
				RouteVersion: scope.RouteVersion, RouteAuthIdentity: live.Credential.RouteAuthIdentity},
			Weights: map[string]int{live.Fleet: 100}, Pods: live.Pods, Proxies: live.Proxies,
			GatewayVersions: live.GatewayVersions, routeCredential: live.Credential,
			writerVictims: victimIPs,
			rdsName:       scope.GatewayNamespace + "/" + scope.GatewayName + "/" + scope.GatewayListener, literalHost: base.Hostname(),
		}, nil
	}
	measured, err := observer.observe(ctx, 1)
	if err != nil {
		return WriterWithdrawalEvidence{}, err
	}
	survivors := make(map[types.UID]string, len(initial.Pods))
	processes := make(map[string]string, len(initial.Pods))
	for _, pod := range initial.Pods {
		if _, duplicate := survivors[types.UID(pod.UID)]; duplicate {
			return WriterWithdrawalEvidence{}, fmt.Errorf("duplicate survivor UID")
		}
		survivors[types.UID(pod.UID)] = pod.ResourceVersion
		processes[pod.UID] = pod.ProcessIncarnation
	}
	evidence := WriterWithdrawalEvidence{WriterWithdrawalScope: initial.Scope, ObservedAt: measured.InventoryObservedAt,
		EndpointVersions: initial.EndpointVersions, GatewayVersions: initial.GatewayVersions, ServingProxyUIDs: measured.ServingProxyUIDs}
	for _, proxy := range measured.Proxies {
		if proxy.Failures != 0 || proxy.Requests[initial.Fleet] != observer.Samples ||
			len(proxy.Healthy) != 1 || !maps.Equal(proxy.Healthy[initial.Fleet], processes) {
			return WriterWithdrawalEvidence{}, fmt.Errorf("Envoy replica %q did not route only to healthy qualified writers", proxy.ProxyUID)
		}
		evidence.Proxies = append(evidence.Proxies, WriterProxyReport{ProxyUID: proxy.ProxyUID, ObservedAt: proxy.ObservedAt, LiveBackends: maps.Clone(survivors)})
	}
	replicas := GatewayReplicaObservation{Versions: initial.GatewayVersions, UIDs: make(map[string]bool, len(initial.Proxies))}
	for uid := range initial.Proxies {
		replicas.UIDs[uid] = true
	}
	if _, err := evidence.verify(initial.Scope, initial.EndpointVersions, survivors, replicas, initial.WithdrawnAt, time.Now()); err != nil {
		return WriterWithdrawalEvidence{}, err
	}
	return evidence, nil
}
