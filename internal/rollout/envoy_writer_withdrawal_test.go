package rollout

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/x509"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/types"

	"github.com/huyz0/opensearch-serverless-operator/internal/java"
)

func TestEnvoyWriterWithdrawalRequiresEveryReplicaToDropTheVictim(t *testing.T) {
	const cluster = "httproute/tenant/catalog-index/rule/0"
	const routeURL = "https://example.com"
	var stale, staleRoute, drained, panicDisabled, stillServing atomic.Bool
	newProxy := func(canBeStale bool) (*httptest.Server, *httptest.Server) {
		admin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Header.Get("Authorization") != "" {
				t.Error("admin request carried the route credential")
			}
			const survivor = `{"socket_address":{"address":"10.0.0.2","port_value":9200}}`
			const victim = `{"socket_address":{"address":"10.0.0.1","port_value":9200}}`
			extra := ""
			if canBeStale && stale.Load() {
				health := "HEALTHY"
				if drained.Load() {
					health = "UNHEALTHY"
				}
				extra = fmt.Sprintf(`,{"endpoint":{"address":%s},"health_status":%q}`, victim, health)
			}
			switch r.URL.Path {
			case "/config_dump":
				if r.URL.Query().Get("resource") == "dynamic_route_configs" {
					extra := ""
					if canBeStale && staleRoute.Load() {
						extra = `,{"match":{"prefix":"/old-write-path"},"route":{"cluster":"stale-index"}}`
					}
					fmt.Fprintf(w, `{"configs":[{"route_config":{"name":"tenant/gateway/https","virtual_hosts":[{"domains":["example.com"],"routes":[{"match":{"prefix":"/"},"route":{"cluster":%q}}%s]}]}}]}`, cluster, extra)
					return
				}
				if r.URL.Query().Get("resource") == "dynamic_active_clusters" {
					threshold := ""
					if panicDisabled.Load() {
						threshold = `"healthy_panic_threshold":{}`
					}
					fmt.Fprintf(w, `{"configs":[{"cluster":{"name":%q,"common_lb_config":{%s}}}]}`, cluster, threshold)
					return
				}
				fmt.Fprintf(w, `{"configs":[{"endpoint_config":{"cluster_name":%q,"endpoints":[{"load_balancing_weight":1,"lb_endpoints":[{"endpoint":{"address":%s}}%s]}]}}]}`, cluster, survivor, extra)
			case "/clusters":
				host := ""
				if canBeStale && stale.Load() {
					health := "DRAINING"
					if stillServing.Load() {
						health = "HEALTHY"
					}
					host = fmt.Sprintf(`,{"address":%s,"health_status":{"eds_health_status":%q}}`, victim, health)
				}
				fmt.Fprintf(w, `{"cluster_statuses":[{"name":%q,"host_statuses":[{"address":%s,"health_status":{"eds_health_status":"HEALTHY"}}%s]}]}`, cluster, survivor, host)
			default:
				http.NotFound(w, r)
			}
		}))
		edge := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			username, password, auth := r.BasicAuth()
			if !auth || username != "auth-reader" || password != "password" {
				w.WriteHeader(http.StatusUnauthorized)
				return
			}
			if r.Host != "example.com" || r.TLS.ServerName != "example.com" || r.URL.Path != "/_serverless/stats" {
				t.Error("client probe lost its authenticated production route")
			}
			fmt.Fprint(w, `{"node":"survivor-node","name":"survivor","process_incarnation":"survivor-jvm","roles":["ingest"],"lease":{"valid_now":true,"expires_in_millis":20000,"ttl_millis":30000}}`)
		}))
		t.Cleanup(admin.Close)
		t.Cleanup(edge.Close)
		return admin, edge
	}
	admin1, edge1 := newProxy(false)
	admin2, edge2 := newProxy(true)
	roots := x509.NewCertPool()
	roots.AddCert(edge1.Certificate())
	roots.AddCert(edge2.Certificate())
	auth := RouteAuthIdentity{AuthSecretName: "index-auth", AuthSecretUID: "secret-uid", AuthSecretVersion: "3", AuthProbeURL: routeURL}
	inventory := EnvoyWriterWithdrawalInventory{
		WithdrawnAt: time.Now(),
		Scope: WriterWithdrawalScope{RouteAuthIdentity: auth, Namespace: "tenant", InstanceUID: "instance", InstanceGeneration: 1,
			OperationUID: "operation", VictimPodUID: "victim", VictimPodVersion: "10", Writer: java.WriterProcess{NodeID: "victim-node", EphemeralID: "victim-jvm"},
			ServiceName: "catalog-index", ServiceUID: "index-service", ServiceVersion: "11", RouteName: "catalog-index", RouteUID: "route", RouteVersion: "12"},
		Fleet: "blue", VictimAddresses: []string{"10.0.0.1"}, EndpointVersions: map[types.UID]string{"slice": "13"},
		Proxies: map[string]EnvoyTunnel{"proxy1": {strings.TrimPrefix(admin1.URL, "http://"), strings.TrimPrefix(edge1.URL, "https://")},
			"proxy2": {strings.TrimPrefix(admin2.URL, "http://"), strings.TrimPrefix(edge2.URL, "https://")}},
		GatewayVersions: map[string]string{"service/gateway": "5"},
		Pods:            map[string]EnvoyBackend{"survivor": {UID: "survivor", ResourceVersion: "14", Fleet: "blue", ProcessIncarnation: "survivor-jvm", Addresses: []string{"10.0.0.2"}}},
		Credential:      &RouteCredential{RouteAuthIdentity: auth, Username: "auth-reader", Password: []byte("password")},
	}
	inventory.Scope.GatewayNamespace, inventory.Scope.GatewayName, inventory.Scope.GatewayListener = "tenant", "gateway", "https"
	inventory.Scope.GatewayUID, inventory.Scope.GatewayVersion, inventory.Scope.GatewayServiceUID = "gateway-uid", "1", "gateway-service-uid"
	read := func(context.Context) (EnvoyWriterWithdrawalInventory, error) { return inventory, nil }
	started := time.Now()
	evidence, err := ObserveEnvoyWriterWithdrawal(context.Background(), routeURL, roots, read)
	if err != nil || len(evidence.Proxies) != 2 {
		t.Fatalf("Envoy observation = %+v, %v", evidence, err)
	}
	public, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	signed, err := SignWriterWithdrawalEvidence(evidence, private)
	if err != nil {
		t.Fatal(err)
	}
	survivors := map[types.UID]string{"survivor": "14"}
	gateway := GatewayReplicaObservation{UIDs: map[string]bool{"proxy1": true, "proxy2": true}, Versions: inventory.GatewayVersions}
	_, err = ObserveSignedWriterWithdrawal(signed, public, inventory.Scope, inventory.EndpointVersions, survivors, gateway, started, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	changedScope := inventory.Scope
	changedScope.Writer.EphemeralID = "replacement"
	if _, err := ObserveSignedWriterWithdrawal(signed, public, changedScope, inventory.EndpointVersions, survivors, gateway, started, time.Now()); err == nil {
		t.Fatal("accepted evidence for an earlier JVM")
	}
	if _, err := ObserveSignedWriterWithdrawal(signed, public, inventory.Scope, map[types.UID]string{"slice": "15"}, survivors, gateway, started, time.Now()); err == nil {
		t.Fatal("accepted evidence for an earlier EndpointSlice")
	}
	gateway.UIDs["new-proxy"] = true
	if _, err := ObserveSignedWriterWithdrawal(signed, public, inventory.Scope, inventory.EndpointVersions, survivors, gateway, started, time.Now()); err == nil {
		t.Fatal("accepted evidence missing a newly serving proxy")
	}
	reads := 0
	reusedInventory := func(context.Context) (EnvoyWriterWithdrawalInventory, error) {
		reads++
		if reads == 2 {
			inventory.EndpointVersions["slice"] = "15"
		}
		return inventory, nil
	}
	if _, err := ObserveEnvoyWriterWithdrawal(context.Background(), routeURL, roots, reusedInventory); err == nil ||
		!strings.Contains(err.Error(), "inventory changed") {
		t.Fatalf("accepted an EndpointSlice change through a reused inventory map: %v", err)
	}
	inventory.EndpointVersions["slice"] = "13"
	originalWithdrawal := inventory.WithdrawnAt
	inventory.WithdrawnAt = time.Now().Add(time.Minute)
	if _, err := ObserveEnvoyWriterWithdrawal(context.Background(), routeURL, roots, read); err == nil {
		t.Fatal("accepted evidence collected before the recorded withdrawal")
	}
	inventory.WithdrawnAt = originalWithdrawal
	staleRoute.Store(true)
	if _, err := ObserveEnvoyWriterWithdrawal(context.Background(), routeURL, roots, read); err == nil {
		t.Fatal("accepted an old active route rule that can still direct writes")
	}
	staleRoute.Store(false)
	stale.Store(true)
	if _, err := ObserveEnvoyWriterWithdrawal(context.Background(), routeURL, roots, read); err == nil {
		t.Fatal("accepted a replica with a healthy victim in EDS")
	}
	drained.Store(true)
	if _, err := ObserveEnvoyWriterWithdrawal(context.Background(), routeURL, roots, read); err == nil {
		t.Fatal("accepted a drained victim while Envoy could route to it in panic mode")
	}
	panicDisabled.Store(true)
	if _, err := ObserveEnvoyWriterWithdrawal(context.Background(), routeURL, roots, read); err != nil {
		t.Fatalf("explicitly drained victim with panic disabled should qualify: %v", err)
	}
	stillServing.Store(true)
	if _, err := ObserveEnvoyWriterWithdrawal(context.Background(), routeURL, roots, read); err == nil {
		t.Fatal("accepted a live healthy victim reported as unhealthy by EDS")
	}
}
