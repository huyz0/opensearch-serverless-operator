package rollout

import (
	"context"
	"crypto/x509"
	"fmt"
	"maps"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"strings"
	"sync/atomic"
	"testing"
)

func TestEnvoyObserverVerifiesActiveConfigurationAndTraffic(t *testing.T) {
	const cluster = "httproute/tenant/catalog-search/rule/0"
	var greenWeight atomic.Int32
	greenWeight.Store(20)
	var changeAssignment, probed, requireAuth atomic.Bool
	address := func(ip string) string {
		return fmt.Sprintf(`{"socket_address":{"address":%q,"port_value":9200}}`, ip)
	}
	admin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "" {
			t.Error("route credentials were sent to Envoy admin")
		}
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/config_dump":
			if !r.URL.Query().Has("include_eds") || r.URL.Query().Get("resource") != "dynamic_endpoint_configs" {
				t.Error("observer did not limit its configuration request to EDS")
			}
			green := greenWeight.Load()
			subzone := "original"
			if changeAssignment.Load() && probed.Load() {
				subzone = "changed"
			}
			fmt.Fprintf(w, `{"configs":[{"endpoint_config":{
			  "cluster_name":%q,"endpoints":[
			    {"locality":{"sub_zone":%q},"load_balancing_weight":%d,"lb_endpoints":[{"endpoint":{"address":%s}}]},
			    {"load_balancing_weight":%d,"lb_endpoints":[{"endpoint":{"address":%s}}]}
			  ]}}]}`, cluster, subzone, 100-green, address("10.0.0.1"), green, address("10.0.0.2"))
		case "/clusters":
			fmt.Fprintf(w, `{"cluster_statuses":[{"name":%q,"host_statuses":[
			  {"address":%s,"health_status":{"eds_health_status":"HEALTHY"}},
			  {"address":%s,"health_status":{"eds_health_status":"HEALTHY"}}
			]}]}`, cluster, address("10.0.0.1"), address("10.0.0.2"))
		default:
			t.Errorf("unexpected admin request %s", r.URL.Path)
			http.NotFound(w, r)
		}
	}))
	defer admin.Close()
	var requests atomic.Int32
	edge := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/_serverless/stats" {
			t.Errorf("unexpected client probe %s %s", r.Method, r.URL.Path)
		}
		if r.Host != "example.com" || r.TLS.ServerName != "example.com" {
			t.Errorf("probe lost the production Host or SNI: %q %q", r.Host, r.TLS.ServerName)
		}
		if requireAuth.Load() {
			user, password, ok := r.BasicAuth()
			if !ok || user != "route-probe" || password != "auth-fixture" {
				w.WriteHeader(http.StatusUnauthorized)
				return
			}
		}
		name := "blue"
		probed.Store(true)
		if requests.Add(1)%5 == 0 {
			name = "green"
		}
		fmt.Fprintf(w, `{"node":%q,"name":%q,"process_incarnation":%q,"roles":["search"],"lease":{
		  "valid_now":true,"expires_in_millis":20000,"ttl_millis":30000}}`, name, name, name+"-jvm")
	}))
	defer edge.Close()
	roots := x509.NewCertPool()
	roots.AddCert(edge.Certificate())
	inventory := EnvoyInventory{
		WeightEvidenceScope: WeightEvidenceScope{
			Namespace: "tenant", InstanceUID: "instance", InstanceGeneration: 3, OperationUID: "operation", Role: "search",
			RouteName: "catalog-search", RouteUID: "route", RouteVersion: "81",
		},
		Weights: map[string]int{"blue": 80, "green": 20},
		Proxies: map[string]EnvoyTunnel{"proxy-uid": {
			AdminAddress: strings.TrimPrefix(admin.URL, "http://"), ClientAddress: strings.TrimPrefix(edge.URL, "https://"),
		}},
		Pods: map[string]EnvoyBackend{
			"blue":  {UID: "blue-uid", ResourceVersion: "1", Fleet: "blue", ProcessIncarnation: "blue-jvm", Addresses: []string{"10.0.0.1"}},
			"green": {UID: "green-uid", ResourceVersion: "2", Fleet: "green", ProcessIncarnation: "green-jvm", Addresses: []string{"10.0.0.2"}},
		},
	}
	observer := EnvoyObserver{
		RouteURL: "https://example.com", Roots: roots, Samples: 500,
		ReadInventory: func(context.Context) (EnvoyInventory, error) { return inventory, nil },
	}
	evidence, err := observer.Observe(context.Background())
	if err != nil || len(evidence.Proxies) != 1 {
		t.Fatalf("Observe() = %#v, %v", evidence, err)
	}
	report := evidence.Proxies[0]
	if !maps.Equal(report.ConfiguredWeights, inventory.Weights) || !maps.Equal(report.Requests, map[string]int{"blue": 400, "green": 100}) ||
		report.Failures != 0 || report.Healthy["green"]["green-uid"] != "green-jvm" {
		t.Fatalf("unexpected proxy report: %#v", report)
	}
	t.Run("authenticated samples pin the credential and never reach admin", func(t *testing.T) {
		secure := inventory
		secure.RouteAuthIdentity = RouteAuthIdentity{AuthSecretName: "route-probe", AuthSecretUID: "secret", AuthSecretVersion: "17", AuthProbeURL: observer.RouteURL}
		secure.routeCredential = &RouteCredential{RouteAuthIdentity: secure.RouteAuthIdentity, Username: "route-probe", Password: []byte("auth-fixture")}
		authenticated := observer
		authenticated.ReadInventory = func(context.Context) (EnvoyInventory, error) { return secure, nil }
		wrongRoute := authenticated
		wrongRoute.RouteURL += "/wrong-path"
		if _, err := wrongRoute.Observe(context.Background()); err == nil || !strings.Contains(err.Error(), "URL differs") {
			t.Fatalf("accepted another URL for an authenticated observation: %v", err)
		}
		if _, err := authenticated.Observe(context.Background()); err == nil || !strings.Contains(err.Error(), "anonymous denial") {
			t.Fatalf("authenticated observation accepted an open route: %v", err)
		}
		requireAuth.Store(true)
		defer requireAuth.Store(false)
		result, err := authenticated.Observe(context.Background())
		if err != nil || result.RouteAuthIdentity != secure.RouteAuthIdentity || result.Proxies[0].Failures != 0 || result.Proxies[0].Requests["green"] != 100 {
			t.Fatalf("authenticated traffic sample failed: %#v %v", result, err)
		}
		secure.routeCredential.Password = []byte("incorrect")
		authenticated.Samples = 8
		result, err = authenticated.observe(context.Background(), 1)
		if err != nil || result.Proxies[0].Failures != authenticated.Samples || len(result.Proxies[0].Requests) != 0 {
			t.Fatalf("wrong password produced successful traffic evidence: %#v %v", result, err)
		}
	})
	greenWeight.Store(19)
	if _, err := observer.Observe(context.Background()); err == nil || !strings.Contains(err.Error(), "configured weights differ") {
		t.Fatalf("accepted proxy still on an older split with identical endpoints: %v", err)
	}
	greenWeight.Store(20)
	changeAssignment.Store(true)
	probed.Store(false)
	if _, err := observer.Observe(context.Background()); err == nil || !strings.Contains(err.Error(), "changed its active assignment") {
		t.Fatalf("accepted a changed assignment with the same weights and endpoints: %v", err)
	}
	changeAssignment.Store(false)
	reads := 0
	observer.ReadInventory = func(context.Context) (EnvoyInventory, error) {
		reads++
		copy := inventory
		if reads == 2 {
			copy.RouteVersion = "82"
		}
		return copy, nil
	}
	if _, err := observer.Observe(context.Background()); err == nil || !strings.Contains(err.Error(), "changed while sampling") {
		t.Fatalf("accepted a route that changed while collecting: %v", err)
	}
	t.Run("IPv4 aliases cannot identify two tunnels", func(t *testing.T) {
		aliased := inventory
		address := netip.MustParseAddrPort(aliased.Proxies["proxy-uid"].AdminAddress)
		tunnel := aliased.Proxies["proxy-uid"]
		tunnel.ClientAddress = fmt.Sprintf("[::ffff:%s]:%d", address.Addr(), address.Port())
		aliased.Proxies = map[string]EnvoyTunnel{"proxy-uid": tunnel}
		check := observer
		check.ReadInventory = func(context.Context) (EnvoyInventory, error) { return aliased, nil }
		if _, err := check.Observe(context.Background()); err == nil || !strings.Contains(err.Error(), "distinct address") {
			t.Fatalf("accepted IPv4 and IPv6 spellings of the same tunnel: %v", err)
		}
	})
	reads = 0
	observer.Samples = 10
	observer.ReadInventory = func(context.Context) (EnvoyInventory, error) {
		reads++
		if reads == 2 {
			// Readers may reuse their backing maps or nested slices across reads.
			inventory.Pods["blue"].Addresses[0] = "10.0.0.9"
		}
		return inventory, nil
	}
	if _, err := observer.observe(context.Background(), 1); err == nil || !strings.Contains(err.Error(), "changed while sampling") {
		t.Fatalf("accepted a backend address change through a reused inventory buffer: %v", err)
	}

}
