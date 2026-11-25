package controller

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"io"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"sync/atomic"
	"testing"

	"github.com/huyz0/opensearch-serverless-operator/api/v1alpha1"
	"github.com/huyz0/opensearch-serverless-operator/internal/security"
)

func TestAccountPodProbeVerifiesCertificateBeforeSendingCredentials(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		requests.Add(1)
		if req.URL.Path != "/_serverless/stats" {
			t.Error("probe did not request Java stats")
		}
		user, password, ok := req.BasicAuth()
		if !ok {
			w.Header().Set("WWW-Authenticate", `Basic realm="opensearch-serverless", charset="UTF-8"`)
			w.WriteHeader(http.StatusUnauthorized)
			_, _ = io.WriteString(w, `{"status":401,"error":{"type":"no_credentials"}}`)
			return
		}
		if (user != "bootstrap" || password != "fixture-admin") && (user != "reader" || password != "fixture-reader") {
			t.Error("probe sent unexpected credentials")
		}
		_, _ = io.WriteString(w, `{"name":"catalog-a","roles":["search"],"process_incarnation":"jvm-a"}`)
	}))
	defer server.Close()
	address, err := netip.ParseAddrPort(server.Listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	roots := x509.NewCertPool()
	roots.AddCert(server.Certificate())
	admin := security.UserAdmin{Username: "bootstrap", Password: "fixture-admin"}
	process := security.AccountProcess{Name: "catalog-a", Role: "search"}
	ctx := context.Background()
	if result, err := observeAccountOnPod(ctx, address, &tls.Config{ServerName: "wrong.invalid", RootCAs: roots}, admin, process, "reader", "", "fixture-reader"); err == nil || result.Incarnation != "" || requests.Load() != 0 {
		t.Fatalf("wrong SAN returned evidence or let credentials through: %v %v %d", result, err, requests.Load())
	}
	instance := &v1alpha1.OpenSearchServerless{}
	instance.Status.HTTPTLSBaseline = &v1alpha1.HTTPTLSBaseline{LeafSHA256: "another-leaf"}
	config, err := managedPodTLSConfig(instance, "initial", "example.com", roots)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := observeAccountOnPod(ctx, address, config, admin, process, "reader", "", "fixture-reader"); err == nil || requests.Load() != 0 {
		t.Fatalf("wrong fleet leaf delivered account credentials: %v", err)
	}
	for _, identity := range []string{"example.com", address.Addr().String()} {
		result, err := observeAccountOnPod(ctx, address, &tls.Config{ServerName: identity, RootCAs: roots}, admin, process, "reader", "", "fixture-reader")
		if err != nil || result.Incarnation != "jvm-a" {
			t.Fatalf("trusted identity %s returned %v, %v", identity, result, err)
		}
	}
	if count := requests.Load(); count != 8 {
		t.Fatalf("unexpected number of authenticated and control requests: %d", count)
	}
}
