package security_test

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/huyz0/opensearch-serverless-operator/internal/security"
)

func TestVerifyAccountRequiresExactProcessAndProviderRefusal(t *testing.T) {
	for _, test := range []struct {
		name, previous, desired, failure string
		want                             bool
	}{
		{name: "rotation", previous: "old-password", desired: "new-password", want: true},
		{name: "creation", desired: "new-password", want: true},
		{name: "deletion", previous: "old-password", want: true},
		{name: "old password still accepted", previous: "new-password", desired: "future-password"},
		{name: "new password still rejected", previous: "old-password", desired: "future-password"},
		{name: "anonymous access", desired: "new-password", failure: "accepted an anonymous"},
		{name: "backend unavailable", previous: "old-password", failure: "HTTP 503"},
		{name: "generic unauthorized", previous: "old-password", failure: "unrecognized HTTP 401"},
		{name: "jvm changed", previous: "old-password", failure: "expected shell process"},
		{name: "connection changed", previous: "old-password", failure: "connection changed"},
	} {
		t.Run(test.name, func(t *testing.T) {
			var requests atomic.Int32
			server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != http.MethodGet || r.URL.Path != "/_serverless/stats" || r.Header.Get("Cache-Control") != "no-cache, no-store" {
					t.Error("credentials were sent outside the expected stats request")
				}
				count := requests.Add(1)
				user, password, hasCredentials := r.BasicAuth()
				if user == "bootstrap" || password == "new-password" || (!hasCredentials && test.name == "anonymous access") {
					incarnation := "pinned-jvm"
					if test.name == "jvm changed" && count > 1 {
						incarnation = "replacement-jvm"
					}
					if test.name == "connection changed" {
						w.Header().Set("Connection", "close")
					}
					fmt.Fprintf(w, `{"name":"catalog-a","roles":["search"],"process_incarnation":%q}`, incarnation)
					return
				}
				if !hasCredentials {
					w.Header().Set("WWW-Authenticate", `Basic realm="opensearch-serverless", charset="UTF-8"`)
					w.WriteHeader(http.StatusUnauthorized)
					_, _ = w.Write([]byte(`{"status":401,"error":{"type":"no_credentials"}}`))
					return
				}
				switch test.name {
				case "backend unavailable":
					w.WriteHeader(http.StatusServiceUnavailable)
				case "generic unauthorized":
					w.WriteHeader(http.StatusUnauthorized)
				default:
					w.Header().Set("WWW-Authenticate", `Basic realm="opensearch-serverless", charset="UTF-8"`)
					w.WriteHeader(http.StatusUnauthorized)
					_, _ = w.Write([]byte(`{"status":401,"error":{"type":"authentication_failed"}}`))
				}
			}))
			defer server.Close()
			client := server.Client()
			client.Transport.(*http.Transport).Proxy = nil
			admin := security.UserAdmin{BaseURL: server.URL, Username: "bootstrap", Password: "fixture-admin", Client: client}
			process := security.AccountProcess{Name: "catalog-a", Role: "search", Incarnation: "pinned-jvm"}
			verified, err := admin.VerifyAccount(context.Background(), process, "reader", test.previous, test.desired)
			if verified != test.want || (test.failure == "" && err != nil) || (test.failure != "" && (err == nil || !strings.Contains(err.Error(), test.failure))) {
				t.Fatalf("VerifyAccount = %v, %v, want %v and error containing %q", verified, err, test.want, test.failure)
			}
		})
	}
}
