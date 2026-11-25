package security_test

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/huyz0/opensearch-serverless-operator/internal/security"
)

func TestUserAdminRequiresVerifiedTLSAndDoesNotRetryAmbiguousWrites(t *testing.T) {
	var mu sync.Mutex
	users := map[string]string{}
	puts := 0
	var loseResponse, redirect atomic.Bool
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		user, password, ok := req.BasicAuth()
		if !ok || user != "bootstrap" || password != "admin-secret" {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		if redirect.Load() {
			http.Redirect(w, req, "/foreign", http.StatusFound)
			return
		}
		name := strings.TrimPrefix(req.URL.Path, "/management/_serverless/security/users/")
		if name == req.URL.Path {
			http.NotFound(w, req)
			return
		}
		mu.Lock()
		defer mu.Unlock()
		switch req.Method {
		case http.MethodPut:
			var body struct {
				Password string `json:"password"`
			}
			if err := json.NewDecoder(req.Body).Decode(&body); err != nil {
				http.Error(w, "bad body", http.StatusBadRequest)
				return
			}
			puts++
			users[name] = body.Password
			if loseResponse.Load() {
				connection, _, err := w.(http.Hijacker).Hijack()
				if err == nil {
					_ = connection.Close()
				}
				return
			}
			_, _ = fmt.Fprintf(w, `{"user":%q,"acknowledged":true}`, name)
		case http.MethodGet:
			_, exists := users[name]
			if !exists {
				w.WriteHeader(http.StatusNotFound)
			}
			_, _ = fmt.Fprintf(w, `{"user":%q,"exists":%t}`, name, exists)
		case http.MethodDelete:
			_, found := users[name]
			delete(users, name)
			if !found {
				w.WriteHeader(http.StatusNotFound)
			}
			_, _ = fmt.Fprintf(w, `{"user":%q,"found":%t}`, name, found)
		}
	}))
	defer server.Close()
	client := server.Client()
	client.Transport.(*http.Transport).Proxy = nil
	admin := security.UserAdmin{BaseURL: server.URL + "/management", Username: "bootstrap", Password: "admin-secret", Client: client}
	ctx := context.Background()
	if err := admin.Put(ctx, "bootstrap", "replacement"); err == nil {
		t.Fatal("bootstrap password cannot be rotated through the account endpoint")
	}
	if _, err := admin.Delete(ctx, "bootstrap"); err == nil {
		t.Fatal("bootstrap password cannot be revoked through the account endpoint")
	}
	if _, err := admin.Exists(ctx, "bootstrap"); err == nil {
		t.Fatal("indexed account existence cannot represent the bootstrap account")
	}
	if exists, err := admin.Exists(ctx, "reader"); err != nil || exists {
		t.Fatalf("missing account: %v %v", exists, err)
	}
	if err := admin.Put(ctx, "reader", "first-secret"); err != nil {
		t.Fatal(err)
	}
	if exists, err := admin.Exists(ctx, "reader"); err != nil || !exists {
		t.Fatalf("created account: %v %v", exists, err)
	}
	if err := admin.Put(ctx, "reader", "new-secret"); err != nil {
		t.Fatal(err)
	}
	if found, err := admin.Delete(ctx, "reader"); err != nil || !found {
		t.Fatalf("delete account: %v %v", found, err)
	}
	if found, err := admin.Delete(ctx, "reader"); err != nil || found {
		t.Fatalf("delete absent account: %v %v", found, err)
	}
	loseResponse.Store(true)
	if err := admin.Put(ctx, "reader", "ambiguous-secret"); !errors.Is(err, security.ErrUserMutationUnknown) || strings.Contains(err.Error(), "secret") {
		t.Fatalf("ambiguous mutation not reported safely: %v", err)
	}
	mu.Lock()
	stored, count := users["reader"], puts
	mu.Unlock()
	if stored != "ambiguous-secret" || count != 3 {
		t.Fatalf("ambiguous write was retried or not applied: calls=%d", count)
	}
	loseResponse.Store(false)
	redirect.Store(true)
	if err := admin.Put(ctx, "reader", "another-secret"); !errors.Is(err, security.ErrUserMutationUnknown) {
		t.Fatalf("redirect accepted: %v", err)
	}
	redirect.Store(false)
	insecure := admin
	insecure.Client = &http.Client{Transport: &http.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: true}}}
	if err := insecure.Put(ctx, "reader", "secret"); err == nil {
		t.Fatal("unverified TLS accepted")
	}
	admin.BaseURL = "http://example.com"
	if err := admin.Put(ctx, "reader", "secret"); err == nil {
		t.Fatal("plaintext account management accepted")
	}
	admin.BaseURL = server.URL + "/management"
	for _, name := range []string{".", "..", "../escape", "_version"} {
		if err := admin.Put(ctx, name, "secret"); err == nil || errors.Is(err, security.ErrUserMutationUnknown) {
			t.Fatalf("invalid account name %q reached dispatch: %v", name, err)
		}
	}
}
