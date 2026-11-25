package main

import (
	"encoding/pem"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
)

func TestBasicAuthUsesExactBytesOnlyAtVerifiedEndpoint(t *testing.T) {
	const password = "fixture:password\n"
	var requests atomic.Int32
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		user, received, ok := r.BasicAuth()
		if !ok || user != "probe" || received != password {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		w.Header().Set("Location", "/redirected")
		w.WriteHeader(http.StatusTemporaryRedirect)
	}))
	defer server.Close()
	root := t.TempDir()
	ca, secret := filepath.Join(root, "ca.pem"), filepath.Join(root, "password")
	if err := os.WriteFile(ca, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw}), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(secret, []byte(password), 0600); err != nil {
		t.Fatal(err)
	}
	host := strings.TrimPrefix(server.URL, "https://")
	client, _, err := newClient(ca, "127.0.0.1", "", "", "")
	if err != nil {
		t.Fatal(err)
	}
	if err := configureBasicAuth(client, host, "probe", secret); err != nil {
		t.Fatal(err)
	}
	const readinessPath = "/_serverless/ready"
	response, err := client.Get(server.URL + readinessPath)
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusTemporaryRedirect || requests.Load() != 1 {
		t.Fatalf("expected one authenticated request and an unfollowed redirect, got %d and %d requests", response.StatusCode, requests.Load())
	}
	for _, endpoint := range []string{"http://" + host + readinessPath, "https://localhost:" + strings.Split(host, ":")[1] + readinessPath, "https://probe@" + host + readinessPath, server.URL + "/"} {
		if _, err := client.Get(endpoint); err == nil {
			t.Fatalf("accepted an endpoint outside the configured origin: %s", endpoint)
		}
	}
	wrongIdentity, _, err := newClient(ca, "wrong.invalid", "", "", "")
	if err != nil {
		t.Fatal(err)
	}
	if err := configureBasicAuth(wrongIdentity, host, "probe", secret); err != nil {
		t.Fatal(err)
	}
	if _, err := wrongIdentity.Get(server.URL + readinessPath); err == nil || requests.Load() != 1 {
		t.Fatal("credentials reached the server without a verified identity")
	}
	plain, _, err := newClient("", "", "", "", "")
	if err != nil {
		t.Fatal(err)
	}
	if err := configureBasicAuth(plain, host, "probe", secret); err == nil {
		t.Fatal("accepted probe credentials without TLS")
	}
}
