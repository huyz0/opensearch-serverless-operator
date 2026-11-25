package main

import (
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestProbeRequiresClientCertificateRequest(t *testing.T) {
	server := httptest.NewUnstartedServer(http.NotFoundHandler())
	server.TLS = &tls.Config{MinVersion: tls.VersionTLS12, MaxVersion: tls.VersionTLS12}
	server.StartTLS()
	defer server.Close()

	leaf := server.Certificate()
	pin := fmt.Sprintf("%x", sha256.Sum256(leaf.Raw))
	key, err := x509.MarshalPKCS8PrivateKey(server.TLS.Certificates[0].PrivateKey)
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	certificate := filepath.Join(dir, "tls.crt")
	keyFile := filepath.Join(dir, "tls.key")
	for path, block := range map[string]*pem.Block{certificate: {Type: "CERTIFICATE", Bytes: leaf.Raw}, keyFile: {Type: "PRIVATE KEY", Bytes: key}} {
		if err := os.WriteFile(path, pem.EncodeToMemory(block), 0600); err != nil {
			t.Fatal(err)
		}
	}
	err = run([]string{"--address", server.Listener.Addr().String(), "--server-name", "127.0.0.1",
		"--server-leaf-sha256", pin, "--certificate", certificate, "--key", keyFile, "--trust", certificate}, io.Discard)
	if err == nil || !strings.Contains(err.Error(), "target did not request a client certificate") {
		t.Fatalf("expected rejection after a server-only TLS handshake, got %v", err)
	}
	err = run([]string{"--address", server.Listener.Addr().String(), "--server-name", "127.0.0.1",
		"--server-leaf-sha256", pin, "--client-leaf-sha256", strings.Repeat("0", 64),
		"--certificate", certificate, "--key", keyFile, "--trust", certificate}, io.Discard)
	if err == nil || !strings.Contains(err.Error(), "mounted client peer leaf SHA-256") {
		t.Fatalf("expected rejection of a mismatched mounted client leaf, got %v", err)
	}
	err = run([]string{"--address", server.Listener.Addr().String(), "--server-name", "127.0.0.1",
		"--server-leaf-sha256", pin, "--without-client-certificate", "--trust", certificate}, io.Discard)
	if err == nil || !strings.Contains(err.Error(), "target accepted a handshake without a client certificate") {
		t.Fatalf("expected explicit evidence of a certless TLS acceptance, got %v", err)
	}
}
