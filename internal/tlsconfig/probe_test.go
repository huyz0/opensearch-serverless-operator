package tlsconfig_test

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"math/big"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/huyz0/opensearch-serverless-operator/internal/tlsconfig"
)

type testCA struct {
	certificate *x509.Certificate
	key         *ecdsa.PrivateKey
	pem         []byte
}

func makeTestCA(t *testing.T, serial int64) testCA {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	cert := &x509.Certificate{SerialNumber: big.NewInt(serial), NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign}
	der, err := x509.CreateCertificate(rand.Reader, cert, cert, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	return testCA{certificate: parsed, key: key, pem: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})}
}

func issueTestLeaf(t *testing.T, ca testCA, serial int64, usage x509.ExtKeyUsage) ([]byte, []byte) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	leaf := &x509.Certificate{SerialNumber: big.NewInt(serial), DNSNames: []string{"node.example"}, NotBefore: time.Now().Add(-time.Minute), NotAfter: time.Now().Add(30 * time.Minute), ExtKeyUsage: []x509.ExtKeyUsage{usage}}
	der, err := x509.CreateCertificate(rand.Reader, leaf, ca.certificate, &key.PublicKey, ca.key)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER})
}

func tlsTestServer(t *testing.T, certPEM, keyPEM, clientTrust []byte) *httptest.Server {
	t.Helper()
	cert, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		t.Fatal(err)
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(clientTrust) {
		t.Fatal("invalid client trust")
	}
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNoContent) }))
	server.TLS = &tls.Config{Certificates: []tls.Certificate{cert}, ClientAuth: tls.RequireAndVerifyClientCert, ClientCAs: roots, MinVersion: tls.VersionTLS12}
	server.StartTLS()
	t.Cleanup(server.Close)
	return server
}

func TestHTTPSProbeChecksTrustIdentityAndCAOverlap(t *testing.T) {
	oldCA, newCA := makeTestCA(t, 1), makeTestCA(t, 2)
	oldServerCert, oldServerKey := issueTestLeaf(t, oldCA, 3, x509.ExtKeyUsageServerAuth)
	newServerCert, newServerKey := issueTestLeaf(t, newCA, 4, x509.ExtKeyUsageServerAuth)
	oldClientCert, oldClientKey := issueTestLeaf(t, oldCA, 5, x509.ExtKeyUsageClientAuth)
	newClientCert, newClientKey := issueTestLeaf(t, newCA, 6, x509.ExtKeyUsageClientAuth)
	overlap := append(append([]byte{}, oldCA.pem...), newCA.pem...)
	oldServer := tlsTestServer(t, oldServerCert, oldServerKey, overlap)
	newServer := tlsTestServer(t, newServerCert, newServerKey, newCA.pem)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	observation, err := tlsconfig.ProbeHTTPS(ctx, oldServer.URL, "node.example", overlap, oldClientCert, oldClientKey)
	if err != nil || observation.CertificateFingerprint == "" || observation.HTTPStatus != http.StatusNoContent {
		t.Fatalf("old leaf did not work during overlap: %+v, %v", observation, err)
	}
	if _, err := tlsconfig.ProbeHTTPS(ctx, newServer.URL, "node.example", overlap, newClientCert, newClientKey); err != nil {
		t.Fatalf("new leaf did not work during overlap: %v", err)
	}
	if _, err := tlsconfig.ProbeHTTPS(ctx, oldServer.URL, "node.example", newCA.pem, oldClientCert, oldClientKey); err == nil {
		t.Fatal("old server CA accepted after removal")
	}
	if _, err := tlsconfig.ProbeHTTPS(ctx, newServer.URL, "node.example", overlap, oldClientCert, oldClientKey); err == nil {
		t.Fatal("old client CA accepted after removal")
	}
	if _, err := tlsconfig.ProbeHTTPS(ctx, oldServer.URL, "wrong.example", overlap, oldClientCert, oldClientKey); err == nil {
		t.Fatal("wrong server identity accepted")
	}
	if _, err := tlsconfig.ProbeHTTPS(ctx, oldServer.URL, "node.example", overlap, nil, nil); err == nil {
		t.Fatal("server requiring mTLS accepted no client certificate")
	}
}
