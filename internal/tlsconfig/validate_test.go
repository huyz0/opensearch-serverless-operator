package tlsconfig_test

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"strings"
	"testing"
	"time"

	"github.com/huyz0/opensearch-serverless-operator/internal/tlsconfig"
)

func TestValidateCertificateIdentityAndUsage(t *testing.T) {
	now := time.Now()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	ca := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "test CA"}, NotBefore: now.Add(-time.Hour), NotAfter: now.Add(time.Hour), IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign}
	caDER, err := x509.CreateCertificate(rand.Reader, ca, ca, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	leafKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	leaf := &x509.Certificate{SerialNumber: big.NewInt(2), DNSNames: []string{"node.example"}, NotBefore: now.Add(-time.Minute), NotAfter: now.Add(30 * time.Minute), ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}}
	leafDER, err := x509.CreateCertificate(rand.Reader, leaf, ca, &leafKey.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, err := x509.MarshalPKCS8PrivateKey(leafKey)
	if err != nil {
		t.Fatal(err)
	}
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: leafDER})
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER})
	trust := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDER})
	leafDigest, trustDigest, err := tlsconfig.PublicMaterialDigests(certPEM, trust)
	if err != nil || leafDigest == "" || trustDigest == "" {
		t.Fatalf("valid public digest failed: %v", err)
	}
	if _, _, err := tlsconfig.PublicMaterialDigests(certPEM, append(append([]byte{}, trust...), keyPEM...)); err == nil {
		t.Fatal("private key in trust input must not be hashed into public status")
	}
	info, err := tlsconfig.Validate(certPEM, keyPEM, trust, "node.example", []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}, now)
	if err != nil || info.Fingerprint == "" {
		t.Fatalf("valid certificate failed: %v", err)
	}
	chain := []byte(" \r\n" + strings.ReplaceAll(string(certPEM), "\n", "\r\n") + " \t\r\n" + strings.TrimSuffix(string(trust), "\n"))
	withChain, err := tlsconfig.Validate(chain, keyPEM, trust, "node.example", []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}, now)
	if err != nil || withChain.Fingerprint != info.Fingerprint {
		t.Fatalf("valid PEM chain did not preserve the leaf fingerprint: %v", err)
	}
	sec1, err := x509.MarshalECPrivateKey(leafKey)
	if err != nil {
		t.Fatal(err)
	}
	sec1PEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: sec1})
	if _, err := tlsconfig.Validate(certPEM, sec1PEM, trust, "node.example", []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}, now); err != nil {
		t.Fatalf("valid SEC1 key failed: %v", err)
	}
	if _, err := tlsconfig.Validate(certPEM, keyPEM, trust, "other.example", []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}, now); err == nil {
		t.Fatal("wrong identity must fail")
	}
	if _, err := tlsconfig.Validate(certPEM, keyPEM, trust, "node.example", []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}, now); err == nil {
		t.Fatal("wrong usage must fail")
	}
	if _, err := tlsconfig.Validate(certPEM, keyPEM, append([]byte("unexpected-data\n"), trust...), "node.example", []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}, now); err == nil {
		t.Fatal("garbage before the trust bundle must fail")
	}
	invalidBlock := "-----BEGIN CERTIFICATE-----\ninvalid!\n-----END CERTIFICATE-----\n"
	withHeader := func(certificate []byte) []byte {
		return []byte(strings.Replace(string(certificate), "-----BEGIN CERTIFICATE-----\n", "-----BEGIN CERTIFICATE-----\nComment: ignored header\n\n", 1))
	}
	for _, tc := range []struct {
		name        string
		certificate []byte
		trust       []byte
	}{
		{name: "invalid root before valid root", certificate: certPEM, trust: []byte(invalidBlock + string(trust))},
		{name: "invalid root between valid roots", certificate: certPEM, trust: []byte(string(trust) + invalidBlock + string(trust))},
		{name: "unterminated block before leaf", certificate: []byte("-----BEGIN CERTIFICATE-----\n" + string(certPEM)), trust: trust},
		{name: "invalid intermediate before valid chain entry", certificate: []byte(string(certPEM) + invalidBlock + string(trust)), trust: trust},
		{name: "private key in chain", certificate: []byte(string(certPEM) + string(keyPEM)), trust: trust},
		{name: "header in leaf", certificate: withHeader(certPEM), trust: trust},
		{name: "header in sole root", certificate: certPEM, trust: withHeader(trust)},
		{name: "header in one overlap root", certificate: certPEM, trust: append(withHeader(trust), trust...)},
		{name: "indented leaf", certificate: []byte(" " + string(certPEM)), trust: trust},
		{name: "indented sole root", certificate: certPEM, trust: []byte(" " + string(trust))},
		{name: "indented overlap root", certificate: certPEM, trust: []byte(string(trust) + " " + string(trust))},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, _, err := tlsconfig.PublicMaterialDigests(tc.certificate, tc.trust); err == nil {
				t.Fatal("invalid PEM must not produce a public digest")
			}
			if _, err := tlsconfig.Validate(tc.certificate, keyPEM, tc.trust, "node.example", []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}, now); err == nil {
				t.Fatal("invalid PEM must not qualify")
			}
		})
	}
}

func TestValidateWindowRequiresContinuousCertificateCoverage(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	transition, until := now.Add(30*time.Minute), now.Add(time.Hour)
	newKey := func() *ecdsa.PrivateKey {
		t.Helper()
		key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		if err != nil {
			t.Fatal(err)
		}
		return key
	}
	issue := func(cert, issuer *x509.Certificate, public *ecdsa.PublicKey, private *ecdsa.PrivateKey) []byte {
		t.Helper()
		der, err := x509.CreateCertificate(rand.Reader, cert, issuer, public, private)
		if err != nil {
			t.Fatal(err)
		}
		return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	}
	rootKey, intermediateKey, leafKey := newKey(), newKey(), newKey()
	root := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "root"},
		NotBefore: now.Add(-time.Hour), NotAfter: until.Add(time.Hour),
		IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign,
	}
	trust := issue(root, root, &rootKey.PublicKey, rootKey)
	old := &x509.Certificate{
		SerialNumber: big.NewInt(2), Subject: pkix.Name{CommonName: "intermediate"},
		NotBefore: now.Add(-time.Hour), NotAfter: transition,
		IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign,
	}
	oldPEM := issue(old, root, &intermediateKey.PublicKey, rootKey)
	usages := []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth}
	names := []string{"node.example", "service.example"}
	leaf := &x509.Certificate{
		SerialNumber: big.NewInt(3), DNSNames: names, ExtKeyUsage: usages,
		NotBefore: now.Add(-time.Minute), NotAfter: until,
	}
	leafPEM := issue(leaf, old, &leafKey.PublicKey, intermediateKey)
	keyDER, err := x509.MarshalPKCS8PrivateKey(leafKey)
	if err != nil {
		t.Fatal(err)
	}
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER})
	for _, test := range []struct {
		name   string
		starts time.Time
		valid  bool
	}{
		{name: "disjoint chains", starts: transition.Add(time.Second)},
		{name: "paths meet at inclusive expiry", starts: transition, valid: true},
		{name: "overlapping chains", starts: transition.Add(-time.Minute), valid: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			next := *old
			next.SerialNumber, next.NotBefore, next.NotAfter = big.NewInt(4), test.starts, until
			chain := append(append([]byte{}, leafPEM...), oldPEM...)
			chain = append(chain, issue(&next, root, &intermediateKey.PublicKey, rootKey)...)
			for _, at := range []time.Time{now, until} {
				if _, err := tlsconfig.Validate(chain, keyPEM, trust, names[0], usages, at); err != nil {
					t.Fatalf("fixture must qualify at both window endpoints: %v", err)
				}
			}
			if _, err := tlsconfig.ValidateWindow(chain, keyPEM, trust, names, usages, now, until); (err == nil) != test.valid {
				t.Errorf("window qualification: want valid=%t, got %v", test.valid, err)
			}
			inventory := []tlsconfig.RolloverLeaf{{Name: "leaf", ChainPEM: chain, Identity: names[0], Usages: usages, RetainFinal: true}}
			if err := tlsconfig.ValidateRollover(trust, trust, inventory, now, until); (err == nil) != test.valid {
				t.Errorf("rollover qualification: want valid=%t, got %v", test.valid, err)
			}
		})
	}
}
