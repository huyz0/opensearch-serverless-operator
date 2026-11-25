package tlsconfig_test

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"encoding/pem"
	"math/big"
	"testing"
	"time"

	"github.com/huyz0/opensearch-serverless-operator/internal/tlsconfig"
)

func TestRolloverRequiresOverlapAndRetainedTrust(t *testing.T) {
	now := time.Now()
	makeChain := func(serial int64, expiry time.Time) ([]byte, []byte) {
		t.Helper()
		key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		if err != nil {
			t.Fatal(err)
		}
		ca := &x509.Certificate{SerialNumber: big.NewInt(serial), NotBefore: now.Add(-time.Hour), NotAfter: expiry, IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign}
		root, err := x509.CreateCertificate(rand.Reader, ca, ca, &key.PublicKey, key)
		if err != nil {
			t.Fatal(err)
		}
		leaf := &x509.Certificate{SerialNumber: big.NewInt(serial + 100), DNSNames: []string{"node.example"}, NotBefore: now.Add(-time.Hour), NotAfter: expiry, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth}}
		der, err := x509.CreateCertificate(rand.Reader, leaf, ca, &key.PublicKey, key)
		if err != nil {
			t.Fatal(err)
		}
		return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: root}), pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	}
	oldCA, oldLeaf := makeChain(1, now.Add(2*time.Hour))
	newCA, newLeaf := makeChain(2, now.Add(2*time.Hour))
	usage := []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth}
	leaves := []tlsconfig.RolloverLeaf{
		{Name: "old", ChainPEM: oldLeaf, Identity: "node.example", Usages: usage},
		{Name: "new", ChainPEM: newLeaf, Identity: "node.example", Usages: usage, RetainFinal: true},
	}
	overlap := append(append([]byte{}, oldCA...), newCA...)
	check := func(want bool, trust, final []byte, inventory []tlsconfig.RolloverLeaf, until time.Time) {
		t.Helper()
		err := tlsconfig.ValidateRollover(trust, final, inventory, now, until)
		if (err == nil) != want {
			t.Fatalf("success=%v, err=%v", want, err)
		}
	}
	check(true, overlap, newCA, leaves, now.Add(time.Hour))
	check(false, newCA, newCA, leaves, now.Add(time.Hour))
	leaves[0].RetainFinal = true
	check(false, overlap, newCA, leaves, now.Add(time.Hour))
	leaves[0].RetainFinal = false
	check(false, overlap, newCA, leaves, now.Add(3*time.Hour))
	leaves[1].Identity = "wrong.example"
	check(false, overlap, newCA, leaves, now.Add(time.Hour))
	leaves[1].Identity = "node.example"
	leaves[1].Name = "old"
	check(false, overlap, newCA, leaves, now.Add(time.Hour))
	leaves[1].Name = "new"
	leaves[1].Usages = []x509.ExtKeyUsage{x509.ExtKeyUsageAny}
	check(false, overlap, newCA, leaves, now.Add(time.Hour))
	leaves[1].Usages = usage
	leaves[1].RetainFinal = false
	check(false, overlap, newCA, leaves, now.Add(time.Hour))
	leaves[1].RetainFinal = true
	leaves[1].ChainPEM = append([]byte("garbage\n"), newLeaf...)
	check(false, overlap, newCA, leaves, now.Add(time.Hour))
}
