package tlsconfig_test

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"encoding/pem"
	"math/big"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	clienttesting "k8s.io/client-go/testing"

	"github.com/huyz0/opensearch-serverless-operator/internal/tlsconfig"
)

func snapshotFixture(t *testing.T) (*fake.Clientset, tlsconfig.SnapshotIntent, time.Time) {
	t.Helper()
	now := time.Now().Truncate(time.Second)
	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	ca := &x509.Certificate{SerialNumber: big.NewInt(1), NotBefore: now.Add(-time.Hour), NotAfter: now.Add(5 * time.Hour), IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign}
	caDER, err := x509.CreateCertificate(rand.Reader, ca, ca, &caKey.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	leaf := &x509.Certificate{SerialNumber: big.NewInt(2), DNSNames: []string{"node.example", "service.example"}, NotBefore: now.Add(-time.Minute), NotAfter: now.Add(4 * time.Hour), ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}}
	leafDER, err := x509.CreateCertificate(rand.Reader, leaf, ca, &key.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	leafSecret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "leaf", Namespace: "n", UID: "leaf-uid", ResourceVersion: "10"}, Type: corev1.SecretTypeTLS, Data: map[string][]byte{corev1.TLSCertKey: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: leafDER}), corev1.TLSPrivateKeyKey: pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER})}}
	trustSecret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "trust", Namespace: "n", UID: "trust-uid", ResourceVersion: "20"}, Data: map[string][]byte{"ca.crt": pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDER})}}
	client := fake.NewSimpleClientset(leafSecret, trustSecret)
	client.PrependReactor("create", "secrets", func(action clienttesting.Action) (bool, runtime.Object, error) {
		secret := action.(clienttesting.CreateAction).GetObject().(*corev1.Secret)
		secret.UID = "snapshot-uid"
		secret.ResourceVersion = "30"
		return false, nil, nil
	})
	controller := true
	intent := tlsconfig.SnapshotIntent{Namespace: "n", Name: "snapshot", Owner: metav1.OwnerReference{APIVersion: "example/v1", Kind: "Workload", Name: "owner", UID: "owner-uid", Controller: &controller}, LeafName: "leaf", LeafUID: leafSecret.UID, LeafResourceVersion: "10", TrustName: "trust", TrustUID: trustSecret.UID, TrustResourceVersion: "20", TrustKey: "ca.crt", Identities: []string{"node.example", "service.example"}, Usages: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}, MinimumValidity: time.Hour}
	return client, intent, now
}

func TestSnapshotRecoversAfterSourceChanges(t *testing.T) {
	client, intent, now := snapshotFixture(t)
	planned, err := tlsconfig.PlanSnapshot(context.Background(), client.CoreV1(), intent, now)
	if err != nil {
		t.Fatal(err)
	}
	created, err := tlsconfig.EnsureSnapshot(context.Background(), client.CoreV1(), planned, now)
	if err != nil || created.Immutable == nil || !*created.Immutable {
		t.Fatalf("snapshot not created: %v", err)
	}
	leaf, err := client.CoreV1().Secrets("n").Get(context.Background(), "leaf", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	leaf.ResourceVersion = "11"
	if _, err := client.CoreV1().Secrets("n").Update(context.Background(), leaf, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	recovered, err := tlsconfig.EnsureSnapshot(context.Background(), client.CoreV1(), planned, now)
	if err != nil || recovered.UID != created.UID {
		t.Fatalf("existing snapshot did not recover: %v", err)
	}
	recovered.Data[corev1.TLSPrivateKeyKey] = []byte("not the private key")
	if _, err := client.CoreV1().Secrets("n").Update(context.Background(), recovered, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := tlsconfig.EnsureSnapshot(context.Background(), client.CoreV1(), planned, now); err == nil {
		t.Fatal("invalid snapshot key was accepted")
	}
}

func TestSnapshotRejectsChangedSourceAndInsufficientValidity(t *testing.T) {
	client, intent, now := snapshotFixture(t)
	planned, err := tlsconfig.PlanSnapshot(context.Background(), client.CoreV1(), intent, now)
	if err != nil {
		t.Fatal(err)
	}
	leaf, err := client.CoreV1().Secrets("n").Get(context.Background(), "leaf", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	leaf.ResourceVersion = "11"
	if _, err := client.CoreV1().Secrets("n").Update(context.Background(), leaf, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := tlsconfig.EnsureSnapshot(context.Background(), client.CoreV1(), planned, now); err == nil {
		t.Fatal("changed source must not be snapshotted")
	}
	client, intent, now = snapshotFixture(t)
	intent.MinimumValidity = 6 * time.Hour
	if _, err := tlsconfig.PlanSnapshot(context.Background(), client.CoreV1(), intent, now); err == nil {
		t.Fatal("insufficient certificate validity accepted")
	}
}

func TestPinnedSnapshotReadDoesNotRecreateDeletedSecret(t *testing.T) {
	client, intent, now := snapshotFixture(t)
	planned, err := tlsconfig.PlanSnapshot(context.Background(), client.CoreV1(), intent, now)
	if err != nil {
		t.Fatal(err)
	}
	staged, err := tlsconfig.EnsureSnapshot(context.Background(), client.CoreV1(), planned, now)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tlsconfig.ReadSnapshot(context.Background(), client.CoreV1(), planned, staged.UID, now); err != nil {
		t.Fatal(err)
	}
	if err := client.CoreV1().Secrets("n").Delete(context.Background(), staged.Name, metav1.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := tlsconfig.ReadSnapshot(context.Background(), client.CoreV1(), planned, staged.UID, now); err == nil {
		t.Fatal("deleted snapshot was accepted")
	}
	if _, err := client.CoreV1().Secrets("n").Get(context.Background(), staged.Name, metav1.GetOptions{}); err == nil {
		t.Fatal("pinned read recreated deleted Secret")
	}
	replacement := staged.DeepCopy()
	replacement.UID = "different-uid"
	if err := client.Tracker().Create(corev1.SchemeGroupVersion.WithResource("secrets"), replacement, "n"); err != nil {
		t.Fatal(err)
	}
	if _, err := tlsconfig.ReadSnapshot(context.Background(), client.CoreV1(), planned, staged.UID, now); err == nil {
		t.Fatal("recreated snapshot UID accepted")
	}
}
