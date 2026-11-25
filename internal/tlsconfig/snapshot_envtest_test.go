//go:build envtest

package tlsconfig_test

import (
	"context"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"sigs.k8s.io/controller-runtime/pkg/envtest"

	"github.com/huyz0/opensearch-serverless-operator/internal/tlsconfig"
)

func TestAPIServerImmutableTLSSnapshotRecovery(t *testing.T) {
	environment := &envtest.Environment{}
	config, err := environment.Start()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := environment.Stop(); err != nil {
			t.Error(err)
		}
	})
	client, err := kubernetes.NewForConfig(config)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	fixture, intent, now := snapshotFixture(t)
	leaf, err := fixture.CoreV1().Secrets("n").Get(ctx, "leaf", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	trust, err := fixture.CoreV1().Secrets("n").Get(ctx, "trust", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	leaf.ObjectMeta = metav1.ObjectMeta{Name: "leaf", Namespace: "default"}
	trust.ObjectMeta = metav1.ObjectMeta{Name: "trust", Namespace: "default"}
	leaf, err = client.CoreV1().Secrets("default").Create(ctx, leaf, metav1.CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	trust, err = client.CoreV1().Secrets("default").Create(ctx, trust, metav1.CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	owner, err := client.CoreV1().ConfigMaps("default").Create(ctx, &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "owner"}}, metav1.CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	controller := true
	intent.Namespace = "default"
	intent.Owner = metav1.OwnerReference{APIVersion: "v1", Kind: "ConfigMap", Name: owner.Name, UID: owner.UID, Controller: &controller}
	intent.LeafUID, intent.LeafResourceVersion = leaf.UID, leaf.ResourceVersion
	intent.TrustUID, intent.TrustResourceVersion = trust.UID, trust.ResourceVersion
	planned, err := tlsconfig.PlanSnapshot(ctx, client.CoreV1(), intent, now)
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := tlsconfig.EnsureSnapshot(ctx, client.CoreV1(), planned, now)
	if err != nil {
		t.Fatal(err)
	}
	legacy := planned
	legacy.LeafPEMSHA256 = ""
	if _, err := tlsconfig.ReadSnapshot(ctx, client.CoreV1(), legacy, snapshot.UID, now); err != nil {
		t.Fatalf("older UID-pinned snapshot no longer qualifies: %v", err)
	}
	// The leaf still verifies under the same root, but the PEM sequence differs.
	wrongChain := snapshot.DeepCopy()
	wrongChain.ObjectMeta = metav1.ObjectMeta{Namespace: snapshot.Namespace, Name: "different-chain", Annotations: snapshot.Annotations, OwnerReferences: snapshot.OwnerReferences}
	wrongChain.Data[corev1.TLSCertKey] = append(wrongChain.Data[corev1.TLSCertKey], snapshot.Data["ca.crt"]...)
	if _, err := client.CoreV1().Secrets("default").Create(ctx, wrongChain, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	conflicting := planned
	conflicting.Name = wrongChain.Name
	if _, err := tlsconfig.EnsureSnapshot(ctx, client.CoreV1(), conflicting, now); err == nil || !strings.Contains(err.Error(), "recorded PEM digest") {
		t.Fatalf("a different chain with the same leaf was not rejected: %v", err)
	}
	changed := snapshot.DeepCopy()
	changed.Data[corev1.TLSCertKey] = []byte("changed")
	if _, err := client.CoreV1().Secrets("default").Update(ctx, changed, metav1.UpdateOptions{}); !apierrors.IsInvalid(err) {
		t.Fatalf("immutable snapshot update was not rejected: %v", err)
	}
	secondFixture, _, _ := snapshotFixture(t)
	secondLeaf, err := secondFixture.CoreV1().Secrets("n").Get(ctx, "leaf", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	leaf.Data = secondLeaf.Data
	if _, err := client.CoreV1().Secrets("default").Update(ctx, leaf, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	recovered, err := tlsconfig.EnsureSnapshot(ctx, client.CoreV1(), planned, now)
	if err != nil || recovered.UID != snapshot.UID {
		t.Fatalf("snapshot was not recovered after source changed: %v", err)
	}
}
