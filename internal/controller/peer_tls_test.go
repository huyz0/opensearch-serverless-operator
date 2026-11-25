package controller

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
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	kubernetesfake "k8s.io/client-go/kubernetes/fake"
	clienttesting "k8s.io/client-go/testing"

	"github.com/huyz0/opensearch-serverless-operator/api/v1alpha1"
	"github.com/huyz0/opensearch-serverless-operator/internal/workload"
)

func TestImmutablePeerTLSIntentAndPodRendering(t *testing.T) {
	ctx := context.Background()
	now := time.Now().Truncate(time.Second)
	immutable := true
	instance := &v1alpha1.OpenSearchServerless{ObjectMeta: metav1.ObjectMeta{Name: "catalog", Namespace: "n", UID: "owner"}, Spec: v1alpha1.OpenSearchServerlessSpec{
		Image: "qualified", TLS: &v1alpha1.TLSConfig{Transport: &v1alpha1.TLSChannel{Source: "Secret", CertificateRef: v1alpha1.LocalReference{Name: "leaf"}, TrustBundleRef: v1alpha1.SecretKeyReference{Name: "trust", Key: "ca.crt"}}},
		Pools: map[string]v1alpha1.Pool{"search": {JVMHeap: "512Mi", Zones: map[string]v1alpha1.Zone{"a": {Replicas: 1}}, Resources: corev1.ResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("1"), corev1.ResourceMemory: resource.MustParse("1Gi")}}}},
	}, Status: v1alpha1.OpenSearchServerlessStatus{PrimaryStore: &v1alpha1.PrimaryStoreStatus{Bucket: "bucket", Region: "region"}}}
	service, err := workload.PeerDNSService(instance)
	if err != nil {
		t.Fatal(err)
	}
	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	ca := &x509.Certificate{SerialNumber: big.NewInt(1), NotBefore: now.Add(-time.Hour), NotAfter: now.Add(4 * time.Hour), IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign}
	caDER, err := x509.CreateCertificate(rand.Reader, ca, ca, &caKey.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	leafKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	leaf := &x509.Certificate{SerialNumber: big.NewInt(2), DNSNames: []string{"*." + service.Name + ".n.svc.cluster.local"}, NotBefore: now.Add(-time.Minute), NotAfter: now.Add(3 * time.Hour), ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth}}
	leafDER, err := x509.CreateCertificate(rand.Reader, leaf, ca, &leafKey.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, err := x509.MarshalPKCS8PrivateKey(leafKey)
	if err != nil {
		t.Fatal(err)
	}
	certSecret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "leaf", Namespace: "n", UID: "leaf-uid", ResourceVersion: "10"}, Type: corev1.SecretTypeTLS, Immutable: &immutable, Data: map[string][]byte{corev1.TLSCertKey: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: leafDER}), corev1.TLSPrivateKeyKey: pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER})}}
	trust := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "trust", Namespace: "n", UID: "trust-uid", ResourceVersion: "20"}, Immutable: &immutable, Data: map[string][]byte{"ca.crt": pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDER})}}
	core := kubernetesfake.NewSimpleClientset(certSecret, trust)
	r := &InstanceReconciler{Core: core.CoreV1(), DevelopmentPeerTLSImage: "qualified", Now: func() time.Time { return now }}
	input, err := r.resolvePeerTLS(ctx, instance, "cluster.local")
	if err != nil {
		t.Fatal(err)
	}
	legacy := input.baseline
	legacy.LeafPEMSHA256 = ""
	instance.Status.PeerTLSBaseline = &legacy
	resumed, err := r.resolvePeerTLS(ctx, instance, "cluster.local")
	if err != nil || resumed.baseline != input.baseline || legacy.LeafPEMSHA256 != "" {
		t.Fatalf("legacy provisional intent did not recover from its recorded source versions: %v", err)
	}
	intent := &v1alpha1.PodCreationIntent{Name: "pod", Role: "search", Zone: "a", Fleet: "initial", Revision: "r", Sequence: 1, PeerDNS: true, PeerTLSLeafUID: "leaf-uid", PeerTLSTrustUID: "trust-uid", PeerTLSLeafSHA256: input.baseline.LeafSHA256, PeerTLSTrustSHA256: input.baseline.TrustSHA256, PeerTLSSnapshotName: input.baseline.SnapshotName}
	if err := r.checkPeerTLSIntent(ctx, instance, intent, input, "cluster.local"); err != nil {
		t.Fatal(err)
	}
	if err := r.checkPeerTLSIntent(ctx, instance, intent, input, "wrong.local"); err == nil {
		t.Fatal("wrong DNS identity accepted")
	}
	core.PrependReactor("create", "secrets", func(action clienttesting.Action) (bool, runtime.Object, error) {
		action.(clienttesting.CreateAction).GetObject().(*corev1.Secret).UID = "snapshot-uid"
		return false, nil, nil
	})
	instance.Status.PeerTLSBaseline = &input.baseline
	instance.Status.PendingPodCreation = intent
	staged, err := r.ensurePeerSnapshot(ctx, instance, intent, input, "cluster.local")
	if err != nil || staged.UID != "snapshot-uid" {
		t.Fatalf("stage failed: %v", err)
	}
	updatedLeaf := certSecret.DeepCopy()
	updatedLeaf.ResourceVersion = "11"
	if _, err := core.CoreV1().Secrets("n").Update(ctx, updatedLeaf, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	recovered, err := r.resolvePeerTLS(ctx, instance, "cluster.local")
	if err != nil || recovered.recoveredSnapshotUID != staged.UID {
		t.Fatalf("could not recover staged material after source update: %v", err)
	}
	if err := r.checkPeerTLSIntent(ctx, instance, intent, recovered, "cluster.local"); err != nil {
		t.Fatal(err)
	}
	if _, err := r.ensurePeerSnapshot(ctx, instance, intent, recovered, "cluster.local"); err != nil {
		t.Fatalf("ambiguous snapshot create did not recover: %v", err)
	}
	instance.Status.PendingPodCreation = nil
	recovered, err = r.resolvePeerTLS(ctx, instance, "cluster.local")
	if err != nil || recovered.recoveredSnapshotUID != staged.UID {
		t.Fatalf("snapshot created before a lost status write could not be recovered after intent retirement: %v", err)
	}
	input.baseline.SnapshotUID = "snapshot-uid"
	intent.PeerTLSSnapshotUID = "snapshot-uid"
	intent.Sequence = 2
	intent.Name = podNameForSequence(instance, intent.Sequence)
	if err := core.CoreV1().Secrets("n").Delete(ctx, certSecret.Name, metav1.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	recovered, err = r.resolvePeerTLS(ctx, instance, "cluster.local")
	if err != nil || recovered.baseline.SnapshotUID != "snapshot-uid" {
		t.Fatalf("new Pod lost its pinned TLS snapshot after the issuer source was deleted: %v", err)
	}
	if err := r.checkPeerTLSIntent(ctx, instance, intent, recovered, "cluster.local"); err != nil {
		t.Fatal(err)
	}
	r.Now = func() time.Time { return now.Add(150 * time.Minute) }
	if err := r.checkPeerTLSIntent(ctx, instance, intent, recovered, "cluster.local"); err == nil {
		t.Fatal("pinned snapshot could not cover the required validity window")
	}
	r.Now = func() time.Time { return now }
	instance.Spec.TLS.Transport.CertificateRef.Name = "different"
	if _, err := r.resolvePeerTLS(ctx, instance, "cluster.local"); err == nil {
		t.Fatal("changed peer certificate reference bypassed controlled rotation")
	}
	instance.Spec.TLS.Transport.CertificateRef.Name = certSecret.Name
	if _, err := core.CoreV1().Secrets("n").Create(ctx, certSecret, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	instance.Status.PeerTLSBaseline = nil
	input.baseline.SnapshotUID = "snapshot-uid"
	intent.PeerTLSSnapshotUID = "snapshot-uid"
	pod, err := renderGatedPod(instance, intent, nil, "cluster.local", "")
	if err != nil {
		t.Fatal(err)
	}
	pod.UID = "pod-uid"
	if err := validateAdmittedGatedPod(pod, pod.DeepCopy()); err != nil {
		t.Fatal(err)
	}
	tampered := pod.DeepCopy()
	for i := range tampered.Spec.Volumes {
		if tampered.Spec.Volumes[i].Name == "serverless-peer-cert" {
			tampered.Spec.Volumes[i].Secret.SecretName = "other"
		}
	}
	if err := validateAdmittedGatedPod(pod, tampered); err == nil {
		t.Fatal("changed peer Secret accepted")
	}
	shadow := pod.DeepCopy()
	shadow.Spec.Containers[0].VolumeMounts = append(shadow.Spec.Containers[0].VolumeMounts, corev1.VolumeMount{Name: "injected", MountPath: "/var/run/serverless"})
	if err := validateAdmittedGatedPod(pod, shadow); err == nil {
		t.Fatal("shadowing peer TLS mount accepted")
	}
	aliased := pod.DeepCopy()
	aliased.Spec.Volumes = append(aliased.Spec.Volumes, corev1.Volume{Name: "alias", VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{SecretName: intent.PeerTLSSnapshotName}}})
	if err := validateAdmittedGatedPod(pod, aliased); err == nil {
		t.Fatal("alias for the peer TLS private key Secret was accepted")
	}
	instance.Status.PeerTLSBaseline = &input.baseline
	modifiedTrust := trust.DeepCopy()
	modifiedTrust.Data["ca.crt"] = append(append([]byte{}, trust.Data["ca.crt"]...), trust.Data["ca.crt"]...)
	if _, err := core.CoreV1().Secrets("n").Update(ctx, modifiedTrust, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	pinned, err := r.resolvePeerTLS(ctx, instance, "cluster.local")
	if err != nil || pinned.baseline.TrustSHA256 != input.baseline.TrustSHA256 {
		t.Fatalf("changed trust source overrode the committed pin: %v", err)
	}
	if _, err := core.CoreV1().Secrets("n").Update(ctx, trust, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	changed := input.baseline
	changed.SnapshotUID = "recreated"
	instance.Status.PeerTLSBaseline = &changed
	intent.PeerTLSSnapshotUID = changed.SnapshotUID
	pinned, err = r.resolvePeerTLS(ctx, instance, "cluster.local")
	if err != nil {
		t.Fatal(err)
	}
	if err := r.checkPeerTLSIntent(ctx, instance, intent, pinned, "cluster.local"); err == nil {
		t.Fatal("snapshot with the wrong UID was accepted")
	}
	instance.Status.PeerTLSBaseline = nil
	mutable := false
	certSecret.Immutable = &mutable
	if _, err := core.CoreV1().Secrets("n").Update(ctx, certSecret, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := r.resolvePeerTLS(ctx, instance, "cluster.local"); err != nil {
		t.Fatalf("mutable source Secret should be accepted for staging: %v", err)
	}
}
