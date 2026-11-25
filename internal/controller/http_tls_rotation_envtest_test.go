//go:build envtest

package controller

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/huyz0/opensearch-serverless-operator/api/v1alpha1"
)

func testHTTPLeafRotation(t *testing.T, c client.Client, kube kubernetes.Interface, namespace string) {
	t.Helper()
	ctx := context.Background()
	const domain = "cluster.local"
	image := "example/image@sha256:" + strings.Repeat("c", 64)
	instance := &v1alpha1.OpenSearchServerless{
		ObjectMeta: metav1.ObjectMeta{Name: "rotate-http-leaf", Namespace: namespace},
		Spec: v1alpha1.OpenSearchServerlessSpec{
			Image: image, ServiceAccountName: "runtime",
			Storage: v1alpha1.Storage{Type: "s3", Bucket: "bucket", Region: "region"},
			Pools: map[string]v1alpha1.Pool{"search": {
				JVMHeap: "512Mi", Zones: map[string]v1alpha1.Zone{"a": {Replicas: 1}},
				Resources: corev1.ResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("1"), corev1.ResourceMemory: resource.MustParse("1Gi")}},
			}},
			TLS: &v1alpha1.TLSConfig{HTTP: &v1alpha1.TLSChannel{
				Source: "Secret", CertificateRef: v1alpha1.LocalReference{Name: "rotation-original"},
				TrustBundleRef: v1alpha1.SecretKeyReference{Name: "rotation-trust", Key: "ca.crt"},
			}},
			Rollout: &v1alpha1.Rollout{Strategy: "BlueGreen", MaxPreviewReplicas: 1,
				Candidate: &v1alpha1.FleetCandidate{Fleet: "green", Image: image, HTTPCertificateRef: &v1alpha1.LocalReference{Name: "rotation-bad"}},
			},
		},
	}
	if err := c.Create(ctx, instance); err != nil {
		t.Fatal(err)
	}
	identity, err := httpPodIdentity(instance, podNameForSequence(instance, 1), domain)
	if err != nil {
		t.Fatal(err)
	}
	podWildcard := "*." + strings.SplitN(identity, ".", 2)[1]
	const backendName = "backend.test"
	ca, leaves := rotationTestLeaves(t, podWildcard, backendName)
	secrets := kube.CoreV1().Secrets(namespace)
	immutable := true
	install := func(name string, kind corev1.SecretType, data map[string][]byte) *corev1.Secret {
		t.Helper()
		s, err := secrets.Create(ctx, &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: name}, Type: kind, Immutable: &immutable, Data: data}, metav1.CreateOptions{})
		if err != nil {
			t.Fatal(err)
		}
		return s
	}
	install("rotation-trust", corev1.SecretTypeOpaque, map[string][]byte{"ca.crt": ca})
	install("rotation-original", corev1.SecretTypeTLS, leaves[0])
	install("rotation-bad", corev1.SecretTypeTLS, leaves[1])
	next := install("rotation-new", corev1.SecretTypeTLS, leaves[2])
	r := &InstanceReconciler{Client: c, Core: kube.CoreV1(), DevelopmentHTTPTLSImage: image, DevelopmentPeerDNSImage: image, DevelopmentClusterDomain: domain}
	old, err := r.resolveHTTPTLS(ctx, instance, domain)
	if err != nil {
		t.Fatal(err)
	}
	instance.Status.HTTPTLSBaseline = &old.baseline
	instance.Status.ServingFleetIntentByRole = map[string]string{"search": "initial", "ingest": "initial"}
	instance.Status.PrimaryStore = &v1alpha1.PrimaryStoreStatus{Bucket: "bucket", Region: "region"}
	if err := c.Status().Update(ctx, instance); err != nil {
		t.Fatal(err)
	}
	if changed, err := r.reconcileCandidatePreview(ctx, instance); !changed || err != nil {
		t.Fatalf("record bad replacement result: %v %v", changed, err)
	}
	condition := meta.FindStatusCondition(instance.Status.Conditions, "PreviewServicesConfigured")
	if instance.Status.CandidateFleet != nil || condition == nil || condition.Reason != "PreviewCertificateUnqualified" {
		t.Fatalf("replacement missing an edge DNS name was staged: %#v", instance.Status)
	}
	malformed := []byte("-----BEGIN CERTIFICATE-----\ninvalid!\n-----END CERTIFICATE-----\n")
	malformedTrust := install("rotation-malformed-trust", corev1.SecretTypeOpaque, map[string][]byte{"ca.crt": append(malformed, ca...)})
	instance.Spec.Rollout.Candidate.HTTPCertificateRef.Name = next.Name
	instance.Spec.Rollout.Candidate.HTTPTrustBundleRef = &v1alpha1.SecretKeyReference{Name: malformedTrust.Name, Key: "ca.crt"}
	if err := c.Update(ctx, instance); err != nil {
		t.Fatal(err)
	}
	if changed, err := r.reconcileCandidatePreview(ctx, instance); !changed || err != nil {
		t.Fatalf("record malformed trust result: %v %v", changed, err)
	}
	condition = meta.FindStatusCondition(instance.Status.Conditions, "PreviewServicesConfigured")
	if instance.Status.CandidateFleet != nil || len(instance.Status.HTTPFleetCertificates) > 0 || condition == nil ||
		condition.Reason != "PreviewCertificateUnqualified" || !strings.Contains(condition.Message, "invalid certificate PEM") {
		t.Fatalf("malformed bundle was not rejected before candidate staging: %#v", instance.Status)
	}
	instance.Spec.Rollout.Candidate.HTTPTrustBundleRef = nil
	instance.Spec.Rollout.RollbackWindow = "6h"
	if err := c.Update(ctx, instance); err != nil {
		t.Fatal(err)
	}
	if changed, err := r.reconcileCandidatePreview(ctx, instance); !changed || err != nil {
		t.Fatalf("record short retained certificate result: %v %v", changed, err)
	}
	condition = meta.FindStatusCondition(instance.Status.Conditions, "PreviewServicesConfigured")
	if instance.Status.CandidateFleet != nil || condition == nil || !strings.Contains(condition.Message, "retained fleet HTTP TLS certificate") {
		t.Fatalf("retained certificate expires during the rollback window: %#v", condition)
	}
	instance.Spec.Rollout.RollbackWindow = "2h"
	if err := c.Update(ctx, instance); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if changed, err := r.reconcileCandidatePreview(ctx, instance); !changed || err != nil {
			t.Fatalf("stage approved replacement: %v %v", changed, err)
		}
	}
	if err := c.Get(ctx, client.ObjectKeyFromObject(instance), instance); err != nil {
		t.Fatal(err)
	}
	if pin := instance.Status.HTTPFleetCertificates["green"]; pin.UID != string(next.UID) || pin.LeafSHA256 == "" ||
		instance.Status.CandidateFleet.HTTPCertificateRef != next.Name || instance.Status.CandidateFleet.SearchServiceUID == "" {
		t.Fatalf("candidate lost the immutable certificate pin: %#v", instance.Status)
	}
	if err := r.qualifyHTTPRotation(ctx, instance, "green", "initial", true); err != nil {
		t.Fatalf("qualify rotation at the start of the rollback window: %v", err)
	}
	later := time.Now().Add(2 * time.Hour)
	r.Now = func() time.Time { return later }
	if err := r.qualifyHTTPRotation(ctx, instance, "green", "initial", true); err == nil || !strings.Contains(err.Error(), "retained fleet HTTP TLS certificate") {
		t.Fatalf("promotion admitted a stale rollback certificate: %v", err)
	}
	if err := r.qualifyHTTPRotation(ctx, instance, "initial", "green", false); err != nil {
		t.Fatalf("rollback could not restore a retained certificate with enough startup validity: %v", err)
	}
	r.Now = nil

	sequence := int64(0)
	for fleet, name := range map[string]string{"initial": "rotation-original", "green": next.Name} {
		sequence++
		pod, err := renderGatedPod(instance, &v1alpha1.PodCreationIntent{
			Name: podNameForSequence(instance, sequence), Sequence: sequence, Generation: instance.Generation,
			Role: "search", Zone: "a", Fleet: fleet, Revision: instance.Status.CandidateFleet.Revision, PeerDNS: true,
		}, nil, domain, "")
		if err != nil {
			t.Fatal(err)
		}
		mount := ""
		for _, volume := range pod.Spec.Volumes {
			if volume.Name == httpCertificateVolume && volume.Secret != nil {
				mount = volume.Secret.SecretName
			}
		}
		if mount != name {
			t.Fatalf("fleet %s mounts %q, want %q", fleet, mount, name)
		}
		if fleet == "green" {
			pod.Status.PodIP = "127.0.0.1"
			assertRotationHandshake(t, ctx, r, instance, pod, leaves[0], false)
			assertRotationHandshake(t, ctx, r, instance, pod, leaves[2], true)
		} else {
			pod.Status.PodIP = "127.0.0.1"
			assertRotationHandshake(t, ctx, r, instance, pod, leaves[2], false)
			assertRotationHandshake(t, ctx, r, instance, pod, leaves[0], true)
		}
	}
	if rotated, err := r.resolveHTTPTLSForFleet(ctx, instance, domain, "green"); err != nil || rotated.baseline.CertificateUID != string(next.UID) {
		t.Fatalf("rotated fleet cannot reconstruct its startup material after restart: %#v %v", rotated, err)
	}
	if err := secrets.Delete(ctx, next.Name, metav1.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	install(next.Name, corev1.SecretTypeTLS, leaves[2])
	if _, err := r.resolveHTTPTLSForFleet(ctx, instance, domain, "green"); err == nil {
		t.Fatal("recreated replacement Secret accepted at the old fleet pin")
	}
	if _, err := r.resolveHTTPTLSForFleet(ctx, instance, domain, "initial"); err != nil {
		t.Fatalf("candidate Secret change affected serving fleet material: %v", err)
	}
}

func testHTTPCARollover(t *testing.T, c client.Client, kube kubernetes.Interface, namespace string) {
	t.Helper()
	ctx := context.Background()
	const domain = "cluster.local"
	image := "example/image@sha256:" + strings.Repeat("d", 64)
	oldRef := v1alpha1.SecretKeyReference{Name: "http-ca-original", Key: "ca.crt"}
	overlapRef := v1alpha1.SecretKeyReference{Name: "http-ca-overlap", Key: "bundle.pem"}
	newRef := v1alpha1.SecretKeyReference{Name: "http-ca-replacement", Key: "ca.crt"}
	candidate := &v1alpha1.FleetCandidate{Fleet: "green", Image: image, HTTPCertificateRef: &v1alpha1.LocalReference{Name: "http-ca-green"}, HTTPTrustBundleRef: &newRef}
	instance := &v1alpha1.OpenSearchServerless{ObjectMeta: metav1.ObjectMeta{Name: "http-ca-rollover", Namespace: namespace}, Spec: v1alpha1.OpenSearchServerlessSpec{
		Image: image, ServiceAccountName: "runtime", Storage: v1alpha1.Storage{Type: "s3", Bucket: "bucket", Region: "region"},
		Pools: map[string]v1alpha1.Pool{"search": {JVMHeap: "512Mi", Zones: map[string]v1alpha1.Zone{"a": {Replicas: 1}},
			Resources: corev1.ResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("1"), corev1.ResourceMemory: resource.MustParse("1Gi")}}}},
		TLS:     &v1alpha1.TLSConfig{HTTP: &v1alpha1.TLSChannel{Source: "Secret", CertificateRef: v1alpha1.LocalReference{Name: "http-ca-blue"}, TrustBundleRef: oldRef}},
		Rollout: &v1alpha1.Rollout{Strategy: "BlueGreen", MaxPreviewReplicas: 1, RollbackWindow: "20m", Candidate: candidate},
	}}
	if err := c.Create(ctx, instance); err != nil {
		t.Fatal(err)
	}
	identity, err := httpPodIdentity(instance, podNameForSequence(instance, 1), domain)
	if err != nil {
		t.Fatal(err)
	}
	wildcard := "*." + strings.SplitN(identity, ".", 2)[1]
	oldCA, oldLeaves := rotationTestLeaves(t, wildcard, "backend.test")
	newCA, newLeaves := rotationTestLeaves(t, wildcard, "backend.test")
	secrets := kube.CoreV1().Secrets(namespace)
	immutable := true
	install := func(name string, kind corev1.SecretType, data map[string][]byte) {
		t.Helper()
		if _, err := secrets.Create(ctx, &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: name}, Immutable: &immutable, Type: kind, Data: data}, metav1.CreateOptions{}); err != nil {
			t.Fatal(err)
		}
	}
	overlap := append(append([]byte(nil), oldCA...), newCA...)
	install(oldRef.Name, corev1.SecretTypeOpaque, map[string][]byte{oldRef.Key: oldCA})
	install(overlapRef.Name, corev1.SecretTypeOpaque, map[string][]byte{overlapRef.Key: overlap})
	install(newRef.Name, corev1.SecretTypeOpaque, map[string][]byte{newRef.Key: newCA})
	install("http-ca-blue", corev1.SecretTypeTLS, oldLeaves[0])
	install("http-ca-green", corev1.SecretTypeTLS, newLeaves[0])
	r := &InstanceReconciler{Client: c, Core: kube.CoreV1(), DevelopmentHTTPTLSImage: image, DevelopmentPeerDNSImage: image, DevelopmentClusterDomain: domain}
	old, err := r.resolveHTTPTLS(ctx, instance, domain)
	if err != nil {
		t.Fatal(err)
	}
	instance.Status.HTTPTLSBaseline = &old.baseline
	instance.Status.ServingFleetIntentByRole = map[string]string{"search": "initial", "ingest": "initial"}
	instance.Status.PrimaryStore = &v1alpha1.PrimaryStoreStatus{Bucket: "bucket", Region: "region"}
	if err := c.Status().Update(ctx, instance); err != nil {
		t.Fatal(err)
	}
	if _, err := r.reconcileCandidatePreview(ctx, instance); err != nil || instance.Status.CandidateFleet != nil {
		t.Fatalf("preview staged a CA without overlap: %v", err)
	}
	condition := meta.FindStatusCondition(instance.Status.Conditions, "PreviewServicesConfigured")
	if condition == nil || !strings.Contains(condition.Message, "target HTTP trust must validate the retained fleet") {
		t.Fatalf("no retained fleet trust check was reported: %#v", condition)
	}
	instance.Spec.Rollout.Candidate.HTTPTrustBundleRef = &overlapRef
	if err := c.Update(ctx, instance); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if changed, err := r.reconcileCandidatePreview(ctx, instance); !changed || err != nil {
			t.Fatalf("stage overlapping CA bundle: %v %v", changed, err)
		}
	}
	if err := c.Get(ctx, client.ObjectKeyFromObject(instance), instance); err != nil {
		t.Fatal(err)
	}
	if !instance.Status.HTTPTrustRotationUsed {
		t.Fatal("staged CA change did not persist the backend trust evidence requirement")
	}
	for _, fleet := range []string{"initial", "green"} {
		wantRef, material := oldRef, oldLeaves[0]
		if fleet == "green" {
			wantRef, material = overlapRef, newLeaves[0]
		}
		pod, err := renderGatedPod(instance, &v1alpha1.PodCreationIntent{Name: podNameForSequence(instance, 1), Sequence: 1, Generation: instance.Generation,
			Role: "search", Zone: "a", Fleet: fleet, Revision: instance.Status.CandidateFleet.Revision, PeerDNS: true}, nil, domain, "")
		if err != nil {
			t.Fatal(err)
		}
		var found bool
		for _, volume := range pod.Spec.Volumes {
			if volume.Name == httpTrustVolume && volume.Secret != nil && volume.Secret.SecretName == wantRef.Name && volume.Secret.Items[0].Key == wantRef.Key {
				found = true
			}
		}
		if !found {
			t.Fatalf("fleet %s mounted the wrong trust bundle", fleet)
		}
		pod.Status.PodIP = "127.0.0.1"
		assertRotationHandshake(t, ctx, r, instance, pod, material, true)
		if fleet == "green" {
			assertRotationHandshake(t, ctx, r, instance, pod, oldLeaves[0], false)
		}
	}
	if err := r.qualifyHTTPRotation(ctx, instance, "green", "initial", true); err != nil {
		t.Fatalf("overlap promotion qualification failed: %v", err)
	}
	if err := r.qualifyHTTPRotation(ctx, instance, "initial", "green", false); err != nil {
		t.Fatalf("qualified rollback to the original trust failed: %v", err)
	}
	// Model the state after completing the promotion and retiring the original fleet.
	instance.Status.ServingFleetIntentByRole = map[string]string{"search": "green", "ingest": "green"}
	instance.Status.CandidateFleet = nil
	if err := c.Status().Update(ctx, instance); err != nil {
		t.Fatal(err)
	}
	if err := secrets.Delete(ctx, oldRef.Name, metav1.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	instance.Spec.Rollout.Candidate = &v1alpha1.FleetCandidate{Fleet: "amber", Image: image, HTTPTrustBundleRef: &newRef}
	if err := c.Update(ctx, instance); err != nil {
		t.Fatal(err)
	}
	if changed, err := r.reconcileCandidatePreview(ctx, instance); !changed || err != nil || instance.Status.CandidateFleet == nil {
		t.Fatalf("trust-only prune after retirement did not stage: %v %v", changed, err)
	}
	amber, err := r.resolveHTTPTLSForFleet(ctx, instance, domain, "amber")
	if err != nil || amber.baseline.TrustName != newRef.Name || amber.baseline.LeafSHA256 != instance.Status.HTTPFleetCertificates["green"].LeafSHA256 {
		t.Fatalf("trust-only prune did not inherit the serving leaf: %v", err)
	}
}

func assertRotationHandshake(t *testing.T, ctx context.Context, r *InstanceReconciler, instance *v1alpha1.OpenSearchServerless, pod *corev1.Pod, material map[string][]byte, accept bool) {
	t.Helper()
	pair, err := tls.X509KeyPair(material["tls.crt"], material["tls.key"])
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) }))
	server.TLS = &tls.Config{Certificates: []tls.Certificate{pair}}
	server.StartTLS()
	defer server.Close()
	port := server.Listener.Addr().(*net.TCPAddr).Port
	probe, endpoint, err := r.podHTTP().developmentPodHTTP(ctx, instance, pod, port)
	if err != nil {
		t.Fatal(err)
	}
	defer probe.CloseIdleConnections()
	response, err := probe.Get(endpoint)
	if response != nil {
		response.Body.Close()
	}
	if accept && (err != nil || response.StatusCode != http.StatusOK) {
		t.Fatalf("approved certificate blocked: %v", err)
	}
	if !accept && (err == nil || !strings.Contains(err.Error(), "pinned leaf")) {
		t.Fatalf("same-CA old leaf was accepted for the candidate: %v", err)
	}
}

func rotationTestLeaves(t *testing.T, wildcard, backend string, extraUsages ...x509.ExtKeyUsage) ([]byte, []map[string][]byte) {
	t.Helper()
	key := func() *ecdsa.PrivateKey {
		t.Helper()
		key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		if err != nil {
			t.Fatal(err)
		}
		return key
	}
	caKey, now := key(), time.Now()
	ca := &x509.Certificate{SerialNumber: big.NewInt(1), NotBefore: now.Add(-time.Hour), NotAfter: now.Add(8 * time.Hour),
		BasicConstraintsValid: true, IsCA: true, KeyUsage: x509.KeyUsageCertSign}
	caDER, err := x509.CreateCertificate(rand.Reader, ca, ca, &caKey.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	leaves := make([]map[string][]byte, 0, 4)
	for i, names := range [][]string{{wildcard, backend}, {wildcard}, {wildcard, backend}, {wildcard, backend}} {
		leaf := &x509.Certificate{SerialNumber: big.NewInt(int64(i + 2)), DNSNames: names, NotBefore: now.Add(-time.Hour), NotAfter: now.Add(4 * time.Hour),
			KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: append([]x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}, extraUsages...)}
		leafKey := key()
		der, err := x509.CreateCertificate(rand.Reader, leaf, ca, &leafKey.PublicKey, caKey)
		if err != nil {
			t.Fatal(err)
		}
		private, err := x509.MarshalECPrivateKey(leafKey)
		if err != nil {
			t.Fatal(err)
		}
		leaves = append(leaves, map[string][]byte{"tls.crt": pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}),
			"tls.key": pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: private})})
	}
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDER}), leaves
}
