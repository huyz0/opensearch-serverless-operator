//go:build envtest

package controller

import (
	"context"
	"crypto/x509"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/huyz0/opensearch-serverless-operator/api/v1alpha1"
	"github.com/huyz0/opensearch-serverless-operator/internal/workload"
)

func testPeerTLSRollout(t *testing.T, c client.Client, kube kubernetes.Interface, namespace string) {
	t.Helper()
	ctx := context.Background()
	const domain = "cluster.local"
	image := "example/image@sha256:" + strings.Repeat("d", 64)
	channel := func(leaf, trust string) *v1alpha1.TLSChannel {
		return &v1alpha1.TLSChannel{
			Source: "Secret", CertificateRef: v1alpha1.LocalReference{Name: leaf},
			TrustBundleRef: v1alpha1.SecretKeyReference{Name: trust, Key: "ca.crt"},
		}
	}
	pool := v1alpha1.Pool{
		JVMHeap: "512Mi", Zones: map[string]v1alpha1.Zone{"a": {Replicas: 1}},
		Resources: corev1.ResourceRequirements{Requests: corev1.ResourceList{
			corev1.ResourceCPU: resource.MustParse("1"), corev1.ResourceMemory: resource.MustParse("1Gi"),
		}},
	}
	instance := &v1alpha1.OpenSearchServerless{
		ObjectMeta: metav1.ObjectMeta{Name: "rotate-peer", Namespace: namespace},
		Spec: v1alpha1.OpenSearchServerlessSpec{
			Image: image, ServiceAccountName: "runtime", Storage: v1alpha1.Storage{Type: "s3", Bucket: "bucket", Region: "region"},
			Pools: map[string]v1alpha1.Pool{"search": pool, "ingest": pool},
			TLS:   &v1alpha1.TLSConfig{Transport: channel("peer-old", "peer-old-ca")},
			Rollout: &v1alpha1.Rollout{Strategy: "BlueGreen", MaxPreviewReplicas: 2, RollbackWindow: "1h",
				Candidate: &v1alpha1.FleetCandidate{Fleet: "green", Image: image, PeerTLS: channel("peer-new", "peer-overlap")},
			},
		},
	}
	unbounded := instance.DeepCopyObject().(*v1alpha1.OpenSearchServerless)
	unbounded.Name = "unbounded-peer-rollout"
	unbounded.Spec.Rollout.RollbackWindow = ""
	if err := c.Create(ctx, unbounded); !apierrors.IsInvalid(err) {
		t.Fatalf("peer rotation without a finite rollback window passed admission: %v", err)
	}
	if err := c.Create(ctx, instance); err != nil {
		t.Fatal(err)
	}
	service, err := workload.PeerDNSService(instance)
	if err != nil {
		t.Fatal(err)
	}
	wildcard := "*." + service.Name + "." + namespace + ".svc." + domain
	oldCA, oldLeaves := rotationTestLeaves(t, wildcard, "", x509.ExtKeyUsageClientAuth)
	newCA, newLeaves := rotationTestLeaves(t, wildcard, "", x509.ExtKeyUsageClientAuth)
	secrets := kube.CoreV1().Secrets(namespace)
	install := func(name string, typ corev1.SecretType, data map[string][]byte) {
		t.Helper()
		if _, err := secrets.Create(ctx, &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: name}, Type: typ, Data: data}, metav1.CreateOptions{}); err != nil {
			t.Fatal(err)
		}
	}
	install("peer-old", corev1.SecretTypeTLS, oldLeaves[1])
	install("peer-new", corev1.SecretTypeTLS, newLeaves[1])
	install("peer-old-ca", corev1.SecretTypeOpaque, map[string][]byte{"ca.crt": oldCA})
	install("peer-new-ca", corev1.SecretTypeOpaque, map[string][]byte{"ca.crt": newCA})
	install("peer-overlap", corev1.SecretTypeOpaque, map[string][]byte{"ca.crt": append(append([]byte(nil), oldCA...), newCA...)})
	r := &InstanceReconciler{Client: c, Core: kube.CoreV1(), DevelopmentPeerTLSImage: image, DevelopmentPeerDNSImage: image, DevelopmentClusterDomain: domain}
	initial, err := r.resolvePeerTLS(ctx, instance, domain)
	if err != nil {
		t.Fatal(err)
	}
	initialSnapshot, err := r.ensurePeerSnapshot(ctx, instance, &v1alpha1.PodCreationIntent{Name: podNameForSequence(instance, 1)}, initial, domain)
	if err != nil {
		t.Fatal(err)
	}
	initial.baseline.SnapshotUID = string(initialSnapshot.UID)
	instance.Status.PeerTLSBaseline = &initial.baseline
	instance.Status.ServingFleetIntentByRole = map[string]string{"search": "initial", "ingest": "initial"}
	if err := c.Status().Update(ctx, instance); err != nil {
		t.Fatal(err)
	}
	reconcile := func() {
		t.Helper()
		if changed, err := r.reconcileCandidatePreview(ctx, instance); !changed || err != nil {
			t.Fatalf("preview reconcile: changed=%v, error=%v", changed, err)
		}
		if err := c.Get(ctx, client.ObjectKeyFromObject(instance), instance); err != nil {
			t.Fatal(err)
		}
	}
	reconcile()
	if cond := meta.FindStatusCondition(instance.Status.Conditions, "PreviewServicesConfigured"); cond == nil ||
		cond.Reason != "PreviewPeerTLSUnqualified" || !strings.Contains(cond.Message, "target leaf under serving trust") ||
		instance.Status.CandidateFleet != nil {
		t.Fatalf("replacement leaf staged before the serving fleet trusted its CA: %#v", cond)
	}
	instance.Spec.Rollout.Candidate.PeerTLS = channel("peer-old", "peer-overlap")
	if err := c.Update(ctx, instance); err != nil {
		t.Fatal(err)
	}
	reconcile()
	pin := instance.Status.PeerTLSByFleet["green"]
	if pin.SnapshotName == "" || pin.SnapshotUID != "" || len(pin.LeafPEMSHA256) != 64 || pin.TrustSHA256 == initial.baseline.TrustSHA256 ||
		instance.Status.CandidateFleet.PeerTLS.TrustBundleRef.Name != "peer-overlap" {
		t.Fatalf("preview did not persist the overlap intent: %#v", pin)
	}
	if _, err := secrets.Get(ctx, pin.SnapshotName, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Fatalf("preview created its snapshot before recording intent: %v", err)
	}
	overlap, err := secrets.Get(ctx, "peer-overlap", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	overlap.Labels = map[string]string{"issuer-revision": "changed"}
	if _, err := secrets.Update(ctx, overlap, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	reconcile()
	replanned := instance.Status.PeerTLSByFleet["green"]
	if replanned.SnapshotName == pin.SnapshotName || replanned.SnapshotUID != "" || replanned.TrustSHA256 != pin.TrustSHA256 {
		t.Fatalf("candidate did not replan the uncommitted snapshot: %#v", replanned)
	}
	reconcile()
	reconcile()
	pin = instance.Status.PeerTLSByFleet["green"]
	if pin.SnapshotUID == "" || pin.SnapshotUID == initial.baseline.SnapshotUID || instance.Status.CandidateFleet.SearchServiceUID == "" || instance.Status.CandidateFleet.IngestServiceUID == "" {
		t.Fatalf("preview identity and snapshot were not committed: %#v", instance.Status)
	}
	if err := r.qualifyPeerRotation(ctx, instance, "green", "initial", true); err != nil {
		t.Fatal(err)
	}
	if err := r.qualifyPeerRotation(ctx, instance, "initial", "green", false); err != nil {
		t.Fatal(err)
	}
	sameCA := &v1alpha1.FleetCandidate{Fleet: "renewed-same-ca", Image: image, PeerTLS: channel("peer-old-renewed", "peer-old-ca")}
	install("peer-old-renewed", corev1.SecretTypeTLS, oldLeaves[3])
	if same, err := r.previewPeerTLS(ctx, instance, sameCA, domain); err != nil || same.LeafSHA256 == initial.baseline.LeafSHA256 || same.TrustSHA256 != initial.baseline.TrustSHA256 {
		t.Fatalf("same-CA leaf renewal did not qualify: %v", err)
	}
	longWindow := instance.DeepCopyObject().(*v1alpha1.OpenSearchServerless)
	longWindow.Spec.Rollout.RollbackWindow = "5h"
	if _, err := r.previewPeerTLS(ctx, longWindow, sameCA, domain); err == nil || !strings.Contains(err.Error(), "expired") {
		t.Fatalf("peer TLS staged a retained leaf that expires inside the rollback window: %v", err)
	}
	if peerTLSForFleet(instance, "green", initial).baseline.SnapshotUID != pin.SnapshotUID ||
		peerTLSForFleet(instance, "initial", initial).baseline.SnapshotUID != initial.baseline.SnapshotUID {
		t.Fatal("Pod creation and recovery did not select the snapshot for their fleet")
	}
	serving := instance.Status.ServingFleetIntentByRole
	serving["search"], serving["ingest"] = "green", "green"
	candidate := &v1alpha1.FleetCandidate{Fleet: "amber", Image: image, PeerTLS: channel("peer-new", "peer-overlap")}
	renewal, err := r.previewPeerTLS(ctx, instance, candidate, domain)
	if err != nil || renewal.LeafSHA256 == pin.LeafSHA256 {
		t.Fatalf("old-plus-new serving trust did not admit the renewed leaf: %v", err)
	}
	candidate.PeerTLS.TrustBundleRef.Name = "peer-new-ca"
	if _, err := r.previewPeerTLS(ctx, instance, candidate, domain); err == nil || !strings.Contains(err.Error(), "serving leaf under target trust") {
		t.Fatalf("pruning old trust admitted a retained old leaf: %v", err)
	}
	renewed, err := r.ensurePeerSnapshot(ctx, instance, &v1alpha1.PodCreationIntent{Name: podNameForSequence(instance, 2)}, &peerTLSInput{baseline: *renewal}, domain)
	if err != nil {
		t.Fatal(err)
	}
	renewal.SnapshotUID = string(renewed.UID)
	instance.Status.PeerTLSByFleet["amber"] = *renewal
	serving["search"], serving["ingest"] = "amber", "amber"
	candidate.Fleet = "violet"
	pruned, err := r.previewPeerTLS(ctx, instance, candidate, domain)
	if err != nil || pruned.LeafSHA256 != renewal.LeafSHA256 || pruned.TrustSHA256 == renewal.TrustSHA256 {
		t.Fatalf("replacement-only trust did not qualify with the new serving leaf: %v", err)
	}
	serving["search"], serving["ingest"] = "initial", "initial"
	if err := secrets.Delete(ctx, initialSnapshot.Name, metav1.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	gated := &v1alpha1.PodCreationIntent{Role: "search", Zone: "a", Fleet: "green", Revision: instance.Status.CandidateFleet.Revision}
	if err := r.checkCandidateAdmission(ctx, instance, gated); err == nil || !strings.Contains(err.Error(), "read pinned TLS snapshot") {
		t.Fatalf("preview admission did not hold after the serving snapshot disappeared: %v", err)
	}
	if err := secrets.Delete(ctx, pin.SnapshotName, metav1.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	if err := r.qualifyPeerRotation(ctx, instance, "green", "amber", true); err == nil {
		t.Fatal("promotion qualified with its peer snapshot missing")
	}
}
