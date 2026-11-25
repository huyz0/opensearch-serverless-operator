package controller

import (
	"context"
	"crypto/sha256"
	"crypto/x509"
	"errors"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/huyz0/opensearch-serverless-operator/api/v1alpha1"
	"github.com/huyz0/opensearch-serverless-operator/internal/tlsconfig"
	"github.com/huyz0/opensearch-serverless-operator/internal/workload"
)

const peerTLSLeafUIDAnnotation = "serverless.opensearch.org/peer-tls-leaf-uid"
const peerTLSTrustUIDAnnotation = "serverless.opensearch.org/peer-tls-trust-uid"
const peerTLSSnapshotUIDAnnotation = "serverless.opensearch.org/peer-tls-snapshot-uid"
const peerTLSSnapshotNameAnnotation = "serverless.opensearch.org/peer-tls-snapshot-name"

var errPeerTLSProvisionalSuperseded = errors.New("issuer replaced the unstaged peer TLS source")

type peerTLSInput struct {
	baseline             v1alpha1.PeerTLSBaseline
	leaf                 *corev1.Secret
	trust                *corev1.Secret
	trustKey             string
	recoveredSnapshotUID types.UID
}

// resolvePeerTLS selects the persisted snapshot once its UID is known.
// Issuer writes cannot replace active material or prevent its reuse.
func (r *InstanceReconciler) resolvePeerTLS(ctx context.Context, instance *v1alpha1.OpenSearchServerless, domain string) (*peerTLSInput, error) {
	if instance.Spec.TLS == nil || instance.Spec.TLS.Transport == nil {
		if instance.Status.PeerTLSBaseline != nil {
			return nil, fmt.Errorf("peer TLS cannot be removed without a rollout")
		}
		return nil, nil
	}
	channel := instance.Spec.TLS.Transport
	if domain == "" || r.DevelopmentPeerTLSImage == "" || r.DevelopmentPeerTLSImage != instance.Spec.Image {
		return nil, fmt.Errorf("development peer TLS requires a qualified image and peer DNS")
	}
	sourceName, err := peerLeafSourceName(instance, channel)
	if err != nil {
		return nil, err
	}
	if baseline := instance.Status.PeerTLSBaseline; baseline != nil {
		slot, issuer := peerIssuanceIdentity(channel)
		if sourceName != baseline.LeafName || slot != baseline.IssuanceSlot || issuer != baseline.IssuerName ||
			channel.TrustBundleRef.Name != baseline.TrustName || channel.TrustBundleRef.Key != baseline.TrustKey {
			return nil, fmt.Errorf("peer TLS input references changed; controlled rotation is required")
		}
		if baseline.SnapshotUID != "" {
			return &peerTLSInput{baseline: *baseline}, nil
		}
		if recovered, err := r.recoverPeerSnapshot(ctx, instance, domain); recovered != nil || err != nil {
			return recovered, err
		}
	}
	input, err := r.readPeerTLSSource(ctx, instance, channel, domain)
	if err != nil {
		return nil, err
	}
	if existing := instance.Status.PeerTLSBaseline; existing != nil {
		expected := *existing
		// An older intent still pins the exact source UID and version. The
		// comparison below must match those before using the new chain hash.
		if expected.LeafPEMSHA256 == "" {
			expected.LeafPEMSHA256 = input.baseline.LeafPEMSHA256
		}
		if expected != input.baseline {
			return nil, errPeerTLSProvisionalSuperseded
		}
	}
	return input, nil
}

func (r *InstanceReconciler) readPeerTLSSource(ctx context.Context, instance *v1alpha1.OpenSearchServerless, channel *v1alpha1.TLSChannel, domain string) (*peerTLSInput, error) {
	name, err := peerLeafSourceName(instance, channel)
	if err != nil {
		return nil, err
	}
	var leaf *corev1.Secret
	if channel.Source == "CertManager" {
		leaf, err = r.readCertManagerPeerLeaf(ctx, instance, channel, name, domain)
	} else {
		leaf, err = r.Core.Secrets(instance.Namespace).Get(ctx, name, metav1.GetOptions{})
	}
	if err != nil {
		return nil, fmt.Errorf("read peer certificate Secret: %w", err)
	}
	trust, err := r.Core.Secrets(instance.Namespace).Get(ctx, channel.TrustBundleRef.Name, metav1.GetOptions{})
	if err != nil {
		return nil, fmt.Errorf("read peer trust Secret: %w", err)
	}
	if leaf.UID == "" || leaf.ResourceVersion == "" || leaf.DeletionTimestamp != nil || leaf.Type != corev1.SecretTypeTLS || trust.UID == "" || trust.ResourceVersion == "" || trust.DeletionTimestamp != nil || len(trust.Data[channel.TrustBundleRef.Key]) == 0 {
		return nil, fmt.Errorf("peer TLS inputs must be non-terminating versioned Secrets with certificate and trust material")
	}
	leafSHA, trustSHA, err := tlsconfig.PublicMaterialDigests(leaf.Data[corev1.TLSCertKey], trust.Data[channel.TrustBundleRef.Key])
	if err != nil {
		return nil, fmt.Errorf("invalid public peer TLS material: %w", err)
	}
	identity := fmt.Sprintf("%s\x00%s\x00%s\x00%s\x00%s\x00%s\x00%s", instance.UID, leaf.UID, leaf.ResourceVersion, trust.UID, trust.ResourceVersion, leafSHA, trustSHA)
	hash := sha256.Sum256([]byte(identity))
	baseline := v1alpha1.PeerTLSBaseline{LeafName: leaf.Name, LeafUID: string(leaf.UID), LeafResourceVersion: leaf.ResourceVersion, TrustName: trust.Name, TrustUID: string(trust.UID), TrustResourceVersion: trust.ResourceVersion, TrustKey: channel.TrustBundleRef.Key, LeafSHA256: leafSHA, TrustSHA256: trustSHA, SnapshotName: fmt.Sprintf("peer-tls-%x", hash[:12])}
	baseline.LeafPEMSHA256 = fmt.Sprintf("%x", sha256.Sum256(leaf.Data[corev1.TLSCertKey]))
	baseline.IssuanceSlot, baseline.IssuerName = peerIssuanceIdentity(channel)
	return &peerTLSInput{baseline: baseline, leaf: leaf, trust: trust, trustKey: baseline.TrustKey}, nil
}

func peerTLSBaselineForFleet(instance *v1alpha1.OpenSearchServerless, fleet string) *v1alpha1.PeerTLSBaseline {
	if baseline, ok := instance.Status.PeerTLSByFleet[fleet]; ok {
		return &baseline
	}
	return instance.Status.PeerTLSBaseline
}

func peerTLSForFleet(instance *v1alpha1.OpenSearchServerless, fleet string, initial *peerTLSInput) *peerTLSInput {
	if baseline, ok := instance.Status.PeerTLSByFleet[fleet]; ok {
		return &peerTLSInput{baseline: baseline}
	}
	return initial
}

func peerTLSPodMatches(pod *corev1.Pod, baseline *v1alpha1.PeerTLSBaseline) bool {
	if baseline == nil {
		return pod.Annotations[peerTLSLeafUIDAnnotation] == "" && pod.Annotations[peerTLSTrustUIDAnnotation] == "" &&
			pod.Annotations[peerTLSSnapshotNameAnnotation] == "" && pod.Annotations[peerTLSSnapshotUIDAnnotation] == ""
	}
	return baseline.SnapshotUID != "" && pod.Annotations[peerTLSLeafUIDAnnotation] == baseline.LeafUID &&
		pod.Annotations[peerTLSTrustUIDAnnotation] == baseline.TrustUID && pod.Annotations[peerTLSSnapshotNameAnnotation] == baseline.SnapshotName &&
		pod.Annotations[peerTLSSnapshotUIDAnnotation] == baseline.SnapshotUID
}

// Recover a snapshot created before its UID was recorded. The original Pod
// intent may already have been retired after a concurrent spec update.
func (r *InstanceReconciler) recoverPeerSnapshot(ctx context.Context, instance *v1alpha1.OpenSearchServerless, domain string) (*peerTLSInput, error) {
	baseline := instance.Status.PeerTLSBaseline
	candidate, err := r.Core.Secrets(instance.Namespace).Get(ctx, baseline.SnapshotName, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read provisional peer TLS snapshot: %w", err)
	}
	podName := podNameForSequence(instance, instance.Status.PodCreationSequence+1)
	if intent := instance.Status.PendingPodCreation; intent != nil {
		if intent.PeerTLSSnapshotName != baseline.SnapshotName || intent.PeerTLSLeafUID != baseline.LeafUID || intent.PeerTLSTrustUID != baseline.TrustUID || intent.PeerTLSLeafSHA256 != baseline.LeafSHA256 || intent.PeerTLSTrustSHA256 != baseline.TrustSHA256 || intent.PeerTLSSnapshotUID != "" {
			return nil, fmt.Errorf("provisional snapshot differs from the pending Pod intent")
		}
		podName = intent.Name
	}
	input := &peerTLSInput{baseline: *baseline, trustKey: "ca.crt"}
	plan, err := input.snapshotIntent(instance, podName, domain)
	if err != nil {
		return nil, err
	}
	verified, err := tlsconfig.ReadSnapshot(ctx, r.Core, plan, candidate.UID, r.now())
	if err != nil {
		return nil, err
	}
	input.leaf, input.trust = verified, verified
	input.recoveredSnapshotUID = verified.UID
	return input, nil
}

// A Pod cannot use a peer TLS snapshot until its UID is committed with the
// creation intent. Retire that intent before choosing new issuer material.
func (r *InstanceReconciler) resetProvisionalPeerTLS(ctx context.Context, instance *v1alpha1.OpenSearchServerless) (ctrl.Result, error) {
	status := &instance.Status
	if status.PeerTLSBaseline == nil || status.PeerTLSBaseline.SnapshotUID != "" || status.PendingGateRelease != nil ||
		status.ActiveOperation != nil || len(status.ServingRevisionByRole) != 0 {
		return ctrl.Result{RequeueAfter: time.Minute}, errPeerTLSProvisionalSuperseded
	}
	if status.PendingPodCreation != nil {
		return r.recoverStaleGatedCreation(ctx, instance)
	}
	pods, err := r.Core.Pods(instance.Namespace).List(ctx, metav1.ListOptions{LabelSelector: workload.InstanceLabel + "=" + string(instance.UID)})
	if err != nil {
		return ctrl.Result{}, err
	}
	if len(pods.Items) != 0 {
		return ctrl.Result{RequeueAfter: time.Minute}, fmt.Errorf("cannot replace a provisional peer TLS baseline while an instance Pod exists")
	}
	before := instance.DeepCopyObject().(*v1alpha1.OpenSearchServerless)
	status.PeerTLSBaseline = nil
	if err := r.Status().Patch(ctx, instance, client.MergeFromWithOptions(before, client.MergeFromWithOptimisticLock{})); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{Requeue: true}, nil
}

func (input *peerTLSInput) snapshotIntent(instance *v1alpha1.OpenSearchServerless, podName, domain string) (tlsconfig.SnapshotIntent, error) {
	service, err := workload.PeerDNSService(instance)
	if err != nil {
		return tlsconfig.SnapshotIntent{}, err
	}
	host, err := workload.PeerHostname(podName)
	if err != nil {
		return tlsconfig.SnapshotIntent{}, err
	}
	identity := fmt.Sprintf("%s.%s.%s.svc.%s", host, service.Name, instance.Namespace, domain)
	if !validDNSHost(identity) {
		return tlsconfig.SnapshotIntent{}, fmt.Errorf("invalid peer DNS identity")
	}
	controller := true
	b := input.baseline
	return tlsconfig.SnapshotIntent{Namespace: instance.Namespace, Name: b.SnapshotName, Owner: metav1.OwnerReference{APIVersion: v1alpha1.GroupVersion.String(), Kind: "OpenSearchServerless", Name: instance.Name, UID: instance.UID, Controller: &controller},
		LeafName: b.LeafName, LeafUID: types.UID(b.LeafUID), LeafResourceVersion: b.LeafResourceVersion,
		TrustName: b.TrustName, TrustUID: types.UID(b.TrustUID), TrustResourceVersion: b.TrustResourceVersion, TrustKey: b.TrustKey,
		LeafFingerprint: b.LeafSHA256, LeafPEMSHA256: b.LeafPEMSHA256, TrustSHA256: b.TrustSHA256, Identities: []string{identity}, Usages: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth}, MinimumValidity: time.Hour}, nil
}

func (r *InstanceReconciler) ensurePeerSnapshot(ctx context.Context, instance *v1alpha1.OpenSearchServerless, intent *v1alpha1.PodCreationIntent, input *peerTLSInput, domain string) (*corev1.Secret, error) {
	plan, err := input.snapshotIntent(instance, intent.Name, domain)
	if err != nil {
		return nil, err
	}
	return tlsconfig.EnsureSnapshot(ctx, r.Core, plan, r.now())
}

func (input *peerTLSInput) validate(host string, now time.Time) error {
	if _, err := tlsconfig.ValidateWindow(input.leaf.Data[corev1.TLSCertKey], input.leaf.Data[corev1.TLSPrivateKeyKey], input.trust.Data[input.trustKey], []string{host}, []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth}, now, now.Add(time.Hour)); err != nil {
		return fmt.Errorf("peer TLS certificate is invalid for the Pod identity or minimum validity window: %w", err)
	}
	return nil
}

func (r *InstanceReconciler) checkPeerTLSIntent(ctx context.Context, instance *v1alpha1.OpenSearchServerless, intent *v1alpha1.PodCreationIntent, input *peerTLSInput, domain string) error {
	if input == nil {
		if intent.PeerTLSLeafUID != "" || intent.PeerTLSTrustUID != "" || intent.PeerTLSLeafSHA256 != "" || intent.PeerTLSTrustSHA256 != "" || intent.PeerTLSSnapshotName != "" || intent.PeerTLSSnapshotUID != "" {
			return fmt.Errorf("unexpected peer TLS identity")
		}
		return nil
	}
	if !intent.PeerDNS || domain == "" || intent.PeerTLSLeafUID != input.baseline.LeafUID || intent.PeerTLSTrustUID != input.baseline.TrustUID || intent.PeerTLSLeafSHA256 != input.baseline.LeafSHA256 || intent.PeerTLSTrustSHA256 != input.baseline.TrustSHA256 || intent.PeerTLSSnapshotName != input.baseline.SnapshotName || intent.PeerTLSSnapshotUID != input.baseline.SnapshotUID {
		return fmt.Errorf("peer TLS inputs differ from recorded Pod intent")
	}
	plan, err := input.snapshotIntent(instance, intent.Name, domain)
	if err != nil {
		return err
	}
	if input.baseline.SnapshotUID != "" {
		_, err := tlsconfig.ReadSnapshot(ctx, r.Core, plan, types.UID(input.baseline.SnapshotUID), r.now())
		return err
	}
	if input.leaf == nil || input.trust == nil {
		return fmt.Errorf("peer TLS snapshot UID has not been recorded")
	}
	return input.validate(plan.Identities[0], r.now())
}
