package controller

import (
	"context"
	"crypto/x509"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	"github.com/huyz0/opensearch-serverless-operator/api/v1alpha1"
	"github.com/huyz0/opensearch-serverless-operator/internal/tlsconfig"
)

func (r *InstanceReconciler) previewPeerTLS(ctx context.Context, instance *v1alpha1.OpenSearchServerless, candidate *v1alpha1.FleetCandidate, domain string) (*v1alpha1.PeerTLSBaseline, error) {
	if candidate.PeerTLS == nil && len(instance.Status.PeerTLSByFleet) == 0 {
		return nil, nil
	}
	source := instance.Status.ServingFleetIntentByRole["search"]
	if source == "" || source != instance.Status.ServingFleetIntentByRole["ingest"] || domain == "" ||
		candidate.Image != instance.Spec.Image || candidate.Image != r.DevelopmentPeerTLSImage {
		return nil, fmt.Errorf("preview peer TLS requires both serving roles on one fleet with the qualified same image and peer DNS")
	}
	current := peerTLSBaselineForFleet(instance, source)
	if current == nil || current.SnapshotUID == "" {
		return nil, fmt.Errorf("preview peer TLS requires a committed serving snapshot")
	}
	if _, recorded := instance.Status.PeerTLSByFleet[candidate.Fleet]; !recorded && len(instance.Status.PeerTLSByFleet) >= 64 {
		return nil, fmt.Errorf("peer TLS fleet history is full")
	}
	if candidate.PeerTLS == nil {
		return current, nil
	}
	request := candidate.PeerTLS
	if request.TrustBundleRef.Name == "" || request.TrustBundleRef.Key == "" {
		return nil, fmt.Errorf("preview peer TLS requires a trust bundle reference")
	}
	next, err := r.readPeerTLSSource(ctx, instance, request, domain)
	if err != nil {
		return nil, err
	}
	if current.LeafSHA256 == next.baseline.LeafSHA256 && current.TrustSHA256 == next.baseline.TrustSHA256 {
		return nil, fmt.Errorf("preview peer TLS must change the leaf or trust bundle")
	}
	now := r.now()
	podName := podNameForSequence(instance, instance.Status.PodCreationSequence+1)
	old, err := r.readPeerFleetSnapshot(ctx, instance, current, podName, domain, now)
	if err != nil {
		return nil, err
	}
	nextPlan, err := next.snapshotIntent(instance, podName, domain)
	if err != nil {
		return nil, err
	}
	until, err := peerTLSRolloutDeadline(instance, now, true)
	if err != nil {
		return nil, err
	}
	if err := validatePeerTLSOverlap(old, old.Data["ca.crt"], next.leaf, next.trust.Data[next.trustKey], nextPlan.Identities[0], now, until); err != nil {
		return nil, err
	}
	return &next.baseline, nil
}

// Replan only before any snapshot UID or preview Service can authorize Pods.
// The caller commits the new intent with the same optimistic status lock.
func (r *InstanceReconciler) replanPreviewPeerTLS(ctx context.Context, instance *v1alpha1.OpenSearchServerless, candidate *v1alpha1.FleetCandidate, previous v1alpha1.PeerTLSBaseline, domain string) bool {
	state := instance.Status.CandidateFleet
	if previous.SnapshotUID != "" || state.SearchServiceUID != "" || state.IngestServiceUID != "" {
		return false
	}
	_, err := r.Core.Secrets(instance.Namespace).Get(ctx, previous.SnapshotName, metav1.GetOptions{})
	if !apierrors.IsNotFound(err) {
		return false
	}
	next, err := r.previewPeerTLS(ctx, instance, candidate, domain)
	if err != nil || next == nil || *next == previous {
		return false
	}
	instance.Status.PeerTLSByFleet[candidate.Fleet] = *next
	return true
}

func (r *InstanceReconciler) readPeerFleetSnapshot(ctx context.Context, instance *v1alpha1.OpenSearchServerless, baseline *v1alpha1.PeerTLSBaseline, pod, domain string, now time.Time) (*corev1.Secret, error) {
	if baseline == nil || baseline.SnapshotUID == "" {
		return nil, fmt.Errorf("peer TLS fleet snapshot is not pinned")
	}
	input := &peerTLSInput{baseline: *baseline}
	plan, err := input.snapshotIntent(instance, pod, domain)
	if err != nil {
		return nil, err
	}
	return tlsconfig.ReadSnapshot(ctx, r.Core, plan, types.UID(baseline.SnapshotUID), now)
}

func (r *InstanceReconciler) qualifyPeerRotation(ctx context.Context, instance *v1alpha1.OpenSearchServerless, target, source string, promote bool) error {
	if len(instance.Status.PeerTLSByFleet) == 0 {
		return nil
	}
	if target == "" || source == "" {
		return fmt.Errorf("peer TLS qualification requires source and target fleets")
	}
	domain, err := r.peerDNSDomain(instance)
	if err != nil {
		return err
	}
	podName := podNameForSequence(instance, instance.Status.PodCreationSequence+1)
	now := r.now()
	outgoingPins := peerTLSBaselineForFleet(instance, source)
	outgoing, err := r.readPeerFleetSnapshot(ctx, instance, outgoingPins, podName, domain, now)
	if err != nil {
		return err
	}
	incomingPins := peerTLSBaselineForFleet(instance, target)
	if incomingPins != nil && *incomingPins == *outgoingPins {
		return nil
	}
	incoming, err := r.readPeerFleetSnapshot(ctx, instance, incomingPins, podName, domain, now)
	if err != nil {
		return err
	}
	input := &peerTLSInput{baseline: *incomingPins}
	plan, err := input.snapshotIntent(instance, podName, domain)
	if err != nil {
		return err
	}
	until, err := peerTLSRolloutDeadline(instance, now, promote)
	if err != nil {
		return err
	}
	return validatePeerTLSOverlap(outgoing, outgoing.Data["ca.crt"], incoming, incoming.Data["ca.crt"], plan.Identities[0], now, until)
}

func peerTLSRolloutDeadline(instance *v1alpha1.OpenSearchServerless, now time.Time, promote bool) (time.Time, error) {
	if promote && instance.Spec.Rollout != nil {
		var rollback v1alpha1.RollbackTarget
		if err := setRollbackDeadline(instance, &rollback, now); err != nil {
			return time.Time{}, err
		}
		if rollback.RetainUntil != nil {
			return rollback.RetainUntil.Add(time.Hour).Add(rolloutObservationAge), nil
		}
		return time.Time{}, fmt.Errorf("peer TLS rotation requires a finite rollback window")
	}
	return now.Add(time.Hour), nil
}

func validatePeerTLSOverlap(outgoing *corev1.Secret, outgoingCA []byte, incoming *corev1.Secret, incomingCA []byte, identity string, now, until time.Time) error {
	usages := []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth}
	// The two fleets can communicate before, during and after the selector switch.
	for _, check := range []struct {
		leaf *corev1.Secret
		ca   []byte
		name string
	}{
		{outgoing, outgoingCA, "serving leaf under serving trust"},
		{incoming, incomingCA, "target leaf under target trust"},
		{outgoing, incomingCA, "serving leaf under target trust"},
		{incoming, outgoingCA, "target leaf under serving trust"},
	} {
		if _, err := tlsconfig.ValidateWindow(check.leaf.Data[corev1.TLSCertKey], check.leaf.Data[corev1.TLSPrivateKeyKey], check.ca, []string{identity}, usages, now, until); err != nil {
			return fmt.Errorf("peer TLS %s: %w", check.name, err)
		}
	}
	return nil
}
