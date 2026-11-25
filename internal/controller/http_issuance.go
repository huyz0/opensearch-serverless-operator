package controller

import (
	"context"
	"crypto/sha256"
	"crypto/x509"
	"errors"
	"fmt"
	"slices"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/huyz0/opensearch-serverless-operator/api/v1alpha1"
	"github.com/huyz0/opensearch-serverless-operator/internal/tlsconfig"
	"github.com/huyz0/opensearch-serverless-operator/internal/workload"
)

const httpIssuerPrefix = "http-issuer-"
const httpSnapshotPrefix = "http-issued-"

var errHTTPLeafStaging = errors.New("HTTP issued leaf snapshot intent recorded")

func httpIssuerName(uid types.UID) string {
	digest := sha256.Sum256([]byte(uid))
	return fmt.Sprintf("%s%x", httpIssuerPrefix, digest[:16])
}

func httpCertificateDNS(instance *v1alpha1.OpenSearchServerless, domain string) ([]any, []string, error) {
	peer, err := workload.PeerDNSService(instance)
	if err != nil {
		return nil, nil, err
	}
	podDomain := fmt.Sprintf("%s.%s.svc.%s", peer.Name, instance.Namespace, domain)
	if !validDNSHost(podDomain) || len(podDomain)+2 > 253 {
		return nil, nil, fmt.Errorf("invalid HTTP Pod DNS identity")
	}
	names := []any{"*." + podDomain}
	identities := []string{"qualification." + podDomain}
	for _, role := range []string{"search", "ingest"} {
		service, err := workload.RoleService(instance, role, "initial", 9200)
		if err != nil {
			return nil, nil, err
		}
		dns := fmt.Sprintf("%s.%s.svc.%s", service.Name, instance.Namespace, domain)
		if !validDNSHost(dns) {
			return nil, nil, fmt.Errorf("invalid HTTP role Service DNS identity")
		}
		names = append(names, dns)
		identities = append(identities, dns)
	}
	return names, identities, nil
}

// Record the exact issuance revision before creating a Secret that can be
// mounted. Retrying the effect uses the recorded source versions and owner.
func (r *InstanceReconciler) stageIssuedHTTPLeaf(ctx context.Context, instance *v1alpha1.OpenSearchServerless, fleet, domain string, trust *corev1.Secret, key string, ca []byte, retained *httpTLSInput) (*httpTLSInput, error) {
	channel := instance.Spec.TLS.HTTP
	slot, issuer := peerIssuanceIdentity(channel)
	sourceName := httpIssuerName(instance.UID)
	trustHash := fmt.Sprintf("%x", sha256.Sum256(ca))
	sans, identities, err := httpCertificateDNS(instance, domain)
	if err != nil {
		return nil, err
	}
	if instance.UID == "" || len(validation.IsDNS1123Subdomain(slot)) != 0 || len(validation.IsDNS1123Subdomain(issuer)) != 0 {
		return nil, fmt.Errorf("HTTP cert-manager issuance requires an instance, slot and namespaced Issuer")
	}
	if r.Certificates == nil || !r.HTTPCertificateIssuers[issuer] {
		return nil, fmt.Errorf("cert-manager Issuer %q is not approved for HTTP TLS", issuer)
	}
	now := r.now()
	previous := instance.Status.PendingHTTPTLS
	var stagingError error
	if previous != nil && previous.Fleet == fleet && previous.Generation == instance.Generation &&
		previous.Domain == domain && previous.SourceName == sourceName && previous.TrustName == trust.Name &&
		previous.TrustUID == string(trust.UID) && previous.TrustKey == key && previous.TrustSHA256 == trustHash {
		if previous.PEMSHA256 == "" {
			return nil, fmt.Errorf("issued HTTP snapshot intent has no certificate chain digest")
		}
		controller := true
		plan := tlsconfig.SnapshotIntent{
			Namespace: instance.Namespace, Name: previous.Name,
			Owner:    metav1.OwnerReference{APIVersion: v1alpha1.GroupVersion.String(), Kind: "OpenSearchServerless", Name: instance.Name, UID: instance.UID, Controller: &controller},
			LeafName: previous.SourceName, LeafUID: types.UID(previous.SourceUID), LeafResourceVersion: previous.SourceVersion,
			TrustName: previous.TrustName, TrustUID: types.UID(previous.TrustUID), TrustResourceVersion: previous.TrustVersion,
			TrustKey: key, LeafFingerprint: previous.LeafSHA256, LeafPEMSHA256: previous.PEMSHA256, TrustSHA256: previous.TrustSHA256, Identities: identities,
			Usages: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}, MinimumValidity: time.Hour,
		}
		snapshot, err := tlsconfig.EnsureSnapshot(ctx, r.Core, plan, now)
		if err == nil {
			input, err := buildHTTPLeaf(snapshot, trust, key, ca, domain)
			if err != nil {
				return nil, err
			}
			input.baseline.IssuanceSlot, input.baseline.IssuerName = slot, issuer
			stagingError = validateIssuedHTTPLeaf(instance, input, retained, now)
			if stagingError == nil {
				return input, nil
			}
		} else {
			stagingError = err
			// A later issuance can replace the provisional intent when no
			// snapshot exists or when the old snapshot verifies historically.
			// Historical material is never used to start or promote a Pod.
			existing, readErr := r.Core.Secrets(instance.Namespace).Get(ctx, previous.Name, metav1.GetOptions{})
			if !apierrors.IsNotFound(readErr) {
				if readErr != nil || previous.ObservedAt.IsZero() {
					return nil, err
				}
				if _, historyErr := tlsconfig.ReadSnapshot(ctx, r.Core, plan, existing.UID, previous.ObservedAt.Time); historyErr != nil {
					return nil, err
				}
			}
		}
	}
	source, err := r.readCertManagerLeaf(ctx, instance, channel, sourceName, sans, []any{"digital signature", "key encipherment", "server auth"})
	if err != nil {
		return nil, err
	}
	if source.Type != corev1.SecretTypeTLS || source.UID == "" || source.ResourceVersion == "" || source.DeletionTimestamp != nil {
		return nil, fmt.Errorf("issued HTTP source must be a live versioned TLS Secret")
	}
	input, err := buildHTTPLeaf(source, trust, key, ca, domain)
	if err != nil {
		return nil, err
	}
	if err := validateIssuedHTTPLeaf(instance, input, retained, now); err != nil {
		return nil, err
	}
	if _, err := tlsconfig.ValidateWindow(input.certificate, input.key, ca, identities, []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}, now, now); err != nil {
		return nil, fmt.Errorf("issued HTTP certificate does not qualify: %w", err)
	}
	id := fmt.Sprintf("%s\x00%s\x00%s\x00%s\x00%s\x00%s\x00%s", instance.UID, source.UID, source.ResourceVersion, trust.UID, trust.ResourceVersion, key, input.baseline.CertificateHash)
	digest := sha256.Sum256([]byte(id))
	next := v1alpha1.HTTPLeafSnapshot{
		Fleet: fleet, Generation: instance.Generation, Domain: domain, Name: fmt.Sprintf("%s%x", httpSnapshotPrefix, digest[:16]),
		SourceName: sourceName, SourceUID: string(source.UID), SourceVersion: source.ResourceVersion,
		LeafSHA256: input.baseline.LeafSHA256, PEMSHA256: input.baseline.CertificateHash,
		TrustName: trust.Name, TrustUID: string(trust.UID), TrustVersion: trust.ResourceVersion, TrustKey: key, TrustSHA256: trustHash,
		ObservedAt: metav1.NewTime(now),
	}
	if previous != nil && previous.Name == next.Name && stagingError != nil {
		return nil, stagingError
	}
	before := instance.DeepCopyObject().(*v1alpha1.OpenSearchServerless)
	if !slices.Contains(instance.Status.HTTPManagedSnapshots, next.Name) {
		if len(instance.Status.HTTPManagedSnapshots) >= 64 {
			return nil, fmt.Errorf("issued HTTP snapshot history is full")
		}
		instance.Status.HTTPManagedSnapshots = append(instance.Status.HTTPManagedSnapshots, next.Name)
	}
	instance.Status.PendingHTTPTLS = &next
	if err := r.Status().Patch(ctx, instance, client.MergeFromWithOptions(before, client.MergeFromWithOptimisticLock{})); err != nil {
		return nil, err
	}
	return nil, errHTTPLeafStaging
}

func validateIssuedHTTPLeaf(instance *v1alpha1.OpenSearchServerless, issued, retained *httpTLSInput, now time.Time) error {
	if retained == nil {
		return issued.validatePod(instance, podNameForSequence(instance, 1), now)
	}
	if err := preserveHTTPLeafNames(retained.certificate, issued.certificate); err != nil {
		return err
	}
	if issued.baseline.LeafSHA256 == retained.baseline.LeafSHA256 {
		return fmt.Errorf("HTTP Issuer has not issued a new leaf")
	}
	return validateHTTPRotationWindow(instance, issued, retained, now, true)
}
