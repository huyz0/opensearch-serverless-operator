package controller

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"reflect"
	"slices"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/huyz0/opensearch-serverless-operator/api/v1alpha1"
	"github.com/huyz0/opensearch-serverless-operator/internal/runtimeconfig"
	"github.com/huyz0/opensearch-serverless-operator/internal/tlsconfig"
	"github.com/huyz0/opensearch-serverless-operator/internal/workload"
)

const (
	httpCertificateVolume = "serverless-http-cert"
	httpTrustVolume       = "serverless-http-trust"
)

type httpTLSInput struct {
	baseline             v1alpha1.HTTPTLSBaseline
	certificate, key, ca []byte
}

func (r *InstanceReconciler) resolveHTTPTLS(ctx context.Context, instance *v1alpha1.OpenSearchServerless, domain string) (*httpTLSInput, error) {
	return r.resolveHTTPTLSForFleet(ctx, instance, domain, "")
}

func (r *InstanceReconciler) resolveHTTPTLSForFleet(ctx context.Context, instance *v1alpha1.OpenSearchServerless, domain, fleet string) (*httpTLSInput, error) {
	if instance.Spec.TLS == nil || instance.Spec.TLS.HTTP == nil {
		if instance.Status.HTTPTLSBaseline != nil || len(instance.Status.HTTPFleetCertificates) != 0 {
			return nil, fmt.Errorf("HTTP TLS inputs are pinned for this instance")
		}
		return nil, nil
	}
	trust, ca, err := r.readHTTPTrustForFleet(ctx, instance, domain, fleet)
	if err != nil {
		return nil, err
	}
	name := instance.Spec.TLS.HTTP.CertificateRef.Name
	key := instance.Spec.TLS.HTTP.TrustBundleRef.Key
	pins := httpTLSBaselineForFleet(instance, fleet)
	if pins == nil && instance.Status.HTTPTLSBaseline != nil {
		return nil, fmt.Errorf("HTTP TLS fleet certificate pin is missing or inconsistent")
	}
	if pins == nil && instance.Spec.TLS.HTTP.Source == "CertManager" {
		if fleet != "" {
			return nil, fmt.Errorf("issued HTTP TLS baseline is not pinned")
		}
		return r.stageIssuedHTTPLeaf(ctx, instance, "", domain, trust, key, ca, nil)
	}
	if pins != nil {
		name = pins.CertificateName
		key = pins.TrustKey
	}
	input, err := r.readHTTPLeaf(ctx, instance, name, trust, key, ca, domain)
	if err != nil {
		return nil, err
	}
	if pins != nil {
		expected := *pins
		input.baseline.IssuanceSlot, input.baseline.IssuerName = expected.IssuanceSlot, expected.IssuerName
		// Previously pinned PEM bytes and Secret UID are sufficient to
		// recover the public leaf fingerprint for older instances.
		if expected.LeafSHA256 == "" {
			expected.LeafSHA256 = input.baseline.LeafSHA256
		}
		if expected != input.baseline {
			return nil, fmt.Errorf("HTTP TLS inputs differ from the pinned baseline; controlled rotation is required")
		}
	}
	return input, nil
}

func httpTLSBaselineForFleet(instance *v1alpha1.OpenSearchServerless, fleet string) *v1alpha1.HTTPTLSBaseline {
	base := instance.Status.HTTPTLSBaseline
	leaf, found := instance.Status.HTTPFleetCertificates[fleet]
	if state := instance.Status.CandidateFleet; state != nil && state.Fleet == fleet {
		if state.HTTPRenewFromIssuer && (!found || !strings.HasPrefix(leaf.Name, httpSnapshotPrefix) || leaf.UID == "" || leaf.LeafSHA256 == "") {
			return nil
		}
		if state.HTTPCertificateRef != "" && (!found || leaf.Name != state.HTTPCertificateRef || leaf.LeafSHA256 == "") {
			return nil
		}
		if ref := state.HTTPTrustBundleRef; ref != nil && (!found || leaf.TrustName != ref.Name || leaf.TrustKey != ref.Key || leaf.TrustUID == "" || leaf.TrustSHA256 == "") {
			return nil
		}
	}
	if base == nil || !found {
		return base
	}
	pins := *base
	pins.CertificateName, pins.CertificateUID, pins.CertificateHash, pins.LeafSHA256 = leaf.Name, leaf.UID, leaf.PEMSHA256, leaf.LeafSHA256
	if leaf.TrustName != "" || leaf.TrustKey != "" || leaf.TrustUID != "" || leaf.TrustSHA256 != "" {
		if leaf.TrustName == "" || leaf.TrustKey == "" || leaf.TrustUID == "" || leaf.TrustSHA256 == "" {
			return nil
		}
		pins.TrustName, pins.TrustKey, pins.TrustUID, pins.TrustHash = leaf.TrustName, leaf.TrustKey, leaf.TrustUID, leaf.TrustSHA256
	}
	return &pins
}

func managedPodTLSConfig(instance *v1alpha1.OpenSearchServerless, fleet, identity string, roots *x509.CertPool) (*tls.Config, error) {
	config := &tls.Config{MinVersion: tls.VersionTLS12, ServerName: identity, RootCAs: roots}
	pins := httpTLSBaselineForFleet(instance, fleet)
	if pins == nil && hasManagedHTTPS(instance) {
		return nil, fmt.Errorf("HTTP TLS fleet certificate pin is missing or inconsistent")
	}
	if pins != nil && pins.LeafSHA256 != "" {
		config.VerifyConnection = func(state tls.ConnectionState) error {
			if len(state.PeerCertificates) == 0 || fmt.Sprintf("%x", sha256.Sum256(state.PeerCertificates[0].Raw)) != pins.LeafSHA256 {
				return fmt.Errorf("HTTPS peer certificate differs from the fleet's pinned leaf")
			}
			return nil
		}
	}
	return config, nil
}

func httpTLSBaseMatches(instance *v1alpha1.OpenSearchServerless, domain string) bool {
	base := instance.Status.HTTPTLSBaseline
	if instance.Spec.TLS == nil || instance.Spec.TLS.HTTP == nil || base == nil || domain == "" || base.Domain != domain {
		return false
	}
	channel := instance.Spec.TLS.HTTP
	if channel.TrustBundleRef.Name != base.TrustName || channel.TrustBundleRef.Key != base.TrustKey {
		return false
	}
	slot, issuer := peerIssuanceIdentity(channel)
	if channel.Source == "CertManager" {
		return slot != "" && issuer != "" && slot == base.IssuanceSlot && issuer == base.IssuerName && strings.HasPrefix(base.CertificateName, httpSnapshotPrefix)
	}
	return channel.Source == "Secret" && base.IssuerName == "" && channel.CertificateRef.Name == base.CertificateName
}

func (r *InstanceReconciler) readHTTPTrustForFleet(ctx context.Context, instance *v1alpha1.OpenSearchServerless, domain, fleet string) (*corev1.Secret, []byte, error) {
	return r.podHTTP().readHTTPTrustForFleet(ctx, instance, domain, fleet)
}

func (r *InstanceReconciler) readHTTPTrustSecret(ctx context.Context, instance *v1alpha1.OpenSearchServerless, ref v1alpha1.SecretKeyReference) (*corev1.Secret, []byte, error) {
	return r.podHTTP().readHTTPTrustSecret(ctx, instance, ref)
}

func (r *InstanceReconciler) readHTTPLeaf(ctx context.Context, instance *v1alpha1.OpenSearchServerless, name string, trust *corev1.Secret, trustKey string, ca []byte, domain string) (*httpTLSInput, error) {
	certificate, err := r.Core.Secrets(instance.Namespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return nil, fmt.Errorf("read HTTP TLS certificate Secret: %w", err)
	}
	if certificate.UID == "" || certificate.ResourceVersion == "" || certificate.DeletionTimestamp != nil || certificate.Immutable == nil || !*certificate.Immutable {
		return nil, fmt.Errorf("HTTP TLS certificate must be a live immutable Secret")
	}
	if strings.HasPrefix(name, httpSnapshotPrefix) {
		owner := metav1.GetControllerOf(certificate)
		if certificate.Type != corev1.SecretTypeOpaque || owner == nil || owner.UID != instance.UID ||
			owner.Name != instance.Name || owner.Kind != "OpenSearchServerless" || owner.APIVersion != v1alpha1.GroupVersion.String() {
			return nil, fmt.Errorf("issued HTTP snapshot has a conflicting owner or Secret type")
		}
	} else if certificate.Type != corev1.SecretTypeTLS {
		return nil, fmt.Errorf("HTTP TLS requires a TLS Secret")
	}
	return buildHTTPLeaf(certificate, trust, trustKey, ca, domain)
}

func buildHTTPLeaf(certificate, trust *corev1.Secret, trustKey string, ca []byte, domain string) (*httpTLSInput, error) {
	input := &httpTLSInput{certificate: certificate.Data[corev1.TLSCertKey], key: certificate.Data[corev1.TLSPrivateKeyKey], ca: ca}
	if len(input.certificate) == 0 || len(input.key) == 0 || len(input.ca) == 0 {
		return nil, fmt.Errorf("HTTP TLS requires a TLS Secret and a nonempty trust bundle")
	}
	fingerprint, trustHash, err := tlsconfig.PublicMaterialDigests(input.certificate, ca)
	if err != nil {
		return nil, err
	}
	input.baseline = v1alpha1.HTTPTLSBaseline{
		CertificateName: certificate.Name, CertificateUID: string(certificate.UID), CertificateHash: fmt.Sprintf("%x", sha256.Sum256(input.certificate)),
		LeafSHA256: fingerprint, TrustName: trust.Name, TrustUID: string(trust.UID), TrustKey: trustKey, TrustHash: trustHash, Domain: domain,
	}
	return input, nil
}

func preserveHTTPLeafNames(oldPEM, newPEM []byte) error {
	oldBlock, _ := pem.Decode(oldPEM)
	newBlock, _ := pem.Decode(newPEM)
	if oldBlock == nil || newBlock == nil || oldBlock.Type != "CERTIFICATE" || newBlock.Type != "CERTIFICATE" {
		return fmt.Errorf("HTTP TLS rotation requires parseable leaf certificates")
	}
	oldLeaf, oldErr := x509.ParseCertificate(oldBlock.Bytes)
	newLeaf, newErr := x509.ParseCertificate(newBlock.Bytes)
	if oldErr != nil || newErr != nil {
		return fmt.Errorf("HTTP TLS rotation requires parseable leaf certificates")
	}
	for _, name := range oldLeaf.DNSNames {
		if !slices.ContainsFunc(newLeaf.DNSNames, func(candidate string) bool { return strings.EqualFold(name, candidate) }) {
			return fmt.Errorf("replacement HTTP TLS certificate must retain all serving DNS names")
		}
	}
	for _, ip := range oldLeaf.IPAddresses {
		if !slices.ContainsFunc(newLeaf.IPAddresses, ip.Equal) {
			return fmt.Errorf("replacement HTTP TLS certificate must retain all serving IP addresses")
		}
	}
	return nil
}

func candidateHTTPCertificateName(candidate *v1alpha1.FleetCandidate) string {
	if candidate.HTTPCertificateRef == nil {
		return ""
	}
	return candidate.HTTPCertificateRef.Name
}

func candidateTLSInputsMatch(state *v1alpha1.CandidateFleetStatus, desired *v1alpha1.FleetCandidate) bool {
	return state.HTTPCertificateRef == candidateHTTPCertificateName(desired) && state.HTTPRenewFromIssuer == desired.HTTPRenewFromIssuer &&
		reflect.DeepEqual(state.HTTPTrustBundleRef, desired.HTTPTrustBundleRef) && reflect.DeepEqual(state.PeerTLS, desired.PeerTLS)
}

// previewHTTPCertificate pins the leaf and trust bundle before allocating
// preview Pods. Omitted inputs inherit the pins shared by the serving roles.
func (r *InstanceReconciler) previewHTTPCertificate(ctx context.Context, instance *v1alpha1.OpenSearchServerless, desired *v1alpha1.FleetCandidate, domain string) (*v1alpha1.HTTPFleetCertificate, error) {
	name := candidateHTTPCertificateName(desired)
	if !desired.HTTPRenewFromIssuer && name == "" && desired.HTTPTrustBundleRef == nil && len(instance.Status.HTTPFleetCertificates) == 0 {
		return nil, nil
	}
	if !httpTLSBaseMatches(instance, domain) || instance.Status.HTTPTLSBaseline.LeafSHA256 == "" || desired.Image != instance.Spec.Image {
		return nil, fmt.Errorf("preview HTTP TLS requires pinned HTTPS and the same approved runtime image")
	}
	searchFleet, ingestFleet := instance.Status.ServingFleetIntentByRole["search"], instance.Status.ServingFleetIntentByRole["ingest"]
	searchPins, ingestPins := httpTLSBaselineForFleet(instance, searchFleet), httpTLSBaselineForFleet(instance, ingestFleet)
	if searchFleet == "" || ingestFleet == "" || searchPins == nil || ingestPins == nil || *searchPins != *ingestPins {
		return nil, fmt.Errorf("serving roles must agree on the current HTTP TLS leaf")
	}
	if len(instance.Status.HTTPFleetCertificates) >= 64 {
		return nil, fmt.Errorf("HTTP TLS fleet certificate history is full")
	}
	if !desired.HTTPRenewFromIssuer && name == "" && desired.HTTPTrustBundleRef == nil {
		if leaf, ok := instance.Status.HTTPFleetCertificates[searchFleet]; ok {
			return &leaf, nil
		}
		return nil, nil
	}
	serving, err := r.resolveHTTPTLSForFleet(ctx, instance, domain, searchFleet)
	if err != nil {
		return nil, err
	}
	if name == "" {
		name = serving.baseline.CertificateName
	}
	key := serving.baseline.TrustKey
	trust, ca, err := r.readHTTPTrustForFleet(ctx, instance, domain, searchFleet)
	if ref := desired.HTTPTrustBundleRef; ref != nil {
		key = ref.Key
		trust, ca, err = r.readHTTPTrustSecret(ctx, instance, *ref)
	}
	if err != nil {
		return nil, err
	}
	var replacement *httpTLSInput
	if desired.HTTPRenewFromIssuer {
		if instance.Spec.TLS.HTTP.Source != "CertManager" || desired.HTTPCertificateRef != nil || instance.Spec.Rollout == nil || instance.Spec.Rollout.RollbackWindow == "" {
			return nil, fmt.Errorf("HTTP issuer renewal requires cert-manager HTTP TLS and a finite rollback window")
		}
		replacement, err = r.stageIssuedHTTPLeaf(ctx, instance, desired.Fleet, domain, trust, key, ca, serving)
	} else {
		replacement, err = r.readHTTPLeaf(ctx, instance, name, trust, key, ca, domain)
	}
	if err != nil {
		return nil, err
	}
	if err := preserveHTTPLeafNames(serving.certificate, replacement.certificate); err != nil {
		return nil, err
	}
	if serving.baseline.LeafSHA256 == replacement.baseline.LeafSHA256 && serving.baseline.TrustHash == replacement.baseline.TrustHash {
		return nil, fmt.Errorf("replacement HTTP TLS inputs must change the leaf or the trust bundle")
	}
	now := r.now()
	if err := validateHTTPRotationWindow(instance, replacement, serving, now, true); err != nil {
		return nil, err
	}
	b := replacement.baseline
	return &v1alpha1.HTTPFleetCertificate{Name: b.CertificateName, UID: b.CertificateUID, PEMSHA256: b.CertificateHash, LeafSHA256: b.LeafSHA256,
		TrustName: b.TrustName, TrustKey: b.TrustKey, TrustUID: b.TrustUID, TrustSHA256: b.TrustHash}, nil
}

func (r *InstanceReconciler) qualifyHTTPRotation(ctx context.Context, instance *v1alpha1.OpenSearchServerless, target, source string, promote bool) error {
	if len(instance.Status.HTTPFleetCertificates) == 0 {
		return nil
	}
	domain, err := r.peerDNSDomain(instance)
	if err != nil {
		return err
	}
	destination, err := r.resolveHTTPTLSForFleet(ctx, instance, domain, target)
	if err != nil {
		return err
	}
	var retained *httpTLSInput
	if promote {
		retained, err = r.resolveHTTPTLSForFleet(ctx, instance, domain, source)
		if err != nil {
			return err
		}
	}
	return validateHTTPRotationWindow(instance, destination, retained, r.now(), promote)
}

func validateHTTPRotationWindow(instance *v1alpha1.OpenSearchServerless, destination, retained *httpTLSInput, now time.Time, promote bool) error {
	if destination == nil || (promote && retained == nil) {
		return fmt.Errorf("HTTP TLS rotation requires the pinned fleet certificates")
	}
	through := now.Add(time.Hour)
	if promote && instance.Spec.Rollout != nil {
		var rollback v1alpha1.RollbackTarget
		if err := setRollbackDeadline(instance, &rollback, now); err != nil {
			return err
		}
		if rollback.RetainUntil != nil {
			deadline := rollback.RetainUntil.Add(time.Hour).Add(rolloutObservationAge)
			if deadline.After(through) {
				through = deadline
			}
		}
	}
	name := podNameForSequence(instance, instance.Status.PodCreationSequence+1)
	if retained != nil {
		if err := retained.validatePodThrough(instance, name, now, through); err != nil {
			return fmt.Errorf("retained fleet HTTP TLS certificate cannot cover the rollback window: %w", err)
		}
	}
	if err := destination.validatePodThrough(instance, name, now, through); err != nil {
		return fmt.Errorf("target fleet HTTP TLS certificate cannot cover the required validity period: %w", err)
	}
	if retained == nil {
		return nil
	}
	if !bytes.Equal(destination.ca, retained.ca) {
		overlap := *retained
		overlap.ca = destination.ca
		if err := overlap.validatePodThrough(instance, name, now, through); err != nil {
			return fmt.Errorf("target HTTP trust must validate the retained fleet through the rollback window: %w", err)
		}
	}
	return nil
}

func httpPodIdentity(instance *v1alpha1.OpenSearchServerless, podName, domain string) (string, error) {
	service, err := workload.PeerDNSService(instance)
	if err != nil {
		return "", err
	}
	host, err := workload.PeerHostname(podName)
	if err != nil {
		return "", err
	}
	identity := fmt.Sprintf("%s.%s.%s.svc.%s", host, service.Name, instance.Namespace, domain)
	if domain == "" || !validDNSHost(identity) {
		return "", fmt.Errorf("HTTP TLS requires a valid Pod DNS identity")
	}
	return identity, nil
}

func (input *httpTLSInput) validatePod(instance *v1alpha1.OpenSearchServerless, name string, now time.Time) error {
	return input.validatePodThrough(instance, name, now, now.Add(time.Hour))
}

func (input *httpTLSInput) validatePodThrough(instance *v1alpha1.OpenSearchServerless, name string, now, through time.Time) error {
	if input == nil {
		return nil
	}
	identity, err := httpPodIdentity(instance, name, input.baseline.Domain)
	if err != nil {
		return err
	}
	if _, err := tlsconfig.ValidateWindow(input.certificate, input.key, input.ca, []string{identity}, []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}, now, through); err != nil {
		return fmt.Errorf("HTTP TLS certificate does not qualify for the Pod: %w", err)
	}
	return nil
}

func addHTTPVolumes(pod *corev1.Pod, pins *v1alpha1.HTTPTLSBaseline) {
	mode := int32(0440)
	pod.Spec.Volumes = append(pod.Spec.Volumes,
		corev1.Volume{Name: httpCertificateVolume, VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{SecretName: pins.CertificateName, DefaultMode: &mode,
			Items: []corev1.KeyToPath{{Key: corev1.TLSCertKey, Path: "tls.crt"}, {Key: corev1.TLSPrivateKeyKey, Path: "tls.key"}}}}},
		corev1.Volume{Name: httpTrustVolume, VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{SecretName: pins.TrustName, DefaultMode: &mode,
			Items: []corev1.KeyToPath{{Key: pins.TrustKey, Path: "ca.crt"}}}}},
	)
	pod.Spec.Containers[0].VolumeMounts = append(pod.Spec.Containers[0].VolumeMounts,
		corev1.VolumeMount{Name: httpCertificateVolume, MountPath: runtimeconfig.HTTPServerCertDir, ReadOnly: true},
		corev1.VolumeMount{Name: httpTrustVolume, MountPath: runtimeconfig.HTTPServerTrustDir, ReadOnly: true},
	)
}

func validateHTTPKeyAccess(pod *corev1.Pod, secretName string) error {
	return validateProtectedSecretAccess(pod, secretName, httpCertificateVolume, httpTrustVolume)
}

func validateManagedTLSKeyAccess(instance *v1alpha1.OpenSearchServerless, pod *corev1.Pod) error {
	if err := validateSecretAccess(pod, func(name string) bool {
		return strings.HasPrefix(name, managedPeerSecretPrefix) || strings.HasPrefix(name, httpIssuerPrefix)
	}, nil); err != nil {
		return err
	}
	pin := httpTLSBaselineForFleet(instance, pod.Labels[workload.FleetLabel])
	if err := validateSecretAccess(pod, func(name string) bool {
		return strings.HasPrefix(name, httpSnapshotPrefix) && (pin == nil || name != pin.CertificateName)
	}, nil); err != nil {
		return err
	}
	if baseline := instance.Status.HTTPTLSBaseline; baseline != nil {
		if err := validateHTTPKeyAccess(pod, baseline.CertificateName); err != nil {
			return err
		}
	}
	for _, leaf := range instance.Status.HTTPFleetCertificates {
		if err := validateHTTPKeyAccess(pod, leaf.Name); err != nil {
			return err
		}
	}
	return nil
}

func validateProtectedSecretAccess(pod *corev1.Pod, secretName string, allowedVolumes ...string) error {
	return validateSecretAccess(pod, func(name string) bool { return name == secretName }, allowedVolumes)
}

func validateSecretAccess(pod *corev1.Pod, protected func(string) bool, allowedVolumes []string) error {
	for _, volume := range pod.Spec.Volumes {
		if volume.Secret != nil && protected(volume.Secret.SecretName) && !slices.Contains(allowedVolumes, volume.Name) {
			return fmt.Errorf("protected Secret cannot be aliased as another volume")
		}
		if volume.Projected != nil {
			for _, source := range volume.Projected.Sources {
				if source.Secret != nil && protected(source.Secret.Name) {
					return fmt.Errorf("protected Secret cannot be added to a projected volume")
				}
			}
		}
	}
	for _, group := range [][]corev1.Container{pod.Spec.Containers, pod.Spec.InitContainers} {
		for _, container := range group {
			for _, source := range container.EnvFrom {
				if source.SecretRef != nil && protected(source.SecretRef.Name) {
					return fmt.Errorf("protected Secret cannot supply container environment variables")
				}
			}
			for _, variable := range container.Env {
				if variable.ValueFrom != nil && variable.ValueFrom.SecretKeyRef != nil && protected(variable.ValueFrom.SecretKeyRef.Name) {
					return fmt.Errorf("protected Secret cannot supply container environment variables")
				}
			}
		}
	}
	return nil
}
