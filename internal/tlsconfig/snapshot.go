package tlsconfig

import (
	"context"
	"crypto/sha256"
	"crypto/x509"
	"encoding/hex"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	typedcore "k8s.io/client-go/kubernetes/typed/core/v1"
)

const (
	snapshotLeafUID      = "serverless.opensearch.org/tls-leaf-source-uid"
	snapshotLeafVersion  = "serverless.opensearch.org/tls-leaf-source-version"
	snapshotTrustUID     = "serverless.opensearch.org/tls-trust-source-uid"
	snapshotTrustVersion = "serverless.opensearch.org/tls-trust-source-version"
)

// SnapshotIntent contains only source identity and public certificate hashes.
// Persist it before EnsureSnapshot. No private-key-derived hash is published.
type SnapshotIntent struct {
	Namespace            string
	Name                 string
	Owner                metav1.OwnerReference
	LeafName             string
	LeafUID              types.UID
	LeafResourceVersion  string
	TrustName            string
	TrustUID             types.UID
	TrustResourceVersion string
	TrustKey             string
	LeafFingerprint      string
	LeafPEMSHA256        string
	TrustSHA256          string
	Identities           []string
	Usages               []x509.ExtKeyUsage
	MinimumValidity      time.Duration
}

func validateSnapshotIdentity(intent SnapshotIntent) error {
	if intent.Namespace == "" || intent.Name == "" || intent.Name == intent.LeafName || intent.Name == intent.TrustName || intent.Owner.UID == "" || intent.Owner.Name == "" || intent.Owner.Kind == "" || intent.Owner.APIVersion == "" || intent.Owner.Controller == nil || !*intent.Owner.Controller || intent.LeafName == "" || intent.LeafUID == "" || intent.LeafResourceVersion == "" || intent.TrustName == "" || intent.TrustUID == "" || intent.TrustResourceVersion == "" || intent.TrustKey == "" || len(intent.Identities) == 0 || len(intent.Usages) == 0 || intent.MinimumValidity <= 0 || intent.MinimumValidity > 5*365*24*time.Hour {
		return fmt.Errorf("complete snapshot, owner, source and verification identity is required")
	}
	return nil
}

func verifySnapshotMaterial(cert, key, trust []byte, intent SnapshotIntent, now time.Time) (CertificateInfo, string, error) {
	info, err := ValidateWindow(cert, key, trust, intent.Identities, intent.Usages, now, now.Add(intent.MinimumValidity))
	if err != nil {
		return CertificateInfo{}, "", fmt.Errorf("certificate or trust does not cover required validity window: %w", err)
	}
	if intent.LeafPEMSHA256 != "" && sha256Hex(cert) != intent.LeafPEMSHA256 {
		return CertificateInfo{}, "", fmt.Errorf("certificate chain differs from the recorded PEM digest")
	}
	return info, sha256Hex(trust), nil
}

func sha256Hex(data []byte) string {
	digest := sha256.Sum256(data)
	return hex.EncodeToString(digest[:])
}

func readSnapshotSources(ctx context.Context, core typedcore.CoreV1Interface, intent SnapshotIntent) (*corev1.Secret, *corev1.Secret, error) {
	leaf, err := core.Secrets(intent.Namespace).Get(ctx, intent.LeafName, metav1.GetOptions{})
	if err != nil {
		return nil, nil, fmt.Errorf("read leaf Secret: %w", err)
	}
	trust, err := core.Secrets(intent.Namespace).Get(ctx, intent.TrustName, metav1.GetOptions{})
	if err != nil {
		return nil, nil, fmt.Errorf("read trust Secret: %w", err)
	}
	if leaf.Type != corev1.SecretTypeTLS || leaf.UID != intent.LeafUID || leaf.ResourceVersion != intent.LeafResourceVersion || leaf.DeletionTimestamp != nil || trust.UID != intent.TrustUID || trust.ResourceVersion != intent.TrustResourceVersion || trust.DeletionTimestamp != nil {
		return nil, nil, fmt.Errorf("TLS source type, identity or version changed")
	}
	return leaf, trust, nil
}

// PlanSnapshot reads and validates the exact source versions and returns the
// non-secret intent. The caller persists that intent before creating a Secret.
func PlanSnapshot(ctx context.Context, core typedcore.CoreV1Interface, intent SnapshotIntent, now time.Time) (SnapshotIntent, error) {
	if core == nil || now.IsZero() {
		return SnapshotIntent{}, fmt.Errorf("Secret client and observation time are required")
	}
	if err := validateSnapshotIdentity(intent); err != nil {
		return SnapshotIntent{}, err
	}
	leaf, trust, err := readSnapshotSources(ctx, core, intent)
	if err != nil {
		return SnapshotIntent{}, err
	}
	info, digest, err := verifySnapshotMaterial(leaf.Data[corev1.TLSCertKey], leaf.Data[corev1.TLSPrivateKeyKey], trust.Data[intent.TrustKey], intent, now)
	if err != nil {
		return SnapshotIntent{}, fmt.Errorf("validate TLS snapshot inputs: %w", err)
	}
	intent.LeafFingerprint = info.Fingerprint
	intent.LeafPEMSHA256 = sha256Hex(leaf.Data[corev1.TLSCertKey])
	intent.TrustSHA256 = digest
	intent.Identities = append([]string(nil), intent.Identities...)
	intent.Usages = append([]x509.ExtKeyUsage(nil), intent.Usages...)
	return intent, nil
}

func validateExistingSnapshot(secret *corev1.Secret, intent SnapshotIntent, now time.Time) error {
	owner := metav1.GetControllerOf(secret)
	if secret.UID == "" || secret.Type != corev1.SecretTypeOpaque || len(secret.Data) != 3 || secret.DeletionTimestamp != nil || secret.Immutable == nil || !*secret.Immutable || owner == nil || owner.UID != intent.Owner.UID || owner.Name != intent.Owner.Name || owner.Kind != intent.Owner.Kind || owner.APIVersion != intent.Owner.APIVersion || secret.Annotations[snapshotLeafUID] != string(intent.LeafUID) || secret.Annotations[snapshotLeafVersion] != intent.LeafResourceVersion || secret.Annotations[snapshotTrustUID] != string(intent.TrustUID) || secret.Annotations[snapshotTrustVersion] != intent.TrustResourceVersion {
		return fmt.Errorf("existing TLS snapshot has conflicting identity")
	}
	info, digest, err := verifySnapshotMaterial(secret.Data[corev1.TLSCertKey], secret.Data[corev1.TLSPrivateKeyKey], secret.Data["ca.crt"], intent, now)
	if err != nil {
		return fmt.Errorf("validate existing TLS snapshot: %w", err)
	}
	if info.Fingerprint != intent.LeafFingerprint || digest != intent.TrustSHA256 {
		return fmt.Errorf("existing TLS snapshot does not match the recorded public material")
	}
	return nil
}

// ReadSnapshot verifies an existing snapshot and its pinned UID without ever
// recreating it. A deleted or replaced snapshot is a recovery hold.
func ReadSnapshot(ctx context.Context, core typedcore.CoreV1Interface, intent SnapshotIntent, expectedUID types.UID, now time.Time) (*corev1.Secret, error) {
	if core == nil || expectedUID == "" || now.IsZero() || intent.LeafFingerprint == "" || intent.TrustSHA256 == "" {
		return nil, fmt.Errorf("pinned snapshot identity is required")
	}
	if err := validateSnapshotIdentity(intent); err != nil {
		return nil, err
	}
	secret, err := core.Secrets(intent.Namespace).Get(ctx, intent.Name, metav1.GetOptions{})
	if err != nil {
		return nil, fmt.Errorf("read pinned TLS snapshot: %w", err)
	}
	if secret.UID != expectedUID {
		return nil, fmt.Errorf("pinned TLS snapshot identity changed")
	}
	if err := validateExistingSnapshot(secret, intent, now); err != nil {
		return nil, err
	}
	return secret, nil
}

// EnsureSnapshot creates or recovers one immutable versioned Secret. A retry
// may recover an existing verified snapshot even after the mutable source has
// changed. A create error can be ambiguous; callers must re-read and retry.
func EnsureSnapshot(ctx context.Context, core typedcore.CoreV1Interface, intent SnapshotIntent, now time.Time) (*corev1.Secret, error) {
	if core == nil || now.IsZero() || intent.LeafFingerprint == "" || intent.TrustSHA256 == "" {
		return nil, fmt.Errorf("persisted snapshot intent and Secret client are required")
	}
	if err := validateSnapshotIdentity(intent); err != nil {
		return nil, err
	}
	secrets := core.Secrets(intent.Namespace)
	existing, err := secrets.Get(ctx, intent.Name, metav1.GetOptions{})
	if err == nil {
		if err := validateExistingSnapshot(existing, intent, now); err != nil {
			return nil, err
		}
		return existing, nil
	}
	if !apierrors.IsNotFound(err) {
		return nil, fmt.Errorf("read TLS snapshot: %w", err)
	}
	leaf, trust, err := readSnapshotSources(ctx, core, intent)
	if err != nil {
		return nil, err
	}
	info, digest, err := verifySnapshotMaterial(leaf.Data[corev1.TLSCertKey], leaf.Data[corev1.TLSPrivateKeyKey], trust.Data[intent.TrustKey], intent, now)
	if err != nil {
		return nil, fmt.Errorf("validate recorded TLS sources: %w", err)
	}
	if info.Fingerprint != intent.LeafFingerprint || digest != intent.TrustSHA256 {
		return nil, fmt.Errorf("TLS sources no longer match the recorded intent")
	}
	immutable := true
	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: intent.Namespace, Name: intent.Name, OwnerReferences: []metav1.OwnerReference{intent.Owner}, Annotations: map[string]string{snapshotLeafUID: string(intent.LeafUID), snapshotLeafVersion: intent.LeafResourceVersion, snapshotTrustUID: string(intent.TrustUID), snapshotTrustVersion: intent.TrustResourceVersion}}, Immutable: &immutable, Type: corev1.SecretTypeOpaque, Data: map[string][]byte{corev1.TLSCertKey: append([]byte(nil), leaf.Data[corev1.TLSCertKey]...), corev1.TLSPrivateKeyKey: append([]byte(nil), leaf.Data[corev1.TLSPrivateKeyKey]...), "ca.crt": append([]byte(nil), trust.Data[intent.TrustKey]...)}}
	created, err := secrets.Create(ctx, secret, metav1.CreateOptions{})
	if apierrors.IsAlreadyExists(err) {
		created, err = secrets.Get(ctx, intent.Name, metav1.GetOptions{})
	}
	if err != nil {
		return nil, fmt.Errorf("create or recover TLS snapshot: %w", err)
	}
	if err := validateExistingSnapshot(created, intent, now); err != nil {
		return nil, err
	}
	return created, nil
}
