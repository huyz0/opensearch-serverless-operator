package tlsconfig

import (
	"context"
	"crypto/x509"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	typedcore "k8s.io/client-go/kubernetes/typed/core/v1"

	"github.com/huyz0/opensearch-serverless-operator/api/v1alpha1"
)

// Resolve reads the specified Secrets in the instance namespace and validates
// the certificate. Secret authorization and provider-specific identity rules
// must be established by the caller before invoking this function.
func Resolve(ctx context.Context, core typedcore.CoreV1Interface, namespace string, input v1alpha1.TLSChannel, identity string, usages []x509.ExtKeyUsage, now time.Time) (CertificateInfo, error) {
	if core == nil || namespace == "" || input.Source != "Secret" || input.CertificateRef.Name == "" || input.TrustBundleRef.Name == "" || input.TrustBundleRef.Key == "" {
		return CertificateInfo{}, fmt.Errorf("valid namespace and TLS Secret references are required")
	}
	leaf, err := core.Secrets(namespace).Get(ctx, input.CertificateRef.Name, metav1.GetOptions{})
	if err != nil {
		return CertificateInfo{}, fmt.Errorf("read certificate Secret: %w", err)
	}
	trust, err := core.Secrets(namespace).Get(ctx, input.TrustBundleRef.Name, metav1.GetOptions{})
	if err != nil {
		return CertificateInfo{}, fmt.Errorf("read trust Secret: %w", err)
	}
	certificate, certExists := leaf.Data[corev1.TLSCertKey]
	key, keyExists := leaf.Data[corev1.TLSPrivateKeyKey]
	bundle, trustExists := trust.Data[input.TrustBundleRef.Key]
	if !certExists || !keyExists || !trustExists {
		return CertificateInfo{}, fmt.Errorf("certificate or trust Secret is missing a required key")
	}
	return Validate(certificate, key, bundle, identity, usages, now)
}
