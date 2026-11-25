package tlsconfig_test

import (
	"context"
	"crypto/x509"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/huyz0/opensearch-serverless-operator/api/v1alpha1"
	"github.com/huyz0/opensearch-serverless-operator/internal/tlsconfig"
)

func TestResolveRejectsMissingSecretKey(t *testing.T) {
	client := fake.NewSimpleClientset(
		&corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: "n", Name: "leaf"}, Data: map[string][]byte{corev1.TLSCertKey: []byte("certificate")}},
		&corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: "n", Name: "trust"}, Data: map[string][]byte{"ca.crt": []byte("ca")}},
	)
	input := v1alpha1.TLSChannel{Source: "Secret", CertificateRef: v1alpha1.LocalReference{Name: "leaf"}, TrustBundleRef: v1alpha1.SecretKeyReference{Name: "trust", Key: "ca.crt"}}
	if _, err := tlsconfig.Resolve(context.Background(), client.CoreV1(), "n", input, "node.example", []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}, time.Now()); err == nil {
		t.Fatal("missing private key must fail")
	}
	if _, err := tlsconfig.Resolve(context.Background(), client.CoreV1(), "other", input, "node.example", []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}, time.Now()); err == nil {
		t.Fatal("Secrets from another namespace must not resolve")
	}
}
