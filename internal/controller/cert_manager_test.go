package controller

import (
	"context"
	"encoding/base64"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	dynamicfake "k8s.io/client-go/dynamic/fake"

	"github.com/huyz0/opensearch-serverless-operator/api/v1alpha1"
)

func TestCurrentPeerCertificateRequestBindsOutput(t *testing.T) {
	ctx := context.Background()
	certificate := &unstructured.Unstructured{Object: map[string]any{
		"status": map[string]any{"revision": int64(2)},
	}}
	certificate.SetName("peer-issuer-fixture")
	certificate.SetUID("issuing-certificate-uid")
	approved := []byte("signed chain")
	issued := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "cert-manager.io/v1", "kind": "CertificateRequest",
		"spec": map[string]any{"issuerRef": map[string]any{"name": "approved-ca", "kind": "Issuer", "group": "cert-manager.io"}},
		"status": map[string]any{
			"certificate": base64.StdEncoding.EncodeToString(approved),
			"conditions":  []any{map[string]any{"type": "Ready", "status": "True"}, map[string]any{"type": "Approved", "status": "True"}},
		},
	}}
	issued.SetName(certificate.GetName() + "-2")
	issued.SetNamespace("ns")
	issued.SetUID("request-uid")
	controller := true
	issued.SetOwnerReferences([]metav1.OwnerReference{{
		Name: certificate.GetName(), UID: certificate.GetUID(), Kind: "Certificate", APIVersion: "cert-manager.io/v1", Controller: &controller,
	}})
	r := &InstanceReconciler{Certificates: dynamicfake.NewSimpleDynamicClient(runtime.NewScheme(), issued)}
	verify := func(issuer string, output []byte) error {
		return r.verifyCertificateRequest(ctx, "ns", certificate, issuer, output)
	}
	if err := verify("approved-ca", approved); err != nil {
		t.Fatal(err)
	}
	if err := verify("approved-ca", []byte("substitute chain")); err == nil {
		t.Fatal("Secret contents did not match the issued chain")
	}
	if err := verify("unapproved-ca", approved); err == nil {
		t.Fatal("request from a different Issuer was accepted")
	}
	certificate.SetUID("replacement-certificate-uid")
	if err := verify("approved-ca", approved); err == nil {
		t.Fatal("request owned by a prior Certificate UID was accepted")
	}
}

func TestManagedPeerSourceIsProtectedFromSidecar(t *testing.T) {
	instance := &v1alpha1.OpenSearchServerless{ObjectMeta: metav1.ObjectMeta{UID: "instance-uid"}}
	pod := &corev1.Pod{Spec: corev1.PodSpec{Containers: []corev1.Container{{
		Name: "sidecar",
		EnvFrom: []corev1.EnvFromSource{{SecretRef: &corev1.SecretEnvSource{
			LocalObjectReference: corev1.LocalObjectReference{Name: managedPeerSecretName("another-instance")},
		}}},
	}}}}
	if err := validateManagedTLSKeyAccess(instance, pod); err == nil {
		t.Fatal("sidecar could import the mutable peer private-key Secret")
	}
}

func TestManagedPeerIssuanceIsBoundedPerInstance(t *testing.T) {
	instance := &v1alpha1.OpenSearchServerless{ObjectMeta: metav1.ObjectMeta{UID: "first-instance"}}
	channel := &v1alpha1.TLSChannel{Source: "CertManager", CertificateRef: v1alpha1.LocalReference{Name: "first-slot"},
		IssuerRef: &v1alpha1.LocalReference{Name: "first-issuer"}}
	name, err := peerLeafSourceName(instance, channel)
	if err != nil {
		t.Fatal(err)
	}
	channel.CertificateRef.Name, channel.IssuerRef.Name = "another-slot", "another-issuer"
	if changed, err := peerLeafSourceName(instance, channel); err != nil || changed != name {
		t.Fatalf("editing the issuance policy could allocate another Certificate: %q, %v", changed, err)
	}
	instance.UID = "second-instance"
	if changed, err := peerLeafSourceName(instance, channel); err != nil || changed == name {
		t.Fatalf("another instance could share the Certificate name: %q, %v", changed, err)
	}
}
