package controller

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"reflect"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/huyz0/opensearch-serverless-operator/api/v1alpha1"
	"github.com/huyz0/opensearch-serverless-operator/internal/workload"
)

var certManagerCertificates = schema.GroupVersionResource{Group: "cert-manager.io", Version: "v1", Resource: "certificates"}
var certManagerRequests = schema.GroupVersionResource{Group: "cert-manager.io", Version: "v1", Resource: "certificaterequests"}

const peerIssuanceOwner = "serverless.opensearch.org/peer-certificate-instance"
const peerIssuanceSlot = "serverless.opensearch.org/peer-certificate-slot"
const managedPeerSecretPrefix = "peer-issuer-"

// Use an instance-scoped Secret name so an issuance slot cannot target an
// arbitrary Secret elsewhere in the namespace.
func peerLeafSourceName(instance *v1alpha1.OpenSearchServerless, channel *v1alpha1.TLSChannel) (string, error) {
	switch channel.Source {
	case "Secret":
		if channel.IssuerRef == nil && channel.CertificateRef.Name != "" {
			return channel.CertificateRef.Name, nil
		}
	case "CertManager":
		if instance.UID != "" && channel.IssuerRef != nil && len(validation.IsDNS1123Subdomain(channel.IssuerRef.Name)) == 0 &&
			len(validation.IsDNS1123Subdomain(channel.CertificateRef.Name)) == 0 {
			return managedPeerSecretName(instance.UID), nil
		}
	}
	return "", fmt.Errorf("peer TLS requires a Secret or a cert-manager issuance slot and namespaced Issuer")
}

func managedPeerSecretName(uid types.UID) string {
	digest := sha256.Sum256([]byte(uid))
	return fmt.Sprintf("%s%x", managedPeerSecretPrefix, digest[:16])
}

func peerIssuanceIdentity(channel *v1alpha1.TLSChannel) (slot, issuer string) {
	if channel.Source == "CertManager" && channel.IssuerRef != nil {
		return channel.CertificateRef.Name, channel.IssuerRef.Name
	}
	return "", ""
}

func (r *InstanceReconciler) readCertManagerPeerLeaf(ctx context.Context, instance *v1alpha1.OpenSearchServerless, channel *v1alpha1.TLSChannel, name, domain string) (*corev1.Secret, error) {
	issuer := channel.IssuerRef.Name
	if r.Certificates == nil || !r.PeerCertificateIssuers[issuer] {
		return nil, fmt.Errorf("cert-manager Issuer %q is not approved for peer TLS", issuer)
	}
	service, err := workload.PeerDNSService(instance)
	if err != nil {
		return nil, err
	}
	dns := fmt.Sprintf("%s.%s.svc.%s", service.Name, instance.Namespace, domain)
	if !validDNSHost(dns) || len(dns)+2 > 253 {
		return nil, fmt.Errorf("invalid cert-manager peer DNS identity")
	}
	return r.readCertManagerLeaf(ctx, instance, channel, name, []any{"*." + dns}, []any{"digital signature", "key encipherment", "server auth", "client auth"})
}

func (r *InstanceReconciler) readCertManagerLeaf(ctx context.Context, instance *v1alpha1.OpenSearchServerless, channel *v1alpha1.TLSChannel, name string, dns, usages []any) (*corev1.Secret, error) {
	issuer := channel.IssuerRef.Name
	request := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "cert-manager.io/v1",
		"kind":       "Certificate",
		"spec": map[string]any{
			"secretName":           name,
			"issuerRef":            map[string]any{"group": "cert-manager.io", "kind": "Issuer", "name": issuer},
			"dnsNames":             dns,
			"duration":             "2160h0m0s",
			"renewBefore":          "720h0m0s",
			"privateKey":           map[string]any{"rotationPolicy": "Always", "algorithm": "RSA", "size": int64(2048), "encoding": "PKCS8"},
			"usages":               usages,
			"revisionHistoryLimit": int64(3),
			"secretTemplate": map[string]any{"annotations": map[string]any{
				peerIssuanceOwner: string(instance.UID), peerIssuanceSlot: channel.CertificateRef.Name,
			}},
		},
	}}
	request.SetNamespace(instance.Namespace)
	request.SetName(name)
	controller := true
	request.SetOwnerReferences([]metav1.OwnerReference{{APIVersion: v1alpha1.GroupVersion.String(), Kind: "OpenSearchServerless", Name: instance.Name, UID: instance.UID, Controller: &controller}})
	certificates := r.Certificates.Resource(certManagerCertificates).Namespace(instance.Namespace)
	observed, err := certificates.Get(ctx, name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		reader := r.APIReader
		if reader == nil {
			reader = r.Client
		}
		var live v1alpha1.OpenSearchServerless
		if err := reader.Get(ctx, client.ObjectKeyFromObject(instance), &live); err != nil {
			return nil, err
		}
		if live.UID != instance.UID || live.ResourceVersion != instance.ResourceVersion || live.DeletionTimestamp != nil {
			return nil, fmt.Errorf("instance changed before requesting a peer certificate")
		}
		// An existing Secret without the owned Certificate could have a
		// different consumer. Do not ask cert-manager to overwrite it.
		if _, err := r.Core.Secrets(instance.Namespace).Get(ctx, name, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
			if err != nil {
				return nil, err
			}
			return nil, fmt.Errorf("peer issuance Secret already exists without its Certificate")
		}
		_, err = certificates.Create(ctx, request, metav1.CreateOptions{})
		if err == nil || apierrors.IsAlreadyExists(err) {
			observed, err = certificates.Get(ctx, name, metav1.GetOptions{})
		}
	}
	if err != nil {
		return nil, fmt.Errorf("resolve cert-manager peer Certificate: %w", err)
	}
	owner := metav1.GetControllerOf(observed)
	if observed.GetUID() == "" || observed.GetDeletionTimestamp() != nil || owner == nil || !reflect.DeepEqual(*owner, request.GetOwnerReferences()[0]) ||
		!reflect.DeepEqual(observed.Object["spec"], request.Object["spec"]) {
		return nil, fmt.Errorf("cert-manager peer Certificate has conflicting owner or issuance settings")
	}
	if !peerCertificateReady(observed) {
		return nil, fmt.Errorf("cert-manager peer Certificate %q is not Ready for its current generation", name)
	}
	secret, err := r.Core.Secrets(instance.Namespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return nil, fmt.Errorf("read cert-manager peer Secret: %w", err)
	}
	if secret.Annotations[peerIssuanceOwner] != string(instance.UID) || secret.Annotations[peerIssuanceSlot] != channel.CertificateRef.Name ||
		secret.Annotations["cert-manager.io/certificate-name"] != name {
		return nil, fmt.Errorf("cert-manager peer Secret lacks the expected issuance identity")
	}
	if owner := metav1.GetControllerOf(secret); owner != nil && (owner.UID != observed.GetUID() || owner.Name != name || owner.APIVersion != "cert-manager.io/v1" || owner.Kind != "Certificate") {
		return nil, fmt.Errorf("cert-manager peer Secret has a conflicting owner")
	}
	if err := r.verifyCertificateRequest(ctx, instance.Namespace, observed, issuer, secret.Data[corev1.TLSCertKey]); err != nil {
		return nil, err
	}
	return secret, nil
}

func (r *InstanceReconciler) verifyCertificateRequest(ctx context.Context, namespace string, certificate *unstructured.Unstructured, issuer string, leaf []byte) error {
	revision, _, _ := unstructured.NestedInt64(certificate.Object, "status", "revision")
	if revision < 1 {
		return fmt.Errorf("cert-manager peer Certificate has no issued revision")
	}
	// cert-manager 1.20 uses the Certificate name and revision for the request name.
	name := fmt.Sprintf("%s-%d", certificate.GetName(), revision)
	issued, err := r.Certificates.Resource(certManagerRequests).Namespace(namespace).Get(ctx, name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return fmt.Errorf("current peer CertificateRequest %q is missing; cert-manager requires StableCertificateRequestName", name)
	}
	if err != nil {
		return fmt.Errorf("read current peer CertificateRequest: %w", err)
	}
	owner := metav1.GetControllerOf(issued)
	ref, _, _ := unstructured.NestedMap(issued.Object, "spec", "issuerRef")
	if issued.GetUID() == "" || issued.GetDeletionTimestamp() != nil || owner == nil ||
		owner.UID != certificate.GetUID() || owner.Name != certificate.GetName() || owner.Kind != "Certificate" || owner.APIVersion != "cert-manager.io/v1" ||
		!reflect.DeepEqual(ref, map[string]any{"name": issuer, "kind": "Issuer", "group": "cert-manager.io"}) {
		return fmt.Errorf("current peer CertificateRequest has conflicting owner or Issuer")
	}
	conditions, _, _ := unstructured.NestedSlice(issued.Object, "status", "conditions")
	finished := map[string]bool{}
	for _, item := range conditions {
		if condition, ok := item.(map[string]any); ok && condition["status"] == "True" {
			if kind, ok := condition["type"].(string); ok {
				finished[kind] = true
			}
		}
	}
	encoded, _, _ := unstructured.NestedString(issued.Object, "status", "certificate")
	signed, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil || !finished["Approved"] || !finished["Ready"] || finished["Denied"] ||
		len(signed) == 0 || !bytes.Equal(signed, leaf) {
		return fmt.Errorf("peer TLS Secret does not match the approved current CertificateRequest")
	}
	return nil
}

func peerCertificateReady(certificate *unstructured.Unstructured) bool {
	conditions, _, _ := unstructured.NestedSlice(certificate.Object, "status", "conditions")
	for _, item := range conditions {
		condition, ok := item.(map[string]any)
		if !ok || condition["type"] != "Ready" {
			continue
		}
		generation, ok := condition["observedGeneration"].(int64)
		return ok && generation == certificate.GetGeneration() && condition["status"] == "True"
	}
	return false
}
