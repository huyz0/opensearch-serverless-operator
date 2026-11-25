package controller

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/x509"
	"fmt"
	"net"
	"net/http"
	"strconv"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	typedcore "k8s.io/client-go/kubernetes/typed/core/v1"

	"github.com/huyz0/opensearch-serverless-operator/api/v1alpha1"
	"github.com/huyz0/opensearch-serverless-operator/internal/java"
	"github.com/huyz0/opensearch-serverless-operator/internal/runtimeconfig"
	"github.com/huyz0/opensearch-serverless-operator/internal/workload"
)

// podHTTPAccess owns direct Pod authentication and trust reads.
type podHTTPAccess struct {
	core                                                    typedcore.CoreV1Interface
	basicAuthImage, httpsImage, peerDNSImage, clusterDomain string
}

func (r *InstanceReconciler) podHTTP() podHTTPAccess {
	return podHTTPAccess{core: r.Core, basicAuthImage: r.DevelopmentBasicAuthImage, httpsImage: r.DevelopmentHTTPTLSImage,
		peerDNSImage: r.DevelopmentPeerDNSImage, clusterDomain: r.DevelopmentClusterDomain}
}

func hasManagedHTTPS(instance *v1alpha1.OpenSearchServerless) bool {
	return instance.Status.HTTPTLSBaseline != nil || (instance.Spec.TLS != nil && instance.Spec.TLS.HTTP != nil)
}

func (r podHTTPAccess) supportsDevelopmentPodHTTP(instance *v1alpha1.OpenSearchServerless) bool {
	if instance == nil {
		return false
	}
	if instance.Spec.Security != nil || instance.Status.BasicAuthBaseline != nil {
		if instance.Spec.Security == nil || instance.Spec.Security.Provider != serverlessBasicAuth || instance.Status.BasicAuthBaseline == nil || r.basicAuthImage != instance.Spec.Image || !hasManagedHTTPS(instance) {
			return false
		}
	}
	return !hasManagedHTTPS(instance) || (instance.Status.HTTPTLSBaseline != nil && instance.Spec.TLS != nil && instance.Spec.TLS.HTTP != nil && r.httpsImage != "" && r.httpsImage == instance.Spec.Image)
}

// developmentPodHTTP builds a direct Pod client. A configured or previously
// pinned HTTPS channel never takes the plaintext path, even on input drift.
func (r podHTTPAccess) developmentPodHTTP(ctx context.Context, instance *v1alpha1.OpenSearchServerless, pod *corev1.Pod, port int) (*http.Client, string, error) {
	return r.developmentPodHTTPWithAuth(ctx, instance, pod, port, nil)
}

type podHealthCredential struct {
	username string
	password []byte
}

func (r podHTTPAccess) developmentPodHTTPWithAuth(ctx context.Context, instance *v1alpha1.OpenSearchServerless, pod *corev1.Pod, port int, credential *podHealthCredential) (*http.Client, string, error) {
	client, endpoint, err := r.developmentPodHTTPTransport(ctx, instance, pod, port)
	if err != nil {
		return nil, "", err
	}
	if instance.Spec.Security != nil || instance.Status.BasicAuthBaseline != nil {
		owner := metav1.GetControllerOf(pod)
		if !hasManagedHTTPS(instance) || instance.Status.BasicAuthBaseline == nil || pod.UID == "" || pod.Namespace != instance.Namespace ||
			owner == nil || owner.UID != instance.UID || pod.Annotations[keystoreUIDAnnotation] != instance.Status.BasicAuthBaseline.SecretUID {
			return nil, "", fmt.Errorf("authenticated health requires the owned Pod and its pinned startup credentials")
		}
		_, password, err := r.resolveBasicAuth(ctx, instance)
		if err != nil {
			return nil, "", err
		}
		if credential != nil && (credential.username == runtimeconfig.BasicAuthUser || bytes.Equal(credential.password, password)) {
			return nil, "", fmt.Errorf("rollout health requires a credential distinct from the bootstrap account")
		}
		if err := java.RequireHealthAuthentication(ctx, client, endpoint); err != nil {
			client.CloseIdleConnections()
			return nil, "", err
		}
		if credential == nil {
			credential = &podHealthCredential{username: runtimeconfig.BasicAuthUser, password: password}
		}
		host := net.JoinHostPort(pod.Status.PodIP, strconv.Itoa(port))
		if err := java.ConfigureHealthBasicAuth(client, host, credential.username, credential.password); err != nil {
			client.CloseIdleConnections()
			return nil, "", err
		}
	} else if credential != nil {
		return nil, "", fmt.Errorf("Pod credential requires managed Basic Auth")
	}
	return client, endpoint, nil
}

// developmentPodHTTPTransport leaves authentication to the request adapter.
func (r podHTTPAccess) developmentPodHTTPTransport(ctx context.Context, instance *v1alpha1.OpenSearchServerless, pod *corev1.Pod, port int) (*http.Client, string, error) {
	if instance == nil || pod == nil || pod.Name == "" || net.ParseIP(pod.Status.PodIP) == nil || port < 1 || port > 65535 {
		return nil, "", fmt.Errorf("approved instance and a directly addressable Pod are required")
	}
	transport := &http.Transport{}
	scheme := "http"
	if hasManagedHTTPS(instance) {
		if !r.supportsDevelopmentPodHTTP(instance) {
			return nil, "", fmt.Errorf("managed Pod HTTPS requires a pinned configuration and approved runtime")
		}
		domain, err := r.peerDNSDomain(instance)
		if err != nil || !httpTLSBaseMatches(instance, domain) || pod.Annotations[peerDomainAnnotation] != domain {
			return nil, "", fmt.Errorf("managed Pod HTTPS identity or trust reference differs from the pinned configuration")
		}
		identity, err := httpPodIdentity(instance, pod.Name, domain)
		if err != nil {
			return nil, "", err
		}
		fleet := pod.Labels[workload.FleetLabel]
		roots, err := r.developmentHTTPTrustPool(ctx, instance, domain, fleet)
		if err != nil {
			return nil, "", err
		}
		transport.TLSClientConfig, err = managedPodTLSConfig(instance, fleet, identity, roots)
		if err != nil {
			return nil, "", err
		}
		scheme = "https"
	}
	client := &http.Client{Timeout: 2 * time.Second, Transport: transport, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	host := net.JoinHostPort(pod.Status.PodIP, strconv.Itoa(port))
	return client, scheme + "://" + host, nil
}

func (r podHTTPAccess) resolveBasicAuth(ctx context.Context, instance *v1alpha1.OpenSearchServerless) (*v1alpha1.BasicAuthBaseline, []byte, error) {
	ref, err := basicAuthReference(instance)
	if err != nil || ref == nil {
		return nil, nil, err
	}
	if r.core == nil || r.basicAuthImage == "" || r.basicAuthImage != instance.Spec.Image ||
		r.httpsImage != instance.Spec.Image || instance.Status.HTTPTLSBaseline == nil {
		return nil, nil, fmt.Errorf("development Basic Auth requires the approved HTTPS image and pinned TLS inputs")
	}
	if instance.Status.BasicAuthBaseline == nil && instance.Status.PodCreationSequence != 0 {
		return nil, nil, fmt.Errorf("Basic Auth must be configured before the first Pod creation")
	}
	secret, err := r.core.Secrets(instance.Namespace).Get(ctx, ref.Name, metav1.GetOptions{})
	if err != nil {
		return nil, nil, fmt.Errorf("read startup authentication Secret: %w", err)
	}
	password := secret.Data[ref.Key]
	if secret.UID == "" || secret.DeletionTimestamp != nil || secret.Immutable == nil || !*secret.Immutable ||
		len(secret.Data["opensearch.keystore"]) == 0 || len(password) == 0 || len(password) > java.MaxHealthPasswordBytes {
		return nil, nil, fmt.Errorf("startup authentication Secret must be live and immutable with a keystore and a valid probe password file")
	}
	baseline := &v1alpha1.BasicAuthBaseline{SecretName: ref.Name, SecretUID: string(secret.UID), Key: ref.Key, BundleSHA256: java.StartupBundleSHA256(secret.Data["opensearch.keystore"], password)}
	if previous := instance.Status.BasicAuthBaseline; previous != nil && *previous != *baseline {
		return nil, nil, fmt.Errorf("startup authentication Secret identity differs from the pinned baseline")
	}
	return baseline, password, nil
}

func (r podHTTPAccess) peerDNSDomain(instance *v1alpha1.OpenSearchServerless) (string, error) {
	if r.peerDNSImage == "" && r.clusterDomain == "" {
		return "", nil
	}
	if r.peerDNSImage == "" || r.clusterDomain == "" || instance.Spec.Image != r.peerDNSImage || !validDNSHost(r.clusterDomain) {
		return "", fmt.Errorf("peer DNS requires an explicitly qualified matching image and valid cluster domain")
	}
	return r.clusterDomain, nil
}

func (r podHTTPAccess) readHTTPTrustForFleet(ctx context.Context, instance *v1alpha1.OpenSearchServerless, domain, fleet string) (*corev1.Secret, []byte, error) {
	if instance.Spec.TLS == nil || instance.Spec.TLS.HTTP == nil {
		return nil, nil, fmt.Errorf("managed HTTP TLS channel is required")
	}
	channel := instance.Spec.TLS.HTTP
	if r.core == nil || (channel.Source != "Secret" && channel.Source != "CertManager") || domain == "" || r.httpsImage == "" || r.httpsImage != instance.Spec.Image {
		return nil, nil, fmt.Errorf("development HTTP TLS requires immutable Secret inputs, an approved image and Pod DNS")
	}
	base := instance.Status.HTTPTLSBaseline
	if base == nil && (instance.Status.PodCreationSequence != 0 || len(instance.Status.HTTPFleetCertificates) != 0) {
		return nil, nil, fmt.Errorf("HTTP TLS must be configured before the first Pod creation")
	}
	if base != nil && !httpTLSBaseMatches(instance, domain) {
		return nil, nil, fmt.Errorf("HTTP TLS configuration differs from the pinned baseline")
	}
	pins := httpTLSBaselineForFleet(instance, fleet)
	if base != nil && pins == nil {
		return nil, nil, fmt.Errorf("HTTP TLS fleet trust pin is missing or inconsistent")
	}
	ref := channel.TrustBundleRef
	if pins != nil {
		ref = v1alpha1.SecretKeyReference{Name: pins.TrustName, Key: pins.TrustKey}
	}
	trust, ca, err := r.readHTTPTrustSecret(ctx, instance, ref)
	if err == nil && pins == nil && trust.ResourceVersion == "" {
		return nil, nil, fmt.Errorf("HTTP TLS trust Secret has no observed resource version")
	}
	if err == nil && pins != nil && (string(trust.UID) != pins.TrustUID || fmt.Sprintf("%x", sha256.Sum256(ca)) != pins.TrustHash) {
		return nil, nil, fmt.Errorf("HTTP TLS trust differs from the pinned fleet bundle")
	}
	return trust, ca, err
}

func (r podHTTPAccess) readHTTPTrustSecret(ctx context.Context, instance *v1alpha1.OpenSearchServerless, ref v1alpha1.SecretKeyReference) (*corev1.Secret, []byte, error) {
	trust, err := r.core.Secrets(instance.Namespace).Get(ctx, ref.Name, metav1.GetOptions{})
	if err != nil {
		return nil, nil, fmt.Errorf("read HTTP TLS trust Secret: %w", err)
	}
	ca := trust.Data[ref.Key]
	if trust.UID == "" || trust.DeletionTimestamp != nil || trust.Immutable == nil || !*trust.Immutable || len(ca) == 0 {
		return nil, nil, fmt.Errorf("HTTP TLS trust must be a live immutable Secret with a nonempty bundle")
	}
	return trust, ca, nil
}

func (r podHTTPAccess) developmentHTTPTrustPool(ctx context.Context, instance *v1alpha1.OpenSearchServerless, domain, fleet string) (*x509.CertPool, error) {
	_, ca, err := r.readHTTPTrustForFleet(ctx, instance, domain, fleet)
	if err != nil {
		return nil, err
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(ca) {
		return nil, fmt.Errorf("pinned HTTP trust bundle is invalid")
	}
	return roots, nil
}
