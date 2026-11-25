package rollout

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"math/big"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	"github.com/huyz0/opensearch-serverless-operator/api/v1alpha1"
	"github.com/huyz0/opensearch-serverless-operator/internal/workload"
)

func TestWriterLeaseProbeUsesManagedPodTLS(t *testing.T) {
	const domain = "cluster.local"
	instance := &v1alpha1.OpenSearchServerless{
		ObjectMeta: metav1.ObjectMeta{Name: "catalog", Namespace: "default", UID: "instance-uid"},
	}
	service, err := workload.PeerDNSService(instance)
	if err != nil {
		t.Fatal(err)
	}
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "writer", Namespace: instance.Namespace, UID: "pod-uid",
			Labels: map[string]string{workload.FleetLabel: "green"}, OwnerReferences: service.OwnerReferences},
		Status: corev1.PodStatus{PodIP: "10.0.0.1"},
	}
	host, err := workload.PeerHostname(pod.Name)
	if err != nil {
		t.Fatal(err)
	}
	dns := service.Name + "." + instance.Namespace + ".svc." + domain
	pub, key, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	ca := &x509.Certificate{SerialNumber: big.NewInt(1), NotBefore: now.Add(-time.Minute), NotAfter: now.Add(time.Hour),
		IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign}
	caDER, err := x509.CreateCertificate(rand.Reader, ca, ca, pub, key)
	if err != nil {
		t.Fatal(err)
	}
	ca, err = x509.ParseCertificate(caDER)
	if err != nil {
		t.Fatal(err)
	}
	leaf := &x509.Certificate{SerialNumber: big.NewInt(2), DNSNames: []string{"*." + dns},
		NotBefore: now.Add(-time.Minute), NotAfter: now.Add(time.Hour), ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}}
	leafDER, err := x509.CreateCertificate(rand.Reader, leaf, ca, pub, key)
	if err != nil {
		t.Fatal(err)
	}

	var requests atomic.Int64
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		requests.Add(1)
		username, password, authorized := req.BasicAuth()
		if !authorized || username != "java-reader" || password != "private" || req.TLS.ServerName != host+"."+dns || req.URL.Path != "/_serverless/stats" {
			t.Error("direct Java probe did not retain its Pod TLS or authentication scope")
		}
		fmt.Fprint(w, `{"node":"node","node_ephemeral_id":"ephemeral","name":"writer","process_incarnation":"incarnation","roles":["ingest"],"lease":{"valid_now":true,"expires_in_millis":20000,"ttl_millis":30000}}`)
	}))
	server.TLS = &tls.Config{Certificates: []tls.Certificate{{Certificate: [][]byte{leafDER, caDER}, PrivateKey: key}}}
	server.StartTLS()
	defer server.Close()
	roots := x509.NewCertPool()
	roots.AddCert(ca)
	probe, err := NewWriterLeaseProbe(roots, domain, map[types.UID]string{pod.UID: strings.TrimPrefix(server.URL, "https://")})
	if err != nil {
		t.Fatal(err)
	}
	fingerprint := fmt.Sprintf("%x", sha256.Sum256(leafDER))
	instance.Status.HTTPTLSBaseline = &v1alpha1.HTTPTLSBaseline{Domain: domain, LeafSHA256: strings.Repeat("0", 64)}
	instance.Status.HTTPFleetCertificates = map[string]v1alpha1.HTTPFleetCertificate{"green": {LeafSHA256: fingerprint}}
	credentials := &RouteCredential{Username: "java-reader", Password: []byte("private")}
	lease, err := probe(context.Background(), instance, pod, credentials)
	if err != nil || lease.NodeEphemeralID != "ephemeral" || requests.Load() != 1 {
		t.Fatalf("managed wildcard probe failed: lease %+v, %v", lease, err)
	}
	instance.Status.HTTPFleetCertificates["green"] = v1alpha1.HTTPFleetCertificate{LeafSHA256: strings.Repeat("1", 64)}
	if _, err := probe(context.Background(), instance, pod, credentials); err == nil || requests.Load() != 1 {
		t.Fatal("accepted a trusted CA certificate with a different pinned fleet leaf")
	}
	mismatchedDomain := "other.local"
	instance.Status.HTTPTLSBaseline.Domain = mismatchedDomain
	instance.Status.HTTPFleetCertificates["green"] = v1alpha1.HTTPFleetCertificate{LeafSHA256: fingerprint}
	probe, err = NewWriterLeaseProbe(roots, mismatchedDomain, map[types.UID]string{pod.UID: strings.TrimPrefix(server.URL, "https://")})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := probe(context.Background(), instance, pod, credentials); err == nil || requests.Load() != 1 {
		t.Fatal("accepted a pinned certificate that does not match the Pod DNS identity")
	}
}
