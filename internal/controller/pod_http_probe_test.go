package controller

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"math/big"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"sync/atomic"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/huyz0/opensearch-serverless-operator/api/v1alpha1"
	"github.com/huyz0/opensearch-serverless-operator/internal/java"
)

func TestManagedHTTPSWarmupRejectsTimeoutAndIdentityDrift(t *testing.T) {
	const incarnation = "9aa977f3-4985-4cb5-9692-5ad1e3e243fe"
	instance := &v1alpha1.OpenSearchServerless{ObjectMeta: metav1.ObjectMeta{Name: "catalog", Namespace: "n", UID: "instance"}}
	instance.Spec.Image = "approved"
	instance.Spec.TLS = &v1alpha1.TLSConfig{HTTP: &v1alpha1.TLSChannel{Source: "Secret", CertificateRef: v1alpha1.LocalReference{Name: "leaf"}, TrustBundleRef: v1alpha1.SecretKeyReference{Name: "trust", Key: "ca.crt"}}}
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "search-a", Namespace: "n", Annotations: map[string]string{peerDomainAnnotation: "cluster.local"}}}
	identity, err := httpPodIdentity(instance, pod.Name, "cluster.local")
	if err != nil {
		t.Fatal(err)
	}
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	cert := &x509.Certificate{SerialNumber: big.NewInt(1), DNSNames: []string{identity}, NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
		IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}}
	der, err := x509.CreateCertificate(rand.Reader, cert, cert, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	bundle := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	immutable := true
	trust := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "trust", Namespace: "n", UID: "ca-original"}, Immutable: &immutable, Data: map[string][]byte{"ca.crt": bundle}}
	core := fake.NewSimpleClientset(trust)
	instance.Status.HTTPTLSBaseline = &v1alpha1.HTTPTLSBaseline{Domain: "cluster.local", CertificateName: "leaf", TrustName: "trust", TrustKey: "ca.crt", TrustUID: string(trust.UID), TrustHash: fmt.Sprintf("%x", sha256.Sum256(bundle))}
	r := &InstanceReconciler{Core: core.CoreV1(), DevelopmentHTTPTLSImage: "approved", DevelopmentPeerDNSImage: "approved", DevelopmentClusterDomain: "cluster.local"}
	type observation struct {
		result, statsProcess string
		timedOut             bool
	}
	var state atomic.Value
	state.Store(observation{"running", incarnation, true})
	var requests atomic.Int32
	var authRequired atomic.Bool
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		requests.Add(1)
		if req.TLS == nil || req.TLS.ServerName != identity {
			t.Error("probe did not verify the Pod DNS identity over TLS")
		}
		if authRequired.Load() {
			user, password, ok := req.BasicAuth()
			if !ok || user != "admin" || password != "fixture-probe-password" {
				w.WriteHeader(http.StatusUnauthorized)
				return
			}
		}
		sample := state.Load().(observation)
		if req.URL.Path == "/_serverless/stats" {
			fmt.Fprintf(w, `{"node":"id","name":"search-a","roles":["search"],"process_incarnation":%q,"lease":{"valid_now":true,"expires_in_millis":30000,"ttl_millis":30000}}`, sample.statsProcess)
			return
		}
		fmt.Fprintf(w, `{"ready":true,"state":"ready","serves_searches":true,"process_incarnation":%q,"warmup_result":%q,"warmup_timed_out":%t}`, incarnation, sample.result, sample.timedOut)
	}))
	server.TLS = &tls.Config{Certificates: []tls.Certificate{{Certificate: [][]byte{der}, PrivateKey: key}}}
	server.StartTLS()
	defer server.Close()
	addr := netip.MustParseAddrPort(server.Listener.Addr().String())
	pod.Status.PodIP = addr.Addr().String()
	ctx := context.Background()
	probe := r.fleetQualification().developmentSearchProbe(instance, int(addr.Port()), time.Second)
	warmup, err := r.observePodWarmup(ctx, instance, pod, int(addr.Port()))
	if err != nil || !warmup.Ready || !warmup.TimedOut || warmup.Finished() {
		t.Fatalf("timed-out worker must still occupy the slot: %#v %v", warmup, err)
	}
	state.Store(observation{"completed", incarnation, true})
	if _, err := probe(ctx, pod); err == nil {
		t.Fatal("strict qualification accepted completion after timeout")
	}
	state.Store(observation{"completed", incarnation, false})
	if result, err := probe(ctx, pod); err != nil || !result.StatsProcessVerified || result.ProcessIncarnation != incarnation {
		t.Fatalf("completed worker did not qualify over HTTPS: %v %v", result, err)
	}
	state.Store(observation{"completed", "51ca9db5-fb85-4d34-9dad-15d058dc2c35", false})
	if _, err := r.observePodWarmup(ctx, instance, pod, int(addr.Port())); err == nil {
		t.Fatal("warm-up sample accepted a different JVM in stats")
	}
	state.Store(observation{"completed", "", false})
	if _, err := probe(ctx, pod); err == nil {
		t.Fatal("strict HTTPS qualification accepted missing stats identity")
	}
	before := requests.Load()
	pod.Name = "search-without-a-san"
	if _, err := r.observePodWarmup(ctx, instance, pod, int(addr.Port())); err == nil || requests.Load() != before {
		t.Fatal("wrong SAN reached the application")
	}
	pod.Name = "search-a"
	t.Run("Basic Auth on the owned Pod", func(t *testing.T) {
		secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "startup", Namespace: "n", UID: "credential-original"}, Immutable: &immutable,
			Data: map[string][]byte{"opensearch.keystore": {1}, "password": []byte("fixture-probe-password")}}
		if _, err := core.CoreV1().Secrets("n").Create(ctx, secret, metav1.CreateOptions{}); err != nil {
			t.Fatal(err)
		}
		secureInstance := instance.DeepCopyObject().(*v1alpha1.OpenSearchServerless)
		secureInstance.Spec.Security = &v1alpha1.SecurityConfig{Provider: serverlessBasicAuth, AuthenticationSecretRef: &v1alpha1.SecretKeyReference{Name: secret.Name, Key: "password"}}
		secureInstance.Spec.Storage.Credentials = &v1alpha1.StorageCredentials{Type: "Keystore", KeystoreSecretRef: &v1alpha1.LocalReference{Name: secret.Name}}
		secureInstance.Status.BasicAuthBaseline = &v1alpha1.BasicAuthBaseline{SecretName: secret.Name, SecretUID: string(secret.UID), Key: "password", BundleSHA256: java.StartupBundleSHA256(secret.Data["opensearch.keystore"], secret.Data["password"])}
		securePod := pod.DeepCopy()
		securePod.UID = "pod"
		controller := true
		securePod.OwnerReferences = []metav1.OwnerReference{{UID: secureInstance.UID, Controller: &controller}}
		securePod.Annotations[keystoreUIDAnnotation] = string(secret.UID)
		r.DevelopmentBasicAuthImage = secureInstance.Spec.Image
		state.Store(observation{"completed", incarnation, false})
		secureProbe := r.fleetQualification().developmentSearchProbe(secureInstance, int(addr.Port()), time.Second)
		if _, err := secureProbe(ctx, securePod); err == nil {
			t.Fatal("accepted a JVM that does not reject anonymous health requests")
		}
		authRequired.Store(true)
		defer authRequired.Store(false)
		if qualified, err := secureProbe(ctx, securePod); err != nil || !qualified.StatsProcessVerified {
			t.Fatalf("authenticated probe did not qualify the JVM: %v %v", qualified, err)
		}
		securePod.Annotations[keystoreUIDAnnotation] = "unrecognized"
		before := requests.Load()
		if _, err := secureProbe(ctx, securePod); err == nil || requests.Load() != before {
			t.Fatal("probe reached a Pod with an unexpected keystore identity")
		}
	})
	before = requests.Load()
	replacement := trust.DeepCopy()
	replacement.UID = "ca-recreated"
	if _, err := core.CoreV1().Secrets("n").Update(ctx, replacement, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := probe(ctx, pod); err == nil || requests.Load() != before {
		t.Fatal("recreated trust Secret allowed an application probe")
	}
}
