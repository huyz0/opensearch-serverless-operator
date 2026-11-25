package controller

import (
	"context"
	"encoding/json"
	"encoding/pem"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/huyz0/opensearch-serverless-operator/api/v1alpha1"
	"github.com/huyz0/opensearch-serverless-operator/internal/plugins"
)

func TestResolvePluginInputsPinsPolicyAndCredentialIdentity(t *testing.T) {
	artifact := "registry.example/plugins/example@sha256:" + strings.Repeat("a", 64)
	image := "registry.example/runtime@sha256:" + strings.Repeat("b", 64)
	installer := "registry.example/installer@sha256:" + strings.Repeat("c", 64)
	data, err := json.Marshal(plugins.Policy{ApprovedArtifacts: []string{artifact}, OpenSearchVersion: "3.0", JavaVersion: "21", MaxBytes: 1024, MaxFiles: 10})
	if err != nil {
		t.Fatal(err)
	}
	immutable := true
	policy := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "policy", Namespace: "n", UID: "policy-uid", ResourceVersion: "1"}, Immutable: &immutable, Data: map[string]string{"policy.json": string(data)}}
	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "pull", Namespace: "n", UID: "secret-uid"}, Immutable: &immutable, Data: map[string][]byte{"credentials.json": []byte(`{}`)}}
	instance := &v1alpha1.OpenSearchServerless{ObjectMeta: metav1.ObjectMeta{Name: "instance", Namespace: "n", UID: "owner"}, Spec: v1alpha1.OpenSearchServerlessSpec{Image: image, Plugins: []v1alpha1.Plugin{{Name: "example", Artifact: artifact, RegistryCredentialsRef: &v1alpha1.LocalReference{Name: "pull"}}}}}
	client := fake.NewSimpleClientset(policy, secret)
	r := &InstanceReconciler{Core: client.CoreV1()}
	resolved, err := r.resolvePluginInputs(context.Background(), instance, "search", installer, image, "policy")
	if err != nil || resolved == nil || resolved.PolicyUID != "policy-uid" || resolved.CredentialsUID != "secret-uid" || resolved.Resolution.RequestSHA256 == "" || resolved.Resolution.PolicySHA256 == "" {
		t.Fatalf("valid plugin inputs rejected: %#v %v", resolved, err)
	}
	intent := pluginInstallIntent(instance, resolved)
	if err := checkPluginIntent(instance, "pod", intent, resolved); err != nil {
		t.Fatal(err)
	}
	legacy := *intent
	legacy.RequestName = pluginSnapshotName(string(instance.UID), "pod", "req", intent.RequestSHA256)
	legacy.PolicyName = pluginSnapshotName(string(instance.UID), "pod", "pol", intent.PolicySHA256)
	if err := checkPluginIntent(instance, "pod", &legacy, resolved); err != nil {
		t.Fatalf("legacy Pod intent was rejected: %v", err)
	}
	if err := checkPluginIntent(instance, "different-pod", &legacy, resolved); err == nil {
		t.Fatal("legacy names accepted for another Pod")
	}
	legacy.PolicyName = intent.PolicyName
	if err := checkPluginIntent(instance, "pod", &legacy, resolved); err == nil {
		t.Fatal("mixed legacy and shared snapshot names accepted")
	}
	changed := *resolved
	changed.CredentialsUID = "recreated-secret"
	if err := checkPluginIntent(instance, "pod", intent, &changed); err == nil {
		t.Fatal("changed credential UID accepted")
	}
	changed = *resolved
	changed.CredentialsHasCA = true
	if err := checkPluginIntent(instance, "pod", intent, &changed); err == nil {
		t.Fatal("changed registry CA selection accepted")
	}
	secret.Data["ca.crt"] = []byte("invalid")
	if _, err := client.CoreV1().Secrets("n").Update(context.Background(), secret, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := r.resolvePluginInputs(context.Background(), instance, "search", installer, image, "policy"); err == nil {
		t.Fatal("invalid registry CA accepted")
	}
	server := httptest.NewTLSServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	defer server.Close()
	secret.Data["ca.crt"] = pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw})
	if _, err := client.CoreV1().Secrets("n").Update(context.Background(), secret, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	withCA, err := r.resolvePluginInputs(context.Background(), instance, "search", installer, image, "policy")
	if err != nil || !withCA.CredentialsHasCA {
		t.Fatalf("valid registry CA not pinned: %v", err)
	}
	changed = *resolved
	changed.PolicyUID = "recreated-policy"
	if err := checkPluginIntent(instance, "pod", intent, &changed); err == nil {
		t.Fatal("changed policy UID accepted")
	}
	if _, err := r.resolvePluginInputs(context.Background(), instance, "search", installer, "other@sha256:"+strings.Repeat("d", 64), "policy"); err == nil {
		t.Fatal("unqualified runtime accepted")
	}
	policy.Immutable = nil
	if _, err := client.CoreV1().ConfigMaps("n").Update(context.Background(), policy, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := r.resolvePluginInputs(context.Background(), instance, "search", installer, image, "policy"); err == nil {
		t.Fatal("mutable policy accepted")
	}
	policy.Immutable = &immutable
	if _, err := client.CoreV1().ConfigMaps("n").Update(context.Background(), policy, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	secret.Immutable = nil
	if _, err := client.CoreV1().Secrets("n").Update(context.Background(), secret, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := r.resolvePluginInputs(context.Background(), instance, "search", installer, image, "policy"); err == nil {
		t.Fatal("mutable credentials accepted")
	}
}
