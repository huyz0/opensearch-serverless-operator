package controller

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/huyz0/opensearch-serverless-operator/api/v1alpha1"
)

func TestResolveKeystoreRequiresImmutableNonemptySecret(t *testing.T) {
	immutable := true
	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: "n", Name: "keystore", UID: "secret-uid"}, Immutable: &immutable, Data: map[string][]byte{"opensearch.keystore": []byte("test")}}
	instance := &v1alpha1.OpenSearchServerless{ObjectMeta: metav1.ObjectMeta{Namespace: "n"}, Spec: v1alpha1.OpenSearchServerlessSpec{Storage: v1alpha1.Storage{Credentials: &v1alpha1.StorageCredentials{Type: "Keystore", KeystoreSecretRef: &v1alpha1.LocalReference{Name: "keystore"}}}}}
	r := &InstanceReconciler{Core: fake.NewSimpleClientset(secret).CoreV1()}
	uid, err := r.resolveKeystore(context.Background(), instance)
	if err != nil || uid != "secret-uid" {
		t.Fatalf("expected immutable keystore identity, got %q: %v", uid, err)
	}
	mutable := secret.DeepCopy()
	mutable.Immutable = nil
	r.Core = fake.NewSimpleClientset(mutable).CoreV1()
	if _, err := r.resolveKeystore(context.Background(), instance); err == nil {
		t.Fatal("mutable keystore must be rejected")
	}
	empty := secret.DeepCopy()
	empty.Data = nil
	r.Core = fake.NewSimpleClientset(empty).CoreV1()
	if _, err := r.resolveKeystore(context.Background(), instance); err == nil {
		t.Fatal("missing keystore must be rejected")
	}
}

func TestResolveKeystoreRejectsUnqualifiedWorkloadIdentity(t *testing.T) {
	instance := &v1alpha1.OpenSearchServerless{Spec: v1alpha1.OpenSearchServerlessSpec{Image: "qualified", Storage: v1alpha1.Storage{Credentials: &v1alpha1.StorageCredentials{Type: "WorkloadIdentity", AWSRoleARN: "arn:aws:iam::123456789012:role/test"}}}}
	r := &InstanceReconciler{}
	if _, err := r.resolveKeystore(context.Background(), instance); err == nil {
		t.Fatal("unqualified workload identity must not start a development Pod")
	}
	r.DevelopmentS3WebIdentityImage = "other"
	if _, err := r.resolveKeystore(context.Background(), instance); err == nil {
		t.Fatal("different image must not qualify")
	}
	r.DevelopmentS3WebIdentityImage = "qualified"
	if _, err := r.resolveKeystore(context.Background(), instance); err != nil {
		t.Fatalf("matching development image should qualify: %v", err)
	}
}

func TestRecoverCreationRejectsRecreatedKeystore(t *testing.T) {
	instance := &v1alpha1.OpenSearchServerless{ObjectMeta: metav1.ObjectMeta{Name: "catalog", UID: "instance", Generation: 1}, Status: v1alpha1.OpenSearchServerlessStatus{PodCreationSequence: 1, ServingFleetIntentByRole: map[string]string{"search": "initial"}}}
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: podNameForSequence(instance, 1), Annotations: map[string]string{creationIntentKey: "instance/1", keystoreUIDAnnotation: "old-secret"}, Labels: map[string]string{"serverless.opensearch.org/role": "search", "serverless.opensearch.org/zone": "a", "serverless.opensearch.org/fleet": "initial", "serverless.opensearch.org/revision": "revision"}}}
	if _, err := recoverCreationIntent(instance, pod, "revision", "old-secret", nil); err != nil {
		t.Fatal(err)
	}
	if _, err := recoverCreationIntent(instance, pod, "revision", "new-secret", nil); err == nil {
		t.Fatal("recreated Secret must not satisfy the previous Pod intent")
	}
}
