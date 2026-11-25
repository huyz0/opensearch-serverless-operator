package controller

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
	ctrl "sigs.k8s.io/controller-runtime"

	"github.com/huyz0/opensearch-serverless-operator/api/v1alpha1"
	"github.com/huyz0/opensearch-serverless-operator/internal/rollout"
)

func TestRolloutCredentialIsBoundToSecretIdentity(t *testing.T) {
	ctx := context.Background()
	instance := &v1alpha1.OpenSearchServerless{ObjectMeta: metav1.ObjectMeta{Namespace: "test"}, Spec: v1alpha1.OpenSearchServerlessSpec{Security: &v1alpha1.SecurityConfig{Provider: serverlessBasicAuth}}}
	instance.Status.BasicAuthBaseline = &v1alpha1.BasicAuthBaseline{SecretName: "bootstrap"}
	immutable := true
	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "ordinary", Namespace: instance.Namespace, UID: "original", ResourceVersion: "1"},
		Type: rollout.RouteCredentialSecretType, Immutable: &immutable, Data: map[string][]byte{"username": []byte("rollout-check"), "password": []byte("fixture-secret")}}
	core := fake.NewSimpleClientset(secret).CoreV1()
	credential, err := rollout.ReadRouteCredential(ctx, core, instance, secret.Name)
	if err != nil || !credential.Unchanged(ctx, core, instance.Namespace) {
		t.Fatalf("valid route credential rejected: %v", err)
	}
	secrets := core.Secrets(instance.Namespace)
	if err := secrets.Delete(ctx, secret.Name, metav1.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	secret.UID = "replacement"
	if _, err := secrets.Create(ctx, secret, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	if credential.Unchanged(ctx, core, instance.Namespace) {
		t.Fatal("same-name Secret replacement left earlier rollout evidence valid")
	}
	secret.Data["username"] = []byte("admin")
	if _, err := secrets.Update(ctx, secret, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := rollout.ReadRouteCredential(ctx, core, instance, secret.Name); err == nil {
		t.Fatal("bootstrap account was allowed as an edge credential")
	}
}

func TestWeightedRolloutRejectsSecureInstanceBeforeClaim(t *testing.T) {
	instance := &v1alpha1.OpenSearchServerless{Spec: v1alpha1.OpenSearchServerlessSpec{
		Rollout: &v1alpha1.Rollout{GatewayBackends: true}, TLS: &v1alpha1.TLSConfig{HTTP: &v1alpha1.TLSChannel{}},
	}}
	op := &v1alpha1.OpenSearchServerlessOperation{Spec: v1alpha1.OperationSpec{Action: "Promote", RoutingMode: "WeightedGateway"}}
	r := &OperationReconciler{weightedEvidence: func(context.Context, *v1alpha1.OpenSearchServerless, *v1alpha1.OpenSearchServerlessOperation, rollout.WeightIntent, observedWeightRoute) (weightedRolloutEvidence, error) {
		t.Fatal("unqualified weighted rollout requested execution evidence")
		return weightedRolloutEvidence{}, nil
	}}
	var reason string
	hold := func(observed, _ string) (ctrl.Result, error) { reason = observed; return ctrl.Result{}, nil }
	if _, err := r.reconcileWeightedOperation(context.Background(), nil, instance, op, false, ctrl.Result{}, hold); err != nil || reason != "WeightedRuntimeUnqualified" {
		t.Fatalf("secure weighted rollout held with %s, %v", reason, err)
	}
	if instance.Status.ActiveOperation != nil || op.Status.WeightedRollout != nil || len(op.Finalizers) != 0 {
		t.Fatal("unsupported weighted rollout prepared a durable effect")
	}
}
