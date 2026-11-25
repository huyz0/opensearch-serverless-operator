package controller

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/huyz0/opensearch-serverless-operator/api/v1alpha1"
	"github.com/huyz0/opensearch-serverless-operator/internal/workload"
)

func TestRoleServiceRejectsUnsafeReadinessAndLocalTraffic(t *testing.T) {
	instance := &v1alpha1.OpenSearchServerless{ObjectMeta: metav1.ObjectMeta{Name: "catalog", Namespace: "n", UID: "uid"}}
	desired, err := workload.RoleService(instance, "search", "blue", 9200)
	if err != nil {
		t.Fatal(err)
	}
	observed := desired.DeepCopy()
	observed.Spec.Type = corev1.ServiceTypeClusterIP
	observed.Spec.ClusterIP = "10.0.0.1"
	if !roleServiceShapeMatches(instance, observed, desired) {
		t.Fatal("valid Service rejected")
	}
	observed.Spec.PublishNotReadyAddresses = true
	if roleServiceShapeMatches(instance, observed, desired) {
		t.Fatal("Service publishing unready endpoints accepted")
	}
	observed.Spec.PublishNotReadyAddresses = false
	local := corev1.ServiceInternalTrafficPolicyLocal
	observed.Spec.InternalTrafficPolicy = &local
	if roleServiceShapeMatches(instance, observed, desired) {
		t.Fatal("node-local Service accepted")
	}
	observed.Spec.InternalTrafficPolicy = nil
	observed.Spec.ClusterIP = corev1.ClusterIPNone
	if roleServiceShapeMatches(instance, observed, desired) {
		t.Fatal("headless role Service accepted")
	}
}
