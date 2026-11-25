package controller

import (
	"context"
	"slices"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/huyz0/opensearch-serverless-operator/api/v1alpha1"
)

func TestConfigMapWatchMapsOnlySameNamespaceAndName(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := v1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	instance := func(namespace, name, repository string) *v1alpha1.OpenSearchServerless {
		return &v1alpha1.OpenSearchServerless{ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: name}, Spec: v1alpha1.OpenSearchServerlessSpec{
			Storage: v1alpha1.Storage{RepositoryRef: &v1alpha1.LocalReference{Name: repository}},
		}}
	}
	matching := instance("n", "match", "store")
	matching.Spec.Availability = &v1alpha1.Availability{EmergencyCapacity: &v1alpha1.EmergencyCapacityPolicy{EvidenceRef: v1alpha1.LocalReference{Name: "outage"}}}
	matching.Spec.Pools = map[string]v1alpha1.Pool{"search": {CapacityProfileRef: "profile"}, "ingest": {CapacityProfileRef: "profile"}}
	matching.Status.PluginSetsByFleet = map[string]v1alpha1.FleetPluginSet{"blue": {InputsByRole: map[string]v1alpha1.PluginInstallIntent{
		"search": {PolicySourceName: "pinned-policy", RequestName: "pinned-request", PolicyName: "pinned-policy-snapshot"},
	}}}
	matching.Status.PendingGateRelease = &v1alpha1.GateReleaseIntent{Pod: v1alpha1.PodCreationIntent{
		PluginInstall: &v1alpha1.PluginInstallIntent{PolicySourceName: "pinned-policy", RequestName: "pending-request", PolicyName: "pending-policy-snapshot"},
	}}
	remote := matching.DeepCopy()
	remote.Namespace, remote.Name = "elsewhere", "remote"
	r := InstanceReconciler{DevelopmentPluginPolicy: "configured-policy"}
	want := []string{"configured-policy", "outage", "pending-policy-snapshot", "pending-request", "pinned-policy", "pinned-policy-snapshot", "pinned-request", "profile", "store"}
	if got := r.watchedConfigMapNames(matching); !slices.Equal(got, want) {
		t.Fatalf("watched references = %v, want %v", got, want)
	}
	r.Client = fake.NewClientBuilder().WithScheme(scheme).WithIndex(&v1alpha1.OpenSearchServerless{}, configMapReferenceIndex, r.watchedConfigMapNames).
		WithObjects(matching, instance("n", "other", "other"), remote).Build()
	for _, name := range want {
		requests := r.instancesForConfigMap(context.Background(), &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Namespace: "n", Name: name}})
		if len(requests) != 1 || requests[0].Namespace != "n" || requests[0].Name != "match" {
			t.Fatalf("unexpected requests for %s: %#v", name, requests)
		}
	}
	if requests := r.instancesForConfigMap(context.Background(), &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Namespace: "n", Name: "unreferenced"}}); len(requests) != 0 {
		t.Fatalf("unreferenced ConfigMap queued instances: %v", requests)
	}
	if requests := r.instancesForConfigMap(context.Background(), &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "store"}}); len(requests) != 0 {
		t.Fatalf("a missing namespace allowed a cross-namespace lookup: %v", requests)
	}
}
