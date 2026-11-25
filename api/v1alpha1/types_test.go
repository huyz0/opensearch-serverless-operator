package v1alpha1_test

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/runtime"

	"github.com/huyz0/opensearch-serverless-operator/api/v1alpha1"
)

func TestSchemeAndDeepCopy(t *testing.T) {
	options := []string{"-Xfoo"}
	scheme := runtime.NewScheme()
	if err := v1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if _, err := scheme.New(v1alpha1.GroupVersion.WithKind("OpenSearchServerless")); err != nil {
		t.Fatal(err)
	}
	original := &v1alpha1.OpenSearchServerless{Spec: v1alpha1.OpenSearchServerlessSpec{
		Storage: v1alpha1.Storage{RepositoryRef: &v1alpha1.LocalReference{Name: "store"}},
		Pools:   map[string]v1alpha1.Pool{"search": {Zones: map[string]v1alpha1.Zone{"a": {Replicas: 2}}, Resources: corev1.ResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("4Gi")}}}},
		Runtime: &v1alpha1.RuntimeConfig{Settings: map[string]string{"key": "value"}, JVM: &v1alpha1.JVMConfig{Options: &options, SystemProperties: map[string]string{"zone": "UTC"}}},
	}}
	copy := original.DeepCopyObject().(*v1alpha1.OpenSearchServerless)
	copy.Spec.Storage.RepositoryRef.Name = "other"
	copy.Spec.Pools["search"].Zones["a"] = v1alpha1.Zone{Replicas: 3}
	copy.Spec.Pools["search"].Resources.Requests[corev1.ResourceMemory] = resource.MustParse("8Gi")
	copy.Spec.Runtime.Settings["key"] = "changed"
	(*copy.Spec.Runtime.JVM.Options)[0] = "changed"
	copy.Spec.Runtime.JVM.SystemProperties["zone"] = "changed"
	if original.Spec.Storage.RepositoryRef.Name != "store" || original.Spec.Pools["search"].Zones["a"].Replicas != 2 {
		t.Fatal("deep copy mutated the original")
	}
	if original.Spec.Runtime.Settings["key"] != "value" || (*original.Spec.Runtime.JVM.Options)[0] != "-Xfoo" || original.Spec.Runtime.JVM.SystemProperties["zone"] != "UTC" {
		t.Fatal("deep copy mutated runtime configuration")
	}
	requests := original.Spec.Pools["search"].Resources.Requests
	if requests.Memory().String() != "4Gi" {
		t.Fatal("deep copy mutated resource requests")
	}
}
