package workload_test

import (
	"slices"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/huyz0/opensearch-serverless-operator/api/v1alpha1"
	"github.com/huyz0/opensearch-serverless-operator/internal/runtimeconfig"
	"github.com/huyz0/opensearch-serverless-operator/internal/storage"
	"github.com/huyz0/opensearch-serverless-operator/internal/workload"
)

func TestRuntimePodRendersRoleAndStore(t *testing.T) {
	instance := &v1alpha1.OpenSearchServerless{ObjectMeta: metav1.ObjectMeta{Name: "catalog", Namespace: "n", UID: "uid"}, Spec: v1alpha1.OpenSearchServerlessSpec{
		Image: "image", ServiceAccountName: "runtime", Pools: map[string]v1alpha1.Pool{"search": {JVMHeap: "2Gi", Zones: map[string]v1alpha1.Zone{"zone-a": {Replicas: 1}}}},
		Runtime: &v1alpha1.RuntimeConfig{Settings: map[string]string{"custom.setting": "with spaces $(POD_IP) $$"}},
	}}
	input := workload.PodInput{Name: "node", Role: "search", Zone: "zone-a", Fleet: "blue", Revision: "r1", IntentKey: "example.com/intent", Intent: "op1", Command: []string{"/entrypoint"}}
	managed := runtimeconfig.ManagedInputs{NodeName: "node", ClusterName: "catalog", Role: "search", Zone: "zone-a", Home: "/work", ConfigPath: "/config", NetworkHost: runtimeconfig.PodIPReference, JVMHeap: "2Gi", Store: storage.S3{Bucket: "bucket", Region: "region", Endpoint: "https://storage.example/prefix/$(POD_IP)/$$"}}
	pod, err := workload.RuntimePod(instance, input, managed, runtimeconfig.Catalog{Settings: map[string]bool{"custom.setting": true}})
	if err != nil {
		t.Fatal(err)
	}
	args := pod.Spec.Containers[0].Args
	for _, expected := range []string{"-Dserverless.roles=search", "-Dserverless.store.bucket=bucket", "-Dcustom.setting=with spaces $$(POD_IP) $$$$", "-Dserverless.store.endpoint=https://storage.example/prefix/$$(POD_IP)/$$$$", "-Dnetwork.host=$(POD_IP)", "-Xmx2147483648"} {
		if !slices.Contains(args, expected) {
			t.Fatalf("missing %q in %v", expected, args)
		}
	}
}
