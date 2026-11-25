package workload_test

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"

	"github.com/huyz0/opensearch-serverless-operator/api/v1alpha1"
	"github.com/huyz0/opensearch-serverless-operator/internal/workload"
)

func TestPodUsesRoleZoneAndCopiesSidecars(t *testing.T) {
	instance := &v1alpha1.OpenSearchServerless{ObjectMeta: metav1.ObjectMeta{Name: "catalog", Namespace: "n", UID: "uid"}, Spec: v1alpha1.OpenSearchServerlessSpec{
		Image: "image", ServiceAccountName: "runtime", Pools: map[string]v1alpha1.Pool{"search": {ProtectFromScaleDown: true, Zones: map[string]v1alpha1.Zone{"zone-a": {Replicas: 1}}, Sidecars: []corev1.Container{{Name: "metrics", Image: "sidecar"}}}},
	}}
	input := workload.PodInput{Name: "catalog-search-1", Role: "search", Zone: "zone-a", Fleet: "blue", Revision: "r1", IntentKey: "example.com/intent", Intent: "op1", Command: []string{"java"}, Args: []string{"-Dtest=literal $(x)"}, DedicatedNode: true, ServingReadinessPort: 9200}
	pod, err := workload.Pod(instance, input)
	if err != nil {
		t.Fatal(err)
	}
	if pod.Spec.NodeSelector[corev1.LabelTopologyZone] != "zone-a" || pod.Labels[workload.RoleLabel] != "search" || len(pod.Spec.SchedulingGates) != 1 {
		t.Fatal("Pod placement or gate missing")
	}
	if pod.Annotations["cluster-autoscaler.kubernetes.io/safe-to-evict"] != "false" {
		t.Fatal("autoscaler protection must be set at Pod creation")
	}
	if pod.Spec.AutomountServiceAccountToken == nil || *pod.Spec.AutomountServiceAccountToken {
		t.Fatal("Pod must not automatically mount a Kubernetes API token")
	}
	pod.Spec.Containers[1].Image = "changed"
	if instance.Spec.Pools["search"].Sidecars[0].Image != "sidecar" || pod.Spec.Containers[0].Args[0] != input.Args[0] {
		t.Fatal("Pod must copy sidecars and preserve literal arguments")
	}
	if pod.Spec.Containers[0].ReadinessProbe.HTTPGet.Path != "/_serverless/ready" {
		t.Fatal("serving readiness probe must use the Java endpoint")
	}
	terms := pod.Spec.Affinity.PodAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution
	if len(terms) != 1 || terms[0].TopologyKey != corev1.LabelHostname || terms[0].NamespaceSelector == nil || len(terms[0].LabelSelector.MatchExpressions) != 1 || terms[0].LabelSelector.MatchExpressions[0].Key != workload.InstanceLabel || terms[0].LabelSelector.MatchExpressions[0].Operator != metav1.LabelSelectorOpExists {
		t.Fatal("dedicated placement must exclude operator-managed Pods across instances and namespaces")
	}
	selector, err := metav1.LabelSelectorAsSelector(terms[0].LabelSelector)
	if err != nil || !selector.Matches(labels.Set{workload.InstanceLabel: "another-instance"}) || selector.Matches(labels.Set{"other": "workload"}) {
		t.Fatalf("dedicated selector does not distinguish managed and unlabelled Pods: %v", err)
	}
}

func TestReplaceOnExitRequiresASingleOpenSearchLifetime(t *testing.T) {
	pool := v1alpha1.Pool{
		Zones:          map[string]v1alpha1.Zone{"a": {Replicas: 1}},
		NativeSidecars: []corev1.Container{{Name: "metrics", Image: "sidecar"}},
	}
	instance := &v1alpha1.OpenSearchServerless{
		ObjectMeta: metav1.ObjectMeta{Name: "catalog", Namespace: "n", UID: "uid"},
		Spec: v1alpha1.OpenSearchServerlessSpec{
			Image: "image", Pools: map[string]v1alpha1.Pool{"ingest": pool},
		},
	}
	input := workload.PodInput{
		Name: "writer", Role: "ingest", Zone: "a", Fleet: "blue", Revision: "r1",
		IntentKey: "example.com/intent", Intent: "op", Command: []string{"java"},
	}
	pod, err := workload.Pod(instance, input)
	if err != nil || workload.PodHasSingleOpenSearchLifetime(pod) {
		t.Fatalf("default Pod must keep its ordinary restart policy: %v", err)
	}
	pool.ReplaceOnExit = true
	instance.Spec.Pools["ingest"] = pool
	pod, err = workload.Pod(instance, input)
	if err != nil || !workload.PodHasSingleOpenSearchLifetime(pod) {
		t.Fatalf("replaceOnExit must disable OpenSearch restarts: %v", err)
	}
	always := corev1.ContainerRestartPolicyAlways
	if *pod.Spec.InitContainers[0].RestartPolicy != always {
		t.Fatal("native sidecar restart policy changed")
	}
	pod.Spec.Containers[0].RestartPolicy = &always
	if workload.PodHasSingleOpenSearchLifetime(pod) {
		t.Fatal("an OpenSearch restart override must invalidate the Pod lifetime")
	}
}

func TestSidecarEmptyDirSizeLimit(t *testing.T) {
	instance := &v1alpha1.OpenSearchServerless{ObjectMeta: metav1.ObjectMeta{Name: "catalog", Namespace: "n", UID: "uid"}, Spec: v1alpha1.OpenSearchServerlessSpec{Image: "image", Pools: map[string]v1alpha1.Pool{"search": {Zones: map[string]v1alpha1.Zone{"a": {Replicas: 1}}, SidecarVolumes: []v1alpha1.SidecarVolume{{Name: "scratch", EmptyDir: true, SizeLimit: "2Gi"}}}}}}
	pod, err := workload.Pod(instance, workload.PodInput{Name: "search", Role: "search", Zone: "a", Fleet: "blue", Revision: "r1", IntentKey: "example.com/intent", Intent: "op", Command: []string{"java"}})
	if err != nil {
		t.Fatal(err)
	}
	if len(pod.Spec.Volumes) != 1 || pod.Spec.Volumes[0].EmptyDir == nil || pod.Spec.Volumes[0].EmptyDir.SizeLimit == nil || pod.Spec.Volumes[0].EmptyDir.SizeLimit.String() != "2Gi" {
		t.Fatal("emptyDir size limit not rendered")
	}
}
