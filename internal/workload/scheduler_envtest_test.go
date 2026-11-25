//go:build envtest

package workload_test

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	nodev1 "k8s.io/api/node/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"
	"sigs.k8s.io/controller-runtime/pkg/envtest"

	"github.com/huyz0/opensearch-serverless-operator/api/v1alpha1"
	"github.com/huyz0/opensearch-serverless-operator/internal/placement"
	"github.com/huyz0/opensearch-serverless-operator/internal/workload"
	podkube "github.com/huyz0/opensearch-serverless-operator/pkg/podlifecycle/kubernetes"
)

// The scheduler sees synthetic Nodes but no kubelets. It tests placement
// decisions only, not cloud provisioning, execution, networking or eviction.
func TestDedicatedHostsAcrossNamespacesWithScheduler(t *testing.T) {
	binary := os.Getenv("KUBE_SCHEDULER_BINARY")
	if binary == "" {
		t.Skip("set KUBE_SCHEDULER_BINARY to a matching kube-scheduler binary")
	}
	environment := &envtest.Environment{}
	environment.ControlPlane.GetAPIServer().Configure().Append("feature-gates", "PodLevelResources=true")
	config, err := environment.Start()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := environment.Stop(); err != nil {
			t.Error(err)
		}
	})
	core, err := kubernetes.NewForConfig(config)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	kubeconfig := clientcmdapi.Config{Clusters: map[string]*clientcmdapi.Cluster{"envtest": {Server: config.Host, CertificateAuthorityData: config.CAData}}, AuthInfos: map[string]*clientcmdapi.AuthInfo{"admin": {ClientCertificateData: config.CertData, ClientKeyData: config.KeyData}}, Contexts: map[string]*clientcmdapi.Context{"test": {Cluster: "envtest", AuthInfo: "admin"}}, CurrentContext: "test"}
	configPath := filepath.Join(t.TempDir(), "scheduler.kubeconfig")
	if err := clientcmd.WriteToFile(kubeconfig, configPath); err != nil {
		t.Fatal(err)
	}
	log, err := os.Create(filepath.Join(t.TempDir(), "scheduler.log"))
	if err != nil {
		t.Fatal(err)
	}
	defer log.Close()
	scheduler := exec.CommandContext(ctx, binary, "--kubeconfig="+configPath, "--leader-elect=false", "--secure-port=0", "--feature-gates=PodLevelResources=true")
	scheduler.Stdout, scheduler.Stderr = log, log
	if err := scheduler.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = scheduler.Process.Kill(); _ = scheduler.Wait() }()
	for _, namespace := range []string{"one", "two"} {
		if _, err := core.CoreV1().Namespaces().Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: namespace}}, metav1.CreateOptions{}); err != nil {
			t.Fatal(err)
		}
		if _, err := core.CoreV1().ServiceAccounts(namespace).Create(ctx, &corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Name: "runtime"}}, metav1.CreateOptions{}); err != nil {
			t.Fatal(err)
		}
	}
	for _, entry := range []struct{ name, zone, role string }{{"node-one", "zone-a", "search"}, {"node-two", "zone-a", "search"}, {"node-three", "zone-b", "ingest"}, {"node-four", "zone-b", "search"}, {"node-five", "zone-a", "ingest"}, {"node-fit", "zone-c", "fit"}} {
		node, err := core.CoreV1().Nodes().Create(ctx, &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: entry.name, Labels: map[string]string{corev1.LabelHostname: entry.name, corev1.LabelTopologyZone: entry.zone, "platform.example/pool": entry.role}}, Spec: corev1.NodeSpec{Taints: []corev1.Taint{{Key: "platform.example/dedicated", Value: entry.role, Effect: corev1.TaintEffectNoSchedule}}}}, metav1.CreateOptions{})
		if err != nil {
			t.Fatal(err)
		}
		node.Status.Capacity = corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("8"), corev1.ResourceMemory: resource.MustParse("16Gi"), corev1.ResourceEphemeralStorage: resource.MustParse("20Gi"), corev1.ResourcePods: resource.MustParse("50")}
		if entry.name == "node-fit" {
			node.Status.Capacity[corev1.ResourceCPU] = resource.MustParse("4")
		}
		node.Status.Allocatable = node.Status.Capacity.DeepCopy()
		node.Status.Conditions = []corev1.NodeCondition{{Type: corev1.NodeReady, Status: corev1.ConditionTrue, LastHeartbeatTime: metav1.Now(), LastTransitionTime: metav1.Now()}}
		node, err = core.CoreV1().Nodes().UpdateStatus(ctx, node, metav1.UpdateOptions{})
		if err != nil {
			t.Fatal(err)
		}
		// No node lifecycle controller runs in envtest to remove the initial
		// not-ready taint when the synthetic node reports Ready.
		node.Spec.Taints = []corev1.Taint{{Key: "platform.example/dedicated", Value: entry.role, Effect: corev1.TaintEffectNoSchedule}}
		if _, err := core.CoreV1().Nodes().Update(ctx, node, metav1.UpdateOptions{}); err != nil {
			t.Fatal(err)
		}
	}
	makePodForRole := func(namespace, name, uid, role, zone string) *corev1.Pod {
		t.Helper()
		instance := &v1alpha1.OpenSearchServerless{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace, UID: types.UID(uid)}, Spec: v1alpha1.OpenSearchServerlessSpec{Image: "example.invalid/runtime", ServiceAccountName: "runtime", Pools: map[string]v1alpha1.Pool{role: {Zones: map[string]v1alpha1.Zone{zone: {Replicas: 1}}, Resources: corev1.ResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("100m"), corev1.ResourceMemory: resource.MustParse("100Mi")}}}}}}
		pod, err := workload.Pod(instance, workload.PodInput{Name: name, Role: role, Zone: zone, Fleet: "initial", Revision: "r1", IntentKey: "example.com/intent", Intent: "test", Command: []string{"/test"}})
		if err != nil {
			t.Fatal(err)
		}
		profile := &placement.Resolved{Profile: placement.Profile{NodeSelector: map[string]string{"platform.example/pool": role}, Taint: corev1.Taint{Key: "platform.example/dedicated", Value: role, Effect: corev1.TaintEffectNoSchedule}}}
		if err := placement.Apply(pod, profile, uid); err != nil {
			t.Fatal(err)
		}
		pod.Spec.SchedulingGates = nil
		pod.OwnerReferences = nil
		return pod
	}
	makePod := func(namespace, name, uid string) *corev1.Pod {
		return makePodForRole(namespace, name, uid, "search", "zone-a")
	}
	create := func(pod *corev1.Pod) {
		t.Helper()
		if _, err := core.CoreV1().Pods(pod.Namespace).Create(ctx, pod, metav1.CreateOptions{}); err != nil {
			t.Fatal(err)
		}
	}
	waitNode := func(namespace, name string) string {
		t.Helper()
		ticker := time.NewTicker(100 * time.Millisecond)
		defer ticker.Stop()
		for {
			pod, err := core.CoreV1().Pods(namespace).Get(ctx, name, metav1.GetOptions{})
			if err != nil {
				t.Fatal(err)
			}
			if pod.Spec.NodeName != "" {
				return pod.Spec.NodeName
			}
			select {
			case <-ctx.Done():
				data, _ := os.ReadFile(log.Name())
				t.Fatalf("Pod %s/%s was not scheduled: %v; status=%+v\n%s", namespace, name, ctx.Err(), pod.Status, data)
			case <-ticker.C:
			}
		}
	}
	create(makePod("one", "first", "instance-one"))
	first := waitNode("one", "first")
	create(makePod("two", "second", "instance-two"))
	second := waitNode("two", "second")
	if first == second {
		t.Fatalf("different namespace instances shared host %s", first)
	}
	shared := makePod("two", "shared", "shared-instance")
	shared.Spec.Affinity = nil
	create(shared)
	waitUnschedulable := func(name string) {
		t.Helper()
		for {
			p, err := core.CoreV1().Pods("two").Get(ctx, name, metav1.GetOptions{})
			if err != nil {
				t.Fatal(err)
			}
			if p.Spec.NodeName != "" {
				t.Fatalf("Pod %s unexpectedly shared a host", name)
			}
			for _, condition := range p.Status.Conditions {
				if condition.Type == corev1.PodScheduled && condition.Status == corev1.ConditionFalse && condition.Reason == "Unschedulable" {
					if !strings.Contains(condition.Message, "anti-affinity") {
						t.Fatalf("Pod %s was blocked for an unexpected reason: %s", name, condition.Message)
					}
					return
				}
			}
			select {
			case <-ctx.Done():
				t.Fatalf("scheduler did not report an unschedulable Pod %s", name)
			case <-time.After(100 * time.Millisecond):
			}
		}
	}
	// An existing dedicated Pod's hard anti-affinity must also block a later
	// operator-managed Pod that does not request dedicated placement itself.
	waitUnschedulable("shared")
	zero := int64(0)
	if err := core.CoreV1().Pods("two").Delete(ctx, "shared", metav1.DeleteOptions{GracePeriodSeconds: &zero}); err != nil {
		t.Fatal(err)
	}
	create(makePod("two", "third", "instance-three"))
	// Observe an actual Unschedulable scheduler condition rather than assuming
	// pending state before the scheduler has processed the Pod.
	waitUnschedulable("third")
	if err := core.CoreV1().Pods("one").Delete(ctx, "first", metav1.DeleteOptions{GracePeriodSeconds: &zero}); err != nil {
		t.Fatal(err)
	}
	if got := waitNode("two", "third"); got != first {
		t.Fatalf("replacement scheduled on %s instead of freed host %s", got, first)
	}
	// Simulate zone-a search hosts being unavailable for new scheduling. The
	// wrong-role zone-a host and wrong-zone search host remain available.
	// Existing Pods remain bound because there is no node controller or kubelet.
	for _, name := range []string{"node-one", "node-two"} {
		node, err := core.CoreV1().Nodes().Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			t.Fatal(err)
		}
		node.Spec.Unschedulable = true
		if _, err := core.CoreV1().Nodes().Update(ctx, node, metav1.UpdateOptions{}); err != nil {
			t.Fatal(err)
		}
	}
	create(makePod("two", "zone-a-replacement", "instance-four"))
	for {
		p, err := core.CoreV1().Pods("two").Get(ctx, "zone-a-replacement", metav1.GetOptions{})
		if err != nil {
			t.Fatal(err)
		}
		if p.Spec.NodeName != "" {
			t.Fatalf("zone-a replacement unexpectedly scheduled on %s", p.Spec.NodeName)
		}
		blocked := false
		for _, condition := range p.Status.Conditions {
			if condition.Type == corev1.PodScheduled && condition.Status == corev1.ConditionFalse && condition.Reason == "Unschedulable" && strings.Contains(condition.Message, "unschedulable") {
				blocked = true
			}
		}
		if blocked {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal("zone-a replacement did not report unavailable hosts")
		case <-time.After(100 * time.Millisecond):
		}
	}
	create(makePodForRole("one", "zone-b-ingest", "instance-five", "ingest", "zone-b"))
	if got := waitNode("one", "zone-b-ingest"); got != "node-three" {
		t.Fatalf("zone-b ingest scheduled on unexpected host %s", got)
	}
	// Compare the operator's fit decision with the real scheduler using a
	// RuntimeClass overhead and a pre-bound resident agent on the fit node.
	if _, err := core.NodeV1().RuntimeClasses().Create(ctx, &nodev1.RuntimeClass{ObjectMeta: metav1.ObjectMeta{Name: "fit-runtime"}, Handler: "runc", Overhead: &nodev1.Overhead{PodFixed: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("250m"), corev1.ResourceMemory: resource.MustParse("32Mi"), corev1.ResourceEphemeralStorage: resource.MustParse("32Mi")}}}, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	resources := func(cpu, memory, ephemeral string) corev1.ResourceRequirements {
		return corev1.ResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse(cpu), corev1.ResourceMemory: resource.MustParse(memory), corev1.ResourceEphemeralStorage: resource.MustParse(ephemeral)}}
	}
	agent := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "node-agent", Namespace: "one"}, Spec: corev1.PodSpec{NodeName: "node-fit", Containers: []corev1.Container{{Name: "agent", Image: "example.invalid/agent", Resources: resources("1", "1Gi", "1Gi")}}}}
	create(agent)
	fitProfile := &placement.Resolved{Profile: placement.Profile{NodeSelector: map[string]string{"platform.example/pool": "fit"}, Taint: corev1.Taint{Key: "platform.example/dedicated", Value: "fit", Effect: corev1.TaintEffectNoSchedule}, Allocatable: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("4"), corev1.ResourceMemory: resource.MustParse("16Gi"), corev1.ResourceEphemeralStorage: resource.MustParse("20Gi"), corev1.ResourcePods: resource.MustParse("50")}, Reserved: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("1"), corev1.ResourceMemory: resource.MustParse("1Gi"), corev1.ResourceEphemeralStorage: resource.MustParse("1Gi"), corev1.ResourcePods: resource.MustParse("1")}, Headroom: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("0"), corev1.ResourceMemory: resource.MustParse("0"), corev1.ResourceEphemeralStorage: resource.MustParse("0"), corev1.ResourcePods: resource.MustParse("0")}}}
	fitPod := func(name, installerCPU, podCPU string) *corev1.Pod {
		t.Helper()
		pod := makePodForRole("two", name, name+"-uid", "search", "zone-c")
		// Replace the helper's role profile with this independently qualified pool.
		pod.Spec.Affinity = nil
		pod.Spec.Tolerations = nil
		delete(pod.Spec.NodeSelector, "platform.example/pool")
		if err := placement.Apply(pod, fitProfile, name+"-uid"); err != nil {
			t.Fatal(err)
		}
		pod.Spec.Containers[0].Resources = resources("2", "1Gi", "1Gi")
		always := corev1.ContainerRestartPolicyAlways
		pod.Spec.InitContainers = []corev1.Container{{Name: "sidecar", Image: "example.invalid/sidecar", RestartPolicy: &always, Resources: resources("400m", "128Mi", "128Mi")}, {Name: "installer", Image: "example.invalid/installer", Resources: resources(installerCPU, "256Mi", "256Mi")}}
		if podCPU != "" {
			// Kubernetes 1.32's alpha validation cannot combine pod-level
			// resources with container ephemeral-storage requests.
			delete(pod.Spec.Containers[0].Resources.Requests, corev1.ResourceEphemeralStorage)
			for i := range pod.Spec.InitContainers {
				delete(pod.Spec.InitContainers[i].Resources.Requests, corev1.ResourceEphemeralStorage)
			}
			pod.Spec.Resources = &corev1.ResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse(podCPU), corev1.ResourceMemory: resource.MustParse("2Gi")}}
		}
		runtimeClass := "fit-runtime"
		pod.Spec.RuntimeClassName = &runtimeClass
		pod.Spec.SchedulingGates = []corev1.PodSchedulingGate{{Name: "serverless.opensearch.org/placement"}}
		return pod
	}
	assertFit := func(name, cpu, podCPU string, expected bool) *corev1.Pod {
		t.Helper()
		pod, err := core.CoreV1().Pods("two").Create(ctx, fitPod(name, cpu, podCPU), metav1.CreateOptions{})
		if err != nil {
			t.Fatal(err)
		}
		if got := pod.Spec.Overhead.Cpu(); got == nil || got.Cmp(resource.MustParse("250m")) != 0 {
			t.Fatal("RuntimeClass overhead was not applied by admission")
		}
		if podCPU != "" && (pod.Spec.Resources == nil || pod.Spec.Resources.Requests.Cpu().Cmp(resource.MustParse(podCPU)) != 0) {
			t.Fatal("Pod-level CPU request did not survive admission")
		}
		if podCPU == "" {
			fitErr := placement.CheckFit(pod, fitProfile)
			if (fitErr == nil) != expected || (!expected && !strings.Contains(fitErr.Error(), "cpu")) {
				t.Fatalf("unexpected operator fit for %s: %v", name, fitErr)
			}
		} else {
			if err := placement.CheckFit(pod, fitProfile); err == nil {
				t.Fatal("development profile must reject missing ephemeral requests")
			}
			short, err := podkube.InsufficientResources(pod, fitProfile.Profile.Allocatable, fitProfile.Profile.Reserved)
			if err != nil || slices.Contains(short, corev1.ResourceCPU) == expected {
				t.Fatalf("unexpected reusable fit for %s: %v, %v", name, short, err)
			}
		}
		pod.Spec.SchedulingGates = nil
		pod, err = core.CoreV1().Pods("two").Update(ctx, pod, metav1.UpdateOptions{})
		if err != nil {
			t.Fatal(err)
		}
		return pod
	}
	waitInsufficientCPU := func(name string) {
		t.Helper()
		for {
			p, err := core.CoreV1().Pods("two").Get(ctx, name, metav1.GetOptions{})
			if err != nil {
				t.Fatal(err)
			}
			if p.Spec.NodeName != "" {
				t.Fatalf("scheduler placed %s above the CPU fit boundary", name)
			}
			insufficient := false
			for _, condition := range p.Status.Conditions {
				if condition.Type == corev1.PodScheduled && condition.Status == corev1.ConditionFalse && strings.Contains(condition.Message, "Insufficient cpu") {
					insufficient = true
				}
			}
			if insufficient {
				break
			}
			select {
			case <-ctx.Done():
				t.Fatal("scheduler did not report insufficient CPU")
			case <-time.After(100 * time.Millisecond):
			}
		}
	}
	// The regular sidecar consumes capacity at the same time as Java and the
	// restartable init sidecar. Test both sides of the boundary after admission.
	regularSidecar := func(name, cpu string, fits bool) {
		t.Helper()
		candidate := fitPod(name, "1", "")
		candidate.Spec.Containers = append(candidate.Spec.Containers, corev1.Container{Name: "metrics", Image: "example.invalid/metrics", Resources: resources(cpu, "128Mi", "128Mi")})
		pod, err := core.CoreV1().Pods("two").Create(ctx, candidate, metav1.CreateOptions{})
		if err != nil {
			t.Fatal(err)
		}
		if err := placement.CheckFit(pod, fitProfile); (err == nil) != fits {
			t.Fatalf("regular sidecar fit for %s: %v", name, err)
		}
		pod.Spec.SchedulingGates = nil
		if _, err := core.CoreV1().Pods("two").Update(ctx, pod, metav1.UpdateOptions{}); err != nil {
			t.Fatal(err)
		}
		if fits {
			if got := waitNode("two", name); got != "node-fit" {
				t.Fatalf("regular sidecar Pod bound to %s", got)
			}
		} else {
			waitInsufficientCPU(name)
		}
		if err := core.CoreV1().Pods("two").Delete(ctx, name, metav1.DeleteOptions{GracePeriodSeconds: &zero}); err != nil {
			t.Fatal(err)
		}
	}
	regularSidecar("regular-sidecar-too-large", "360m", false)
	regularSidecar("regular-sidecar-exact-fit", "350m", true)
	assertFit("too-large", "2360m", "", false)
	waitInsufficientCPU("too-large")
	if err := core.CoreV1().Pods("two").Delete(ctx, "too-large", metav1.DeleteOptions{GracePeriodSeconds: &zero}); err != nil {
		t.Fatal(err)
	}
	assertFit("pod-level-too-large", "2350m", "2760m", false)
	waitInsufficientCPU("pod-level-too-large")
	if err := core.CoreV1().Pods("two").Delete(ctx, "pod-level-too-large", metav1.DeleteOptions{GracePeriodSeconds: &zero}); err != nil {
		t.Fatal(err)
	}
	assertFit("exact-fit", "2350m", "2750m", true)
	if got := waitNode("two", "exact-fit"); got != "node-fit" {
		t.Fatalf("exact-fit Pod bound to unexpected node %s", got)
	}
}
