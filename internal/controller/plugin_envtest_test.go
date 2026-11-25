//go:build envtest

package controller

import (
	"context"
	"encoding/json"
	"encoding/pem"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"

	"github.com/huyz0/opensearch-serverless-operator/api/v1alpha1"
	"github.com/huyz0/opensearch-serverless-operator/internal/plugins"
	"github.com/huyz0/opensearch-serverless-operator/internal/workload"
)

func TestPluginStagingAndGateReleaseOnAPIServer(t *testing.T) {
	environment := &envtest.Environment{CRDDirectoryPaths: []string{filepath.Join("..", "..", "config", "generated-dev")}, ErrorIfCRDPathMissing: true}
	config, err := environment.Start()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := environment.Stop(); err != nil {
			t.Error(err)
		}
	})
	scheme := runtime.NewScheme()
	if err := v1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	kube, err := client.New(config, client.Options{Scheme: scheme})
	if err != nil {
		t.Fatal(err)
	}
	core, err := kubernetes.NewForConfig(config)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	artifact := "registry.example/plugins/demo@sha256:" + strings.Repeat("a", 64)
	image := "registry.example/runtime@sha256:" + strings.Repeat("b", 64)
	installer := "registry.example/installer@sha256:" + strings.Repeat("c", 64)
	policyJSON, err := json.Marshal(plugins.Policy{ApprovedArtifacts: []string{artifact}, OpenSearchVersion: "3.0", JavaVersion: "21", MaxBytes: 1024, MaxFiles: 10})
	if err != nil {
		t.Fatal(err)
	}
	immutable := true
	tlsFixture := httptest.NewTLSServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	defer tlsFixture.Close()
	registryCA := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: tlsFixture.Certificate().Raw})
	if _, err := core.CoreV1().ConfigMaps("default").Create(ctx, &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "plugin-policy"}, Immutable: &immutable, Data: map[string]string{"policy.json": string(policyJSON)}}, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := core.CoreV1().Secrets("default").Create(ctx, &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "plugin-pull"}, Immutable: &immutable, Data: map[string][]byte{"credentials.json": []byte(`{}`), "ca.crt": registryCA}}, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	instance := &v1alpha1.OpenSearchServerless{ObjectMeta: metav1.ObjectMeta{Name: "plugin-test", Namespace: "default"}, Spec: v1alpha1.OpenSearchServerlessSpec{Image: image, ServiceAccountName: "runtime", Storage: v1alpha1.Storage{Type: "s3", Bucket: "bucket", Region: "region"}, Plugins: []v1alpha1.Plugin{{Name: "demo", Artifact: artifact, RegistryCredentialsRef: &v1alpha1.LocalReference{Name: "plugin-pull"}}}, Pools: map[string]v1alpha1.Pool{"search": {JVMHeap: "512Mi", Zones: map[string]v1alpha1.Zone{"a": {Replicas: 2}}, Resources: corev1.ResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("1"), corev1.ResourceMemory: resource.MustParse("1Gi")}}}}}}
	if err := kube.Create(ctx, instance); err != nil {
		t.Fatal(err)
	}
	instance.Status.PrimaryStore = &v1alpha1.PrimaryStoreStatus{Bucket: "bucket", Region: "region"}
	instance.Status.ServingFleetIntentByRole = map[string]string{"search": "initial"}
	if err := kube.Status().Update(ctx, instance); err != nil {
		t.Fatal(err)
	}
	key := types.NamespacedName{Namespace: instance.Namespace, Name: instance.Name}
	fakeNow := time.Now()
	r := &InstanceReconciler{Now: func() time.Time { return fakeNow }, DevelopmentPendingTimeout: time.Minute, DevelopmentReleaseLimit: 1, DevelopmentReleaseWindow: 10 * time.Minute, Client: kube, Core: core.CoreV1(), DevelopmentReleaseGates: true, DevelopmentPluginInstallerImage: installer, DevelopmentPluginRuntimeImage: image, DevelopmentPluginPolicy: "plugin-policy"}
	step := func() {
		t.Helper()
		if err := kube.Get(ctx, key, instance); err != nil {
			t.Fatal(err)
		}
		if _, err := r.reconcileGatedPod(ctx, instance); err != nil {
			t.Fatal(err)
		}
	}
	step()
	if err := kube.Get(ctx, key, instance); err != nil {
		t.Fatal(err)
	}
	intent := instance.Status.PendingPodCreation
	if intent == nil || intent.PluginInstall == nil || intent.PluginInstall.RequestUID != "" || !intent.PluginInstall.CredentialsHasCA {
		t.Fatalf("plugin intent was not recorded before staging: %#v", instance.Status)
	}
	if _, err := core.CoreV1().ConfigMaps("default").Get(ctx, intent.PluginInstall.RequestName, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Fatalf("snapshot existed before intent: %v", err)
	}
	step()
	if err := kube.Get(ctx, key, instance); err != nil {
		t.Fatal(err)
	}
	intent = instance.Status.PendingPodCreation
	if intent == nil || intent.PluginInstall.RequestUID == "" || intent.PluginInstall.PolicyUID == "" {
		t.Fatal("plugin snapshot identities were not persisted")
	}
	if _, err := core.CoreV1().Pods("default").Get(ctx, intent.Name, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Fatalf("Pod existed before snapshot UIDs were persisted: %v", err)
	}
	step()
	pod, err := core.CoreV1().Pods("default").Get(ctx, intent.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if len(pod.Spec.InitContainers) != 1 || !slices.Contains(pod.Spec.InitContainers[0].Args, intent.PluginInstall.RequestSHA256) || !slices.Contains(pod.Spec.InitContainers[0].Args, intent.PluginInstall.PolicySHA256) || !slices.Contains(pod.Spec.InitContainers[0].Args, "/inputs/credentials/ca.crt") {
		t.Fatal("pinned installer inputs missing")
	}
	step()
	step()
	step()
	pod, err = core.CoreV1().Pods("default").Get(ctx, intent.Name, metav1.GetOptions{})
	if err != nil || hasPlacementGate(pod) {
		t.Fatalf("qualified plugin Pod gate did not release: %v", err)
	}
	if err := kube.Get(ctx, key, instance); err != nil || instance.Status.ProvisioningAttempt == nil || instance.Status.ProvisioningAttempt.PluginInstall == nil {
		t.Fatal("released plugin Pod did not persist installer tracking")
	}
	if err := core.CoreV1().Pods("default").Bind(ctx, &corev1.Binding{ObjectMeta: metav1.ObjectMeta{Name: pod.Name, UID: pod.UID}, Target: corev1.ObjectReference{Kind: "Node", Name: "synthetic-node"}}, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	track := func() {
		t.Helper()
		if err := kube.Get(ctx, key, instance); err != nil {
			t.Fatal(err)
		}
		if _, _, err := r.reconcileProvisioningAttempt(ctx, instance); err != nil {
			t.Fatal(err)
		}
	}
	track()
	if err := kube.Get(ctx, key, instance); err != nil || instance.Status.ProvisioningAttempt.Phase != "CheckingPluginInstall" {
		t.Fatal("bound installer was not tracked")
	}
	fakeNow = fakeNow.Add(2 * time.Minute)
	track()
	if err := kube.Get(ctx, key, instance); err != nil || instance.Status.ProvisioningAttempt.Reason != "PluginInstallTimeout" {
		t.Fatal("installer timeout did not persist a hold")
	}
	pod, err = core.CoreV1().Pods("default").Get(ctx, pod.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	pod.Status.Phase = corev1.PodRunning
	pod.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}
	pod.Status.InitContainerStatuses = []corev1.ContainerStatus{{Name: "plugin-installer", State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ExitCode: 0}}}}
	if _, err := core.CoreV1().Pods("default").UpdateStatus(ctx, pod, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	track()
	if err := kube.Get(ctx, key, instance); err != nil || instance.Status.ProvisioningAttempt != nil {
		t.Fatal("successful installer did not clear hold")
	}
	// A ready Pod can disappear soon after startup. Persisted release history
	// still bounds the next gate release even if a manager restarts.
	if err := kube.Get(ctx, key, instance); err != nil || len(instance.Status.RecentPodReleases) != 1 {
		t.Fatal("release history was not persisted")
	}
	step()
	if err := kube.Get(ctx, key, instance); err != nil || instance.Status.PendingPodCreation != nil || instance.Status.PendingGateRelease != nil || len(instance.Status.RecentPodReleases) != 1 {
		t.Fatal("release budget did not hold before creating second Pod")
	}
	fakeNow = fakeNow.Add(9 * time.Minute)
	step() // persist creation intent after budget expiry
	step() // persist snapshot identities
	if err := kube.Get(ctx, key, instance); err != nil {
		t.Fatal(err)
	}
	second := instance.Status.PendingPodCreation
	if second == nil || second.Name == intent.Name || second.PluginInstall == nil || *second.PluginInstall != *intent.PluginInstall {
		t.Fatalf("same plugin inputs did not reuse the original immutable snapshots: %#v", second)
	}
	step() // create held Pod
	competitor := instance.DeepCopyObject().(*v1alpha1.OpenSearchServerless)
	if _, err := r.reconcileGatedPod(ctx, instance); err != nil {
		t.Fatal(err)
	}
	if _, err := r.reconcileGatedPod(ctx, competitor); !apierrors.IsConflict(err) {
		t.Fatalf("stale concurrent release did not conflict: %v", err)
	}
	if err := kube.Get(ctx, key, instance); err != nil || instance.Status.PendingGateRelease == nil || len(instance.Status.RecentPodReleases) != 2 {
		t.Fatal("release did not resume after budget window")
	}
	step()
	step()
	staged, err := core.CoreV1().ConfigMaps("default").Get(ctx, intent.PluginInstall.RequestName, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := core.CoreV1().ConfigMaps("default").Delete(ctx, staged.Name, metav1.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	staged.UID, staged.ResourceVersion = "", ""
	if _, err := core.CoreV1().ConfigMaps("default").Create(ctx, staged, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	if err := kube.Get(ctx, key, instance); err != nil {
		t.Fatal(err)
	}
	if _, err := r.reconcileGatedPod(ctx, instance); err == nil {
		t.Fatal("same-name replacement snapshot accepted")
	}
	// A throttled zone must not consume the sole pending slot when a different
	// zone has an eligible deficit.
	split := &v1alpha1.OpenSearchServerless{ObjectMeta: metav1.ObjectMeta{Name: "budget-split", Namespace: "default"}, Spec: v1alpha1.OpenSearchServerlessSpec{Image: image, ServiceAccountName: "runtime", Storage: v1alpha1.Storage{Type: "s3", Bucket: "bucket", Region: "region"}, Pools: map[string]v1alpha1.Pool{"search": {JVMHeap: "512Mi", Zones: map[string]v1alpha1.Zone{"a": {Replicas: 1}, "b": {Replicas: 1}}, Resources: corev1.ResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("1"), corev1.ResourceMemory: resource.MustParse("1Gi")}}}}}}
	if err := kube.Create(ctx, split); err != nil {
		t.Fatal(err)
	}
	split.Status.PrimaryStore = &v1alpha1.PrimaryStoreStatus{Bucket: "bucket", Region: "region"}
	split.Status.ServingFleetIntentByRole = map[string]string{"search": "initial"}
	split.Status.RecentPodReleases = []v1alpha1.PodReleaseRecord{{PodUID: "old-a", Role: "search", Zone: "a", StartedAt: metav1.NewTime(fakeNow)}}
	if err := kube.Status().Update(ctx, split); err != nil {
		t.Fatal(err)
	}
	if _, err := r.reconcileGatedPod(ctx, split); err != nil {
		t.Fatal(err)
	}
	if err := kube.Get(ctx, types.NamespacedName{Namespace: "default", Name: split.Name}, split); err != nil || split.Status.PendingPodCreation == nil || split.Status.PendingPodCreation.Zone != "b" {
		t.Fatal("throttled zone prevented eligible zone creation")
	}
	// A terminal Pod blocks its own role/AZ while another zone can proceed.
	separate := split.DeepCopyObject().(*v1alpha1.OpenSearchServerless)
	separate.Name, separate.UID, separate.ResourceVersion = "blocked-split", "", ""
	separate.Status = v1alpha1.OpenSearchServerlessStatus{}
	if err := kube.Create(ctx, separate); err != nil {
		t.Fatal(err)
	}
	controller := true
	terminal, err := core.CoreV1().Pods("default").Create(ctx, &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "terminal-pod", Labels: map[string]string{workload.InstanceLabel: string(separate.UID), workload.RoleLabel: "search", workload.ZoneLabel: "a", workload.FleetLabel: "initial"}, OwnerReferences: []metav1.OwnerReference{{APIVersion: v1alpha1.GroupVersion.String(), Kind: "OpenSearchServerless", Name: separate.Name, UID: separate.UID, Controller: &controller}}}, Spec: corev1.PodSpec{NodeName: "synthetic-node", Containers: []corev1.Container{{Name: "opensearch", Image: image}}}}, metav1.CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	terminal.Status.Phase = corev1.PodFailed
	if _, err := core.CoreV1().Pods("default").UpdateStatus(ctx, terminal, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	separate.Status.PrimaryStore = &v1alpha1.PrimaryStoreStatus{Bucket: "bucket", Region: "region"}
	separate.Status.ServingFleetIntentByRole = map[string]string{"search": "initial"}
	separate.Status.ProvisioningAttempt = &v1alpha1.ProvisioningAttempt{Name: terminal.Name, PodUID: string(terminal.UID), Role: "search", Zone: "a", Phase: "Blocked", Reason: "PodTerminal", StartedAt: metav1.NewTime(fakeNow)}
	if err := kube.Status().Update(ctx, separate); err != nil {
		t.Fatal(err)
	}
	if _, changed, err := r.archiveBlockedAttempt(ctx, separate); err != nil || !changed {
		t.Fatalf("archive failed: %v", err)
	}
	if err := kube.Get(ctx, types.NamespacedName{Namespace: "default", Name: separate.Name}, separate); err != nil {
		t.Fatal(err)
	}
	retry := &v1alpha1.OpenSearchServerlessOperation{ObjectMeta: metav1.ObjectMeta{Name: "terminal-retry", Namespace: "default"}, Spec: v1alpha1.OperationSpec{InstanceRef: v1alpha1.InstanceReference{Name: separate.Name, UID: string(separate.UID)}, Action: "RetryProvisioning", ProvisioningRetry: &v1alpha1.ProvisioningRetry{PodUID: string(terminal.UID), AcknowledgeNewAttempt: true}}}
	if err := kube.Create(ctx, retry); err != nil {
		t.Fatal(err)
	}
	op := &OperationReconciler{Client: kube, Core: core.CoreV1()}
	if _, err := op.reconcileProvisioningRetry(ctx, retry); err != nil {
		t.Fatal(err)
	}
	if err := kube.Get(ctx, types.NamespacedName{Namespace: "default", Name: retry.Name}, retry); err != nil {
		t.Fatal(err)
	}
	if _, err := op.reconcileProvisioningRetry(ctx, retry); err != nil {
		t.Fatal(err)
	}
	if err := kube.Get(ctx, types.NamespacedName{Namespace: "default", Name: retry.Name}, retry); err != nil {
		t.Fatal(err)
	}
	if condition := meta.FindStatusCondition(retry.Status.Conditions, "Ready"); condition == nil || condition.Reason != "PodStillExists" {
		t.Fatal("terminal Pod must be absent before retry")
	}
	if _, err := r.reconcileGatedPod(ctx, separate); err != nil {
		t.Fatal(err)
	}
	if err := kube.Get(ctx, types.NamespacedName{Namespace: "default", Name: separate.Name}, separate); err != nil || separate.Status.PendingPodCreation == nil || separate.Status.PendingPodCreation.Zone != "b" || len(separate.Status.BlockedProvisioningAttempts) != 1 {
		t.Fatal("terminal Pod prevented another AZ from progressing")
	}
	// A group below its Kubernetes Ready minimum takes priority over an
	// alphabetically earlier role that needs only desired capacity.
	priority := split.DeepCopyObject().(*v1alpha1.OpenSearchServerless)
	priority.Name, priority.UID, priority.ResourceVersion = "floor-priority", "", ""
	priority.Status = v1alpha1.OpenSearchServerlessStatus{}
	pool := priority.Spec.Pools["search"]
	pool.Zones = map[string]v1alpha1.Zone{"a": {Replicas: 1, MinServing: 1}}
	priority.Spec.Pools = map[string]v1alpha1.Pool{"search": pool, "ingest": {JVMHeap: "512Mi", Zones: map[string]v1alpha1.Zone{"a": {Replicas: 1}}, Resources: pool.Resources}}
	if err := kube.Create(ctx, priority); err != nil {
		t.Fatal(err)
	}
	priority.Status.PrimaryStore = &v1alpha1.PrimaryStoreStatus{Bucket: "bucket", Region: "region"}
	priority.Status.ServingFleetIntentByRole = map[string]string{"search": "initial", "ingest": "initial"}
	if err := kube.Status().Update(ctx, priority); err != nil {
		t.Fatal(err)
	}
	if _, err := r.reconcileGatedPod(ctx, priority); err != nil {
		t.Fatal(err)
	}
	if err := kube.Get(ctx, types.NamespacedName{Namespace: "default", Name: priority.Name}, priority); err != nil || priority.Status.PendingPodCreation == nil || priority.Status.PendingPodCreation.Role != "search" {
		t.Fatal("below-floor role was not prioritized")
	}
}
