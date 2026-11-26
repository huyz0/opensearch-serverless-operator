package controller

import (
	"context"
	"encoding/json"
	"github.com/huyz0/opensearch-serverless-operator/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	kubernetesfake "k8s.io/client-go/kubernetes/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"testing"
	"time"
)

func TestIsolatedBoundAttemptRecoversOnlyWhenInputsAndReadinessMatch(t *testing.T) {
	ctx := context.Background()
	scheme := runtime.NewScheme()
	if err := v1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	plugin := &v1alpha1.PluginInstallIntent{RequestUID: "request", PolicyUID: "policy"}
	input, _ := json.Marshal(plugin)
	now := metav1.NewTime(time.Now())
	held := v1alpha1.ProvisioningAttempt{Name: "pod", PodUID: "uid", Role: "search", Zone: "a", Phase: "Blocked", Reason: "PluginInstallTimeout", StartedAt: now, PluginInstall: plugin}
	other := v1alpha1.ProvisioningAttempt{Name: "other", PodUID: "other-uid", Role: "ingest", Zone: "b", Phase: "Blocked", Reason: "PodGone", StartedAt: now}
	instance := &v1alpha1.OpenSearchServerless{ObjectMeta: metav1.ObjectMeta{Name: "instance", Namespace: "n", UID: "owner"}, Status: v1alpha1.OpenSearchServerlessStatus{BlockedProvisioningAttempts: []v1alpha1.ProvisioningAttempt{held, other}}}
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "pod", Namespace: "n", ResourceVersion: "1", Labels: map[string]string{"serverless.opensearch.org/role": "search", "serverless.opensearch.org/zone": "a"}, UID: "uid", Annotations: map[string]string{pluginIntentAnnotation: string(input)}, OwnerReferences: []metav1.OwnerReference{*metav1.NewControllerRef(instance, v1alpha1.GroupVersion.WithKind("OpenSearchServerless"))}}, Spec: corev1.PodSpec{NodeName: "node"}, Status: corev1.PodStatus{Phase: corev1.PodRunning, Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}}}
	core := kubernetesfake.NewSimpleClientset(pod)
	kube := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(instance).WithObjects(instance).Build()
	r := &InstanceReconciler{Client: kube, Core: core.CoreV1()}
	key := types.NamespacedName{Namespace: "n", Name: "instance"}
	observe := func() bool {
		t.Helper()
		if err := kube.Get(ctx, key, instance); err != nil {
			t.Fatal(err)
		}
		_, changed, err := r.observeIsolatedAttempts(ctx, instance)
		if err != nil {
			t.Fatal(err)
		}
		return changed
	}
	if observe() {
		t.Fatal("Ready without successful installer cleared hold")
	}
	pod.Status.InitContainerStatuses = []corev1.ContainerStatus{{Name: "plugin-installer", State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ExitCode: 0}}}}
	if _, err := core.CoreV1().Pods("n").UpdateStatus(ctx, pod, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	if !observe() {
		t.Fatal("recovered Pod did not clear hold")
	}
	if err := kube.Get(ctx, key, instance); err != nil || len(instance.Status.BlockedProvisioningAttempts) != 1 || instance.Status.BlockedProvisioningAttempts[0].PodUID != "other-uid" {
		t.Fatal("recovery removed another group's hold")
	}
}

func TestIsolatedBoundAttemptFailsClosedOnReplacement(t *testing.T) {
	ctx := context.Background()
	scheme := runtime.NewScheme()
	if err := v1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	instance := &v1alpha1.OpenSearchServerless{ObjectMeta: metav1.ObjectMeta{Name: "instance", Namespace: "n", UID: "owner"}, Status: v1alpha1.OpenSearchServerlessStatus{BlockedProvisioningAttempts: []v1alpha1.ProvisioningAttempt{{Name: "pod", PodUID: "old", Role: "search", Zone: "a", Phase: "Blocked", Reason: "StartupTimeout", StartedAt: metav1.NewTime(time.Now())}}}}
	owner := true
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "pod", Namespace: "n", UID: "new", OwnerReferences: []metav1.OwnerReference{{UID: "owner", Controller: &owner}}}, Spec: corev1.PodSpec{NodeName: "node"}}
	core := kubernetesfake.NewSimpleClientset(pod)
	kube := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(instance).WithObjects(instance).Build()
	r := &InstanceReconciler{Client: kube, Core: core.CoreV1()}
	if _, changed, err := r.observeIsolatedAttempts(ctx, instance); err != nil || !changed {
		t.Fatalf("replacement not recorded: %v", err)
	}
	if err := kube.Get(ctx, types.NamespacedName{Namespace: "n", Name: "instance"}, instance); err != nil || instance.Status.BlockedProvisioningAttempts[0].Reason != "IdentityChanged" {
		t.Fatal("replacement was accepted")
	}
}

func TestIsolatedMissingPodBecomesExplicitRetryHold(t *testing.T) {
	ctx := context.Background()
	scheme := runtime.NewScheme()
	if err := v1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	instance := &v1alpha1.OpenSearchServerless{ObjectMeta: metav1.ObjectMeta{Name: "instance", Namespace: "n", UID: "owner"}, Status: v1alpha1.OpenSearchServerlessStatus{BlockedProvisioningAttempts: []v1alpha1.ProvisioningAttempt{{Name: "pod", PodUID: "old", Role: "search", Zone: "a", Phase: "Blocked", Reason: "StartupTimeout", StartedAt: metav1.NewTime(time.Now())}}}}
	core := kubernetesfake.NewSimpleClientset()
	kube := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(instance).WithObjects(instance).Build()
	r := &InstanceReconciler{Client: kube, Core: core.CoreV1()}
	if _, changed, err := r.observeIsolatedAttempts(ctx, instance); err != nil || !changed {
		t.Fatalf("missing Pod not recorded: %v", err)
	}
	if err := kube.Get(ctx, types.NamespacedName{Namespace: "n", Name: "instance"}, instance); err != nil || instance.Status.BlockedProvisioningAttempts[0].Reason != "PodGone" {
		t.Fatal("missing Pod did not require explicit retry")
	}
}
