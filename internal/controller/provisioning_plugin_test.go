package controller

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/huyz0/opensearch-serverless-operator/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	kubernetesfake "k8s.io/client-go/kubernetes/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestBoundPluginInstallTimeoutHoldsAndRecovers(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	scheme := runtime.NewScheme()
	if err := v1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	instance := &v1alpha1.OpenSearchServerless{ObjectMeta: metav1.ObjectMeta{Name: "instance", Namespace: "n", UID: "owner"}, Status: v1alpha1.OpenSearchServerlessStatus{ProvisioningAttempt: &v1alpha1.ProvisioningAttempt{Name: "pod", PodUID: "pod-uid", StartedAt: metav1.NewTime(now.Add(-time.Hour)), Phase: "Watching", PluginInstall: &v1alpha1.PluginInstallIntent{RequestUID: "request-uid", PolicyUID: "policy-uid"}}}}
	identity, _ := json.Marshal(v1alpha1.PluginInstallIntent{RequestUID: "request-uid", PolicyUID: "policy-uid"})
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "pod", Namespace: "n", ResourceVersion: "1", UID: "pod-uid", Annotations: map[string]string{pluginIntentAnnotation: string(identity)}, OwnerReferences: []metav1.OwnerReference{*metav1.NewControllerRef(instance, v1alpha1.GroupVersion.WithKind("OpenSearchServerless"))}}, Spec: corev1.PodSpec{NodeName: "node"}}
	core := kubernetesfake.NewSimpleClientset(pod)
	kube := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(instance).WithObjects(instance).Build()
	r := &InstanceReconciler{Client: kube, Core: core.CoreV1(), DevelopmentPendingTimeout: time.Minute, Now: func() time.Time { return now }}
	key := types.NamespacedName{Namespace: "n", Name: "instance"}
	step := func() {
		t.Helper()
		if err := kube.Get(ctx, key, instance); err != nil {
			t.Fatal(err)
		}
		if _, _, err := r.reconcileProvisioningAttempt(ctx, instance); err != nil {
			t.Fatal(err)
		}
	}
	step()
	if err := kube.Get(ctx, key, instance); err != nil || instance.Status.ProvisioningAttempt.Phase != "CheckingPluginInstall" || !instance.Status.ProvisioningAttempt.StartedAt.Equal(&metav1.Time{Time: now}) {
		t.Fatal("binding must start a fresh installer observation deadline")
	}
	now = now.Add(2 * time.Minute)
	step()
	if err := kube.Get(ctx, key, instance); err != nil || instance.Status.ProvisioningAttempt.Phase != "Blocked" || instance.Status.ProvisioningAttempt.Reason != "PluginInstallTimeout" {
		t.Fatal("stuck installer did not hold provisioning")
	}
	step()
	if _, err := core.CoreV1().Pods("n").Get(ctx, "pod", metav1.GetOptions{}); err != nil {
		t.Fatal("bound Pod must be retained")
	}
	pod.Status.InitContainerStatuses = []corev1.ContainerStatus{{Name: "plugin-installer", State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ExitCode: 1}}}}
	if _, err := core.CoreV1().Pods("n").UpdateStatus(ctx, pod, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	step()
	if err := kube.Get(ctx, key, instance); err != nil || instance.Status.ProvisioningAttempt == nil {
		t.Fatal("failed installer must not release hold")
	}
	pod.Status.InitContainerStatuses[0].State.Terminated.ExitCode = 0
	pod.Status.Phase = corev1.PodRunning
	pod.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}
	if _, err := core.CoreV1().Pods("n").UpdateStatus(ctx, pod, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	step()
	if err := kube.Get(ctx, key, instance); err != nil || instance.Status.ProvisioningAttempt != nil {
		t.Fatal("successful installation must clear the hold")
	}
}

func TestPluginInstallerCompleteRequiresOneSuccessfulStatus(t *testing.T) {
	status := corev1.ContainerStatus{Name: "plugin-installer", State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ExitCode: 0}}}
	if pluginInstallerComplete(&corev1.Pod{}) || pluginInstallerComplete(&corev1.Pod{Status: corev1.PodStatus{InitContainerStatuses: []corev1.ContainerStatus{status, status}}}) || !pluginInstallerComplete(&corev1.Pod{Status: corev1.PodStatus{InitContainerStatuses: []corev1.ContainerStatus{status}}}) {
		t.Fatal("incorrect installer status qualification")
	}
}
