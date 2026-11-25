package controller

import (
	"context"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	kubernetesfake "k8s.io/client-go/kubernetes/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/huyz0/opensearch-serverless-operator/api/v1alpha1"
)

func TestBoundDNSPreflightTimeoutHoldsAndRecovers(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	scheme := runtime.NewScheme()
	if err := v1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	instance := &v1alpha1.OpenSearchServerless{ObjectMeta: metav1.ObjectMeta{Name: "instance", Namespace: "n", UID: "owner"}, Status: v1alpha1.OpenSearchServerlessStatus{ProvisioningAttempt: &v1alpha1.ProvisioningAttempt{Name: "pod", PodUID: "pod-uid", StartedAt: metav1.NewTime(now.Add(-time.Hour)), Phase: "Watching", PeerDNS: true}}}
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "pod", Namespace: "n", ResourceVersion: "1", UID: "pod-uid", Annotations: map[string]string{peerDomainAnnotation: "cluster.local", peerDNSPreflightAnnotation: "v1"}, OwnerReferences: []metav1.OwnerReference{*metav1.NewControllerRef(instance, v1alpha1.GroupVersion.WithKind("OpenSearchServerless"))}}, Spec: corev1.PodSpec{NodeName: "node"}}
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
	if err := kube.Get(ctx, key, instance); err != nil || instance.Status.ProvisioningAttempt.Phase != "CheckingPeerDNS" || !instance.Status.ProvisioningAttempt.StartedAt.Equal(&metav1.Time{Time: now}) {
		t.Fatal("binding must start a fresh DNS observation deadline")
	}
	now = now.Add(2 * time.Minute)
	step()
	if err := kube.Get(ctx, key, instance); err != nil || instance.Status.ProvisioningAttempt.Phase != "Blocked" || instance.Status.ProvisioningAttempt.Reason != "PeerDNSPreflightTimeout" {
		t.Fatal("failed preflight did not hold further provisioning")
	}
	if _, err := core.CoreV1().Pods("n").Get(ctx, "pod", metav1.GetOptions{}); err != nil {
		t.Fatal("bound Pod must not be deleted")
	}
	step()
	pod.Status.InitContainerStatuses = []corev1.ContainerStatus{{Name: "peer-dns-preflight", State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ExitCode: 0}}}}
	pod.Status.Phase = corev1.PodRunning
	pod.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}
	if _, err := core.CoreV1().Pods("n").UpdateStatus(ctx, pod, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	step()
	if err := kube.Get(ctx, key, instance); err != nil || instance.Status.ProvisioningAttempt != nil {
		t.Fatal("completed preflight should clear the hold")
	}
}
