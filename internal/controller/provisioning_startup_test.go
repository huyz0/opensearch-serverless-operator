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
	kubetesting "k8s.io/client-go/testing"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/huyz0/opensearch-serverless-operator/api/v1alpha1"
	"github.com/huyz0/opensearch-serverless-operator/internal/workload"
)

func TestProvisioningWithdrawalRequiresCurrentObservation(t *testing.T) {
	ctx := context.Background()
	scheme := runtime.NewScheme()
	if err := v1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name, reason, holdReason string
		changeRole, race         bool
	}{
		{name: "observed Pod", reason: "PendingTimeout"},
		{name: "changed placement", reason: "PendingTimeout", changeRole: true, holdReason: "IdentityChanged"},
		{name: "unrecognized withdrawal", reason: "Other", holdReason: "InvalidAttempt"},
		{name: "changed version during withdrawal", reason: "PendingTimeout", race: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			instance := &v1alpha1.OpenSearchServerless{ObjectMeta: metav1.ObjectMeta{Name: "instance", Namespace: "n", UID: "owner"}}
			instance.Status.ProvisioningAttempt = &v1alpha1.ProvisioningAttempt{
				Name: "pod", PodUID: "uid", Role: "search", Zone: "a",
				StartedAt: metav1.NewTime(time.Unix(1000, 0)), Phase: "Withdrawing", Reason: tc.reason,
			}
			pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
				Name: "pod", Namespace: "n", UID: "uid", ResourceVersion: "1",
				OwnerReferences: []metav1.OwnerReference{*metav1.NewControllerRef(instance, v1alpha1.GroupVersion.WithKind("OpenSearchServerless"))},
				Labels:          map[string]string{workload.RoleLabel: "search", workload.ZoneLabel: "a"},
			}}
			if tc.changeRole {
				pod.Labels[workload.RoleLabel] = "ingest"
			}
			core := kubernetesfake.NewSimpleClientset(pod)
			if tc.race {
				reads := 0
				core.PrependReactor("get", "pods", func(kubetesting.Action) (bool, runtime.Object, error) {
					reads++
					if reads == 2 {
						changed := pod.DeepCopy()
						changed.Labels[workload.ZoneLabel] = "b"
						changed.ResourceVersion = "2"
						return true, changed, nil
					}
					return false, nil, nil
				})
			}
			kube := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(instance).WithObjects(instance).Build()
			r := &InstanceReconciler{Client: kube, Core: core.CoreV1()}
			_, _, err := r.reconcileProvisioningAttempt(ctx, instance)
			if (err != nil) != tc.race {
				t.Fatalf("unexpected withdrawal result: %v", err)
			}
			if tc.holdReason != "" && (instance.Status.ProvisioningAttempt.Phase != "Blocked" || instance.Status.ProvisioningAttempt.Reason != tc.holdReason) {
				t.Fatalf("wrong hold: %#v", instance.Status.ProvisioningAttempt)
			}
			deleted := false
			for _, action := range core.Actions() {
				if deletion, ok := action.(kubetesting.DeleteAction); ok {
					preconditions := deletion.GetDeleteOptions().Preconditions
					if preconditions == nil || preconditions.UID == nil || *preconditions.UID != pod.UID ||
						preconditions.ResourceVersion == nil || *preconditions.ResourceVersion != pod.ResourceVersion {
						t.Fatal("withdrawal did not pin the qualified Pod UID and version")
					}
					deleted = true
				}
			}
			if deleted != (!tc.race && tc.holdReason == "") {
				t.Fatalf("unexpected Pod deletion: %t", deleted)
			}
		})
	}
}

func TestStartupHoldAndTerminalPodRecovery(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	scheme := runtime.NewScheme()
	if err := v1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	instance := &v1alpha1.OpenSearchServerless{ObjectMeta: metav1.ObjectMeta{Name: "instance", Namespace: "n", UID: "owner"}, Status: v1alpha1.OpenSearchServerlessStatus{ProvisioningAttempt: &v1alpha1.ProvisioningAttempt{Name: "pod", PodUID: "uid", StartedAt: metav1.NewTime(now.Add(-time.Hour)), Phase: "Watching"}}}
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "pod", Namespace: "n", ResourceVersion: "1", UID: "uid", OwnerReferences: []metav1.OwnerReference{*metav1.NewControllerRef(instance, v1alpha1.GroupVersion.WithKind("OpenSearchServerless"))}}, Spec: corev1.PodSpec{NodeName: "node"}}
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
	if err := kube.Get(ctx, key, instance); err != nil || instance.Status.ProvisioningAttempt.Phase != "CheckingStartup" {
		t.Fatal("bound unready Pod did not retain slot")
	}
	now = now.Add(2 * time.Minute)
	step()
	if err := kube.Get(ctx, key, instance); err != nil || instance.Status.ProvisioningAttempt.Reason != "StartupTimeout" {
		t.Fatal("startup timeout did not hold")
	}
	pod.Status.Phase = corev1.PodRunning
	pod.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}
	if _, err := core.CoreV1().Pods("n").UpdateStatus(ctx, pod, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	step()
	if err := kube.Get(ctx, key, instance); err != nil || instance.Status.ProvisioningAttempt != nil {
		t.Fatal("Ready Pod did not clear startup hold")
	}
	instance.Status.ProvisioningAttempt = &v1alpha1.ProvisioningAttempt{Name: "pod", PodUID: "uid", StartedAt: metav1.NewTime(now), Phase: "CheckingStartup"}
	if err := kube.Status().Update(ctx, instance); err != nil {
		t.Fatal(err)
	}
	pod.Status.Phase = corev1.PodFailed
	if _, err := core.CoreV1().Pods("n").UpdateStatus(ctx, pod, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	step()
	if err := kube.Get(ctx, key, instance); err != nil || instance.Status.ProvisioningAttempt.Reason != "PodTerminal" {
		t.Fatal("terminal Pod did not hold")
	}
	if err := core.CoreV1().Pods("n").Delete(ctx, "pod", metav1.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	step()
	if err := kube.Get(ctx, key, instance); err != nil || instance.Status.ProvisioningAttempt.Reason != "PodGone" {
		t.Fatal("removed terminal Pod did not transition to recoverable hold")
	}
}

func TestStartupReadyRequiresRunningAndReady(t *testing.T) {
	pod := &corev1.Pod{Status: corev1.PodStatus{Phase: corev1.PodPending, Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}}}
	if podStartupReady(pod) {
		t.Fatal("pending Pod accepted")
	}
	pod.Status.Phase = corev1.PodRunning
	if !podStartupReady(pod) {
		t.Fatal("running Ready Pod rejected")
	}
	pod.Status.Conditions = append(pod.Status.Conditions, pod.Status.Conditions[0])
	if podStartupReady(pod) {
		t.Fatal("ambiguous Ready conditions accepted")
	}
}
