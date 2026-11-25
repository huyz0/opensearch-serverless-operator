package controller

import (
	"context"
	"fmt"
	"testing"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	clientfake "sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/huyz0/opensearch-serverless-operator/api/v1alpha1"
	"github.com/huyz0/opensearch-serverless-operator/internal/workload"
)

func TestStaleCreationOnlyWithdrawsMatchingGatedPod(t *testing.T) {
	instance := &v1alpha1.OpenSearchServerless{ObjectMeta: metav1.ObjectMeta{Name: "catalog", Namespace: "n", UID: "instance", Generation: 2, ResourceVersion: "1"}, Status: v1alpha1.OpenSearchServerlessStatus{PodCreationSequence: 1}}
	intent := &v1alpha1.PodCreationIntent{Name: podNameForSequence(instance, 1), Role: "search", Zone: "a", Fleet: "initial", Revision: "old", Generation: 1, Sequence: 1}
	instance.Status.PendingPodCreation = intent
	owner := true
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: intent.Name, Namespace: "n", UID: "pod-uid", ResourceVersion: "12", OwnerReferences: []metav1.OwnerReference{{APIVersion: v1alpha1.GroupVersion.String(), Kind: "OpenSearchServerless", Name: instance.Name, UID: instance.UID, Controller: &owner}}, Annotations: map[string]string{creationIntentKey: fmt.Sprintf("%s/%d", instance.UID, intent.Sequence)}, Labels: map[string]string{workload.InstanceLabel: string(instance.UID), workload.RoleLabel: intent.Role, workload.ZoneLabel: intent.Zone, workload.FleetLabel: intent.Fleet, "serverless.opensearch.org/revision": intent.Revision}}, Spec: corev1.PodSpec{SchedulingGates: []corev1.PodSchedulingGate{{Name: "serverless.opensearch.org/placement"}}}}
	scheme := runtime.NewScheme()
	if err := v1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	for _, scenario := range []struct {
		name   string
		mutate func(*corev1.Pod)
	}{
		{"wrong revision", func(p *corev1.Pod) { p.Labels["serverless.opensearch.org/revision"] = "other" }},
		{"wrong owner", func(p *corev1.Pod) { p.OwnerReferences[0].UID = "other" }},
		{"gate released", func(p *corev1.Pod) { p.Spec.SchedulingGates = nil }},
		{"bound", func(p *corev1.Pod) { p.Spec.NodeName = "node" }},
		{"wrong intent", func(p *corev1.Pod) { p.Annotations[creationIntentKey] = "other/1" }},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			original := instance.DeepCopyObject().(*v1alpha1.OpenSearchServerless)
			changed := pod.DeepCopy()
			scenario.mutate(changed)
			kube := clientfake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(original).WithObjects(original.DeepCopyObject().(*v1alpha1.OpenSearchServerless)).Build()
			r := &InstanceReconciler{Client: kube, Core: fake.NewSimpleClientset(changed).CoreV1()}
			if _, err := r.recoverStaleGatedCreation(context.Background(), original); err == nil {
				t.Fatal("conflicting stale Pod accepted")
			}
			if original.Status.PendingPodCreation == nil || original.Status.PendingGateRelease != nil {
				t.Fatal("conflicting Pod changed recovery state")
			}
		})
	}
	missing := instance.DeepCopyObject().(*v1alpha1.OpenSearchServerless)
	missingClient := clientfake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(missing).WithObjects(missing.DeepCopyObject().(*v1alpha1.OpenSearchServerless)).Build()
	r := &InstanceReconciler{Client: missingClient, Core: fake.NewSimpleClientset().CoreV1()}
	if _, err := r.recoverStaleGatedCreation(context.Background(), missing); err != nil {
		t.Fatal(err)
	}
	if missing.Status.PendingPodCreation != nil || len(missing.Status.RetiredPodCreations) != 1 || missing.Status.RetiredPodCreations[0].Name != intent.Name {
		t.Fatal("missing Pod name was not durably retired")
	}
	copy := missing.DeepCopyObject().(*v1alpha1.OpenSearchServerless)
	copy.Status.RetiredPodCreations[0].Name = "changed"
	if missing.Status.RetiredPodCreations[0].Name == "changed" {
		t.Fatal("retired creation records must be deep copied")
	}
	full := instance.DeepCopyObject().(*v1alpha1.OpenSearchServerless)
	full.Status.RetiredPodCreations = make([]v1alpha1.RetiredPodCreation, 64)
	if _, err := r.recoverStaleGatedCreation(context.Background(), full); err == nil || full.Status.PendingPodCreation == nil {
		t.Fatal("full retired history must hold new retirement")
	}
	kube := clientfake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(instance).WithObjects(instance.DeepCopyObject().(*v1alpha1.OpenSearchServerless)).Build()
	r = &InstanceReconciler{Client: kube, Core: fake.NewSimpleClientset(pod).CoreV1()}
	if _, err := r.recoverStaleGatedCreation(context.Background(), instance); err != nil {
		t.Fatal(err)
	}
	if instance.Status.PendingPodCreation != nil || instance.Status.PendingGateRelease == nil || !instance.Status.PendingGateRelease.WithdrawalRequested || instance.Status.PendingGateRelease.PodUID != string(pod.UID) {
		t.Fatal("matching Pod was not pinned for withdrawal")
	}
	for _, released := range []bool{false, true} {
		t.Run(fmt.Sprintf("blocked gate released=%t", released), func(t *testing.T) {
			ctx := context.Background()
			held := instance.DeepCopyObject().(*v1alpha1.OpenSearchServerless)
			held.Status.PendingGateRelease, held.Status.RetiredPodCreations = nil, nil
			held.Status.BlockedProvisioningAttempts = []v1alpha1.ProvisioningAttempt{{
				Name: "earlier-attempt", PodUID: "earlier", Role: "search", Zone: "a", StartedAt: metav1.Now(), Phase: "Blocked", Reason: "PodGone",
			}}
			c := clientfake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(held).WithObjects(held).Build()
			core := fake.NewSimpleClientset(pod.DeepCopy()).CoreV1()
			r := &InstanceReconciler{Client: c, Core: core}
			if _, err := r.stageGatedWithdrawal(ctx, held, pod); err != nil || held.Status.PendingGateRelease == nil {
				t.Fatalf("failed to stage withdrawal: %v", err)
			}
			if released {
				racingPod := pod.DeepCopy()
				racingPod.Spec.SchedulingGates = nil
				if _, err := core.Pods("n").Update(ctx, racingPod, metav1.UpdateOptions{}); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := r.reconcileGateRelease(ctx, held, ""); err != nil {
				t.Fatal(err)
			}
			_, err := core.Pods("n").Get(ctx, pod.Name, metav1.GetOptions{})
			if released {
				if err != nil || held.Status.ProvisioningAttempt == nil || !held.Status.RetiredPodCreations[0].Released {
					t.Fatalf("the concurrently released Pod was not preserved for tracking: %v", err)
				}
				if _, changed, err := r.reconcileRetiredPodCreations(ctx, held); err != nil || changed {
					t.Fatalf("retired fence interrupted tracking of the pinned released UID: %v", err)
				}
				if err := core.Pods("n").Delete(ctx, pod.Name, metav1.DeleteOptions{}); err != nil {
					t.Fatal(err)
				}
				if _, changed, err := r.reconcileRetiredPodCreations(ctx, held); err != nil || changed || held.Status.RetiredPodCreations[0].PodUID != string(pod.UID) {
					t.Fatalf("retired fence cleared before the provisioning observer accounted for the released UID: %v", err)
				}
				return
			}
			if !apierrors.IsNotFound(err) {
				t.Fatalf("blocked gated Pod was not withdrawn: %v", err)
			}
			if _, err := r.reconcileGateRelease(ctx, held, ""); err != nil || held.Status.PendingGateRelease != nil ||
				len(held.Status.BlockedProvisioningAttempts) != 1 || len(held.Status.RetiredPodCreations) != 1 {
				t.Fatalf("withdrawal did not free the pending slot and preserve the zone hold: %v", err)
			}
		})
	}
}
