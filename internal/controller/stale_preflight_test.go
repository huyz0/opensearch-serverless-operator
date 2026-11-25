package controller

import (
	"context"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	ctrl "sigs.k8s.io/controller-runtime"
	clientfake "sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/huyz0/opensearch-serverless-operator/api/v1alpha1"
	"github.com/huyz0/opensearch-serverless-operator/internal/workload"
)

func TestStalePreflightRetiresWithoutCurrentSources(t *testing.T) {
	instance := &v1alpha1.OpenSearchServerless{ObjectMeta: metav1.ObjectMeta{Name: "catalog", Namespace: "n", UID: "uid", Generation: 2, ResourceVersion: "1"}, Spec: v1alpha1.OpenSearchServerlessSpec{Security: &v1alpha1.SecurityConfig{}}, Status: v1alpha1.OpenSearchServerlessStatus{PodCreationSequence: 1, ServingFleetIntentByRole: map[string]string{"search": "initial"}}}
	instance.Status.PendingPodCreation = &v1alpha1.PodCreationIntent{Name: podNameForSequence(instance, 1), Sequence: 1, Role: "search", Zone: "a", Fleet: "initial", Revision: "old", Generation: 1}
	scheme := runtime.NewScheme()
	if err := v1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	kube := clientfake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(instance).WithObjects(instance.DeepCopyObject().(*v1alpha1.OpenSearchServerless)).Build()
	// Deliberately inconsistent DNS qualification means current source
	// resolution cannot be used, but the generation already proves staleness.
	r := &InstanceReconciler{Client: kube, Core: fake.NewSimpleClientset().CoreV1(), DevelopmentPeerDNSImage: "missing-domain", DevelopmentGatedPods: true}
	if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: instance.Namespace, Name: instance.Name}}); err != nil {
		t.Fatal(err)
	}
	if err := kube.Get(context.Background(), types.NamespacedName{Namespace: instance.Namespace, Name: instance.Name}, instance); err != nil {
		t.Fatal(err)
	}
	if instance.Status.PendingPodCreation != nil || len(instance.Status.RetiredPodCreations) != 1 {
		t.Fatal("stale creation was not retired")
	}
}

func TestStalePreflightRejectsAmbiguousBootstrap(t *testing.T) {
	instance := &v1alpha1.OpenSearchServerless{ObjectMeta: metav1.ObjectMeta{Name: "catalog", UID: "uid", Generation: 2}, Status: v1alpha1.OpenSearchServerlessStatus{ServingFleetIntentByRole: map[string]string{"search": "initial"}, PendingPodCreation: &v1alpha1.PodCreationIntent{Name: "old", Role: "search", Fleet: "initial", Generation: 1, WarmupMode: "Bootstrap"}}}
	r := &InstanceReconciler{}
	if _, handled, err := r.recoverStalePreflight(context.Background(), instance); !handled || err == nil {
		t.Fatal("missing bootstrap identity must hold stale recovery")
	}
}

func TestExpiredGateReleaseSettlesWithoutCurrentInputs(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := v1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	now := time.Unix(1000, 0)
	started := metav1.NewTime(now.Add(-time.Minute))
	for _, test := range []struct {
		name         string
		exists       bool
		gated        bool
		releaseRaces bool
	}{
		{name: "withdraw gated Pod", exists: true, gated: true},
		{name: "remember missing Pod"},
		{name: "observe already released Pod", exists: true},
		{name: "observe release racing withdrawal", exists: true, gated: true, releaseRaces: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			instance := &v1alpha1.OpenSearchServerless{
				ObjectMeta: metav1.ObjectMeta{Name: "catalog", Namespace: "n", UID: "owner", ResourceVersion: "1", Generation: 1},
				Status:     v1alpha1.OpenSearchServerlessStatus{PodCreationSequence: 1, ServingFleetIntentByRole: map[string]string{"search": "initial"}},
			}
			name := podNameForSequence(instance, 1)
			instance.Status.PendingGateRelease = &v1alpha1.GateReleaseIntent{PodUID: "pod-uid", StartedAt: started,
				Pod: v1alpha1.PodCreationIntent{Name: name, Sequence: 1, Generation: 1, Role: "search", Zone: "a", Fleet: "initial", Revision: "recorded"}}
			instance.Status.SearchWarmupSlot = &v1alpha1.SearchWarmupSlot{Name: name, PodUID: "pod-uid"}
			instance.Status.RecentPodReleases = []v1alpha1.PodReleaseRecord{{PodUID: "pod-uid", Role: "search", Zone: "a", StartedAt: started}}
			core := fake.NewSimpleClientset()
			if test.exists {
				controller := true
				pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "n", UID: "pod-uid", ResourceVersion: "1",
					OwnerReferences: []metav1.OwnerReference{{APIVersion: v1alpha1.GroupVersion.String(), Kind: "OpenSearchServerless", Name: instance.Name, UID: instance.UID, Controller: &controller}},
					Annotations:     map[string]string{creationIntentKey: "owner/1"},
					Labels:          map[string]string{workload.InstanceLabel: "owner", workload.FleetLabel: "initial", workload.RoleLabel: "search", workload.ZoneLabel: "a", podRevisionLabel: "recorded"},
				}}
				if test.gated {
					pod.Spec.SchedulingGates = []corev1.PodSchedulingGate{{Name: "serverless.opensearch.org/placement"}}
				}
				if _, err := core.CoreV1().Pods("n").Create(ctx, pod, metav1.CreateOptions{}); err != nil {
					t.Fatal(err)
				}
			}
			kube := clientfake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(instance).WithObjects(instance).Build()
			r := &InstanceReconciler{Client: kube, Core: core.CoreV1(), DevelopmentGatedPods: true, DevelopmentPendingTimeout: time.Minute, Now: func() time.Time { return now }}
			key := types.NamespacedName{Namespace: instance.Namespace, Name: instance.Name}
			step := func() {
				t.Helper()
				if result, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: key}); err != nil || !result.Requeue {
					t.Fatalf("recovery did not advance: %+v, %v", result, err)
				}
				if err := kube.Get(ctx, key, instance); err != nil {
					t.Fatal(err)
				}
			}
			assertReleased := func() {
				t.Helper()
				attempt := instance.Status.ProvisioningAttempt
				if instance.Status.PendingGateRelease != nil || attempt == nil || attempt.PodUID != "pod-uid" || attempt.Phase != "Watching" {
					t.Fatalf("release was not recovered: %+v", attempt)
				}
				if _, err := core.CoreV1().Pods("n").Get(ctx, name, metav1.GetOptions{}); err != nil {
					t.Fatalf("released Pod was withdrawn: %v", err)
				}
			}
			step()
			if test.exists && !test.gated {
				assertReleased()
				return
			}
			if pending := instance.Status.PendingGateRelease; pending == nil || !pending.WithdrawalRequested || len(instance.Status.RetiredPodCreations) != 1 {
				t.Fatal("expired release was not durably retired")
			}
			if test.gated {
				pod, err := core.CoreV1().Pods("n").Get(ctx, name, metav1.GetOptions{})
				if err != nil {
					t.Fatalf("Pod removed before withdrawal was persisted: %v", err)
				}
				if test.releaseRaces {
					pod.Spec.SchedulingGates = nil
					if _, err := core.CoreV1().Pods("n").Update(ctx, pod, metav1.UpdateOptions{}); err != nil {
						t.Fatal(err)
					}
					step()
					assertReleased()
					if record := instance.Status.RetiredPodCreations[0]; !record.Released || record.PodUID != string(pod.UID) {
						t.Fatalf("released identity was omitted from the retirement record: %+v", record)
					}
					return
				}
				step()
			}
			if _, err := core.CoreV1().Pods("n").Get(ctx, name, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
				t.Fatalf("expected expired Pod to be absent: %v", err)
			}
			step()
			if instance.Status.PendingGateRelease != nil || instance.Status.SearchWarmupSlot != nil || instance.Status.ProvisioningAttempt != nil || len(instance.Status.RecentPodReleases) != 1 {
				t.Fatal("expiry did not free the slot while retaining the release reservation")
			}
		})
	}
}
