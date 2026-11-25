//go:build envtest

package controller

import (
	"context"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/huyz0/opensearch-serverless-operator/api/v1alpha1"
)

func testPreviewYieldsToServingRecovery(t *testing.T, c client.Client, kube kubernetes.Interface, namespace string) {
	for _, stage := range []string{"before-creation", "after-creation", "release-reserved", "release-wins", "history-guard", "released", "binding-wins", "serving-recovers", "timeout-first", "released-rate-limit"} {
		t.Run(stage, func(t *testing.T) {
			ctx := context.Background()
			image := "example/image@sha256:" + strings.Repeat("b", 64)
			instance := &v1alpha1.OpenSearchServerless{
				ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: "preview-" + stage},
				Spec: v1alpha1.OpenSearchServerlessSpec{
					Image: image, ServiceAccountName: "runtime",
					Storage: v1alpha1.Storage{Type: "s3", Bucket: "bucket", Region: "region"},
					Pools: map[string]v1alpha1.Pool{"search": {
						WarmOnJoin: true, JVMHeap: "512Mi", Zones: map[string]v1alpha1.Zone{"a": {Replicas: 1, MinServing: 1}},
						Resources: corev1.ResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("1"), corev1.ResourceMemory: resource.MustParse("1Gi")}},
					}},
					Rollout: &v1alpha1.Rollout{Strategy: "BlueGreen", MaxPreviewReplicas: 1,
						Candidate: &v1alpha1.FleetCandidate{Fleet: "green", Image: image}},
				},
			}
			if err := c.Create(ctx, instance); err != nil {
				t.Fatal(err)
			}
			revision, err := podRevision(instance, "")
			if err != nil {
				t.Fatal(err)
			}
			instance.Status.PrimaryStore = &v1alpha1.PrimaryStoreStatus{Bucket: "bucket", Region: "region"}
			instance.Status.ServingFleetIntentByRole = map[string]string{"search": "initial", "ingest": "initial"}
			instance.Status.PodCreationSequence = 1
			if err := c.Status().Update(ctx, instance); err != nil {
				t.Fatal(err)
			}
			r := &InstanceReconciler{Client: c, Core: kube.CoreV1(), DevelopmentGatedPods: true, DevelopmentReleaseGates: true,
				DevelopmentWarmupImage: image, DevelopmentReleaseLimit: 1}
			released := stage == "released" || stage == "binding-wins" || stage == "serving-recovers" || stage == "timeout-first" || stage == "released-rate-limit"
			if released && stage != "released-rate-limit" {
				r.DevelopmentReleaseLimit = 2
			}
			for range 2 {
				if changed, err := r.reconcileCandidatePreview(ctx, instance); err != nil || !changed {
					t.Fatalf("stage preview Services: %v %v", changed, err)
				}
			}
			pods := kube.CoreV1().Pods(namespace)
			serving, err := renderGatedPod(instance, &v1alpha1.PodCreationIntent{
				Name: podNameForSequence(instance, 1), Sequence: 1, Generation: instance.Generation,
				Role: "search", Zone: "a", Fleet: "initial", Revision: revision,
			}, nil, "", "")
			if err != nil {
				t.Fatal(err)
			}
			serving.Spec.NodeName = "source-node"
			serving.Spec.SchedulingGates = nil
			serving, err = pods.Create(ctx, serving, metav1.CreateOptions{})
			if err != nil {
				t.Fatal(err)
			}
			serving.Status.Phase = corev1.PodRunning
			serving.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}
			serving, err = pods.UpdateStatus(ctx, serving, metav1.UpdateOptions{})
			if err != nil {
				t.Fatal(err)
			}
			reconcile := func() {
				t.Helper()
				if err := c.Get(ctx, client.ObjectKeyFromObject(instance), instance); err != nil {
					t.Fatal(err)
				}
				if _, err := r.reconcileGatedPod(ctx, instance); err != nil {
					t.Fatal(err)
				}
			}
			if stage == "history-guard" {
				instance.Status.PodCreationSequence = maxRetiredPodCreations
				for sequence := int64(2); sequence <= maxRetiredPodCreations; sequence++ {
					instance.Status.RetiredPodCreations = append(instance.Status.RetiredPodCreations, v1alpha1.RetiredPodCreation{
						Name: podNameForSequence(instance, sequence), Sequence: sequence, Role: "search", Zone: "a", Fleet: "old", Revision: revision,
					})
				}
				if err := c.Status().Update(ctx, instance); err != nil {
					t.Fatal(err)
				}
				for range 3 {
					reconcile()
					if instance.Status.PendingPodCreation != nil {
						t.Fatal("preview reserved the final retired-creation entry")
					}
				}
				return
			}
			for i := 0; i < 5 && instance.Status.PendingPodCreation == nil; i++ {
				reconcile()
			}
			intent := instance.Status.PendingPodCreation
			if intent == nil || intent.Fleet != "green" {
				t.Fatalf("preview creation was not staged: %#v", intent)
			}
			if stage != "before-creation" {
				reconcile()
				if instance.Status.PendingPodCreation != nil {
					t.Fatal("preview creation still reserved after observing the Pod")
				}
			}
			if released || stage == "release-reserved" || stage == "release-wins" {
				reconcile()
				if instance.Status.PendingGateRelease == nil || instance.Status.SearchWarmupSlot == nil {
					t.Fatal("preview gate release and warm-up slot were not reserved")
				}
			}
			if released {
				reconcile()
				reconcile()
				if attempt := instance.Status.ProvisioningAttempt; attempt == nil || attempt.Phase != "Watching" {
					t.Fatalf("preview was not already released: %#v", attempt)
				}
			}
			if stage == "timeout-first" {
				r.DevelopmentPendingTimeout = time.Second
				now := r.now().Add(2 * time.Second)
				r.Now = func() time.Time { return now }
				if _, changed, err := r.reconcileProvisioningAttempt(ctx, instance); !changed || err != nil || instance.Status.ProvisioningAttempt.Reason != "PendingTimeout" {
					t.Fatalf("preview did not reach its pending timeout before the serving disruption: %v", err)
				}
			}
			serving.Status.Conditions = append(serving.Status.Conditions, corev1.PodCondition{
				Type: corev1.DisruptionTarget, Status: corev1.ConditionTrue, Reason: "PreemptionByScheduler",
			})
			serving, err = pods.UpdateStatus(ctx, serving, metav1.UpdateOptions{})
			if err != nil {
				t.Fatal(err)
			}
			expectedCharges := 0
			if released {
				expectedCharges = 1
				if !exerciseReleasedPreviewWithdrawal(t, ctx, r, instance, serving, stage) {
					return
				}
			} else {
				reconcile()
			}
			if len(instance.Status.RetiredPodCreations) != 1 || instance.Status.RetiredPodCreations[0].Name != intent.Name {
				t.Fatalf("preview reservation was not retired: %#v", instance.Status)
			}
			if stage == "release-wins" {
				preview, err := pods.Get(ctx, intent.Name, metav1.GetOptions{})
				if err != nil {
					t.Fatal(err)
				}
				preview.Spec.SchedulingGates = nil
				if _, err := pods.Update(ctx, preview, metav1.UpdateOptions{}); err != nil {
					t.Fatal(err)
				}
				reconcile()
				active := instance.Status.ProvisioningAttempt
				if active == nil || active.PodUID != string(preview.UID) || instance.Status.PendingGateRelease != nil ||
					instance.Status.SearchWarmupSlot == nil || !instance.Status.RetiredPodCreations[0].Released || len(instance.Status.RecentPodReleases) != 1 {
					t.Fatalf("released preview was not kept under its existing reservation: %#v", instance.Status)
				}
				instance.Spec.Rollout.Candidate = nil
				if err := c.Update(ctx, instance); err != nil {
					t.Fatal(err)
				}
				for i := 0; i < 4 && instance.Status.ProvisioningAttempt != nil; i++ {
					if _, _, err := r.reconcileProvisioningAttempt(ctx, instance); err != nil {
						t.Fatal(err)
					}
				}
				if instance.Status.ProvisioningAttempt != nil || instance.Status.SearchWarmupSlot != nil || len(instance.Status.RetiredPodCreations) != 1 ||
					instance.Status.RetiredPodCreations[0].Released || instance.Status.RetiredPodCreations[0].PodUID != "" || len(instance.Status.RecentPodReleases) != 1 {
					t.Fatalf("cancellation did not reuse the original tombstone and retain the release charge: %#v", instance.Status)
				}
				return
			}
			for i := 0; i < 5 && instance.Status.PendingGateRelease != nil; i++ {
				reconcile()
			}
			if instance.Status.PendingPodCreation != nil || instance.Status.PendingGateRelease != nil || instance.Status.SearchWarmupSlot != nil {
				t.Fatalf("preview still reserves recovery capacity: %#v", instance.Status)
			}
			if len(instance.Status.RecentPodReleases) != expectedCharges {
				t.Fatalf("withdrawal left %d release charges, want %d", len(instance.Status.RecentPodReleases), expectedCharges)
			}
			if _, err := pods.Get(ctx, intent.Name, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
				t.Fatalf("unreleased preview Pod survived withdrawal: %v", err)
			}
			reconcile()
			if instance.Status.PendingPodCreation != nil {
				t.Fatalf("disruption alone created a second Pod: %#v", instance.Status.PendingPodCreation)
			}
			zero := int64(0)
			if err := pods.Delete(ctx, serving.Name, metav1.DeleteOptions{GracePeriodSeconds: &zero}); err != nil {
				t.Fatal(err)
			}
			if stage == "released-rate-limit" {
				reconcile()
				if instance.Status.PendingPodCreation != nil {
					t.Fatal("serving replacement bypassed the role and zone rate limit")
				}
				window, _ := r.releaseBudget()
				now := r.now().Add(window + 2*time.Second)
				r.Now = func() time.Time { return now }
			}
			reconcile()
			if next := instance.Status.PendingPodCreation; next == nil || next.Fleet != "initial" || next.Name == intent.Name {
				t.Fatalf("serving replacement did not acquire the freed slot: %#v", next)
			}
		})
	}
}

// exerciseReleasedPreviewWithdrawal returns true when the preview is gone and
// the fixture can verify which workload gets the next provisioning slot.
func exerciseReleasedPreviewWithdrawal(t *testing.T, ctx context.Context, r *InstanceReconciler, instance *v1alpha1.OpenSearchServerless, serving *corev1.Pod, stage string) bool {
	t.Helper()
	pods := r.Core.Pods(instance.Namespace)
	provision := func() {
		t.Helper()
		if _, changed, err := r.reconcileProvisioningAttempt(ctx, instance); !changed || err != nil {
			t.Fatalf("reconcile released preview: changed=%v, error=%v", changed, err)
		}
	}
	provision()
	attempt := instance.Status.ProvisioningAttempt
	if attempt == nil || attempt.Phase != "Withdrawing" || attempt.Reason != previewRecoveryReason || attempt.Cancellation == nil {
		t.Fatalf("preempting a released preview did not persist withdrawal first: %#v", attempt)
	}
	if _, err := pods.Get(ctx, attempt.Name, metav1.GetOptions{}); err != nil {
		t.Fatalf("preview disappeared before withdrawal was durable: %v", err)
	}
	switch stage {
	case "binding-wins":
		if err := pods.Bind(ctx, &corev1.Binding{
			ObjectMeta: metav1.ObjectMeta{Name: attempt.Name, Namespace: instance.Namespace}, Target: corev1.ObjectReference{Kind: "Node", Name: "preview-node"},
		}, metav1.CreateOptions{}); err != nil {
			t.Fatal(err)
		}
	case "serving-recovers":
		serving.Status.Conditions[1].Status = corev1.ConditionFalse
		if _, err := pods.UpdateStatus(ctx, serving, metav1.UpdateOptions{}); err != nil {
			t.Fatal(err)
		}
	}
	provision()
	if stage == "released" || stage == "timeout-first" || stage == "released-rate-limit" {
		provision()
		if instance.Status.ProvisioningAttempt != nil || instance.Status.SearchWarmupSlot != nil {
			t.Fatal("deleted preview retained the provisioning or warm-up slot")
		}
		return true
	}
	preview, err := pods.Get(ctx, attempt.Name, metav1.GetOptions{})
	if err != nil || string(preview.UID) != attempt.PodUID || preview.DeletionTimestamp != nil || attempt.Phase != "Watching" || attempt.Cancellation != nil {
		t.Fatalf("canceling withdrawal did not keep the same preview: %v", err)
	}
	return false
}
