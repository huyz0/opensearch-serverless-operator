//go:build envtest

package controller

import (
	"context"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/huyz0/opensearch-serverless-operator/api/v1alpha1"
	"github.com/huyz0/opensearch-serverless-operator/internal/workload"
)

func testPodDisruptionSignal(t *testing.T, c client.Client, kube kubernetes.Interface, namespace string) {
	t.Helper()
	ctx := context.Background()
	instance := &v1alpha1.OpenSearchServerless{
		ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: "disruption-observation"},
		Spec: v1alpha1.OpenSearchServerlessSpec{
			Image: "example/image@sha256:" + strings.Repeat("a", 64), ServiceAccountName: "runtime",
			Storage: v1alpha1.Storage{Type: "s3", Bucket: "bucket", Region: "region"},
			Pools: map[string]v1alpha1.Pool{
				"search": {Zones: map[string]v1alpha1.Zone{"a": {Replicas: 1, MinServing: 1}}},
				"ingest": {Zones: map[string]v1alpha1.Zone{"b": {Replicas: 1, MinServing: 1}}},
			},
		},
	}
	if err := c.Create(ctx, instance); err != nil {
		t.Fatal(err)
	}
	instance.Status.ServingFleetIntentByRole = map[string]string{"search": "initial", "ingest": "initial"}
	if err := c.Status().Update(ctx, instance); err != nil {
		t.Fatal(err)
	}
	core := kube.CoreV1()
	pods := core.Pods(namespace)
	for role, zone := range map[string]string{"search": "a", "ingest": "b"} {
		pod, err := pods.Create(ctx, &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name: instance.Name + "-" + role, OwnerReferences: []metav1.OwnerReference{*metav1.NewControllerRef(instance, v1alpha1.GroupVersion.WithKind("OpenSearchServerless"))},
				Labels: map[string]string{workload.InstanceLabel: string(instance.UID), workload.RoleLabel: role,
					workload.ZoneLabel: zone, workload.FleetLabel: "initial", "serverless.opensearch.org/revision": "revision"},
			},
			Spec: corev1.PodSpec{NodeName: "node-" + zone, Containers: []corev1.Container{{Name: "jvm", Image: "example/image"}}},
		}, metav1.CreateOptions{})
		if err != nil {
			t.Fatal(err)
		}
		pod.Status.Phase = corev1.PodRunning
		pod.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}
		if role == "search" {
			pod.Status.Conditions = append(pod.Status.Conditions, corev1.PodCondition{Type: corev1.DisruptionTarget, Status: corev1.ConditionTrue, Reason: "PreemptionByScheduler"})
		}
		if _, err := pods.UpdateStatus(ctx, pod, metav1.UpdateOptions{}); err != nil {
			t.Fatal(err)
		}
	}
	r := &InstanceReconciler{Client: c, Core: core}
	operations := &OperationReconciler{Core: core}
	checkCapacity := func(want int32) {
		t.Helper()
		if changed, err := r.refreshZoneCapacity(ctx, instance); err != nil || !changed {
			t.Fatalf("refresh capacity = %v, %v", changed, err)
		}
		if err := c.Get(ctx, client.ObjectKeyFromObject(instance), instance); err != nil {
			t.Fatal(err)
		}
		capacity := instance.Status.ZoneCapacity
		if len(capacity) != 2 || capacity[0].Role != "ingest" || capacity[0].DisruptionTargetPods != 0 ||
			capacity[1].Role != "search" || capacity[1].Zone != "a" || capacity[1].DisruptionTargetPods != want || capacity[1].KubernetesReady != 1 ||
			capacity[1].ReadyDisruptionTargetPods != want || capacity[1].BelowMinServing || capacity[1].BelowMinServingAfterDisruption != (want == 1) {
			t.Fatalf("capacity after disruption signal = %#v", capacity)
		}
	}
	checkCapacity(1)
	if _, err := operations.observeSourceFleetRevision(ctx, instance, "search", "initial"); err == nil {
		t.Fatal("source fleet met its floor using a Pod marked for disruption")
	}
	if revision, err := operations.observeSourceFleetRevision(ctx, instance, "ingest", "initial"); err != nil || revision != "revision" {
		t.Fatalf("search disruption affected index qualification: %s %v", revision, err)
	}
	search, err := pods.Get(ctx, instance.Name+"-search", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	search.Status.Conditions[1].Status = corev1.ConditionFalse
	if _, err := pods.UpdateStatus(ctx, search, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	checkCapacity(0)
	if revision, err := operations.observeSourceFleetRevision(ctx, instance, "search", "initial"); err != nil || revision != "revision" {
		t.Fatalf("canceled disruption continued blocking the live Pod: %s %v", revision, err)
	}
}

func testDisruptionOrdersServingProvisioning(t *testing.T, c client.Client, kube kubernetes.Interface, namespace string) {
	t.Helper()
	ctx := context.Background()
	for _, test := range []struct {
		name, want string
		signal     corev1.ConditionStatus
		zonal      int32
		regional   int32
		spare      int32
	}{
		{"draining-regional", "search", corev1.ConditionTrue, 0, 1, 1},
		{"draining-zonal", "search", corev1.ConditionTrue, 1, 0, 1},
		{"cleared", "ingest", corev1.ConditionFalse, 1, 1, 1},
		{"at-target", "ingest", corev1.ConditionTrue, 1, 1, 0},
	} {
		t.Run(test.name, func(t *testing.T) {
			image := "example/image@sha256:" + strings.Repeat("a", 64)
			pool := v1alpha1.Pool{JVMHeap: "256Mi", Resources: corev1.ResourceRequirements{Requests: corev1.ResourceList{
				corev1.ResourceCPU: resource.MustParse("250m"), corev1.ResourceMemory: resource.MustParse("512Mi"),
			}}}
			search, ingest := pool, pool
			search.Zones = map[string]v1alpha1.Zone{"a": {Replicas: 1, WarmSpare: test.spare, MinServing: test.zonal}}
			ingest.Zones = map[string]v1alpha1.Zone{"b": {Replicas: 2}}
			availability := &v1alpha1.Availability{MaxPendingProvisionablePods: 1}
			if test.regional != 0 {
				availability.MinServingTotal = map[string]int32{"search": test.regional}
			}
			instance := &v1alpha1.OpenSearchServerless{
				ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: "priority-" + test.name},
				Spec: v1alpha1.OpenSearchServerlessSpec{Image: image, ServiceAccountName: "runtime",
					Storage:      v1alpha1.Storage{Type: "s3", Bucket: "bucket", Region: "region"},
					Pools:        map[string]v1alpha1.Pool{"search": search, "ingest": ingest},
					Availability: availability,
				},
			}
			if err := c.Create(ctx, instance); err != nil {
				t.Fatal(err)
			}
			instance.Status.PrimaryStore = &v1alpha1.PrimaryStoreStatus{Bucket: "bucket", Region: "region"}
			instance.Status.ServingFleetIntentByRole = map[string]string{"search": "initial", "ingest": "initial"}
			if err := c.Status().Update(ctx, instance); err != nil {
				t.Fatal(err)
			}
			revisions, err := podRevisions(instance, "")
			if err != nil {
				t.Fatal(err)
			}
			pods := kube.CoreV1().Pods(namespace)
			for role, zone := range map[string]string{"search": "a", "ingest": "b"} {
				pod, err := pods.Create(ctx, &corev1.Pod{
					ObjectMeta: metav1.ObjectMeta{Name: instance.Name + "-" + role,
						OwnerReferences: []metav1.OwnerReference{*metav1.NewControllerRef(instance, v1alpha1.GroupVersion.WithKind("OpenSearchServerless"))},
						Labels: map[string]string{workload.InstanceLabel: string(instance.UID), workload.RoleLabel: role,
							workload.ZoneLabel: zone, workload.FleetLabel: "initial", podRevisionLabel: revisions.current}},
					Spec: corev1.PodSpec{NodeName: "node-" + zone, Containers: []corev1.Container{{Name: "opensearch", Image: image}}},
				}, metav1.CreateOptions{})
				if err != nil {
					t.Fatal(err)
				}
				pod.Status.Phase = corev1.PodRunning
				pod.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}
				if role == "search" {
					pod.Status.Conditions = append(pod.Status.Conditions, corev1.PodCondition{Type: corev1.DisruptionTarget, Status: test.signal, Reason: "PreemptionByScheduler"})
				}
				if _, err := pods.UpdateStatus(ctx, pod, metav1.UpdateOptions{}); err != nil {
					t.Fatal(err)
				}
			}
			r := &InstanceReconciler{Client: c, Core: kube.CoreV1(), DevelopmentReleaseGates: true}
			for range 2 {
				if _, err := r.reconcileGatedPod(ctx, instance); err != nil {
					t.Fatal(err)
				}
				if err := c.Get(ctx, client.ObjectKeyFromObject(instance), instance); err != nil {
					t.Fatal(err)
				}
			}
			if intent := instance.Status.PendingPodCreation; intent == nil || intent.Role != test.want {
				t.Fatalf("next Pod creation = %#v, want %s", intent, test.want)
			}
		})
	}
}
