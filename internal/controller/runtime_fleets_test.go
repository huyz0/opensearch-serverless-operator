package controller

import (
	"context"
	"slices"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	"sigs.k8s.io/controller-runtime/pkg/client"
	clientfake "sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/huyz0/opensearch-serverless-operator/api/v1alpha1"
	"github.com/huyz0/opensearch-serverless-operator/internal/runtimeconfig"
)

func TestFirstPodIntentPinsStartupSettings(t *testing.T) {
	ctx := context.Background()
	image := "registry.example/runtime@sha256:" + strings.Repeat("b", 64)
	instance := &v1alpha1.OpenSearchServerless{
		ObjectMeta: metav1.ObjectMeta{Name: "catalog", Namespace: "n", UID: "owner"},
		Spec: v1alpha1.OpenSearchServerlessSpec{Image: image, ServiceAccountName: "runtime",
			Storage: v1alpha1.Storage{Type: "s3", Bucket: "data", Region: "region"},
			Runtime: &v1alpha1.RuntimeConfig{JVM: &v1alpha1.JVMConfig{SystemProperties: map[string]string{"user.timezone": "UTC"}}},
			Pools: map[string]v1alpha1.Pool{"search": {JVMHeap: "256Mi", Resources: corev1.ResourceRequirements{Requests: corev1.ResourceList{
				corev1.ResourceCPU: resource.MustParse("250m"), corev1.ResourceMemory: resource.MustParse("512Mi"),
			}}, Zones: map[string]v1alpha1.Zone{"a": {Replicas: 1}}}}},
		Status: v1alpha1.OpenSearchServerlessStatus{PrimaryStore: &v1alpha1.PrimaryStoreStatus{Bucket: "data", Region: "region"},
			ServingFleetIntentByRole: map[string]string{"search": "initial"}},
	}
	scheme := runtime.NewScheme()
	if err := v1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	store := clientfake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(instance).WithObjects(instance).Build()
	kube := fake.NewSimpleClientset()
	r := &InstanceReconciler{Client: store, Core: kube.CoreV1(), DevelopmentRuntimeSettingsImage: image}
	key := types.NamespacedName{Namespace: instance.Namespace, Name: instance.Name}
	if err := store.Get(ctx, key, instance); err != nil {
		t.Fatal(err)
	}
	if _, err := r.reconcileGatedPod(ctx, instance); err != nil {
		t.Fatal(err)
	}
	var fresh v1alpha1.OpenSearchServerless
	if err := store.Get(ctx, key, &fresh); err != nil {
		t.Fatal(err)
	}
	pin := fresh.Status.RuntimeByFleet["initial"]
	if pin.Common == nil || pin.Common.JVM.SystemProperties["user.timezone"] != "UTC" || fresh.Status.PendingPodCreation == nil {
		t.Fatalf("runtime was not recorded with the first intent: %#v", fresh.Status)
	}
	pods, err := kube.CoreV1().Pods(instance.Namespace).List(ctx, metav1.ListOptions{})
	if err != nil || len(pods.Items) != 0 {
		t.Fatal("a Pod was created before pinning")
	}
	before := fresh.DeepCopyObject().(*v1alpha1.OpenSearchServerless)
	fresh.Spec.Runtime.JVM.SystemProperties["user.timezone"] = "Asia/Tokyo"
	if err := r.Patch(ctx, &fresh, client.MergeFrom(before)); err != nil {
		t.Fatal(err)
	}
	rendered, err := renderGatedPod(&fresh, fresh.Status.PendingPodCreation, nil, "", image)
	if err != nil || !slices.Contains(rendered.Spec.Containers[0].Args, "-Duser.timezone=UTC") ||
		slices.Contains(rendered.Spec.Containers[0].Args, "-Duser.timezone=Asia/Tokyo") {
		t.Fatalf("direct edit replaced the initial fleet runtime: %v", err)
	}
}

func TestPreviewRuntimePinsObservedServingArguments(t *testing.T) {
	ctx := context.Background()
	image := "registry.example/runtime@sha256:" + strings.Repeat("a", 64)
	common := func(zone string) *v1alpha1.RuntimeConfig {
		return &v1alpha1.RuntimeConfig{JVM: &v1alpha1.JVMConfig{
			Options:          &[]string{"-XX:+ExitOnOutOfMemoryError"},
			SystemProperties: map[string]string{"user.timezone": zone},
		}}
	}
	search := func(wait string) v1alpha1.RuntimeConfig {
		return v1alpha1.RuntimeConfig{Settings: map[string]string{"serverless.search.warm_on_join.max_wait": wait}}
	}
	pool := v1alpha1.Pool{JVMHeap: "256Mi", Resources: corev1.ResourceRequirements{Requests: corev1.ResourceList{
		corev1.ResourceCPU: resource.MustParse("250m"), corev1.ResourceMemory: resource.MustParse("512Mi"),
	}}, Zones: map[string]v1alpha1.Zone{"a": {Replicas: 1}}}
	instance := &v1alpha1.OpenSearchServerless{
		ObjectMeta: metav1.ObjectMeta{Name: "catalog", Namespace: "n", UID: "instance"},
		Spec: v1alpha1.OpenSearchServerlessSpec{Image: image, Runtime: common("UTC"), ServiceAccountName: "runtime",
			Pools: map[string]v1alpha1.Pool{"search": pool, "ingest": pool}, Rollout: &v1alpha1.Rollout{Strategy: "BlueGreen", RollbackWindow: "2m"}},
		Status: v1alpha1.OpenSearchServerlessStatus{PrimaryStore: &v1alpha1.PrimaryStoreStatus{Bucket: "data", Region: "region"},
			ServingFleetIntentByRole: map[string]string{"search": "initial", "ingest": "initial"}},
	}
	applyFleetRuntime(instance, v1alpha1.FleetRuntime{Common: common("UTC"), ByRole: map[string]v1alpha1.RuntimeConfig{"search": search("45s")}})
	kube := fake.NewSimpleClientset()
	r := &InstanceReconciler{Core: kube.CoreV1(), DevelopmentRuntimeSettingsImage: image}
	render := func(fleet, role, qualified string) (*corev1.Pod, error) {
		t.Helper()
		return renderGatedPod(instance, &v1alpha1.PodCreationIntent{Name: fleet + "-" + role, Fleet: fleet, Role: role,
			Zone: "a", Sequence: 1, Revision: "revision"}, nil, "", qualified)
	}
	if _, err := render("initial", "search", ""); err == nil {
		t.Fatal("an unqualified image accepted custom startup values")
	}
	for _, role := range []string{"search", "ingest"} {
		pod, err := render("initial", role, image)
		if err != nil {
			t.Fatal(err)
		}
		pod.UID = types.UID(pod.Name)
		pod.Spec.SchedulingGates = nil
		pod.Spec.NodeName = "node"
		pod.Status = corev1.PodStatus{Phase: corev1.PodRunning, Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}}
		if _, err := kube.CoreV1().Pods(instance.Namespace).Create(ctx, pod, metav1.CreateOptions{}); err != nil {
			t.Fatal(err)
		}
	}
	candidate := &v1alpha1.FleetCandidate{Fleet: "green", Image: image, RuntimeOverride: &v1alpha1.FleetRuntime{
		Common: common("Asia/Tokyo"), ByRole: map[string]v1alpha1.RuntimeConfig{"ingest": search("90s")},
	}}
	if pins, err := r.previewFleetRuntime(ctx, instance, candidate); pins != nil || err == nil {
		t.Fatal("candidate applied a search-only setting to ingest")
	}
	candidate.RuntimeOverride.ByRole = map[string]v1alpha1.RuntimeConfig{"search": search("90s")}
	instance.Spec.Runtime = common("Europe/Dublin")
	if pins, err := r.previewFleetRuntime(ctx, instance, candidate); pins != nil || err == nil {
		t.Fatal("unobserved source changes were backfilled")
	}
	implicit := &v1alpha1.FleetCandidate{Fleet: "amber", Image: image}
	if pins, err := r.previewFleetRuntime(ctx, instance, implicit); pins != nil || err == nil {
		t.Fatal("implicit preview accepted a concurrent edit to source settings")
	}
	applyFleetRuntime(instance, v1alpha1.FleetRuntime{})
	if pins, err := r.previewFleetRuntime(ctx, instance, implicit); pins != nil || err == nil {
		t.Fatal("implicit preview accepted removal of the live source settings")
	}
	applyFleetRuntime(instance, v1alpha1.FleetRuntime{Common: common("UTC"), ByRole: map[string]v1alpha1.RuntimeConfig{"search": search("45s")}})
	instance.Spec.Runtime = common("UTC")
	if pins, err := r.previewFleetRuntime(ctx, instance, implicit); len(pins) != 2 || err != nil {
		t.Fatalf("implicit preview did not pin the verified source and inherited settings: %v", err)
	}
	pins, err := r.previewFleetRuntime(ctx, instance, candidate)
	if err != nil || len(pins) != 2 {
		t.Fatalf("candidate did not pin both startup configurations: %v", err)
	}
	instance.Status.RuntimeByFleet = pins
	instance.Spec.Runtime = common("Europe/Dublin")
	for _, check := range []struct{ fleet, role, zone, wait string }{
		{"initial", "search", "UTC", "45s"}, {"green", "search", "Asia/Tokyo", "90s"},
		{"initial", "ingest", "UTC", ""}, {"green", "ingest", "Asia/Tokyo", ""},
	} {
		pod, err := render(check.fleet, check.role, image)
		if err != nil {
			t.Fatal(err)
		}
		args := pod.Spec.Containers[0].Args
		want := []string{"-XX:+ExitOnOutOfMemoryError"}
		if check.wait != "" {
			want = append(want, "-Dserverless.search.warm_on_join.max_wait="+check.wait)
		}
		want = append(want, "-Duser.timezone="+check.zone)
		if !runtimeconfig.MatchesPodRuntime(args, want) || checkFleetRuntimePod(instance, check.fleet, check.role, pod) != nil {
			t.Fatalf("%s %s did not use the pinned runtime: %v", check.fleet, check.role, args)
		}
		other := "initial"
		if check.fleet == other {
			other = "green"
		}
		if err := checkFleetRuntimePod(instance, other, check.role, pod); err == nil {
			t.Fatalf("%s runtime was accepted for another fleet", check.fleet)
		}
		wrongStore := pod.DeepCopy()
		for i, arg := range wrongStore.Spec.Containers[0].Args {
			if strings.HasPrefix(arg, "-Dserverless.store.bucket=") {
				wrongStore.Spec.Containers[0].Args[i] = "-Dserverless.store.bucket=wrong-store"
			}
		}
		if err := checkFleetRuntimePod(instance, check.fleet, check.role, wrongStore); err == nil {
			t.Error("runtime witness accepted a different primary store")
		}
		for _, drift := range []string{"-XX:+UseParallelGC", "-Duser.timezone=GMT", "-Dnode.name=other"} {
			changed := pod.DeepCopy()
			changed.Spec.Containers[0].Args = append(changed.Spec.Containers[0].Args, drift)
			if err := checkFleetRuntimePod(instance, check.fleet, check.role, changed); err == nil {
				t.Errorf("%s %s runtime witness ignored an extra argument %q", check.fleet, check.role, drift)
			}
		}
		pod.Spec.Containers[0].Env = append(pod.Spec.Containers[0].Env, corev1.EnvVar{Name: "_JAVA_OPTIONS", Value: "-Duser.timezone=GMT"})
		if err := checkFleetRuntimePod(instance, check.fleet, check.role, pod); err == nil {
			t.Fatal("runtime witness ignored a JVM launcher override")
		}
	}
	revisions, err := podRevisions(instance, "")
	if err != nil || revisions.forFleetTemplate("green").current == revisions.forFleetTemplate("initial").current {
		t.Fatalf("candidate runtime did not have its own revision: %v", err)
	}
}
