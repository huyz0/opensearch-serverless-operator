package controller

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/huyz0/opensearch-serverless-operator/api/v1alpha1"
	"github.com/huyz0/opensearch-serverless-operator/internal/plugins"
	"github.com/huyz0/opensearch-serverless-operator/internal/workload"
)

func TestPreviewPinsOnlyApprovedPluginFleetTransition(t *testing.T) {
	ctx := context.Background()
	artifact := func(digest string) string {
		return "registry.example/plugins/demo@sha256:" + strings.Repeat(digest, 64)
	}
	base, next, unqualified := artifact("a"), artifact("b"), artifact("c")
	image := "registry.example/java@sha256:" + strings.Repeat("d", 64)
	installer := "registry.example/installer@sha256:" + strings.Repeat("e", 64)
	selection := func(ref string) []v1alpha1.Plugin {
		return []v1alpha1.Plugin{{Name: "demo", Artifact: ref, Roles: []string{"search"}}}
	}
	policy := plugins.Policy{
		ApprovedArtifacts: []string{base, next, unqualified},
		OpenSearchVersion: "3.10.0", JavaVersion: "25", MaxBytes: 1 << 20, MaxFiles: 10,
	}
	raw, err := json.Marshal(policy)
	if err != nil {
		t.Fatal(err)
	}
	fingerprint := func(config []v1alpha1.Plugin) string {
		t.Helper()
		search, err := plugins.ResolveRole(config, "search", raw)
		if err != nil {
			t.Fatal(err)
		}
		if search == nil {
			return plugins.FleetDigest("", "")
		}
		return plugins.FleetDigest("", search.RequestSHA256)
	}
	policy.ApprovedFleetTransitions = []plugins.FleetTransition{
		{From: fingerprint(selection(base)), To: fingerprint(selection(next))},
		{From: fingerprint(selection(unqualified)), To: fingerprint(selection(next))},
		{From: fingerprint(selection(next)), To: fingerprint(nil)},
	}
	raw, err = json.Marshal(policy)
	if err != nil {
		t.Fatal(err)
	}
	immutable := true
	config := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: "policy", Namespace: "runtime", UID: "approval-uid", ResourceVersion: "5"},
		Immutable:  &immutable, Data: map[string]string{"policy.json": string(raw)},
	}
	core := fake.NewSimpleClientset(config)
	instance := &v1alpha1.OpenSearchServerless{
		ObjectMeta: metav1.ObjectMeta{Name: "catalog", Namespace: "runtime", UID: "owner"},
		Spec: v1alpha1.OpenSearchServerlessSpec{Image: image, Plugins: selection(base),
			Rollout: &v1alpha1.Rollout{Strategy: "BlueGreen", RollbackWindow: "2m"}},
		Status: v1alpha1.OpenSearchServerlessStatus{ServingFleetIntentByRole: map[string]string{"search": "initial", "ingest": "initial"}},
	}
	r := &InstanceReconciler{Core: core.CoreV1(), DevelopmentPluginInstallerImage: installer, DevelopmentPluginRuntimeImage: image, DevelopmentPluginPolicy: "policy"}
	initialInputs, err := r.pluginsForRole(ctx, instance, "search")
	if err != nil {
		t.Fatal(err)
	}
	observe := func(fleet string, search v1alpha1.PluginInstallIntent) {
		t.Helper()
		search.RequestUID, search.PolicyUID = "request-uid", "policy-snapshot-uid"
		encoded, err := json.Marshal(search)
		if err != nil {
			t.Fatal(err)
		}
		owned := true
		fsGroup := int64(10001)
		for _, role := range []string{"search", "ingest"} {
			name := fleet + "-" + role
			pod := &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{Name: name, UID: "pod-" + types.UID(name), Labels: map[string]string{
					workload.InstanceLabel: string(instance.UID), workload.FleetLabel: fleet, workload.RoleLabel: role,
				}, OwnerReferences: []metav1.OwnerReference{{UID: instance.UID, Name: instance.Name, APIVersion: v1alpha1.GroupVersion.String(), Kind: "OpenSearchServerless", Controller: &owned}}},
				Spec: corev1.PodSpec{NodeName: "worker", SecurityContext: &corev1.PodSecurityContext{FSGroup: &fsGroup},
					Containers: []corev1.Container{{Name: "opensearch", Image: image}}},
				Status: corev1.PodStatus{Phase: corev1.PodRunning, Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}},
			}
			if role == "search" {
				pod.Annotations = map[string]string{pluginIntentAnnotation: string(encoded)}
				pod, err = withPinnedPluginInstaller(pod, &search)
				if err != nil {
					t.Fatal(err)
				}
				pod.Status.InitContainerStatuses = []corev1.ContainerStatus{{
					Name: "plugin-installer", State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ExitCode: 0}},
				}}
			}
			if _, err := core.CoreV1().Pods(instance.Namespace).Create(ctx, pod, metav1.CreateOptions{}); err != nil {
				t.Fatal(err)
			}
		}
	}
	observe("initial", *pluginInstallIntent(instance, initialInputs))
	candidate := &v1alpha1.FleetCandidate{Fleet: "green", Image: image, PluginOverride: &v1alpha1.PluginSet{Plugins: selection(unqualified)}}
	if pins, err := r.previewPluginFleet(ctx, instance, candidate); err == nil || pins != nil {
		t.Fatal("installation approval permitted unapproved fleet coexistence")
	}
	candidate.PluginOverride.Plugins = selection(next)
	instance.Spec.Plugins = selection(unqualified)
	if pins, err := r.previewPluginFleet(ctx, instance, candidate); err == nil || pins != nil {
		t.Fatal("candidate backfilled a different set over the serving plugin set")
	}
	instance.Spec.Plugins = selection(base)
	pins, err := r.previewPluginFleet(ctx, instance, candidate)
	if err != nil || len(pins) != 2 {
		t.Fatalf("approved transition did not pin both fleets: %v", err)
	}
	instance.Status.PluginSetsByFleet = pins
	forged, err := core.CoreV1().Pods(instance.Namespace).Get(ctx, "initial-search", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	forged.Spec.InitContainers = nil
	if err := checkFleetPluginPod(instance, "initial", "search", forged); err == nil {
		t.Fatal("a matching annotation without an installer was accepted")
	}
	for _, fleet := range []string{"initial", "green"} {
		search, err := r.pluginsForFleetRole(ctx, instance, fleet, "search")
		if err != nil || search == nil || search.Resolution.RequestSHA256 != pins[fleet].InputsByRole["search"].RequestSHA256 {
			t.Fatalf("search replacement did not resolve fleet %s's pin: %v", fleet, err)
		}
		if ingest, err := r.pluginsForFleetRole(ctx, instance, fleet, "ingest"); err != nil || ingest != nil {
			t.Fatalf("ingest inherited a search-only plugin: %v", err)
		}
	}
	revisions, err := podRevisions(instance, "")
	if err != nil {
		t.Fatal(err)
	}
	initial, green := revisions.forFleetTemplate("initial"), revisions.forFleetTemplate("green")
	if initial.current == green.current || initial.accepts(green.current) || green.accepts(initial.current) {
		t.Fatal("plugin changes did not isolate the fleet revisions")
	}
	instance.Status.ServingFleetIntentByRole["search"], instance.Status.ServingFleetIntentByRole["ingest"] = "green", "green"
	observe("green", pins["green"].InputsByRole["search"])
	inherited := &v1alpha1.FleetCandidate{Fleet: "amber", Image: image}
	newPins, err := r.previewPluginFleet(ctx, instance, inherited)
	if err != nil || newPins["amber"].InputsByRole["search"] != pins["green"].InputsByRole["search"] {
		t.Fatalf("subsequent preview did not inherit promoted set: %v", err)
	}
	inherited.PluginOverride = &v1alpha1.PluginSet{}
	nextPolicy := config.DeepCopy()
	nextPolicy.Name, nextPolicy.UID, nextPolicy.ResourceVersion = "next-policy", "next-approval", "8"
	if _, err := core.CoreV1().ConfigMaps("runtime").Create(ctx, nextPolicy, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	r.DevelopmentPluginPolicy = nextPolicy.Name
	newPins, err = r.previewPluginFleet(ctx, instance, inherited)
	if err != nil || len(newPins["amber"].InputsByRole) != 0 || len(newPins["amber"].Plugins) != 0 {
		t.Fatalf("approved explicit removal did not yield an empty set: %v", err)
	}
	if _, err := r.pluginsForFleetRole(ctx, instance, "green", "search"); err != nil {
		t.Fatalf("changing current approvals invalidated a retained fleet install policy: %v", err)
	}
	config.ResourceVersion = "6"
	if _, err := core.CoreV1().ConfigMaps("runtime").Update(ctx, config, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := r.pluginsForFleetRole(ctx, instance, "green", "search"); err == nil {
		t.Fatal("replacement ignored a changed policy source version")
	}
}
