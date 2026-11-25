package controller

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	"sigs.k8s.io/controller-runtime/pkg/client"
	clientfake "sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/huyz0/opensearch-serverless-operator/api/v1alpha1"
	"github.com/huyz0/opensearch-serverless-operator/internal/workload"
)

func TestCandidateRequestDoesNotChangeServingPodRevision(t *testing.T) {
	instance := &v1alpha1.OpenSearchServerless{Spec: v1alpha1.OpenSearchServerlessSpec{Image: "serving"}}
	before, err := podRevision(instance, "")
	if err != nil {
		t.Fatal(err)
	}
	instance.Spec.Rollout = &v1alpha1.Rollout{Strategy: "BlueGreen", MaxPreviewReplicas: 3, RollbackWindow: "15m",
		Candidate: &v1alpha1.FleetCandidate{Fleet: "green", Image: "candidate"}}
	after, err := podRevision(instance, "")
	if err != nil || after != before {
		t.Fatalf("candidate request changed serving revision: %s %s %v", before, after, err)
	}
	instance.Spec.Image = "candidate"
	candidate, err := podRevision(instance, "")
	if err != nil || candidate == before {
		t.Fatalf("different candidate image did not change revision: %s %v", candidate, err)
	}
}

func TestScaleChangeKeepsPodTemplateRevision(t *testing.T) {
	instance := &v1alpha1.OpenSearchServerless{Spec: v1alpha1.OpenSearchServerlessSpec{Image: "runtime", Pools: map[string]v1alpha1.Pool{
		"search": {JVMHeap: "512Mi", Zones: map[string]v1alpha1.Zone{"a": {Replicas: 1, MinServing: 1}}},
		"ingest": {JVMHeap: "512Mi", Zones: map[string]v1alpha1.Zone{"a": {Replicas: 1, MinServing: 1}}},
	}}}
	start, err := podRevision(instance, "cluster.local")
	if err != nil || instance.Spec.Pools["search"].Zones["a"].Replicas != 1 {
		t.Fatalf("revision calculation mutated the template or failed: %v", err)
	}
	pool := instance.Spec.Pools["search"]
	pool.Zones["a"] = v1alpha1.Zone{Replicas: 3, WarmSpare: 1, MinServing: 2}
	instance.Spec.Availability = &v1alpha1.Availability{EmergencyCapacity: &v1alpha1.EmergencyCapacityPolicy{MaxTemporaryPerZone: 2}}
	changed, err := podRevision(instance, "cluster.local")
	if err != nil || changed != start {
		t.Fatalf("scale change altered the Pod revision: %s → %s, %v", start, changed, err)
	}
	pool.Zones["b"] = v1alpha1.Zone{Replicas: 1}
	changed, err = podRevision(instance, "cluster.local")
	if err != nil || changed == start {
		t.Fatalf("new zone did not change the revision: %s, %v", changed, err)
	}
	delete(pool.Zones, "b")
	pool.JVMHeap = "1Gi"
	instance.Spec.Pools["search"] = pool
	changed, err = podRevision(instance, "cluster.local")
	if err != nil || changed == start {
		t.Fatalf("JVM heap change retained old revision: %s, %v", changed, err)
	}
}

func TestAvailabilityDependentRevisionSurvivesBudgetChange(t *testing.T) {
	ctx := context.Background()
	scheme := runtime.NewScheme()
	if err := v1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	instance := &v1alpha1.OpenSearchServerless{
		ObjectMeta: metav1.ObjectMeta{Name: "catalog", Namespace: "test", UID: "instance"},
		Spec: v1alpha1.OpenSearchServerlessSpec{Image: "serving",
			Availability: &v1alpha1.Availability{EmergencyCapacity: &v1alpha1.EmergencyCapacityPolicy{MaxTemporaryPerZone: 1}},
			Rollout:      &v1alpha1.Rollout{Strategy: "BlueGreen"},
		},
	}
	before, err := podRevisions(instance, "")
	if err != nil || before.withAvailability == "" || before.withAvailability == before.current {
		t.Fatalf("expected an older budget-dependent revision: %#v %v", before, err)
	}
	instance.Status.ServingFleetIntentByRole = map[string]string{"ingest": "initial"}
	pinServingRevision(instance, "ingest", "initial", before.withAvailability)
	c := clientfake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(instance).WithObjects(instance).Build()
	r := &InstanceReconciler{Client: c, Core: fake.NewSimpleClientset().CoreV1()}
	if err := c.Get(ctx, client.ObjectKeyFromObject(instance), instance); err != nil {
		t.Fatal(err)
	}
	if recorded, err := r.recordLegacyTemplateRevision(ctx, instance); err != nil || !recorded {
		t.Fatalf("upgrade did not preserve the serving pin with all Pods absent: %t %v", recorded, err)
	}
	instance.Spec.Availability.EmergencyCapacity.MaxTemporaryPerZone = 2
	after, err := podRevisions(instance, "")
	if err != nil || after.current != before.current || after.withAvailability == before.withAvailability {
		t.Fatalf("adjusting the emergency budget must only change the old hash: %#v %v", after, err)
	}
	got, err := after.forFleet(instance, nil, "ingest", "initial")
	if err != nil || got != before.withAvailability {
		t.Fatalf("replacement lost the serving revision: %q %v", got, err)
	}
}

func TestLegacyCapacityRevisionSurvivesScaleAndRolloutStaging(t *testing.T) {
	ctx := context.Background()
	scheme := runtime.NewScheme()
	if err := v1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	instance := &v1alpha1.OpenSearchServerless{ObjectMeta: metav1.ObjectMeta{Name: "catalog", Namespace: "test", UID: "instance"},
		Spec: v1alpha1.OpenSearchServerlessSpec{Image: "serving", Pools: map[string]v1alpha1.Pool{
			"search": {Zones: map[string]v1alpha1.Zone{"a": {Replicas: 1, MinServing: 1}}},
			"ingest": {Zones: map[string]v1alpha1.Zone{"a": {Replicas: 1, MinServing: 1}}},
		}}}
	legacy, err := hashPodTemplate(instance.Spec, "")
	if err != nil {
		t.Fatal(err)
	}
	instance.Status.TemplateRevisionAliases = map[string]string{"older-policy-revision": legacy}
	owned := corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "search", Namespace: "test", UID: "pod",
		Labels:          map[string]string{workload.InstanceLabel: string(instance.UID), workload.RoleLabel: "search", workload.FleetLabel: "initial", podRevisionLabel: legacy},
		OwnerReferences: []metav1.OwnerReference{*metav1.NewControllerRef(instance, v1alpha1.GroupVersion.WithKind("OpenSearchServerless"))}}}
	c := clientfake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(instance).WithObjects(instance).Build()
	r := &InstanceReconciler{Client: c, Core: fake.NewSimpleClientset(&owned).CoreV1()}
	if err := c.Get(ctx, client.ObjectKeyFromObject(instance), instance); err != nil {
		t.Fatal(err)
	}
	if recorded, err := r.recordLegacyTemplateRevision(ctx, instance); err != nil || !recorded {
		t.Fatalf("upgrade alias was not recorded: %t %v", recorded, err)
	}
	var live v1alpha1.OpenSearchServerless
	if err := c.Get(ctx, client.ObjectKeyFromObject(instance), &live); err != nil {
		t.Fatal(err)
	}
	if live.Status.TemplateRevisionAliases[legacy] == "" || live.Status.TemplateRevisionAliases["older-policy-revision"] != live.Status.TemplateRevisionAliases[legacy] {
		t.Fatal("previous and older policy aliases were not flattened to the same template")
	}
	live.Spec.Pools["search"].Zones["a"] = v1alpha1.Zone{Replicas: 2, MinServing: 1}
	live.Spec.Rollout = &v1alpha1.Rollout{Strategy: "BlueGreen", Candidate: &v1alpha1.FleetCandidate{Fleet: "green", Image: "serving"}}
	current, err := podRevisions(&live, "")
	if err != nil || !current.accepts(legacy) || !current.accepts("older-policy-revision") {
		t.Fatalf("scale and staging lost existing labels: %#v %v", current, err)
	}
	if label, err := current.forFleet(&live, []corev1.Pod{owned}, "search", "initial"); err != nil || label != legacy {
		t.Fatalf("new source Pod would introduce a mixed revision: %s %v", label, err)
	}

	// An older controller may already have mixed two count-dependent labels.
	// Current spec cannot authenticate the earlier label without its alias.
	live.Status.TemplateRevisionAliases = nil
	withoutCheckpoint, err := podRevisions(&live, "")
	if err != nil {
		t.Fatal(err)
	}
	newer := owned.DeepCopy()
	newer.Labels[podRevisionLabel] = withoutCheckpoint.previous
	if _, err := withoutCheckpoint.forFleet(&live, []corev1.Pod{*newer, owned}, "search", "initial"); err == nil {
		t.Fatal("accepted a mixed legacy fleet without evidence for the prior template")
	}
}

func TestServingRevisionSurvivesLossOfAllPods(t *testing.T) {
	instance := &v1alpha1.OpenSearchServerless{ObjectMeta: metav1.ObjectMeta{Name: "catalog", Namespace: "test", UID: "instance"}}
	instance.Status.ServingFleetIntentByRole = map[string]string{"search": "blue"}
	revisions := templateRevisions{current: "canonical", previous: "unrecorded", aliases: map[string]string{"old": "canonical", "older": "canonical"}}
	pod := corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "search", Namespace: instance.Namespace, UID: "pod", ResourceVersion: "1",
		Labels:          map[string]string{workload.InstanceLabel: string(instance.UID), workload.RoleLabel: "search", workload.FleetLabel: "blue", podRevisionLabel: "old"},
		OwnerReferences: []metav1.OwnerReference{*metav1.NewControllerRef(instance, v1alpha1.GroupVersion.WithKind("OpenSearchServerless"))}},
		Spec: corev1.PodSpec{NodeName: "node"}, Status: corev1.PodStatus{Phase: corev1.PodRunning, Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}}}
	if adoptServingRevisionPins(instance, nil, revisions) {
		t.Fatal("adopted a serving pin without observing a Pod")
	}
	provisional := pod.DeepCopy()
	provisional.Spec.NodeName = ""
	provisional.Spec.SchedulingGates = []corev1.PodSchedulingGate{{Name: "serverless.opensearch.org/placement"}}
	instance.Spec.Rollout = &v1alpha1.Rollout{Strategy: "BlueGreen"}
	if adoptServingRevisionPins(instance, []corev1.Pod{*provisional}, revisions) {
		t.Fatal("a gated Pod established a serving revision")
	}
	if label, err := (templateRevisions{current: "different"}).forFleet(instance, nil, "search", "blue"); err != nil || label != "different" {
		t.Fatalf("an abandoned first attempt blocked startup with a new template: %s %v", label, err)
	}
	foreign := pod.DeepCopy()
	foreign.OwnerReferences[0].Name = "not-catalog"
	if adoptServingRevisionPins(instance, []corev1.Pod{*foreign}, revisions) {
		t.Fatal("adopted a revision from a conflicting owner")
	}
	if !adoptServingRevisionPins(instance, []corev1.Pod{pod}, revisions) {
		t.Fatal("did not adopt the verified serving label")
	}
	if label, err := revisions.forFleet(instance, nil, "search", "blue"); err != nil || label != "old" {
		t.Fatalf("replacement without surviving Pods used %q: %v", label, err)
	}
	if label, err := revisions.forFleet(instance, nil, "search", "green"); err != nil || label != "canonical" {
		t.Fatalf("another fleet incorrectly inherited %q: %v", label, err)
	}
	conflicting := pod.DeepCopy()
	conflicting.Labels[podRevisionLabel] = "unrecorded"
	if _, err := revisions.forFleet(instance, []corev1.Pod{*conflicting}, "search", "blue"); err == nil {
		t.Fatal("an individually compatible revision without an alias overrode the serving revision pin")
	}
	other := pod.DeepCopy()
	other.Name, other.UID = "other", "other-uid"
	other.Labels[podRevisionLabel] = "older"
	mixed := []corev1.Pod{*other, pod}
	if label, err := revisions.forFleet(instance, mixed, "search", "blue"); err != nil || label != "old" {
		t.Fatalf("a mixed serving cohort lost its literal replacement pin: %q %v", label, err)
	}
	unpinned := instance.DeepCopy()
	unpinned.Status.ServingRevisionByRole = nil
	if label, err := revisions.forFleet(unpinned, mixed, "search", "blue"); err != nil || label != "canonical" {
		t.Fatalf("an unpinned mixed cohort did not use the canonical revision: %q %v", label, err)
	}
	if !adoptServingRevisionPins(unpinned, mixed, revisions) || unpinned.Status.ServingRevisionByRole["search"].Revision != "canonical" {
		t.Fatal("an owned mixed cohort did not establish the canonical serving pin")
	}
	if servingRevisionMatchesIntent(instance, &v1alpha1.PodCreationIntent{Role: "search", Fleet: "blue", Revision: "canonical"}) {
		t.Fatal("allowed a pending creation with a different revision")
	}
	if _, err := (templateRevisions{current: "different"}).forFleet(instance, nil, "search", "blue"); err == nil {
		t.Fatal("a changed runtime template replaced the pinned rollout revision")
	}
	instance.Spec.Rollout = nil
	newTemplate := templateRevisions{current: "different"}
	if label, err := newTemplate.forFleet(instance, nil, "search", "blue"); err != nil || label != "different" {
		t.Fatalf("previous in-place template-change behavior was blocked: %s %v", label, err)
	}
	pod.Labels[podRevisionLabel] = "different"
	if !adoptServingRevisionPins(instance, []corev1.Pod{pod}, newTemplate) || instance.Status.ServingRevisionByRole["search"].Revision != "different" {
		t.Fatal("verified replacement template was not recorded")
	}
}

func TestLegacyTemplateAliasKeepsFleetLabelThroughPolicyChange(t *testing.T) {
	ctx := context.Background()
	scheme := runtime.NewScheme()
	if err := v1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	instance := &v1alpha1.OpenSearchServerless{
		ObjectMeta: metav1.ObjectMeta{Name: "catalog", Namespace: "test", UID: "instance"},
		Spec:       v1alpha1.OpenSearchServerlessSpec{Image: "serving", Rollout: &v1alpha1.Rollout{Strategy: "BlueGreen", MaxPreviewReplicas: 3}},
	}
	before, err := podRevisions(instance, "")
	if err != nil || before.current == before.legacy {
		t.Fatalf("expected different legacy and current digests: %#v %v", before, err)
	}
	pod := corev1.Pod{ObjectMeta: metav1.ObjectMeta{
		Name: "search", Namespace: instance.Namespace, UID: "pod",
		Labels:          map[string]string{workload.InstanceLabel: string(instance.UID), workload.RoleLabel: "search", workload.FleetLabel: "initial", podRevisionLabel: before.legacy},
		OwnerReferences: []metav1.OwnerReference{*metav1.NewControllerRef(instance, v1alpha1.GroupVersion.WithKind("OpenSearchServerless"))},
	}}
	c := clientfake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(instance).WithObjects(instance).Build()
	r := &InstanceReconciler{Client: c, Core: fake.NewSimpleClientset(&pod).CoreV1()}
	if err := c.Get(ctx, client.ObjectKeyFromObject(instance), instance); err != nil {
		t.Fatal(err)
	}
	if recorded, err := r.recordLegacyTemplateRevision(ctx, instance); err != nil || !recorded {
		t.Fatalf("legacy Pod alias was not persisted: %t %v", recorded, err)
	}
	reloaded := &v1alpha1.OpenSearchServerless{}
	if err := c.Get(ctx, client.ObjectKeyFromObject(instance), reloaded); err != nil {
		t.Fatal(err)
	}
	reloaded.Spec.Rollout.RollbackWindow = "30m"
	reloaded.Spec.Rollout.MaxPreviewReplicas = 5
	after, err := podRevisions(reloaded, "")
	if err != nil || after.current != before.current || after.legacy == before.legacy || !after.accepts(before.legacy) {
		t.Fatalf("policy change lost the persisted alias: %#v %v", after, err)
	}
	if revision, err := after.forFleet(reloaded, []corev1.Pod{pod}, "search", "initial"); err != nil || revision != before.legacy {
		t.Fatalf("replacement lost the fleet revision pin: %s %v", revision, err)
	}
	if revision, err := after.forFleet(reloaded, []corev1.Pod{pod}, "search", "new"); err != nil || revision != before.current {
		t.Fatalf("new fleet did not use the current digest: %s %v", revision, err)
	}
	reloaded.Status.CandidateFleet = &v1alpha1.CandidateFleetStatus{Fleet: "preview", Revision: before.legacy}
	if revision, err := after.forFleet(reloaded, nil, "search", "preview"); err != nil || revision != before.legacy {
		t.Fatalf("empty staged fleet lost its existing revision pin: %s %v", revision, err)
	}
	reloaded.Spec.Image = "new-runtime"
	changed, err := podRevisions(reloaded, "")
	if err != nil || changed.accepts(before.legacy) {
		t.Fatalf("changed runtime inherited a stale alias: %#v %v", changed, err)
	}
}

func TestCandidateRebaseRequiresUnchangedTemplateAndNoPendingWork(t *testing.T) {
	instance := &v1alpha1.OpenSearchServerless{ObjectMeta: metav1.ObjectMeta{Name: "catalog", Namespace: "n", UID: "owner", Generation: 2}, Spec: v1alpha1.OpenSearchServerlessSpec{Image: "serving", Rollout: &v1alpha1.Rollout{Strategy: "BlueGreen", Candidate: &v1alpha1.FleetCandidate{Fleet: "green", Image: "candidate"}}}, Status: v1alpha1.OpenSearchServerlessStatus{CandidateFleet: &v1alpha1.CandidateFleetStatus{Fleet: "green", Image: "candidate", Generation: 1}}}
	copy := instance.DeepCopyObject().(*v1alpha1.OpenSearchServerless)
	copy.Spec.Image = "candidate"
	revision, err := podRevision(copy, "")
	if err != nil {
		t.Fatal(err)
	}
	instance.Status.CandidateFleet.Revision = revision
	r := &InstanceReconciler{Core: fake.NewSimpleClientset().CoreV1()}
	check := func() (bool, error) {
		return r.canRebaseCandidate(context.Background(), instance, instance.Spec.Rollout.Candidate)
	}
	if ok, err := check(); !ok || err != nil {
		t.Fatalf("unchanged candidate rejected: %v", err)
	}
	instance.Status.PendingPodCreation = &v1alpha1.PodCreationIntent{Name: "pending"}
	if ok, err := check(); ok || err == nil {
		t.Fatal("pending creation accepted")
	}
	instance.Status.PendingPodCreation = nil
	instance.Status.RetiredPodCreations = []v1alpha1.RetiredPodCreation{{Fleet: "green"}}
	if ok, err := check(); ok || err == nil {
		t.Fatal("late candidate creation accepted")
	}
	instance.Status.RetiredPodCreations = nil
	instance.Spec.Image = "changed-serving-template"
	// The candidate image replaces the serving image in the fingerprint, so
	// a serving-image-only change does not alter this candidate's template.
	if ok, err := check(); !ok || err != nil {
		t.Fatalf("identical candidate template rejected: %v", err)
	}
	instance.Spec.ServiceAccountName = "changed"
	if ok, err := check(); ok || err != nil {
		t.Fatal("changed candidate template accepted")
	}
}
