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
	kubernetesfake "k8s.io/client-go/kubernetes/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/huyz0/opensearch-serverless-operator/api/v1alpha1"
	"github.com/huyz0/opensearch-serverless-operator/internal/rollout"
	"github.com/huyz0/opensearch-serverless-operator/internal/workload"
)

func TestRolloutProgressPersistsAndRecovers(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	instance := &v1alpha1.OpenSearchServerless{ObjectMeta: metav1.ObjectMeta{Name: "catalog", Namespace: "n", UID: "i", ResourceVersion: "1"}}
	instance.Status.ActiveOperation = &v1alpha1.OperationClaim{Name: "promotion", UID: "o", Action: "Promote", Generation: 1}
	instance.Status.ServingFleetIntentByRole = map[string]string{"search": "blue", "ingest": "blue"}
	op := &v1alpha1.OpenSearchServerlessOperation{ObjectMeta: metav1.ObjectMeta{Name: "promotion", Namespace: "n", UID: "o", Generation: 1}, Spec: v1alpha1.OperationSpec{InstanceRef: v1alpha1.InstanceReference{Name: "catalog", UID: "i"}, Action: "Promote", TargetFleet: "green", ExpectedServingFleetByRole: map[string]string{"search": "blue", "ingest": "blue"}}}
	scheme := runtime.NewScheme()
	if err := v1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	c := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(op).WithObjects(op).Build()
	key := types.NamespacedName{Name: op.Name, Namespace: op.Namespace}
	if err := c.Get(ctx, key, op); err != nil {
		t.Fatal(err)
	}
	stale := op.DeepCopyObject().(*v1alpha1.OpenSearchServerlessOperation)
	roles := map[string]v1alpha1.RolloutRoleStatus{
		"search": {ServiceUID: "s", SourceFleet: "blue", TargetFleet: "green", CandidateRevision: "rs", Phase: "Prepared"},
		"ingest": {ServiceUID: "w", SourceFleet: "blue", TargetFleet: "green", CandidateRevision: "rw", Phase: "Prepared"},
	}
	if err := prepareRolloutProgress(ctx, c, instance, op, roles); err != nil {
		t.Fatal(err)
	}
	if err := prepareRolloutProgress(ctx, c, instance, stale, roles); !apierrors.IsConflict(err) || stale.Status.RolloutRoles != nil {
		t.Fatalf("stale prepare should conflict without modifying caller: %v", err)
	}
	snapshots := map[string]rollout.SwitchSnapshot{}
	for role, state := range roles {
		snapshots[role] = rollout.SwitchSnapshot{Now: now, MaxAge: time.Minute, ClaimInstanceUID: "i", ClaimOperationUID: "o", Service: rollout.ServiceObservation{InstanceUID: "i", ServiceUID: state.ServiceUID, ResourceVersion: "1", Role: role, Fleet: "blue", Owned: true, RoutingShapeValid: true, ObservedAt: now}, Candidate: &rollout.CandidateEvidence{InstanceUID: "i", OperationUID: "o", Role: role, Fleet: "green", Revision: state.CandidateRevision, CapacityQualified: true, PeerQualified: true, SecurityQualified: true, RollbackCompatible: true, ObservedAt: now}}
	}
	searchService, err := workload.RoleService(instance, "search", "blue", 9200)
	if err != nil {
		t.Fatal(err)
	}
	searchService.UID, searchService.ResourceVersion = "s", "1"
	searchService.Spec.Type, searchService.Spec.ClusterIP = corev1.ServiceTypeClusterIP, "10.0.0.1"
	ingestService, err := workload.RoleService(instance, "ingest", "blue", 9200)
	if err != nil {
		t.Fatal(err)
	}
	ingestService.UID, ingestService.ResourceVersion = "w", "1"
	ingestService.Spec.Type, ingestService.Spec.ClusterIP = corev1.ServiceTypeClusterIP, "10.0.0.2"
	core := kubernetesfake.NewSimpleClientset(searchService, ingestService)
	r := &OperationReconciler{Client: c, Core: core.CoreV1(), Now: func() time.Time { return now }}
	candidates := map[string]*rollout.CandidateEvidence{"search": snapshots["search"].Candidate, "ingest": snapshots["ingest"].Candidate}
	ingestService.Spec.Selector[workload.FleetLabel] = "foreign"
	if _, err := core.CoreV1().Services("n").Update(ctx, ingestService, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	observedAction, err := r.observeAndAdvanceRollout(ctx, instance, op, candidates, nil, time.Minute)
	if err != nil || observedAction.Switch.Kind != rollout.Hold || observedAction.Role != "ingest" {
		t.Fatalf("live conflicting Service accepted: %#v %v", observedAction, err)
	}
	ingestService.Spec.Selector[workload.FleetLabel] = "blue"
	if _, err := core.CoreV1().Services("n").Update(ctx, ingestService, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	observedAction, err = r.observeAndAdvanceRollout(ctx, instance, op, candidates, nil, time.Minute)
	if err != nil || observedAction.Switch.Kind != rollout.PatchService || observedAction.Role != "search" {
		t.Fatalf("live Service observation did not plan switch: %#v %v", observedAction, err)
	}
	other := snapshots["ingest"]
	other.Service.Fleet = "foreign"
	snapshots["ingest"] = other
	a, err := advanceRolloutProgress(ctx, c, instance, op, snapshots)
	if err != nil || a.Switch.Kind != rollout.Hold || a.Role != "ingest" {
		t.Fatalf("conflicting other Service accepted: %#v %v", a, err)
	}
	other.Service.Fleet = "blue"
	snapshots["ingest"] = other
	a, err = advanceRolloutProgress(ctx, c, instance, op, snapshots)
	if err != nil || a.Switch.Kind != rollout.PatchService {
		t.Fatalf("expected patch plan: %#v %v", a, err)
	}
	s := snapshots["search"]
	s.Service.Fleet, s.Service.ResourceVersion = "green", "2"
	snapshots["search"] = s
	a, err = advanceRolloutProgress(ctx, c, instance, op, snapshots)
	if err != nil || a.Switch.Kind != rollout.RecordApplied {
		t.Fatalf("lost patch response not recorded: %#v %v", a, err)
	}
	// Re-fetch as a restarted reconciler, then require a new route observation.
	restarted := &v1alpha1.OpenSearchServerlessOperation{}
	if err := c.Get(ctx, key, restarted); err != nil {
		t.Fatal(err)
	}
	a, err = advanceRolloutProgress(ctx, c, instance, restarted, snapshots)
	if err != nil || a.Switch.Kind != rollout.Hold {
		t.Fatalf("route must be observed: %#v %v", a, err)
	}
	s.Route = &rollout.RouteEvidence{InstanceUID: "i", OperationUID: "o", Role: "search", Fleet: "green", ServiceUID: "s", ServiceVersion: "2", EndpointsConverged: true, ClientProbePassed: true, ObservedAt: now}
	snapshots["search"] = s
	a, err = advanceRolloutProgress(ctx, c, instance, restarted, snapshots)
	if err != nil || a.Switch.Kind != rollout.RecordConverged {
		t.Fatalf("convergence not persisted: %#v %v", a, err)
	}
	a, err = advanceRolloutProgress(ctx, c, instance, restarted, snapshots)
	if err != nil || a.Role != "ingest" || a.Switch.Kind != rollout.PatchService {
		t.Fatalf("ingest did not advance: %#v %v", a, err)
	}
	instance.Status.ActiveOperation.UID = "other"
	if _, err := advanceRolloutProgress(ctx, c, instance, restarted, snapshots); err == nil {
		t.Fatal("changed operation claim accepted")
	}
}
