package controller

import (
	"context"
	"errors"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	kubernetesfake "k8s.io/client-go/kubernetes/fake"
	kubetesting "k8s.io/client-go/testing"

	"github.com/huyz0/opensearch-serverless-operator/api/v1alpha1"
	"github.com/huyz0/opensearch-serverless-operator/internal/rollout"
	"github.com/huyz0/opensearch-serverless-operator/internal/workload"
)

func TestObservedRoleSwitchRejectsConflictAndUnqualifiedTarget(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	instance := &v1alpha1.OpenSearchServerless{ObjectMeta: metav1.ObjectMeta{Name: "catalog", Namespace: "n", UID: "instance", ResourceVersion: "1"}}
	op := &v1alpha1.OpenSearchServerlessOperation{ObjectMeta: metav1.ObjectMeta{Name: "promote", Namespace: "n", UID: "operation", ResourceVersion: "1", Generation: 1}, Spec: v1alpha1.OperationSpec{InstanceRef: v1alpha1.InstanceReference{Name: "catalog", UID: "instance"}, Action: "Promote", TargetFleet: "green", ExpectedServingFleetByRole: map[string]string{"search": "blue", "ingest": "blue"}}}
	instance.Status.ServingFleetIntentByRole = map[string]string{"search": "blue", "ingest": "blue"}
	instance.Status.ActiveOperation = &v1alpha1.OperationClaim{Name: "promote", UID: "operation", Action: "Promote", Generation: 1}
	service, err := workload.RoleService(instance, "search", "blue", 9200)
	if err != nil {
		t.Fatal(err)
	}
	service.UID = "service"
	service.ResourceVersion = "10"
	service.Spec.Type = corev1.ServiceTypeClusterIP
	service.Spec.ClusterIP = "10.0.0.1"
	core := kubernetesfake.NewSimpleClientset(service)
	r := &OperationReconciler{Core: core.CoreV1(), Now: func() time.Time { return now }}
	intent := rollout.SwitchIntent{InstanceUID: "instance", OperationUID: "operation", Role: "search", ServiceUID: "service", SourceFleet: "blue", TargetFleet: "green", CandidateRevision: "revision", Phase: rollout.Prepared}
	candidate := &rollout.CandidateEvidence{InstanceUID: "instance", OperationUID: "operation", Role: "search", Fleet: "green", Revision: "revision", CapacityQualified: true, PeerQualified: true, SecurityQualified: true, RollbackCompatible: true, ObservedAt: now}
	plan := func() rollout.SwitchAction {
		t.Helper()
		action, err := r.planObservedRoleSwitch(context.Background(), instance, op, intent, candidate, nil, time.Minute)
		if err != nil {
			t.Fatal(err)
		}
		return action
	}
	if action := plan(); action.Kind != rollout.PatchService || action.ResourceVersion != "10" {
		t.Fatalf("valid plan rejected: %#v", action)
	}
	t.Run("evidence expires on the final Service read", func(t *testing.T) {
		candidate.ValidUntil = now.Add(time.Second)
		action := plan()
		original := now
		defer func() { now, candidate.ValidUntil = original, time.Time{} }()
		staleCore := kubernetesfake.NewSimpleClientset(service.DeepCopy())
		staleCore.PrependReactor("get", "services", func(kubetesting.Action) (bool, runtime.Object, error) {
			now = candidate.ValidUntil
			return false, nil, nil
		})
		adapter := &OperationReconciler{Core: staleCore.CoreV1()}
		err := adapter.applyPlannedRoleSwitch(context.Background(), instance, action, func() error {
			if !now.Before(candidate.ValidUntil) {
				return errRolloutPlanChanged
			}
			return nil
		})
		if !errors.Is(err, errRolloutPlanChanged) || len(staleCore.Actions()) != 1 || staleCore.Actions()[0].GetVerb() != "get" {
			t.Fatalf("expired evidence authorized a Service mutation: %v", err)
		}
	})
	instance.Status.ActiveOperation.UID = "other"
	if action := plan(); action.Kind != rollout.Hold || action.Reason != "ClaimMismatch" {
		t.Fatal("stale claim accepted")
	}
	instance.Status.ActiveOperation.UID = "operation"
	instance.Status.ServingFleetIntentByRole["search"] = "other"
	if action := plan(); action.Kind != rollout.Hold || action.Reason != "ServingIntentConflict" {
		t.Fatal("stale serving intent accepted")
	}
	instance.Status.ServingFleetIntentByRole["search"] = "blue"
	candidate.SecurityQualified = false
	if action := plan(); action.Kind != rollout.Hold || action.Reason != "CandidateNotQualified" {
		t.Fatal("unqualified candidate accepted")
	}
	candidate.SecurityQualified = true
	service.Spec.Selector[workload.FleetLabel] = "foreign"
	if _, err := core.CoreV1().Services("n").Update(context.Background(), service, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	if action := plan(); action.Kind != rollout.Hold || action.Reason != "SelectorConflict" {
		t.Fatal("foreign selector accepted")
	}
}

func TestObservedRollbackAdoptsOnlyExpectedExistingTarget(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	instance := &v1alpha1.OpenSearchServerless{ObjectMeta: metav1.ObjectMeta{Name: "catalog", Namespace: "n", UID: "instance", ResourceVersion: "1"}}
	instance.Status.ServingFleetIntentByRole = map[string]string{"search": "blue", "ingest": "green"}
	op := &v1alpha1.OpenSearchServerlessOperation{ObjectMeta: metav1.ObjectMeta{Name: "rollback", Namespace: "n", UID: "operation", ResourceVersion: "1", Generation: 1}, Spec: v1alpha1.OperationSpec{InstanceRef: v1alpha1.InstanceReference{Name: "catalog", UID: "instance"}, Action: "Rollback", TargetFleet: "blue", ExpectedServingFleetByRole: map[string]string{"search": "blue", "ingest": "green"}}}
	instance.Status.ActiveOperation = &v1alpha1.OperationClaim{Name: "rollback", UID: "operation", Action: "Rollback", Generation: 1}
	service, err := workload.RoleService(instance, "search", "blue", 9200)
	if err != nil {
		t.Fatal(err)
	}
	service.UID, service.ResourceVersion = "service", "10"
	service.Spec.Type, service.Spec.ClusterIP = corev1.ServiceTypeClusterIP, "10.0.0.1"
	core := kubernetesfake.NewSimpleClientset(service)
	r := &OperationReconciler{Core: core.CoreV1(), Now: func() time.Time { return now }}
	intent := rollout.SwitchIntent{InstanceUID: "instance", OperationUID: "operation", Role: "search", ServiceUID: "service", SourceFleet: "blue", TargetFleet: "blue", CandidateRevision: "revision", AdoptExisting: true, Phase: rollout.Prepared}
	a, err := r.planObservedRoleSwitch(context.Background(), instance, op, intent, nil, nil, time.Minute)
	if err != nil || a.Kind != rollout.RecordApplied || a.Reason != "ExistingTargetObserved" {
		t.Fatalf("existing target not adopted: %#v %v", a, err)
	}
	op.Spec.Action = "Promote"
	if _, err = r.planObservedRoleSwitch(context.Background(), instance, op, intent, nil, nil, time.Minute); err == nil {
		t.Fatal("promotion accepted adoption")
	}
	op.Spec.Action = "Rollback"
	service.Spec.Selector[workload.FleetLabel] = "green"
	if _, err = core.CoreV1().Services("n").Update(context.Background(), service, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	a, err = r.planObservedRoleSwitch(context.Background(), instance, op, intent, nil, nil, time.Minute)
	if err != nil || a.Kind != rollout.Hold || a.Reason != "SelectorConflict" {
		t.Fatalf("changed selector accepted: %#v %v", a, err)
	}
}
