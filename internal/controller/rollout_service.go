package controller

import (
	"context"
	"fmt"
	"reflect"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	"github.com/huyz0/opensearch-serverless-operator/api/v1alpha1"
	"github.com/huyz0/opensearch-serverless-operator/internal/rollout"
	"github.com/huyz0/opensearch-serverless-operator/internal/workload"
)

// planObservedRoleSwitch reads the live Service and checks the supplied
// operation claim. Candidate and route evidence remain the caller's separately
// qualified inputs; no Service is modified by this function.
func (r *OperationReconciler) planObservedRoleSwitch(ctx context.Context, instance *v1alpha1.OpenSearchServerless, operation *v1alpha1.OpenSearchServerlessOperation, intent rollout.SwitchIntent, candidate *rollout.CandidateEvidence, route *rollout.RouteEvidence, maxAge time.Duration) (rollout.SwitchAction, error) {
	if r.Core == nil || instance == nil || operation == nil || instance.UID == "" || operation.UID == "" || operation.Name == "" || operation.Generation <= 0 || instance.ResourceVersion == "" || operation.ResourceVersion == "" || instance.DeletionTimestamp != nil || operation.DeletionTimestamp != nil || instance.Namespace != operation.Namespace || instance.Name != operation.Spec.InstanceRef.Name || string(instance.UID) != operation.Spec.InstanceRef.UID || intent.InstanceUID != string(instance.UID) || intent.OperationUID != string(operation.UID) {
		return rollout.SwitchAction{}, fmt.Errorf("fresh live instance and operation identities are required")
	}
	if err := operation.Spec.Validate(); err != nil {
		return rollout.SwitchAction{}, fmt.Errorf("invalid switch operation: %w", err)
	}
	if (operation.Spec.Action != "Promote" && operation.Spec.Action != "Rollback") || operation.Spec.TargetFleet != intent.TargetFleet || operation.Spec.ExpectedServingFleetByRole[intent.Role] != intent.SourceFleet {
		return rollout.SwitchAction{}, fmt.Errorf("switch intent differs from operation")
	}
	if intent.AdoptExisting && operation.Spec.Action != "Rollback" {
		return rollout.SwitchAction{}, fmt.Errorf("only rollback can adopt an existing target")
	}
	claim := operationClaimFor(operation)
	if instance.Status.ActiveOperation == nil || *instance.Status.ActiveOperation != claim {
		return rollout.SwitchAction{Kind: rollout.Hold, Reason: "ClaimMismatch"}, nil
	}
	if instance.Status.ServingFleetIntentByRole[intent.Role] != intent.SourceFleet {
		return rollout.SwitchAction{Kind: rollout.Hold, Reason: "ServingIntentConflict"}, nil
	}
	snapshot, err := r.observeRoleSwitch(ctx, instance, operation, intent, candidate, route, maxAge)
	if err != nil {
		return rollout.SwitchAction{}, err
	}
	return rollout.PlanSwitch(intent, snapshot)
}

// observeRoleSwitch reads the Service from the API rather than constructing a
// traffic observation from desired state. Its caller checks the operation claim.
func (r *OperationReconciler) observeRoleSwitch(ctx context.Context, instance *v1alpha1.OpenSearchServerless, operation *v1alpha1.OpenSearchServerlessOperation, intent rollout.SwitchIntent, candidate *rollout.CandidateEvidence, route *rollout.RouteEvidence, maxAge time.Duration) (rollout.SwitchSnapshot, error) {
	if r.Core == nil || instance == nil || operation == nil {
		return rollout.SwitchSnapshot{}, fmt.Errorf("Service client, instance and operation are required")
	}
	before, err := workload.RoleService(instance, intent.Role, intent.SourceFleet, 9200)
	if err != nil {
		return rollout.SwitchSnapshot{}, err
	}
	after, err := workload.RoleService(instance, intent.Role, intent.TargetFleet, 9200)
	if err != nil {
		return rollout.SwitchSnapshot{}, err
	}
	service, err := r.Core.Services(instance.Namespace).Get(ctx, before.Name, metav1.GetOptions{})
	if err != nil {
		return rollout.SwitchSnapshot{}, fmt.Errorf("observe role Service: %w", err)
	}
	now := time.Now()
	if r.Now != nil {
		now = r.Now()
	}
	shapeMatches := roleServiceShapeMatches(instance, service, before)
	observed := rollout.ServiceObservation{InstanceUID: string(instance.UID), ServiceUID: string(service.UID), ResourceVersion: service.ResourceVersion, Role: intent.Role, Owned: shapeMatches, RoutingShapeValid: shapeMatches, ObservedAt: now}
	switch {
	case reflect.DeepEqual(service.Spec.Selector, before.Spec.Selector):
		observed.Fleet = intent.SourceFleet
	case reflect.DeepEqual(service.Spec.Selector, after.Spec.Selector):
		observed.Fleet = intent.TargetFleet
	}
	return rollout.SwitchSnapshot{Now: now, MaxAge: maxAge, ClaimInstanceUID: string(instance.UID), ClaimOperationUID: string(operation.UID), Service: observed, Candidate: candidate, Route: route}, nil
}

// applyPlannedRoleSwitch binds the conditional patch to the exact Service
// version observed by the planner. authorize checks the plan immediately
// before the patch, after Service validation.
// A successful patch is not proof of endpoint or client-route convergence.
func (r *OperationReconciler) applyPlannedRoleSwitch(ctx context.Context, instance *v1alpha1.OpenSearchServerless, action rollout.SwitchAction, authorize func() error) error {
	if action.Kind != rollout.PatchService || action.ServiceUID == "" || action.ResourceVersion == "" {
		return fmt.Errorf("a planned Service patch with exact identity and version is required")
	}
	return switchRoleServiceConditional(ctx, r.Core, instance, action.Role, types.UID(action.ServiceUID), action.ResourceVersion, action.SourceFleet, action.TargetFleet, authorize)
}
