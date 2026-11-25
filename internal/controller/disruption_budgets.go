package controller

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"time"

	policyv1 "k8s.io/api/policy/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/apimachinery/pkg/util/validation"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/huyz0/opensearch-serverless-operator/api/v1alpha1"
	"github.com/huyz0/opensearch-serverless-operator/internal/workload"
)

func disruptionBudgetName(instance *v1alpha1.OpenSearchServerless, fleet, role, zone string) string {
	hash := sha256.Sum256([]byte(fmt.Sprintf("%s/%s/%s/%s", instance.UID, fleet, role, zone)))
	prefix := instance.Name
	if len(prefix) > 40 {
		prefix = strings.TrimRight(prefix[:40], "-.")
	}
	return fmt.Sprintf("%s-pdb-%x", prefix, hash[:8])
}

func (r *InstanceReconciler) reconcileDisruptionBudgets(ctx context.Context, instance *v1alpha1.OpenSearchServerless) (ctrl.Result, bool, error) {
	if r.Policy == nil {
		return ctrl.Result{}, false, fmt.Errorf("disruption budget client is unavailable")
	}
	plan, err := r.planDisruptionBudgets(ctx, instance)
	if err != nil {
		return ctrl.Result{}, false, err
	}
	type budgetInput struct {
		Name string                           `json:"name"`
		Spec policyv1.PodDisruptionBudgetSpec `json:"spec"`
	}
	inputs := make([]budgetInput, 0, len(plan.desired))
	for _, budget := range plan.desired {
		inputs = append(inputs, budgetInput{Name: budget.Name, Spec: budget.Spec})
	}
	data, err := json.Marshal(inputs)
	if err != nil {
		return ctrl.Result{}, false, err
	}
	want := v1alpha1.DisruptionBudgetIntent{Generation: instance.Generation, Revision: fmt.Sprintf("%x", sha256.Sum256(data))}
	if instance.Status.DisruptionBudgetIntent == nil || *instance.Status.DisruptionBudgetIntent != want {
		before := instance.DeepCopyObject().(*v1alpha1.OpenSearchServerless)
		instance.Status.DisruptionBudgetIntent = &want
		meta.SetStatusCondition(&instance.Status.Conditions, metav1.Condition{Type: "DisruptionBudgetsConfigured", Status: metav1.ConditionUnknown, Reason: "Reconciling", Message: "Recorded disruption budget intent is being applied", ObservedGeneration: instance.Generation})
		if err := r.Status().Patch(ctx, instance, client.MergeFromWithOptions(before, client.MergeFromWithOptimisticLock{})); err != nil {
			return ctrl.Result{}, false, err
		}
		return ctrl.Result{Requeue: true}, false, nil
	}
	budgets := r.Policy.PodDisruptionBudgets(instance.Namespace)
	for _, expected := range plan.desired {
		actual, err := budgets.Get(ctx, expected.Name, metav1.GetOptions{})
		if apierrors.IsNotFound(err) {
			if !budgetReconciling(instance) {
				return r.budgetCondition(ctx, instance, metav1.ConditionUnknown, "Reconciling", "Recorded disruption budget intent is being applied")
			}
			current, err := r.disruptionBudgetIntentCurrent(ctx, instance)
			if err != nil || !current {
				return ctrl.Result{Requeue: !current}, false, err
			}
			if _, err := budgets.Create(ctx, expected, metav1.CreateOptions{}); err != nil && !apierrors.IsAlreadyExists(err) {
				return ctrl.Result{}, false, fmt.Errorf("create disruption budget: %w", err)
			}
			return ctrl.Result{Requeue: true}, false, nil
		}
		if err != nil {
			return ctrl.Result{}, false, fmt.Errorf("read disruption budget: %w", err)
		}
		if !disruptionBudgetShapeMatches(instance, actual, expected) {
			return r.budgetCondition(ctx, instance, metav1.ConditionFalse, "Conflict", "A disruption budget conflicts with recorded ownership or policy")
		}
		if !reflect.DeepEqual(actual.Spec.MinAvailable, expected.Spec.MinAvailable) {
			if !budgetReconciling(instance) {
				return r.budgetCondition(ctx, instance, metav1.ConditionUnknown, "Reconciling", "Recorded disruption budget intent is being applied")
			}
			minimum := *expected.Spec.MinAvailable
			updateCtx, stop := ctx, func() {}
			if guard, protected := plan.searchZoneFloors[expected.Name]; protected && actual.Spec.MinAvailable.IntVal > minimum.IntVal {
				if !guard.matches(actual) {
					return ctrl.Result{Requeue: true}, false, nil
				}
				updateCtx, stop = context.WithDeadline(ctx, guard.deadline)
			}
			current, err := r.disruptionBudgetIntentCurrent(updateCtx, instance)
			if err != nil || !current {
				stop()
				return ctrl.Result{Requeue: !current}, false, err
			}
			actual.Spec.MinAvailable = &minimum
			_, err = budgets.Update(updateCtx, actual, metav1.UpdateOptions{})
			stop()
			if err != nil {
				return ctrl.Result{}, false, fmt.Errorf("update disruption budget: %w", err)
			}
			return ctrl.Result{Requeue: true}, false, nil
		}
		condition := meta.FindStatusCondition(actual.Status.Conditions, policyv1.DisruptionAllowedCondition)
		if actual.Generation <= 0 || actual.Status.ObservedGeneration != actual.Generation || (condition != nil && condition.Reason == "SyncFailed") {
			return r.budgetCondition(ctx, instance, metav1.ConditionUnknown, "AwaitingObservation", "Waiting for the disruption controller to observe all budgets")
		}
	}
	return r.budgetCondition(ctx, instance, metav1.ConditionTrue, "Observed", "Disruption controller observed all current role and zone budgets")
}

type disruptionBudgetPlan struct {
	desired          []*policyv1.PodDisruptionBudget
	searchZoneFloors map[string]searchZoneFloor
}

// A zero search target can lower a positive floor only while the protection
// evidence is fresh and the budget is still the version checked during planning.
type searchZoneFloor struct {
	uid, version string
	deadline     time.Time
}

func (guard searchZoneFloor) matches(budget *policyv1.PodDisruptionBudget) bool {
	return guard.uid != "" && guard.uid == string(budget.UID) && guard.version != "" &&
		guard.version == budget.ResourceVersion && time.Now().Before(guard.deadline)
}

func (r *InstanceReconciler) planDisruptionBudgets(ctx context.Context, instance *v1alpha1.OpenSearchServerless) (disruptionBudgetPlan, error) {
	var plan disruptionBudgetPlan
	var searchFloorEvidence *time.Time
	plan.searchZoneFloors = make(map[string]searchZoneFloor)
	budgets := r.Policy.PodDisruptionBudgets(instance.Namespace)
	for role, pool := range instance.Spec.Pools {
		fleet := instance.Status.ServingFleetIntentByRole[role]
		for zone, config := range pool.Zones {
			if len(validation.IsValidLabelValue(zone)) != 0 || len(validation.IsValidLabelValue(fleet)) != 0 {
				return disruptionBudgetPlan{}, fmt.Errorf("zone or fleet cannot be represented as a Kubernetes label")
			}
			budget, err := workload.DisruptionBudget(instance, disruptionBudgetName(instance, fleet, role, zone), fleet, role, zone, config.MinServing)
			if err != nil {
				return disruptionBudgetPlan{}, err
			}
			if r.DevelopmentSearchScaleDown && role == "search" && config.Replicas == 0 && config.WarmSpare == 0 {
				guard := searchZoneFloor{}
				actual, err := budgets.Get(ctx, budget.Name, metav1.GetOptions{})
				if err != nil && !apierrors.IsNotFound(err) {
					return disruptionBudgetPlan{}, err
				}
				if err == nil && disruptionBudgetShapeMatches(instance, actual, budget) && actual.Spec.MinAvailable != nil &&
					actual.Spec.MinAvailable.Type == intstr.Int && actual.Spec.MinAvailable.IntVal > 0 {
					if searchFloorEvidence == nil {
						deadline := r.retiringSearchZoneFloorDeadline(ctx, instance)
						searchFloorEvidence = &deadline
					}
					if searchFloorEvidence.IsZero() {
						minimum := *actual.Spec.MinAvailable
						budget.Spec.MinAvailable = &minimum
					} else {
						guard = searchZoneFloor{uid: string(actual.UID), version: actual.ResourceVersion, deadline: *searchFloorEvidence}
					}
				}
				plan.searchZoneFloors[budget.Name] = guard
			}
			plan.desired = append(plan.desired, budget)
		}
	}
	for _, policy := range developmentCandidatePolicies(instance) {
		group := policy.Group
		if len(validation.IsValidLabelValue(group.Zone)) != 0 || len(validation.IsValidLabelValue(group.Fleet)) != 0 {
			return disruptionBudgetPlan{}, fmt.Errorf("candidate zone or fleet cannot be represented as a Kubernetes label")
		}
		minimum := instance.Spec.Pools[group.Role].Zones[group.Zone].MinServing
		if int(minimum) > policy.Desired {
			minimum = int32(policy.Desired)
		}
		budget, err := workload.DisruptionBudget(instance, disruptionBudgetName(instance, group.Fleet, group.Role, group.Zone), group.Fleet, group.Role, group.Zone, minimum)
		if err != nil {
			return disruptionBudgetPlan{}, err
		}
		plan.desired = append(plan.desired, budget)
	}
	sort.Slice(plan.desired, func(i, j int) bool { return plan.desired[i].Name < plan.desired[j].Name })
	return plan, nil
}

func (r *InstanceReconciler) disruptionBudgetIntentCurrent(ctx context.Context, expected *v1alpha1.OpenSearchServerless) (bool, error) {
	reader := r.APIReader
	if reader == nil {
		reader = r.Client
	}
	var latest v1alpha1.OpenSearchServerless
	if err := reader.Get(ctx, client.ObjectKeyFromObject(expected), &latest); err != nil {
		return false, client.IgnoreNotFound(err)
	}
	return latest.UID == expected.UID && latest.ResourceVersion == expected.ResourceVersion &&
		latest.DeletionTimestamp == nil && latest.Status.ActiveOperation == nil, nil
}

func disruptionBudgetShapeMatches(instance *v1alpha1.OpenSearchServerless, actual, expected *policyv1.PodDisruptionBudget) bool {
	owner := metav1.GetControllerOf(actual)
	return actual.DeletionTimestamp == nil && owner != nil && owner.UID == instance.UID && owner.Name == instance.Name &&
		owner.Kind == "OpenSearchServerless" && owner.APIVersion == v1alpha1.GroupVersion.String() &&
		reflect.DeepEqual(actual.Spec.Selector, expected.Spec.Selector) && actual.Spec.MaxUnavailable == nil &&
		actual.Spec.MinAvailable != nil && actual.Spec.MinAvailable.Type == intstr.Int && actual.Spec.MinAvailable.IntVal >= 0 &&
		(actual.Spec.UnhealthyPodEvictionPolicy == nil || *actual.Spec.UnhealthyPodEvictionPolicy == policyv1.IfHealthyBudget)
}

// observeRolloutBudgets verifies the full serving floors on the target fleet.
// A bounded preview can legitimately have weaker floors while staging, but
// those budgets cannot protect the fleet during promotion.
func (r *OperationReconciler) observeRolloutBudgets(ctx context.Context, instance *v1alpha1.OpenSearchServerless, fleet string) (bool, error) {
	if r.Policy == nil {
		return false, fmt.Errorf("disruption budget client is unavailable")
	}
	budgets := r.Policy.PodDisruptionBudgets(instance.Namespace)
	for _, role := range []string{"search", "ingest"} {
		hasZone := false
		for zone, config := range instance.Spec.Pools[role].Zones {
			if int64(config.Replicas)+int64(config.WarmSpare) == 0 {
				continue
			}
			hasZone = true
			expected, err := workload.DisruptionBudget(instance, disruptionBudgetName(instance, fleet, role, zone), fleet, role, zone, config.MinServing)
			if err != nil {
				return false, err
			}
			actual, err := budgets.Get(ctx, expected.Name, metav1.GetOptions{})
			if apierrors.IsNotFound(err) {
				return false, nil
			}
			if err != nil {
				return false, fmt.Errorf("read target disruption budget: %w", err)
			}
			condition := meta.FindStatusCondition(actual.Status.Conditions, policyv1.DisruptionAllowedCondition)
			if actual.UID == "" || actual.ResourceVersion == "" || !disruptionBudgetShapeMatches(instance, actual, expected) ||
				!reflect.DeepEqual(actual.Spec.MinAvailable, expected.Spec.MinAvailable) || actual.Generation <= 0 ||
				actual.Status.ObservedGeneration != actual.Generation || actual.Status.DesiredHealthy != config.MinServing ||
				actual.Status.CurrentHealthy < config.MinServing || condition == nil || condition.Reason == "SyncFailed" {
				return false, nil
			}
		}
		if !hasZone {
			return false, nil
		}
	}
	return true, nil
}

func budgetReconciling(instance *v1alpha1.OpenSearchServerless) bool {
	condition := meta.FindStatusCondition(instance.Status.Conditions, "DisruptionBudgetsConfigured")
	return condition != nil && condition.Status == metav1.ConditionUnknown && condition.Reason == "Reconciling" && condition.ObservedGeneration == instance.Generation
}

func (r *InstanceReconciler) budgetCondition(ctx context.Context, instance *v1alpha1.OpenSearchServerless, status metav1.ConditionStatus, reason, message string) (ctrl.Result, bool, error) {
	before := instance.DeepCopyObject().(*v1alpha1.OpenSearchServerless)
	meta.SetStatusCondition(&instance.Status.Conditions, metav1.Condition{Type: "DisruptionBudgetsConfigured", Status: status, Reason: reason, Message: message, ObservedGeneration: instance.Generation})
	if !reflect.DeepEqual(before.Status, instance.Status) {
		if err := r.Status().Patch(ctx, instance, client.MergeFromWithOptions(before, client.MergeFromWithOptimisticLock{})); err != nil {
			return ctrl.Result{}, false, err
		}
		return ctrl.Result{Requeue: true}, false, nil
	}
	if status != metav1.ConditionTrue {
		return ctrl.Result{RequeueAfter: time.Minute}, false, nil
	}
	return ctrl.Result{}, true, nil
}
