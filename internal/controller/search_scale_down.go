package controller

import (
	"context"
	"time"

	coreapi "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/huyz0/opensearch-serverless-operator/api/v1alpha1"
	"github.com/huyz0/opensearch-serverless-operator/internal/workload"
	"github.com/huyz0/opensearch-serverless-operator/pkg/podlifecycle"
	podkube "github.com/huyz0/opensearch-serverless-operator/pkg/podlifecycle/kubernetes"
)

const searchScaleDownMaxAge = 5 * time.Second
const searchScaleDownDenialBackoff = 30 * time.Second

// Keep an existing positive PDB floor on a retiring zone until the regular
// search endpoints can cover the move. Provisioning can continue meanwhile.
func (r *InstanceReconciler) retiringSearchZoneFloorDeadline(ctx context.Context, instance *v1alpha1.OpenSearchServerless) time.Time {
	status := instance.Status
	if !r.DevelopmentObserveEndpoints || !r.fleetQualification().supportsDevelopmentSearchProbe(instance) ||
		!instance.Spec.Pools["search"].WarmOnJoin || instance.Spec.Pools["search"].WarmReadiness != "Strict" ||
		instance.Spec.Availability == nil || instance.Spec.Availability.MaxConcurrentRetirements < 1 ||
		status.ActiveOperation != nil || status.CandidateFleet != nil || status.RollbackTarget != nil ||
		status.FleetRetirement != nil || status.EmergencyRetirement != nil || status.EmergencyCapacityProposal != nil {
		return time.Time{}
	}
	observed, err := r.fleetQualification().observeServingRoleProtection(ctx, instance, "search", searchScaleDownMaxAge, r.fleetQualification().qualifyDevelopmentSearchPod, status.ColdBootstrap, true)
	if err != nil || !observed.satisfiesRegularServingTargets(instance, "search") || !observed.leasesCurrent(time.Now()) {
		return time.Time{}
	}
	return searchScaleDownDeadline(observed)
}

func (r *InstanceReconciler) reconcileSearchScaleDown(ctx context.Context, instance *v1alpha1.OpenSearchServerless) (ctrl.Result, bool, error) {
	pending := instance.Status.ScaleDownRetirement
	retry := ctrl.Result{RequeueAfter: searchScaleDownMaxAge}
	writeIntent := func(intent *v1alpha1.ScaleDownRetirementIntent, retryAfter *metav1.Time) (ctrl.Result, bool, error) {
		before := instance.DeepCopyObject().(*v1alpha1.OpenSearchServerless)
		instance.Status.ScaleDownRetirement = intent
		instance.Status.ScaleDownRetryAfter = retryAfter
		err := r.Status().Patch(ctx, instance, client.MergeFromWithOptions(before, client.MergeFromWithOptimisticLock{}))
		return ctrl.Result{Requeue: true}, true, err
	}
	cancel := func() (ctrl.Result, bool, error) {
		if pending != nil {
			return writeIntent(nil, nil)
		}
		return ctrl.Result{}, false, nil
	}
	if r.Core == nil {
		return ctrl.Result{}, false, nil
	}
	if pending != nil {
		pod, err := r.Core.Pods(instance.Namespace).Get(ctx, pending.Name, metav1.GetOptions{})
		if apierrors.IsNotFound(err) || (err == nil && string(pod.UID) != pending.PodUID) {
			return writeIntent(nil, nil)
		}
		if err != nil {
			return retry, true, err
		}
		if scaleDownEvictionInFlight(pod) {
			// The UID holds the retirement slot until it is gone. Reconciliation
			// can still restore capacity if a different Pod disappears.
			return ctrl.Result{}, false, nil
		}
	}
	availability, status := instance.Spec.Availability, instance.Status
	pool := instance.Spec.Pools["search"]
	fleet := status.ServingFleetIntentByRole["search"]
	if pending == nil && status.ScaleDownRetryAfter != nil && r.now().Before(status.ScaleDownRetryAfter.Time) {
		return ctrl.Result{}, false, nil
	}
	if !r.DevelopmentSearchScaleDown || !r.DevelopmentObserveEndpoints || r.Policy == nil || instance.DeletionTimestamp != nil ||
		availability == nil || availability.MaxConcurrentRetirements < 1 || fleet == "" ||
		!pool.WarmOnJoin || pool.WarmReadiness != "Strict" || !r.fleetQualification().supportsDevelopmentSearchProbe(instance) ||
		status.PrimaryStore == nil || status.ActiveOperation != nil || status.FleetRetirement != nil ||
		status.CandidateFleet != nil || status.RollbackTarget != nil || status.EmergencyRetirement != nil ||
		status.EmergencyCapacityProposal != nil || status.PendingPodCreation != nil || status.PendingGateRelease != nil ||
		status.ProvisioningAttempt != nil || status.SearchWarmupSlot != nil ||
		(pending != nil && (pending.Generation != instance.Generation || pending.Role != "search" || pending.Fleet != fleet)) {
		return cancel()
	}
	pods, stable, err := r.scaleDownPodInventory(ctx, instance)
	if err != nil {
		return retry, pending != nil, err
	}
	if !stable {
		return cancel()
	}
	counts := make(map[string]int64, len(pool.Zones))
	for _, pod := range pods {
		if pod.Labels[workload.RoleLabel] == "search" && pod.Labels[workload.FleetLabel] == fleet && podStartupReady(&pod) {
			counts[pod.Labels[workload.ZoneLabel]]++
		}
	}
	surplus := false
	for zone, desired := range pool.Zones {
		target := int64(desired.Replicas) + int64(desired.WarmSpare)
		surplus = surplus || counts[zone] > target
	}
	if !surplus || instance.Spec.Validate() != nil {
		return cancel()
	}
	if err := r.checkPinnedPrimaryStore(ctx, instance); err != nil {
		return cancel()
	}
	// Keep explicitly zeroed zones in scope until their Pods are gone.
	observed, err := r.fleetQualification().observeServingRoleProtection(ctx, instance, "search", searchScaleDownMaxAge, r.fleetQualification().qualifyDevelopmentSearchPod, status.ColdBootstrap, true)
	if err != nil {
		return cancel()
	}
	action, err := planSearchScaleDown(instance, observed)
	if err != nil {
		return retry, pending != nil, err
	}
	if action.Kind != podlifecycle.EvictPod || !r.singleScaleDownBudget(ctx, instance, observed.PodSnapshot, action) {
		return cancel()
	}
	want := v1alpha1.ScaleDownRetirementIntent{Generation: instance.Generation, Role: "search", Fleet: fleet,
		Zone: action.Group.Zone, Name: action.Pod.Name, PodUID: action.Pod.UID}
	if pending == nil {
		return writeIntent(&want, nil)
	}
	if *pending != want {
		return cancel()
	}
	service, err := r.Core.Services(instance.Namespace).Get(ctx, observed.Endpoints.ServiceName, metav1.GetOptions{})
	if err != nil || string(service.UID) != observed.ServiceUID || service.ResourceVersion != observed.ServiceVersion {
		return retry, true, nil
	}
	if err := recheckEndpointSlices(ctx, r.Discovery.EndpointSlices(instance.Namespace), observed.Endpoints.ServiceName, observed.Endpoints.SliceVersions); err != nil {
		return retry, true, nil
	}
	if _, stable, err := r.scaleDownPodInventory(ctx, instance); err != nil || !stable ||
		!fleetPodsUnchanged(ctx, r.Core.Pods(instance.Namespace), observed.InstanceUID, "search", fleet, observed.PodSnapshot) {
		return retry, true, err
	}
	reader := r.APIReader
	if reader == nil {
		reader = r.Client
	}
	var live v1alpha1.OpenSearchServerless
	if err := reader.Get(ctx, client.ObjectKeyFromObject(instance), &live); err != nil {
		return retry, true, client.IgnoreNotFound(err)
	}
	now := time.Now()
	if live.UID != instance.UID || live.ResourceVersion != instance.ResourceVersion || live.DeletionTimestamp != nil ||
		now.Sub(observed.ObservedAt) >= searchScaleDownMaxAge || !observed.leasesCurrent(now) {
		return retry, true, nil
	}
	version := observed.Endpoints.ReadyPodVersions[types.UID(action.Pod.UID)]
	if version == "" {
		return cancel()
	}
	evictionCtx, stop := context.WithDeadline(ctx, searchScaleDownDeadline(observed))
	err = podkube.EvictAtVersion(evictionCtx, r.Core, action.Pod, version)
	stop()
	if err == nil {
		return retry, true, nil
	}
	if !apierrors.IsTooManyRequests(err) && !apierrors.IsForbidden(err) && !apierrors.IsUnauthorized(err) &&
		!apierrors.IsInvalid(err) && !apierrors.IsConflict(err) && !apierrors.IsNotFound(err) {
		ctrl.LoggerFrom(ctx).Error(err, "Search scale-down eviction outcome requires rechecking", "pod", pending.Name)
		return ctrl.Result{}, false, nil
	}
	// Release an explicit rejection after checking that the exact victim
	// has not started eviction. Ambiguous responses retain the slot.
	pod, err := r.Core.Pods(instance.Namespace).Get(ctx, pending.Name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) || (err == nil && string(pod.UID) != pending.PodUID) {
		return writeIntent(nil, nil)
	}
	if err != nil {
		return retry, true, err
	}
	if scaleDownEvictionInFlight(pod) {
		return ctrl.Result{}, false, nil
	}
	retryAfter := metav1.NewTime(r.now().Add(searchScaleDownDenialBackoff))
	return writeIntent(nil, &retryAfter)
}

func searchScaleDownDeadline(observed servingRoleProtection) time.Time {
	deadline := observed.ObservedAt.Add(searchScaleDownMaxAge)
	for _, qualification := range observed.Pods {
		if qualification.LeaseValidUntil.Before(deadline) {
			deadline = qualification.LeaseValidUntil
		}
	}
	return deadline
}

func scaleDownEvictionInFlight(pod *coreapi.Pod) bool {
	if pod.DeletionTimestamp != nil {
		return true
	}
	for _, condition := range pod.Status.Conditions {
		if condition.Type == coreapi.DisruptionTarget && condition.Status == coreapi.ConditionTrue && condition.Reason == "EvictionByEvictionAPI" {
			return true
		}
	}
	return false
}

func (r *InstanceReconciler) scaleDownPodInventory(ctx context.Context, instance *v1alpha1.OpenSearchServerless) ([]coreapi.Pod, bool, error) {
	selector := labels.SelectorFromSet(labels.Set{workload.InstanceLabel: string(instance.UID)}).String()
	pods, err := r.Core.Pods(instance.Namespace).List(ctx, metav1.ListOptions{LabelSelector: selector, Limit: 513})
	if err != nil {
		return nil, false, err
	}
	if pods.Continue != "" || len(pods.Items) > 512 {
		return nil, false, nil
	}
	for _, pod := range pods.Items {
		owner := metav1.GetControllerOf(&pod)
		if owner == nil || owner.UID != instance.UID || pod.DeletionTimestamp != nil ||
			workload.PodDisruptionTarget(&pod) || pod.Annotations[emergencyCapacityAnnotation] == "true" {
			return nil, false, nil
		}
	}
	return pods.Items, true, nil
}

func (r *InstanceReconciler) singleScaleDownBudget(ctx context.Context, instance *v1alpha1.OpenSearchServerless, pods []coreapi.Pod, action podlifecycle.Action) bool {
	group := action.Group
	budgetName := disruptionBudgetName(instance, group.Fleet, group.Role, group.Zone)
	floor := instance.Spec.Pools[group.Role].Zones[group.Zone].MinServing
	expected, err := workload.DisruptionBudget(instance, budgetName, group.Fleet, group.Role, group.Zone, floor)
	if err != nil {
		return false
	}
	for _, pod := range pods {
		if string(pod.UID) != action.Pod.UID {
			continue
		}
		budgets, err := r.Policy.PodDisruptionBudgets(pod.Namespace).List(ctx, metav1.ListOptions{Limit: 513})
		if err != nil || budgets.Continue != "" || len(budgets.Items) > 512 {
			return false
		}
		matches := 0
		for _, budget := range budgets.Items {
			selector, err := metav1.LabelSelectorAsSelector(budget.Spec.Selector)
			if err != nil {
				return false
			}
			if selector.Matches(labels.Set(pod.Labels)) {
				if budget.Name != expected.Name || !retirementBudgetReady(instance, &budget, expected) {
					return false
				}
				matches++
			}
		}
		return matches == 1
	}
	return false
}

func planSearchScaleDown(instance *v1alpha1.OpenSearchServerless, observed servingRoleProtection) (podlifecycle.Action, error) {
	group := func(zone string) podlifecycle.Group {
		return podlifecycle.Group{Workload: string(instance.UID), Fleet: observed.Fleet, Role: "search", Zone: zone}
	}
	if bootstrap := observed.BootstrapPod; bootstrap != nil {
		if _, ready := observed.Endpoints.ReadyPodVersions[types.UID(bootstrap.UID)]; ready &&
			observed.satisfiesRegularServingTargets(instance, "search") {
			return podlifecycle.Action{Kind: podlifecycle.EvictPod, Group: group(bootstrap.Zone), Pod: bootstrap.Identity, Reason: "BootstrapSurplus"}, nil
		}
		return podlifecycle.Action{Kind: podlifecycle.Wait, Reason: "BootstrapStillNeeded"}, nil
	}
	var snapshot podlifecycle.Snapshot
	for zone, config := range instance.Spec.Pools["search"].Zones {
		snapshot.Policies = append(snapshot.Policies, podlifecycle.GroupPolicy{
			Group: group(zone), Desired: int(config.Replicas) + int(config.WarmSpare), MinServing: int(config.MinServing),
		})
	}
	for _, pod := range observed.PodSnapshot {
		_, ready := observed.Endpoints.ReadyPodVersions[pod.UID]
		snapshot.Pods = append(snapshot.Pods, podlifecycle.Pod{
			Identity: failbackPodIdentity(instance, &pod).Identity, Group: group(pod.Labels[workload.ZoneLabel]),
			Bound: pod.Spec.NodeName != "", Eligible: ready,
		})
	}
	return podlifecycle.PlanScaleDown(snapshot, podlifecycle.RetirementBudget{
		MinServingRole: servingRegionalFloor(instance, "search"), MaxConcurrentRetirements: int(instance.Spec.Availability.MaxConcurrentRetirements),
	})
}
