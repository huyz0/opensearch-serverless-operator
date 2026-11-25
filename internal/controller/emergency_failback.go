package controller

import (
	"context"
	"fmt"
	"reflect"
	"time"

	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/huyz0/opensearch-serverless-operator/api/v1alpha1"
	"github.com/huyz0/opensearch-serverless-operator/internal/workload"
	"github.com/huyz0/opensearch-serverless-operator/pkg/podlifecycle"
	podkube "github.com/huyz0/opensearch-serverless-operator/pkg/podlifecycle/kubernetes"
)

const bootstrapReplacementAnnotation = "serverless.opensearch.org/replaces-bootstrap"

type emergencyFailbackPod struct {
	podlifecycle.Identity
	Version, Zone string
}

// readFailbackEvidence uses the configured report while emergency capacity is
// enabled. After policy removal, only the report pinned during admission can
// qualify cleanup, and its contents must still match the current generation.
func (r *InstanceReconciler) readFailbackEvidence(ctx context.Context, instance *v1alpha1.OpenSearchServerless) (emergencyCapacityEvidence, *corev1.ConfigMap, error) {
	if availability := instance.Spec.Availability; availability != nil && availability.EmergencyCapacity != nil {
		return r.readEmergencyCapacityEvidence(ctx, instance, availability.EmergencyCapacity.EvidenceRef.Name)
	}
	pin := instance.Status.EmergencyFailbackEvidence
	if pin == nil || pin.Name == "" || pin.UID == "" {
		return emergencyCapacityEvidence{}, nil, fmt.Errorf("no pinned emergency failback report")
	}
	evidence, report, err := r.readEmergencyCapacityEvidence(ctx, instance, pin.Name)
	if err == nil && string(report.UID) != pin.UID {
		return emergencyCapacityEvidence{}, nil, fmt.Errorf("pinned emergency failback report was replaced")
	}
	return evidence, report, err
}

func (r *InstanceReconciler) patchFailbackEvidence(ctx context.Context, instance *v1alpha1.OpenSearchServerless, pin *v1alpha1.EmergencyFailbackEvidence) (ctrl.Result, bool, error) {
	before := instance.DeepCopy()
	instance.Status.EmergencyFailbackEvidence = pin
	err := r.Status().Patch(ctx, instance, client.MergeFromWithOptions(before, client.MergeFromWithOptimisticLock{}))
	return ctrl.Result{Requeue: true}, true, err
}

func matchesSearchBootstrapPod(instance *v1alpha1.OpenSearchServerless, pod *corev1.Pod) bool {
	bootstrap := instance.Status.ColdBootstrap
	if bootstrap == nil || bootstrap.PodUID == "" || bootstrap.Name != pod.Name || bootstrap.PodUID != string(pod.UID) ||
		bootstrap.Fleet != instance.Status.ServingFleetIntentByRole["search"] || pod.Labels[workload.FleetLabel] != bootstrap.Fleet ||
		pod.Labels[workload.RoleLabel] != "search" || pod.Labels[workload.InstanceLabel] != string(instance.UID) ||
		pod.Annotations[warmupModeAnnotation] != "Bootstrap" || pod.Annotations[emergencyCapacityAnnotation] == "true" {
		return false
	}
	owner := metav1.GetControllerOf(pod)
	return owner != nil && owner.UID == instance.UID && owner.Name == instance.Name && owner.Kind == "OpenSearchServerless" && owner.APIVersion == v1alpha1.GroupVersion.String()
}

// planBootstrapReplacement permits one extra regular search Pod while a
// serving cold-bootstrap Pod and emergency capacity are still in place.
// Repeat the same checks at creation and gate release.
func (r *InstanceReconciler) planBootstrapReplacement(ctx context.Context, instance *v1alpha1.OpenSearchServerless, intent *v1alpha1.PodCreationIntent, uid string) *v1alpha1.PodCreationIntent {
	availability, bootstrap := instance.Spec.Availability, instance.Status.ColdBootstrap
	if !r.DevelopmentEmergencyFailback || availability == nil ||
		availability.MaxRollingSurgePerGroup < 1 || availability.MaxConcurrentRetirements < 1 || availability.MaxPendingProvisionablePods < 1 || availability.MaxConcurrentSearchWarmups < 1 ||
		bootstrap == nil || bootstrap.Fleet == "" || instance.Status.ActiveOperation != nil || instance.Status.EmergencyRetirement != nil ||
		instance.Status.ScaleDownRetirement != nil ||
		instance.Status.ProvisioningAttempt != nil || instance.DeletionTimestamp != nil {
		return nil
	}
	if slot := instance.Status.SearchWarmupSlot; slot != nil && (intent == nil || slot.Name != intent.Name || slot.PodUID != uid) {
		return nil
	}
	boot, err := r.Core.Pods(instance.Namespace).Get(ctx, bootstrap.Name, metav1.GetOptions{})
	if err != nil || !matchesSearchBootstrapPod(instance, boot) || boot.DeletionTimestamp != nil || !podStartupReady(boot) || boot.Spec.NodeName == "" {
		return nil
	}
	zone := boot.Labels[workload.ZoneLabel]
	pool := instance.Spec.Pools["search"]
	if _, exists := pool.Zones[zone]; !exists || !pool.WarmOnJoin || pool.WarmReadiness != "Strict" {
		return nil
	}
	if intent != nil && (intent.Role != "search" || intent.Zone != zone || intent.Fleet != bootstrap.Fleet || intent.WarmupMode != "Strict" || intent.BootstrapReplacementUID != bootstrap.PodUID) {
		return nil
	}
	blocked, err := blockedProvisioningGroups(instance)
	if err != nil || blocked[provisioningGroup{"search", zone}] {
		return nil
	}
	evidence, cm, err := r.readFailbackEvidence(ctx, instance)
	if err != nil || evidence.UnavailableZones == nil || len(evidence.UnavailableZones) != 0 ||
		(intent != nil && (intent.EmergencyEvidenceUID != string(cm.UID) || intent.EmergencyEvidenceVersion != cm.ResourceVersion)) {
		return nil
	}
	selector := labels.SelectorFromSet(labels.Set{workload.InstanceLabel: string(instance.UID)}).String()
	pods, err := r.Core.Pods(instance.Namespace).List(ctx, metav1.ListOptions{LabelSelector: selector, Limit: 513})
	if err != nil || pods.Continue != "" || len(pods.Items) > 512 {
		return nil
	}
	regular, ready := map[string]int64{}, map[string]int64{}
	var hasEmergency, seenBootstrap, seenIntent bool
	for _, pod := range pods.Items {
		owner := metav1.GetControllerOf(&pod)
		if owner == nil || owner.UID != instance.UID || owner.Name != instance.Name || owner.Kind != "OpenSearchServerless" || owner.APIVersion != v1alpha1.GroupVersion.String() || pod.DeletionTimestamp != nil {
			return nil
		}
		if intent != nil && pod.Name == intent.Name {
			if (uid != "" && string(pod.UID) != uid) || pod.Annotations[bootstrapReplacementAnnotation] != bootstrap.PodUID ||
				pod.Annotations[creationIntentKey] != fmt.Sprintf("%s/%d", instance.UID, intent.Sequence) || pod.Spec.NodeName != "" || !hasPlacementGate(&pod) {
				return nil
			}
			seenIntent = true
			continue
		}
		if pod.Spec.NodeName == "" && !terminalCapacityPod(&pod) {
			return nil
		}
		if pod.Labels[workload.RoleLabel] != "search" || pod.Labels[workload.FleetLabel] != bootstrap.Fleet {
			continue
		}
		if workload.PodDisruptionTarget(&pod) {
			return nil
		}
		if pod.UID == boot.UID {
			seenBootstrap = pod.ResourceVersion == boot.ResourceVersion
		}
		if pod.Annotations[emergencyCapacityAnnotation] == "true" {
			hasEmergency = hasEmergency || (podStartupReady(&pod) && pod.Annotations[warmupModeAnnotation] == "Strict")
			continue
		}
		podZone := pod.Labels[workload.ZoneLabel]
		regular[podZone]++
		if podStartupReady(&pod) {
			ready[podZone]++
		}
	}
	if !hasEmergency || !seenBootstrap || (uid != "" && !seenIntent) {
		return nil
	}
	for name, desired := range pool.Zones {
		target := int64(desired.Replicas) + int64(desired.WarmSpare)
		if ready[name] < target || (name == zone && (target < 1 || regular[name] != target)) {
			return nil
		}
	}
	fit := evidence.FitByRole["search"][zone]
	profile, err := r.resolveCapacityProfile(ctx, instance, "search", zone)
	if fit.AdditionalPods < 1 || err != nil || profile == nil || fit.ProfileUID != profile.UID || fit.ProfileVersion != profile.ResourceVersion || !r.now().Before(evidence.ExpiresAt) {
		return nil
	}
	return &v1alpha1.PodCreationIntent{Role: "search", Fleet: bootstrap.Fleet, Zone: zone, BootstrapReplacementUID: bootstrap.PodUID,
		EmergencyEvidenceUID: string(cm.UID), EmergencyEvidenceVersion: cm.ResourceVersion}
}

// emergencyFailbackCandidate checks the capacity that remains after all
// emergency Pods are removed. The result is a candidate for one retirement.
// It must still pass disruption, outage and execution checks.
func (observed servingRoleProtection) emergencyFailbackCandidate(instance *v1alpha1.OpenSearchServerless, role string, now time.Time) (emergencyFailbackPod, bool) {
	if instance.Spec.Availability == nil || instance.Spec.Availability.MaxConcurrentRetirements < 1 ||
		observed.InstanceUID != string(instance.UID) || observed.Generation != instance.Generation ||
		observed.Fleet != instance.Status.ServingFleetIntentByRole[role] || (len(observed.EmergencyPods) == 0 && observed.BootstrapPod == nil) ||
		observed.ObservedAt.IsZero() || observed.ObservedAt.After(now) || now.Sub(observed.ObservedAt) > 5*time.Second {
		return emergencyFailbackPod{}, false
	}
	if !observed.satisfiesRegularServingTargets(instance, role) {
		return emergencyFailbackPod{}, false
	}
	if observed.BootstrapPod != nil {
		return *observed.BootstrapPod, true
	}
	return observed.EmergencyPods[0], true
}

func (observed servingRoleProtection) leasesCurrent(now time.Time) bool {
	for _, pod := range observed.Pods {
		if !now.Before(pod.LeaseValidUntil) {
			return false
		}
	}
	return len(observed.Pods) != 0
}

func (r *InstanceReconciler) reconcileEmergencyRetirement(ctx context.Context, instance *v1alpha1.OpenSearchServerless) (ctrl.Result, bool, error) {
	pending := instance.Status.EmergencyRetirement
	if pending == nil && (instance.Status.FleetRetirement != nil || instance.Status.ScaleDownRetirement != nil) {
		return ctrl.Result{}, false, nil
	}
	retry := ctrl.Result{RequeueAfter: 5 * time.Second}
	writeIntent := func(intent *v1alpha1.EmergencyRetirementIntent) (ctrl.Result, bool, error) {
		before := instance.DeepCopyObject().(*v1alpha1.OpenSearchServerless)
		instance.Status.EmergencyRetirement = intent
		err := r.Status().Patch(ctx, instance, client.MergeFromWithOptions(before, client.MergeFromWithOptimisticLock{}))
		return ctrl.Result{Requeue: true}, true, err
	}
	cancel := func() (ctrl.Result, bool, error) {
		if pending == nil {
			return ctrl.Result{}, false, nil
		}
		return writeIntent(nil)
	}
	if r.Core == nil {
		return ctrl.Result{}, false, nil
	}
	if pending != nil {
		pod, err := r.Core.Pods(instance.Namespace).Get(ctx, pending.Name, metav1.GetOptions{})
		if apierrors.IsNotFound(err) || (err == nil && string(pod.UID) != pending.PodUID) {
			return writeIntent(nil)
		}
		if err != nil {
			return retry, true, err
		}
		if pod.DeletionTimestamp != nil {
			// Retain the retirement slot while the Pod exits, but allow
			// provisioning to recover capacity if conditions worsen.
			return ctrl.Result{}, false, nil
		}
	}
	availability := instance.Spec.Availability
	if !r.DevelopmentEmergencyFailback || r.Policy == nil || instance.DeletionTimestamp != nil || availability == nil ||
		availability.MaxConcurrentRetirements < 1 || instance.Status.ActiveOperation != nil ||
		instance.Status.PendingPodCreation != nil || instance.Status.PendingGateRelease != nil || instance.Status.ProvisioningAttempt != nil ||
		instance.Status.SearchWarmupSlot != nil || (pending != nil && pending.Generation != instance.Generation) {
		return cancel()
	}
	if availability.EmergencyCapacity == nil && instance.Status.EmergencyFailbackEvidence == nil {
		return cancel()
	}
	if pending == nil && instance.Status.EmergencyCapacityProposal != nil {
		return ctrl.Result{}, false, nil
	}
	if err := instance.Spec.Validate(); err != nil || instance.Status.PrimaryStore == nil {
		return cancel()
	}
	if err := r.checkPinnedPrimaryStore(ctx, instance); err != nil {
		return cancel()
	}
	selector := labels.SelectorFromSet(labels.Set{workload.InstanceLabel: string(instance.UID)}).String()
	pods, err := r.Core.Pods(instance.Namespace).List(ctx, metav1.ListOptions{LabelSelector: selector, Limit: 513})
	if err != nil || pods.Continue != "" || len(pods.Items) > 512 {
		return cancel()
	}
	roles := map[string]bool{}
	admissionReports := map[string]bool{}
	bootstrap := instance.Status.ColdBootstrap
	var bootstrapReplacementExists, bootstrapReplacementReady bool
	for _, pod := range pods.Items {
		if pod.DeletionTimestamp != nil {
			return cancel()
		}
		if pod.Annotations[emergencyCapacityAnnotation] == "true" {
			roles[pod.Labels[workload.RoleLabel]] = true
			if metav1.IsControlledBy(&pod, instance) {
				admissionReports[pod.Annotations[emergencyEvidenceUIDAnnotation]] = true
			}
		}
		if bootstrap != nil && pod.Annotations[bootstrapReplacementAnnotation] == bootstrap.PodUID && bootstrap.PodUID != "" &&
			pod.Labels[workload.RoleLabel] == "search" && pod.Labels[workload.FleetLabel] == bootstrap.Fleet {
			bootstrapReplacementExists = true
			if podStartupReady(&pod) && pod.Annotations[warmupModeAnnotation] == "Strict" {
				roles["search"] = true
				bootstrapReplacementReady = true
			}
		}
	}
	if len(roles) == 0 {
		if !bootstrapReplacementExists && pending == nil && instance.Status.EmergencyFailbackEvidence != nil {
			return r.patchFailbackEvidence(ctx, instance, nil)
		}
		return cancel()
	}
	evidence, cm, err := r.readFailbackEvidence(ctx, instance)
	if err != nil {
		return cancel()
	}
	if instance.Status.EmergencyFailbackEvidence == nil && admissionReports[string(cm.UID)] {
		// Backfill an upgraded instance only from the report identity recorded
		// on one of its existing temporary Pods.
		return r.patchFailbackEvidence(ctx, instance, &v1alpha1.EmergencyFailbackEvidence{
			LocalReference: v1alpha1.LocalReference{Name: cm.Name}, UID: string(cm.UID),
		})
	}
	if evidence.UnavailableZones == nil || len(evidence.UnavailableZones) != 0 {
		return cancel()
	}
	for _, role := range []string{"search", "ingest"} {
		if !roles[role] || (pending != nil && role != pending.Role) {
			continue
		}
		observe := r.fleetQualification().observeServingIngestProtection
		if role == "search" {
			observe = func(ctx context.Context, instance *v1alpha1.OpenSearchServerless, maxAge time.Duration) (servingRoleProtection, error) {
				if bootstrap == nil || (!bootstrapReplacementReady && (pending == nil || pending.PodUID != bootstrap.PodUID)) {
					observed, err := r.observeServingSearchProtection(ctx, instance, maxAge)
					if err == nil || bootstrap == nil {
						return observed, err
					}
				}
				return r.observeSearchProtection(ctx, instance, maxAge, bootstrap)
			}
		}
		observed, err := observe(ctx, instance, 5*time.Second)
		if err != nil {
			continue
		}
		victim, ok := observed.emergencyFailbackCandidate(instance, role, time.Now())
		if !ok {
			continue
		}
		want := v1alpha1.EmergencyRetirementIntent{Generation: instance.Generation, Role: role, Fleet: observed.Fleet, Zone: victim.Zone, Name: victim.Name, PodUID: victim.UID}
		if pending == nil {
			if r.retirementBudgetAllows(ctx, instance, want.Fleet, want.Role, want.Zone) {
				return writeIntent(&want)
			}
			continue
		}
		if *pending != want {
			return cancel()
		}
		if !r.retirementBudgetAllows(ctx, instance, want.Fleet, want.Role, want.Zone) {
			return cancel()
		}
		currentEvidence, latest, err := r.readFailbackEvidence(ctx, instance)
		if err != nil || latest.UID != cm.UID || latest.ResourceVersion != cm.ResourceVersion || currentEvidence.UnavailableZones == nil || len(currentEvidence.UnavailableZones) != 0 {
			return cancel()
		}
		service, err := workload.RoleService(instance, role, observed.Fleet, 9200)
		if err != nil {
			return cancel()
		}
		liveService, err := r.Core.Services(instance.Namespace).Get(ctx, service.Name, metav1.GetOptions{})
		if err != nil || string(liveService.UID) != observed.ServiceUID || liveService.ResourceVersion != observed.ServiceVersion {
			return cancel()
		}
		reader := r.APIReader
		if reader == nil {
			reader = r.Client
		}
		var live v1alpha1.OpenSearchServerless
		if err := reader.Get(ctx, client.ObjectKeyFromObject(instance), &live); err != nil {
			return retry, true, client.IgnoreNotFound(err)
		}
		if err := recheckEndpointSlices(ctx, r.Discovery.EndpointSlices(instance.Namespace), observed.Endpoints.ServiceName, observed.Endpoints.SliceVersions); err != nil ||
			!fleetPodsUnchanged(ctx, r.Core.Pods(instance.Namespace), observed.InstanceUID, role, observed.Fleet, observed.PodSnapshot) {
			return retry, true, nil
		}
		checkedAt := time.Now()
		if live.UID != instance.UID || live.ResourceVersion != instance.ResourceVersion || live.DeletionTimestamp != nil ||
			checkedAt.Sub(observed.ObservedAt) >= 5*time.Second || !observed.leasesCurrent(checkedAt) || !r.now().Before(currentEvidence.ExpiresAt) {
			return retry, true, nil
		}
		// Keep the intent on failed and ambiguous responses. Re-read the target
		// before any retry and never submit an eviction for a replacement UID.
		err = podkube.EvictAtVersion(ctx, r.Core, victim.Identity, victim.Version)
		if apierrors.IsTooManyRequests(err) || apierrors.IsConflict(err) || apierrors.IsNotFound(err) {
			err = nil
		}
		return retry, true, err
	}
	return cancel()
}

func (r *InstanceReconciler) retirementBudgetAllows(ctx context.Context, instance *v1alpha1.OpenSearchServerless, fleet, role, zone string) bool {
	floor := instance.Spec.Pools[role].Zones[zone].MinServing
	name := disruptionBudgetName(instance, fleet, role, zone)
	expected, err := workload.DisruptionBudget(instance, name, fleet, role, zone, floor)
	if err != nil {
		return false
	}
	actual, err := r.Policy.PodDisruptionBudgets(instance.Namespace).Get(ctx, name, metav1.GetOptions{})
	return err == nil && retirementBudgetReady(instance, actual, expected)
}

func retirementBudgetReady(instance *v1alpha1.OpenSearchServerless, actual, expected *policyv1.PodDisruptionBudget) bool {
	if !disruptionBudgetShapeMatches(instance, actual, expected) || !reflect.DeepEqual(actual.Spec.MinAvailable, expected.Spec.MinAvailable) ||
		actual.Generation <= 0 || actual.Status.ObservedGeneration != actual.Generation || actual.Status.DisruptionsAllowed <= 0 || len(actual.Status.DisruptedPods) != 0 {
		return false
	}
	condition := meta.FindStatusCondition(actual.Status.Conditions, "DisruptionAllowed")
	return condition == nil || condition.Status == metav1.ConditionTrue
}
