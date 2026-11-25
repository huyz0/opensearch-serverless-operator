package controller

import (
	"context"
	"fmt"
	"math"
	"reflect"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/huyz0/opensearch-serverless-operator/api/v1alpha1"
	"github.com/huyz0/opensearch-serverless-operator/internal/placement"
	"github.com/huyz0/opensearch-serverless-operator/internal/workload"
	"github.com/huyz0/opensearch-serverless-operator/pkg/podlifecycle"
)

type gatedPodInventory struct {
	snapshot                                        podlifecycle.Snapshot
	readyByGroup, servingReadyByGroup               map[podlifecycle.Group]int
	servingSearchStarted, emergencyPresent, hasHeld bool
	pending                                         int
}

func (r *InstanceReconciler) reconcileGatedPodCapacity(ctx context.Context, instance *v1alpha1.OpenSearchServerless, inputs gatedPodInputs) (ctrl.Result, error) {
	revisions := inputs.revisions
	selector := labels.SelectorFromSet(labels.Set{workload.InstanceLabel: string(instance.UID)}).String()
	pods, err := r.Core.Pods(instance.Namespace).List(ctx, metav1.ListOptions{LabelSelector: selector})
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("list instance Pods: %w", err)
	}
	if bootstrap := instance.Status.ColdBootstrap; bootstrap != nil {
		if instance.Spec.Pools["search"].WarmReadiness != "Strict" || bootstrap.Name == "" || bootstrap.PodUID == "" || bootstrap.Fleet != instance.Status.ServingFleetIntentByRole["search"] {
			return ctrl.Result{RequeueAfter: time.Minute}, fmt.Errorf("bootstrap slot is incomplete or requires migration")
		}
		pod, err := r.Core.Pods(instance.Namespace).Get(ctx, bootstrap.Name, metav1.GetOptions{})
		if apierrors.IsNotFound(err) {
			before := instance.DeepCopyObject().(*v1alpha1.OpenSearchServerless)
			instance.Status.ColdBootstrap = nil
			if err := r.Status().Patch(ctx, instance, client.MergeFromWithOptions(before, client.MergeFromWithOptimisticLock{})); err != nil {
				return ctrl.Result{}, err
			}
			return ctrl.Result{Requeue: true}, nil
		}
		if err != nil {
			return ctrl.Result{}, err
		}
		owner := metav1.GetControllerOf(pod)
		if owner == nil || owner.UID != instance.UID || string(pod.UID) != bootstrap.PodUID || pod.Annotations[warmupModeAnnotation] != "Bootstrap" || pod.Labels[workload.InstanceLabel] != string(instance.UID) || pod.Labels[workload.RoleLabel] != "search" || pod.Labels[workload.FleetLabel] != bootstrap.Fleet {
			return ctrl.Result{RequeueAfter: time.Minute}, fmt.Errorf("bootstrap Pod identity or mode changed")
		}
	}
	inventory, err := r.observeGatedPodInventory(ctx, instance, pods.Items, inputs.domain)
	if err != nil {
		return ctrl.Result{RequeueAfter: time.Minute}, err
	}
	if len(pods.Items) > 0 {
		before := instance.DeepCopyObject().(*v1alpha1.OpenSearchServerless)
		if adoptServingRevisionPins(instance, pods.Items, revisions) {
			if err := r.Status().Patch(ctx, instance, client.MergeFromWithOptions(before, client.MergeFromWithOptimisticLock{})); err != nil {
				return ctrl.Result{}, err
			}
			return ctrl.Result{Requeue: true}, nil
		}
	}
	servingComplete, err := applyGatedCapacityPolicies(instance, inputs, &inventory)
	if err != nil {
		return ctrl.Result{}, err
	}
	if inventory.pending > 0 {
		return r.reconcileHeldGatedPod(ctx, instance, inputs, pods.Items, inventory, servingComplete)
	}
	return r.planGatedPodCreation(ctx, instance, inputs, pods.Items, inventory)
}

func (r *InstanceReconciler) observeGatedPodInventory(ctx context.Context, instance *v1alpha1.OpenSearchServerless, pods []corev1.Pod, domain string) (gatedPodInventory, error) {
	snapshot := podlifecycle.Snapshot{}
	readyByGroup := map[podlifecycle.Group]int{}
	servingReadyByGroup := map[podlifecycle.Group]int{}
	servingSearchStarted := false
	emergencyPresent := false
	pending := 0
	hasHeld := false
	seenBootstrap := false
	for _, pod := range pods {
		owner := metav1.GetControllerOf(&pod)
		if owner == nil || owner.UID != instance.UID || pod.UID == "" {
			return gatedPodInventory{}, fmt.Errorf("Pod with instance label has a conflicting owner or identity")
		}
		if pod.Annotations[peerDomainAnnotation] != domain {
			return gatedPodInventory{}, fmt.Errorf("existing Pod peer DNS configuration differs; explicit migration is required")
		}
		if domain != "" && pod.Annotations[peerDNSPreflightAnnotation] != "v1" {
			return gatedPodInventory{}, fmt.Errorf("existing Pod lacks the required peer DNS preflight; explicit migration is required")
		}
		if !peerTLSPodMatches(&pod, peerTLSBaselineForFleet(instance, pod.Labels[workload.FleetLabel])) {
			return gatedPodInventory{}, fmt.Errorf("existing Pod peer TLS differs; explicit migration is required")
		}
		group := podlifecycle.Group{Workload: string(instance.UID), Fleet: pod.Labels[workload.FleetLabel], Role: pod.Labels[workload.RoleLabel], Zone: pod.Labels[workload.ZoneLabel]}
		if group.Fleet == "" || group.Role == "" || group.Zone == "" {
			return gatedPodInventory{}, fmt.Errorf("owned Pod has incomplete placement labels")
		}
		if err := r.checkObservedPodPlugins(ctx, instance, &pod, group.Fleet, group.Role); err != nil {
			return gatedPodInventory{}, err
		}
		if group.Role == "search" && instance.Spec.Pools["search"].WarmReadiness == "Strict" {
			mode := pod.Annotations[warmupModeAnnotation]
			if mode != "Strict" && mode != "Bootstrap" {
				return gatedPodInventory{}, fmt.Errorf("search Pod lacks a compatible persisted warm-up mode; migration is required")
			}
			if mode == "Bootstrap" && group.Fleet == instance.Status.ServingFleetIntentByRole["search"] {
				slot := instance.Status.ColdBootstrap
				if seenBootstrap || slot == nil || slot.Name != pod.Name || slot.PodUID != string(pod.UID) {
					return gatedPodInventory{}, fmt.Errorf("bootstrap Pod does not match durable slot")
				}
				seenBootstrap = true
			}
		}
		terminal := pod.Status.Phase == corev1.PodFailed || pod.Status.Phase == corev1.PodSucceeded
		emergency := pod.Annotations[emergencyCapacityAnnotation] == "true"
		if podReadyForServing(&pod) {
			servingReadyByGroup[group]++
			if !emergency {
				readyByGroup[group]++
			}
		}
		// A disruption signal alone does not prove the process has stopped.
		// Keep a separate check for whether a new cold bootstrap is needed.
		if group.Role == "search" && group.Fleet == instance.Status.ServingFleetIntentByRole["search"] &&
			pod.DeletionTimestamp == nil && pod.Spec.NodeName != "" && podStartupReady(&pod) {
			servingSearchStarted = true
		}
		if pod.Spec.NodeName == "" && pod.DeletionTimestamp == nil && !terminal {
			pending++
			hasHeld = hasHeld || hasPlacementGate(&pod)
		}
		// A temporary Pod can serve traffic and consume a pending slot, but
		// cannot replace a regular Pod in the original placement targets.
		if emergency {
			emergencyPresent = true
			continue
		}
		snapshot.Pods = append(snapshot.Pods, podlifecycle.Pod{Identity: podlifecycle.Identity{Namespace: pod.Namespace, Name: pod.Name, UID: string(pod.UID), OwnerUID: string(instance.UID)}, Group: group, Bound: pod.Spec.NodeName != "", Terminating: pod.DeletionTimestamp != nil, Terminal: terminal})
	}
	return gatedPodInventory{snapshot: snapshot, readyByGroup: readyByGroup,
		servingReadyByGroup: servingReadyByGroup, servingSearchStarted: servingSearchStarted, emergencyPresent: emergencyPresent, pending: pending, hasHeld: hasHeld}, nil
}

func applyGatedCapacityPolicies(instance *v1alpha1.OpenSearchServerless, inputs gatedPodInputs, inventory *gatedPodInventory) (bool, error) {
	snapshot := &inventory.snapshot
	revisions, blockedGroups := inputs.revisions, inputs.blockedGroups
	emergencyPresent := inventory.emergencyPresent
	for role, pool := range instance.Spec.Pools {
		if role == "search" && pool.WarmOnJoin && instance.Status.SearchWarmupSlot != nil {
			continue
		}
		for zone, desired := range pool.Zones {
			if blockedGroups[provisioningGroup{role, zone}] {
				continue
			}
			snapshot.Policies = append(snapshot.Policies, podlifecycle.GroupPolicy{Group: podlifecycle.Group{Workload: string(instance.UID), Fleet: instance.Status.ServingFleetIntentByRole[role], Role: role, Zone: zone}, Desired: int(int64(desired.Replicas) + int64(desired.WarmSpare)), MinServing: int(desired.MinServing)})
		}
	}
	// Preview demand is admitted only after all serving groups reach desired
	// capacity. Count terminating and terminal preview Pods against the
	// global limit until they disappear.
	servingComplete := !emergencyPresent
	for role, pool := range instance.Spec.Pools {
		for zone, desired := range pool.Zones {
			policy := podlifecycle.GroupPolicy{Group: podlifecycle.Group{Workload: string(instance.UID), Fleet: instance.Status.ServingFleetIntentByRole[role], Role: role, Zone: zone}, Desired: int(int64(desired.Replicas) + int64(desired.WarmSpare)), MinServing: int(desired.MinServing)}
			assessment, err := podlifecycle.Assess(*snapshot, policy)
			if err != nil {
				return false, err
			}
			if assessment.Deficit > 0 || inventory.readyByGroup[policy.Group] < policy.Desired || blockedGroups[provisioningGroup{role, zone}] {
				servingComplete = false
			}
		}
	}
	// Leave one history entry for a serving creation that also needs recovery.
	if servingComplete && len(instance.Status.RetiredPodCreations) < maxRetiredPodCreations-1 && instance.Spec.Rollout != nil && instance.Status.CandidateFleet != nil && revisions.forFleetTemplate(instance.Status.CandidateFleet.Fleet).accepts(instance.Status.CandidateFleet.Revision) {
		candidateCount := 0
		for _, pod := range snapshot.Pods {
			if pod.Group.Fleet == instance.Status.CandidateFleet.Fleet {
				candidateCount++
			}
		}
		if candidateCount < int(instance.Spec.Rollout.MaxPreviewReplicas) {
			for _, policy := range developmentCandidatePolicies(instance) {
				if blockedGroups[provisioningGroup{policy.Group.Role, policy.Group.Zone}] || (policy.Group.Role == "search" && instance.Spec.Pools["search"].WarmOnJoin && instance.Status.SearchWarmupSlot != nil) {
					continue
				}
				snapshot.Policies = append(snapshot.Policies, policy)
			}
		}
	}
	return servingComplete, nil
}

func (r *InstanceReconciler) reconcileHeldGatedPod(ctx context.Context, instance *v1alpha1.OpenSearchServerless, inputs gatedPodInputs, pods []corev1.Pod, inventory gatedPodInventory, servingComplete bool) (ctrl.Result, error) {
	domain, keystoreUID, peerTLS := inputs.domain, inputs.keystoreUID, inputs.peerTLS
	revisions, blockedGroups := inputs.revisions, inputs.blockedGroups
	// A single held Pod bounds quota use until gate release and pending budgets
	// are implemented. Existing unbound Pods always consume this slot.
	if inventory.pending == 1 {
		for i := range pods {
			pod := &pods[i]
			if pod.Spec.NodeName != "" || pod.DeletionTimestamp != nil || pod.Status.Phase == corev1.PodFailed || pod.Status.Phase == corev1.PodSucceeded || !hasPlacementGate(pod) {
				continue
			}
			fleet := pod.Labels[workload.FleetLabel]
			candidate := instance.Status.CandidateFleet
			if !servingComplete && candidate != nil && fleet == candidate.Fleet && fleet != instance.Status.ServingFleetIntentByRole[pod.Labels[workload.RoleLabel]] {
				return r.stageGatedWithdrawal(ctx, instance, pod)
			}
			if !r.DevelopmentReleaseGates {
				break
			}
			if blockedGroups[provisioningGroup{pod.Labels[workload.RoleLabel], pod.Labels[workload.ZoneLabel]}] {
				return r.stageGatedWithdrawal(ctx, instance, pod)
			}
			profile, err := r.resolveCapacityProfile(ctx, instance, pod.Labels[workload.RoleLabel], pod.Labels[workload.ZoneLabel])
			if err != nil {
				return ctrl.Result{RequeueAfter: time.Minute}, err
			}
			creation, err := recoverCreationIntent(instance, pod, revisions.forFleetTemplate(fleet).compatibleOrCurrent(pod.Labels[podRevisionLabel]), keystoreUID, profile)
			if err != nil {
				return ctrl.Result{RequeueAfter: time.Minute}, err
			}
			if err := r.checkPeerTLSIntent(ctx, instance, creation, peerTLSForFleet(instance, fleet, peerTLS), domain); err != nil {
				return ctrl.Result{RequeueAfter: time.Minute}, err
			}
			desired, err := renderGatedPod(instance, creation, profile, domain, r.DevelopmentRuntimeSettingsImage)
			if err != nil {
				return ctrl.Result{RequeueAfter: time.Minute}, err
			}
			if err := validateAdmittedGatedPod(desired, pod); err != nil {
				return ctrl.Result{RequeueAfter: time.Minute}, err
			}
			if profile != nil {
				if err := placement.CheckFit(pod, profile); err != nil {
					return ctrl.Result{RequeueAfter: time.Minute}, err
				}
			}
			needsWarmSlot := creation.Role == "search" && instance.Spec.Pools["search"].WarmOnJoin
			if needsWarmSlot && (instance.Status.SearchWarmupSlot != nil || (instance.Spec.Availability != nil && instance.Spec.Availability.MaxConcurrentSearchWarmups == 0)) {
				return ctrl.Result{RequeueAfter: time.Minute}, nil
			}
			now := r.now()
			window, limit := r.releaseBudget()
			records, wait, err := reservePodRelease(instance.Status.RecentPodReleases, creation.Role, creation.Zone, string(pod.UID), now, window, limit)
			if err != nil {
				return ctrl.Result{RequeueAfter: time.Minute}, err
			}
			if wait > 0 {
				before := instance.DeepCopyObject().(*v1alpha1.OpenSearchServerless)
				meta.SetStatusCondition(&instance.Status.Conditions, metav1.Condition{Type: "ProvisioningRateLimited", Status: metav1.ConditionTrue, Reason: "ReleaseBudgetExhausted", Message: fmt.Sprintf("Pod gate releases for %s in %s are limited by the release budget or history capacity", creation.Role, creation.Zone), ObservedGeneration: instance.Generation})
				if !reflect.DeepEqual(before.Status, instance.Status) {
					if err := r.Status().Patch(ctx, instance, client.MergeFromWithOptions(before, client.MergeFromWithOptimisticLock{})); err != nil {
						return ctrl.Result{}, err
					}
				}
				return ctrl.Result{RequeueAfter: wait}, nil
			}
			before := instance.DeepCopyObject().(*v1alpha1.OpenSearchServerless)
			meta.RemoveStatusCondition(&instance.Status.Conditions, "ProvisioningRateLimited")
			instance.Status.RecentPodReleases = records
			instance.Status.PendingGateRelease = &v1alpha1.GateReleaseIntent{PodUID: string(pod.UID), Pod: *creation, StartedAt: metav1.NewTime(now)}
			if needsWarmSlot {
				instance.Status.SearchWarmupSlot = &v1alpha1.SearchWarmupSlot{Name: pod.Name, PodUID: string(pod.UID)}
			}
			if err := r.Status().Patch(ctx, instance, client.MergeFromWithOptions(before, client.MergeFromWithOptimisticLock{})); err != nil {
				return ctrl.Result{}, err
			}
			return ctrl.Result{Requeue: true}, nil
		}
	}
	if !inventory.hasHeld && meta.FindStatusCondition(instance.Status.Conditions, "ProvisioningRateLimited") != nil {
		before := instance.DeepCopyObject().(*v1alpha1.OpenSearchServerless)
		meta.RemoveStatusCondition(&instance.Status.Conditions, "ProvisioningRateLimited")
		if err := r.Status().Patch(ctx, instance, client.MergeFromWithOptions(before, client.MergeFromWithOptimisticLock{})); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{Requeue: true}, nil
	}
	return ctrl.Result{RequeueAfter: time.Minute}, nil
}

func chooseGatedCapacity(instance *v1alpha1.OpenSearchServerless, inventory gatedPodInventory, release bool, now time.Time, window time.Duration, limit int) (podlifecycle.Action, time.Duration, error) {
	regionalShortfalls := regionalReadyShortfalls(instance, inventory.servingReadyByGroup)
	if release {
		return selectReleaseEligibleCapacity(inventory.snapshot, inventory.readyByGroup, regionalShortfalls, instance.Status.RecentPodReleases, now, window, limit)
	}
	action, err := selectFloorPriorityCapacity(inventory.snapshot, inventory.readyByGroup, regionalShortfalls)
	return action, 0, err
}

func (r *InstanceReconciler) planGatedPodCreation(ctx context.Context, instance *v1alpha1.OpenSearchServerless, inputs gatedPodInputs, pods []corev1.Pod, inventory gatedPodInventory) (ctrl.Result, error) {
	var now time.Time
	var window time.Duration
	var limit int
	if r.DevelopmentReleaseGates {
		window, limit = r.releaseBudget()
		now = r.now()
	}
	action, wait, err := chooseGatedCapacity(instance, inventory, r.DevelopmentReleaseGates, now, window, limit)
	if err != nil {
		return ctrl.Result{}, err
	}
	var emergency *v1alpha1.EmergencyCapacityProposal
	var bootstrapReplacement *v1alpha1.PodCreationIntent
	if action.Kind != podlifecycle.CreatePod && r.DevelopmentReleaseGates {
		if proposal, err := r.planEmergencyCapacity(ctx, instance); err == nil && proposal != nil {
			window, limit := r.releaseBudget()
			_, emergencyWait, err := inspectReleaseBudget(instance.Status.RecentPodReleases, proposal.Role, proposal.Zone, "", r.now(), window, limit)
			if err != nil {
				return ctrl.Result{}, err
			}
			if emergencyWait == 0 {
				emergency = proposal
				action = podlifecycle.Action{Kind: podlifecycle.CreatePod, Group: podlifecycle.Group{
					Workload: string(instance.UID), Fleet: proposal.Fleet, Role: proposal.Role, Zone: proposal.Zone,
				}}
			} else if wait == 0 || emergencyWait < wait {
				wait = emergencyWait
			}
		}
	}
	if action.Kind != podlifecycle.CreatePod && r.DevelopmentReleaseGates {
		if replacement := r.planBootstrapReplacement(ctx, instance, nil, ""); replacement != nil {
			window, limit := r.releaseBudget()
			_, replacementWait, err := inspectReleaseBudget(instance.Status.RecentPodReleases, replacement.Role, replacement.Zone, "", r.now(), window, limit)
			if err != nil {
				return ctrl.Result{}, err
			}
			if replacementWait == 0 {
				bootstrapReplacement = replacement
				action = podlifecycle.Action{Kind: podlifecycle.CreatePod, Group: podlifecycle.Group{
					Workload: string(instance.UID), Fleet: replacement.Fleet, Role: replacement.Role, Zone: replacement.Zone,
				}}
			} else if wait == 0 || replacementWait < wait {
				wait = replacementWait
			}
		}
	}
	if action.Kind != podlifecycle.CreatePod {
		before := instance.DeepCopyObject().(*v1alpha1.OpenSearchServerless)
		if wait > 0 {
			meta.SetStatusCondition(&instance.Status.Conditions, metav1.Condition{Type: "ProvisioningRateLimited", Status: metav1.ConditionTrue, Reason: "ReleaseBudgetExhausted", Message: "All deficient role and AZ groups are limited by release budget or history capacity", ObservedGeneration: instance.Generation})
		} else {
			meta.RemoveStatusCondition(&instance.Status.Conditions, "ProvisioningRateLimited")
		}
		if !reflect.DeepEqual(before.Status, instance.Status) {
			if err := r.Status().Patch(ctx, instance, client.MergeFromWithOptions(before, client.MergeFromWithOptimisticLock{})); err != nil {
				return ctrl.Result{}, err
			}
		}
		if wait > 0 {
			return ctrl.Result{RequeueAfter: wait}, nil
		}
		return ctrl.Result{RequeueAfter: time.Minute}, nil
	}
	return r.persistGatedPodCreation(ctx, instance, inputs, pods, inventory.servingSearchStarted, action.Group, emergency, bootstrapReplacement)
}

func (r *InstanceReconciler) persistGatedPodCreation(ctx context.Context, instance *v1alpha1.OpenSearchServerless, inputs gatedPodInputs, pods []corev1.Pod, servingSearchStarted bool, group podlifecycle.Group, emergency *v1alpha1.EmergencyCapacityProposal, bootstrapReplacement *v1alpha1.PodCreationIntent) (ctrl.Result, error) {
	domain, keystoreUID, peerTLS := inputs.domain, inputs.keystoreUID, inputs.peerTLS
	revisions := inputs.revisions
	if instance.Status.PodCreationSequence == math.MaxInt64 {
		return ctrl.Result{}, fmt.Errorf("Pod creation sequence exhausted")
	}
	sequence := instance.Status.PodCreationSequence + 1
	profile, err := r.resolveCapacityProfile(ctx, instance, group.Role, group.Zone)
	if err != nil {
		return ctrl.Result{RequeueAfter: time.Minute}, err
	}
	revision, err := revisions.forFleet(instance, pods, group.Role, group.Fleet)
	if err != nil {
		return ctrl.Result{RequeueAfter: time.Minute}, err
	}
	intent := &v1alpha1.PodCreationIntent{Name: podNameForSequence(instance, sequence), Role: group.Role, Zone: group.Zone, Fleet: group.Fleet, Revision: revision, Generation: instance.Generation, Sequence: sequence, KeystoreUID: keystoreUID, PeerDNS: domain != ""}
	peerTLS = peerTLSForFleet(instance, intent.Fleet, peerTLS)
	httpTLS, err := r.resolveHTTPTLSForFleet(ctx, instance, domain, intent.Fleet)
	if err != nil {
		return ctrl.Result{RequeueAfter: time.Minute}, err
	}
	if err := httpTLS.validatePod(instance, intent.Name, r.now()); err != nil {
		return ctrl.Result{RequeueAfter: time.Minute}, err
	}
	if emergency != nil {
		intent.EmergencyEvidenceUID, intent.EmergencyEvidenceVersion = emergency.EvidenceUID, emergency.EvidenceVersion
	}
	if bootstrapReplacement != nil {
		intent.BootstrapReplacementUID = bootstrapReplacement.BootstrapReplacementUID
		intent.EmergencyEvidenceUID, intent.EmergencyEvidenceVersion = bootstrapReplacement.EmergencyEvidenceUID, bootstrapReplacement.EmergencyEvidenceVersion
	}
	pluginInputs, err := r.pluginsForFleetRole(ctx, instance, group.Fleet, group.Role)
	if err != nil {
		return ctrl.Result{RequeueAfter: time.Minute}, err
	}
	intent.PluginInstall = pluginInstallIntent(instance, pluginInputs)
	if group.Role == "search" && instance.Spec.Pools["search"].WarmReadiness == "Strict" {
		intent.WarmupMode = "Strict"
		if instance.Status.ColdBootstrap == nil && !servingSearchStarted && group.Fleet == instance.Status.ServingFleetIntentByRole["search"] {
			intent.WarmupMode = "Bootstrap"
		}
	}
	if peerTLS != nil {
		intent.PeerTLSLeafUID = peerTLS.baseline.LeafUID
		intent.PeerTLSTrustUID = peerTLS.baseline.TrustUID
		intent.PeerTLSLeafSHA256 = peerTLS.baseline.LeafSHA256
		intent.PeerTLSTrustSHA256 = peerTLS.baseline.TrustSHA256
		intent.PeerTLSSnapshotName = peerTLS.baseline.SnapshotName
		intent.PeerTLSSnapshotUID = peerTLS.baseline.SnapshotUID
		if err := r.checkPeerTLSIntent(ctx, instance, intent, peerTLS, domain); err != nil {
			return ctrl.Result{RequeueAfter: time.Minute}, err
		}
	}
	if profile != nil {
		intent.CapacityProfileUID = profile.UID
		intent.CapacityProfileVersion = profile.ResourceVersion
	}
	// Validate the exact template before persisting its intent.
	previewIntent := *intent
	if intent.PluginInstall != nil {
		plugin := *intent.PluginInstall
		plugin.RequestUID, plugin.PolicyUID = "not-yet-staged", "not-yet-staged"
		previewIntent.PluginInstall = &plugin
	}
	if peerTLS != nil && previewIntent.PeerTLSSnapshotUID == "" {
		previewIntent.PeerTLSSnapshotUID = "not-yet-staged"
	}
	if _, err := renderGatedPod(instance, &previewIntent, profile, domain, r.DevelopmentRuntimeSettingsImage); err != nil {
		return ctrl.Result{RequeueAfter: time.Minute}, err
	}
	before := instance.DeepCopyObject().(*v1alpha1.OpenSearchServerless)
	if instance.Status.PodCreationSequence == 0 && len(instance.Status.RuntimeByFleet) == 0 &&
		instance.Spec.Image == r.DevelopmentRuntimeSettingsImage && qualifiedPluginImage.MatchString(instance.Spec.Image) {
		config := configuredFleetRuntime(instance)
		for role := range instance.Spec.Pools {
			if _, err := runtimeArgs(config, role, startupCatalog(instance.Spec.Image, r.DevelopmentRuntimeSettingsImage, role)); err != nil {
				return ctrl.Result{RequeueAfter: time.Minute}, fmt.Errorf("initial %s runtime settings: %w", role, err)
			}
		}
		instance.Status.RuntimeByFleet = map[string]v1alpha1.FleetRuntime{intent.Fleet: config}
	}
	if peerTLS != nil && instance.Status.PeerTLSBaseline == nil {
		baseline := peerTLS.baseline
		instance.Status.PeerTLSBaseline = &baseline
	}
	meta.RemoveStatusCondition(&instance.Status.Conditions, "ProvisioningRateLimited")
	instance.Status.PodCreationSequence = sequence
	instance.Status.PendingPodCreation = intent
	if emergency != nil {
		instance.Status.EmergencyFailbackEvidence = &v1alpha1.EmergencyFailbackEvidence{
			LocalReference: instance.Spec.Availability.EmergencyCapacity.EvidenceRef,
			UID:            emergency.EvidenceUID,
		}
	}
	if intent.WarmupMode == "Bootstrap" {
		instance.Status.ColdBootstrap = &v1alpha1.ColdBootstrapStatus{Name: intent.Name, Fleet: intent.Fleet, Sequence: intent.Sequence}
	}
	if err := r.Status().Patch(ctx, instance, client.MergeFromWithOptions(before, client.MergeFromWithOptimisticLock{})); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{Requeue: true}, nil
}
