package controller

import (
	"context"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"sigs.k8s.io/controller-runtime/pkg/client"
	kubejson "sigs.k8s.io/json"

	"github.com/huyz0/opensearch-serverless-operator/api/v1alpha1"
	"github.com/huyz0/opensearch-serverless-operator/internal/placement"
	"github.com/huyz0/opensearch-serverless-operator/internal/workload"
)

type emergencyCapacityEvidence struct {
	InstanceUID      string                                 `json:"instanceUID"`
	Generation       int64                                  `json:"generation"`
	ObservedAt       time.Time                              `json:"observedAt"`
	ExpiresAt        time.Time                              `json:"expiresAt"`
	UnavailableZones []string                               `json:"unavailableZones"`
	FitByRole        map[string]map[string]emergencyZoneFit `json:"fitByRole"`
}

type emergencyZoneFit struct {
	AdditionalPods int    `json:"additionalPods"`
	ProfileUID     string `json:"profileUID"`
	ProfileVersion string `json:"profileVersion"`
}

const emergencyCapacityAnnotation = "serverless.opensearch.org/emergency-capacity"
const emergencyEvidenceUIDAnnotation = "serverless.opensearch.org/emergency-evidence-uid"
const emergencyEvidenceVersionAnnotation = "serverless.opensearch.org/emergency-evidence-version"

// sampleEmergencyCapacity records a diagnostic proposal. Invalid or expired
// platform evidence clears the proposal without blocking ordinary recovery.
func (r *InstanceReconciler) sampleEmergencyCapacity(ctx context.Context, instance *v1alpha1.OpenSearchServerless) (bool, error) {
	if (instance.Spec.Availability == nil || instance.Spec.Availability.EmergencyCapacity == nil) &&
		instance.Status.EmergencyCapacityProposal == nil && meta.FindStatusCondition(instance.Status.Conditions, "EmergencyCapacityEvaluated") == nil {
		return false, nil
	}
	before := instance.DeepCopyObject().(*v1alpha1.OpenSearchServerless)
	proposal, err := r.planEmergencyCapacity(ctx, instance)
	instance.Status.EmergencyCapacityProposal = proposal
	if instance.Spec.Availability == nil || instance.Spec.Availability.EmergencyCapacity == nil {
		meta.RemoveStatusCondition(&instance.Status.Conditions, "EmergencyCapacityEvaluated")
	} else {
		condition := metav1.Condition{Type: "EmergencyCapacityEvaluated", Status: metav1.ConditionTrue, Reason: "NoAdditionalCapacityProposed",
			Message: "No temporary Pod meets the current outage, fit and capacity limits", ObservedGeneration: instance.Generation}
		if err != nil {
			condition.Status, condition.Reason, condition.Message = metav1.ConditionUnknown, "EvidenceUnavailable", err.Error()
		}
		if proposal != nil {
			condition.Reason, condition.Message = "TemporaryCapacityProposed", "A role and surviving zone meet the temporary capacity criteria"
		}
		meta.SetStatusCondition(&instance.Status.Conditions, condition)
	}
	if apiequality.Semantic.DeepEqual(before.Status, instance.Status) {
		return false, nil
	}
	if err := r.Status().Patch(ctx, instance, client.MergeFromWithOptions(before, client.MergeFromWithOptimisticLock{})); err != nil {
		return false, err
	}
	return true, nil
}

func (r *InstanceReconciler) planEmergencyCapacity(ctx context.Context, instance *v1alpha1.OpenSearchServerless) (*v1alpha1.EmergencyCapacityProposal, error) {
	return r.planEmergencyForIntent(ctx, instance, nil, "")
}

func (r *InstanceReconciler) planEmergencyForIntent(ctx context.Context, instance *v1alpha1.OpenSearchServerless, intent *v1alpha1.PodCreationIntent, uid string) (*v1alpha1.EmergencyCapacityProposal, error) {
	availability := instance.Spec.Availability
	if availability == nil || availability.EmergencyCapacity == nil {
		return nil, nil
	}
	policy := availability.EmergencyCapacity
	evidence, cm, err := r.readEmergencyCapacityEvidence(ctx, instance, policy.EvidenceRef.Name)
	if err != nil {
		return nil, err
	}
	unavailable := make(map[string]bool, len(evidence.UnavailableZones))
	for _, zone := range evidence.UnavailableZones {
		unavailable[zone] = true
	}
	if len(unavailable) == 0 {
		return nil, nil
	}
	return r.planEmergencyWithEvidence(ctx, instance, intent, uid, evidence, cm, unavailable)
}

func (r *InstanceReconciler) readEmergencyCapacityEvidence(ctx context.Context, instance *v1alpha1.OpenSearchServerless, name string) (emergencyCapacityEvidence, *corev1.ConfigMap, error) {
	var evidence emergencyCapacityEvidence
	if r.Core == nil {
		return evidence, nil, fmt.Errorf("Kubernetes client is unavailable for emergency capacity")
	}
	cm, err := r.Core.ConfigMaps(instance.Namespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return evidence, nil, fmt.Errorf("read emergency capacity evidence: %w", err)
	}
	raw := cm.Data["evidence.json"]
	if cm.UID == "" || cm.ResourceVersion == "" || cm.DeletionTimestamp != nil || len(raw) == 0 || len(raw) > 64<<10 {
		return evidence, nil, fmt.Errorf("invalid emergency evidence ConfigMap")
	}
	// Reject duplicate fields so a later clearance cannot override an outage
	// in the same report, and so conflicting capacity claims stay invalid.
	strictErrors, parseErr := kubejson.UnmarshalStrict([]byte(raw), &evidence)
	now := r.now()
	if parseErr != nil || len(strictErrors) != 0 || evidence.InstanceUID != string(instance.UID) ||
		evidence.Generation != instance.Generation || evidence.ObservedAt.IsZero() || evidence.ObservedAt.After(now) ||
		!now.Before(evidence.ExpiresAt) || evidence.ExpiresAt.Sub(evidence.ObservedAt) > 5*time.Minute {
		return evidence, nil, fmt.Errorf("emergency evidence is malformed, stale or for a different instance")
	}
	unavailable := make(map[string]bool, len(evidence.UnavailableZones))
	for _, zone := range evidence.UnavailableZones {
		if zone == "" || unavailable[zone] {
			return evidence, nil, fmt.Errorf("emergency evidence has invalid or duplicate unavailable zones")
		}
		unavailable[zone] = true
	}
	return evidence, cm, nil
}

func (r *InstanceReconciler) planEmergencyWithEvidence(ctx context.Context, instance *v1alpha1.OpenSearchServerless, intent *v1alpha1.PodCreationIntent, uid string, evidence emergencyCapacityEvidence, cm *corev1.ConfigMap, unavailable map[string]bool) (*v1alpha1.EmergencyCapacityProposal, error) {
	availability := instance.Spec.Availability
	policy := availability.EmergencyCapacity
	if intent != nil && (intent.EmergencyEvidenceUID != string(cm.UID) || intent.EmergencyEvidenceVersion != cm.ResourceVersion) {
		return nil, nil
	}
	selector := labels.SelectorFromSet(labels.Set{workload.InstanceLabel: string(instance.UID)}).String()
	pods, err := r.Core.Pods(instance.Namespace).List(ctx, metav1.ListOptions{LabelSelector: selector, Limit: 513})
	if err != nil || pods.Continue != "" || len(pods.Items) > 512 {
		return nil, fmt.Errorf("cannot verify complete Pod counts for emergency capacity")
	}
	blocked, err := blockedProvisioningGroups(instance)
	if err != nil {
		return nil, err
	}
	freeSlots := int(availability.MaxPendingProvisionablePods)
	temporaryByZone := make(map[string]int)
	excluded := ""
	for i := range pods.Items {
		pod := &pods.Items[i]
		owner := metav1.GetControllerOf(pod)
		if owner == nil || owner.UID != instance.UID || owner.Name != instance.Name || owner.Kind != "OpenSearchServerless" ||
			owner.APIVersion != v1alpha1.GroupVersion.String() || pod.UID == "" {
			return nil, fmt.Errorf("Pod ownership conflicts with emergency capacity observation")
		}
		if intent != nil && pod.Name == intent.Name {
			if (uid != "" && uid != string(pod.UID)) || pod.Annotations[creationIntentKey] != fmt.Sprintf("%s/%d", instance.UID, intent.Sequence) ||
				pod.Annotations[emergencyCapacityAnnotation] != "true" || pod.Annotations[emergencyEvidenceUIDAnnotation] != intent.EmergencyEvidenceUID ||
				pod.Annotations[emergencyEvidenceVersionAnnotation] != intent.EmergencyEvidenceVersion || pod.DeletionTimestamp != nil ||
				pod.Spec.NodeName != "" || !hasPlacementGate(pod) {
				return nil, fmt.Errorf("emergency Pod identity changed or is no longer unbound and gated")
			}
			excluded = pod.Name
			continue
		}
		if pod.Annotations[emergencyCapacityAnnotation] == "true" {
			temporaryByZone[pod.Labels[workload.ZoneLabel]]++
		}
		if pod.Spec.NodeName == "" && pod.DeletionTimestamp == nil && !terminalCapacityPod(pod) {
			freeSlots--
		}
	}
	if freeSlots <= 0 || instance.Status.ActiveOperation != nil || instance.Status.ProvisioningAttempt != nil ||
		(instance.Status.PendingPodCreation != nil && (intent == nil || instance.Status.PendingPodCreation.Name != intent.Name)) ||
		(instance.Status.PendingGateRelease != nil && (intent == nil || instance.Status.PendingGateRelease.Pod.Name != intent.Name)) {
		return nil, nil
	}
	for _, role := range []string{"search", "ingest"} {
		if policy.MaxTemporaryByRole[role] == 0 || (intent != nil && intent.Role != role) ||
			(role == "search" && instance.Status.SearchWarmupSlot != nil &&
				(intent == nil || instance.Status.SearchWarmupSlot.Name != intent.Name || instance.Status.SearchWarmupSlot.PodUID != uid)) {
			continue
		}
		fleet := instance.Status.ServingFleetIntentByRole[role]
		configuredZones := instance.Spec.Pools[role].Zones
		maximum := int(policy.MaxTemporaryByRole[role])
		for _, pod := range pods.Items {
			config := configuredZones[pod.Labels[workload.ZoneLabel]]
			if pod.Name != excluded && pod.Labels[workload.RoleLabel] == role && pod.Annotations[emergencyCapacityAnnotation] == "true" &&
				int64(config.Replicas)+int64(config.WarmSpare) == 0 {
				maximum--
			}
		}
		if maximum <= 0 {
			continue
		}
		zones := make([]placement.EmergencyZone, 0, len(configuredZones))
		deadlines := make(map[string]time.Time)
		for zone, desired := range configuredZones {
			if fleet == "" || int64(desired.Replicas)+int64(desired.WarmSpare) == 0 {
				continue
			}
			observed := placement.EmergencyZone{Zone: zone, Unavailable: unavailable[zone], MaxTemporary: int(policy.MaxTemporaryPerZone)}
			for i := range pods.Items {
				pod := &pods.Items[i]
				if pod.Name == excluded || pod.Labels[workload.RoleLabel] != role || pod.Labels[workload.ZoneLabel] != zone {
					continue
				}
				if pod.Annotations[emergencyCapacityAnnotation] == "true" {
					observed.Committed++
				}
				if pod.Labels[workload.FleetLabel] != fleet || pod.DeletionTimestamp != nil || terminalCapacityPod(pod) || workload.PodDisruptionTarget(pod) {
					continue
				}
				if podStartupReady(pod) && pod.Spec.NodeName != "" {
					observed.Ready++
				} else if !hasPlacementGate(pod) {
					observed.Pending++
				}
			}
			// The planner counts this role's commitments separately. Apply
			// other roles' commitments to the shared zone limit.
			otherCommitments := temporaryByZone[zone] - observed.Committed
			observed.MaxTemporary = max(0, observed.MaxTemporary-otherCommitments)
			fit := evidence.FitByRole[role][zone]
			if !observed.Unavailable && (intent == nil || intent.Zone == zone) && !blocked[provisioningGroup{role, zone}] && fit.AdditionalPods > 0 && observed.Ready > 0 {
				profile, err := r.resolveCapacityProfile(ctx, instance, role, zone)
				if err == nil && profile != nil && profile.UID == fit.ProfileUID && profile.ResourceVersion == fit.ProfileVersion {
					observed.Fit = fit.AdditionalPods
					deadlines[zone] = minTime(evidence.ExpiresAt, profile.Profile.ExpiresAt.Time)
				}
			}
			zones = append(zones, observed)
		}
		zone, err := placement.PlanEmergencyCapacity(zones, int(availability.MinServingTotal[role]), maximum, freeSlots)
		if err != nil {
			return nil, err
		}
		if zone != "" {
			deadline := deadlines[zone].UTC().Truncate(time.Second)
			if !r.now().Before(deadline) {
				return nil, fmt.Errorf("emergency evidence expired during fit verification")
			}
			return &v1alpha1.EmergencyCapacityProposal{
				Role: role, Fleet: fleet, Zone: zone, EvidenceUID: string(cm.UID), EvidenceVersion: cm.ResourceVersion,
				ExpiresAt: metav1.NewTime(deadline),
			}, nil
		}
	}
	return nil, nil
}

func (r *InstanceReconciler) emergencyIntentStillFits(ctx context.Context, instance *v1alpha1.OpenSearchServerless, intent *v1alpha1.PodCreationIntent, uid string) bool {
	if intent.BootstrapReplacementUID != "" {
		return r.planBootstrapReplacement(ctx, instance, intent, uid) != nil
	}
	if intent.EmergencyEvidenceUID == "" && intent.EmergencyEvidenceVersion == "" {
		return true
	}
	proposal, err := r.planEmergencyForIntent(ctx, instance, intent, uid)
	return err == nil && proposal != nil && proposal.Role == intent.Role && proposal.Zone == intent.Zone && proposal.Fleet == intent.Fleet
}

func terminalCapacityPod(pod *corev1.Pod) bool {
	return pod.Status.Phase == corev1.PodFailed || pod.Status.Phase == corev1.PodSucceeded
}
