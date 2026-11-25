package controller

import (
	"context"
	"crypto/ed25519"
	"fmt"
	"slices"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/huyz0/opensearch-serverless-operator/api/v1alpha1"
	"github.com/huyz0/opensearch-serverless-operator/internal/rollout"
	"github.com/huyz0/opensearch-serverless-operator/internal/workload"
)

// reconcileFleetRetirement releases retained capacity after the platform has
// completed drain and fenced the old processes. It never evicts a running Pod.
func (r *InstanceReconciler) reconcileFleetRetirement(ctx context.Context, instance *v1alpha1.OpenSearchServerless) (bool, error) {
	if r.Core == nil || instance.Name != r.retirementInstanceName || len(r.retirementEvidenceKey) != ed25519.PublicKeySize {
		return false, nil
	}
	fleet, revisions := rollout.FleetRetirementTarget(instance, r.now())
	if fleet == "" {
		return false, nil
	}
	before := instance.DeepCopyObject().(*v1alpha1.OpenSearchServerless)
	record := func(status metav1.ConditionStatus, reason, message string) (bool, error) {
		changed := meta.SetStatusCondition(&instance.Status.Conditions, metav1.Condition{
			Type: "FleetRetirement", Status: status, Reason: reason, Message: message, ObservedGeneration: instance.Generation,
		})
		if !changed && status != metav1.ConditionTrue {
			return false, nil
		}
		err := r.Status().Patch(ctx, instance, client.MergeFromWithOptions(before, client.MergeFromWithOptimisticLock{}))
		return err == nil, err
	}
	name := rollout.FleetRetirementEvidenceName(types.UID(instance.Status.LastRolloutCommit.UID))
	cm, err := r.Core.ConfigMaps(instance.Namespace).Get(ctx, name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return false, nil
	}
	if err != nil {
		return record(metav1.ConditionFalse, "RetirementEvidenceUnavailable", "Fleet retirement evidence could not be read")
	}
	evidence, err := rollout.ReadSignedFleetRetirementEvidence(cm.Data, r.retirementEvidenceKey)
	if err == nil && evidence.Phase == rollout.RetirementReserve && instance.Status.FleetRetirement != nil {
		// The signed reservation request may remain in the ConfigMap during drain.
		return false, nil
	}
	if err != nil || cm.UID == "" || cm.DeletionTimestamp != nil || !evidence.Qualifies(instance, r.now()) {
		return record(metav1.ConditionFalse, "RetirementEvidenceUnqualified", "Retirement evidence must identify the displaced fleet and the requested retirement phase")
	}
	archived := &instance.Status.RetiredCandidateFleets
	index := slices.IndexFunc(*archived, func(retired v1alpha1.RetiredCandidateFleet) bool { return retired.Fleet == fleet })
	if index == -1 && len(*archived) >= 64 {
		return record(metav1.ConditionFalse, "RetirementHistoryFull", "Retained identity history has reached its limit")
	}
	if index >= 0 {
		previous := (*archived)[index]
		_, valid := v1alpha1.ResolveFleetRoleRevisions(previous.Revision, previous.RevisionsByRole)
		if !previous.PreviewOnly || !valid {
			return record(metav1.ConditionFalse, "RetirementIdentityConflict", "Archived fleet identity differs from the retirement target")
		}
	}
	if evidence.Phase == rollout.RetirementReserve {
		if instance.Status.PendingPodCreation != nil || instance.Status.PendingGateRelease != nil || instance.Status.ProvisioningAttempt != nil ||
			instance.Status.SearchWarmupSlot != nil || instance.Status.EmergencyRetirement != nil || instance.Status.ScaleDownRetirement != nil ||
			len(instance.Status.BlockedProvisioningAttempts) != 0 {
			return record(metav1.ConditionFalse, "PodLifecycleBusy", "Pod lifecycle work must settle before reserving fleet retirement")
		}
	}
	pods, err := r.observeDisplacedFleetPods(ctx, instance, fleet)
	if err != nil {
		return record(metav1.ConditionFalse, "RetirementInventoryUnavailable", err.Error())
	}
	if !evidence.Qualifies(instance, r.now()) {
		return false, nil
	}
	revision, byRole := revisions.Fields()
	if evidence.Phase == rollout.RetirementReserve {
		instance.Status.FleetRetirement = &v1alpha1.FleetRetirementReservation{UID: string(cm.UID), CommitUID: string(evidence.CommitUID), Fleet: fleet, Revision: revision,
			RevisionsByRole: byRole, ReservedAt: metav1.NewMicroTime(r.now().UTC().Truncate(time.Microsecond)), PodUIDs: pods}
		return record(metav1.ConditionUnknown, "DrainReserved", "The displaced fleet is reserved for platform drain and fencing")
	}
	if slices.ContainsFunc(instance.Status.RetiredPodCreations, func(pod v1alpha1.RetiredPodCreation) bool {
		return pod.Fleet == fleet && pod.Released
	}) {
		return record(metav1.ConditionFalse, "RetiredFleetPodsRemain", "A displaced Pod release has not settled")
	}
	if len(pods) != 0 {
		return record(metav1.ConditionFalse, "RetiredFleetPodsRemain", "A displaced Pod still exists")
	}
	if index == -1 {
		*archived = append(*archived, v1alpha1.RetiredCandidateFleet{Fleet: fleet, Revision: revision, RevisionsByRole: byRole})
	} else {
		retired := &(*archived)[index]
		// The promotion's retained role pins describe the latest serving Pods.
		// The preview record may predate an accepted revision alias migration.
		retired.Revision, retired.RevisionsByRole, retired.PreviewOnly = revision, byRole, false
	}
	instance.Status.RollbackTarget = nil
	instance.Status.FleetRetirement = nil
	delete(instance.Status.HTTPFleetCertificates, fleet)
	delete(instance.Status.PeerTLSByFleet, fleet)
	delete(instance.Status.PluginSetsByFleet, fleet)
	delete(instance.Status.RuntimeByFleet, fleet)
	return record(metav1.ConditionTrue, "FleetFenced", fmt.Sprintf("Fleet %q was fenced and releases its retained capacity", fleet))
}

func (r *InstanceReconciler) observeDisplacedFleetPods(ctx context.Context, instance *v1alpha1.OpenSearchServerless, fleet string) ([]string, error) {
	pinned := make(map[string]bool)
	if reserved := instance.Status.FleetRetirement; reserved != nil {
		for _, uid := range reserved.PodUIDs {
			pinned[uid] = true
		}
	}
	var displaced []string
	options := metav1.ListOptions{Limit: 200}
	for range 32 {
		page, err := r.Core.Pods(instance.Namespace).List(ctx, options)
		if err != nil {
			return nil, fmt.Errorf("cannot read the namespace Pod inventory for retirement: %w", err)
		}
		for i := range page.Items {
			pod := &page.Items[i]
			labeled := pod.Labels[workload.InstanceLabel] == string(instance.UID)
			referencesInstance := slices.ContainsFunc(pod.OwnerReferences, func(ref metav1.OwnerReference) bool { return ref.UID == instance.UID })
			if !labeled && !referencesInstance && !pinned[string(pod.UID)] {
				continue
			}
			owner := metav1.GetControllerOf(pod)
			podFleet := pod.Labels[workload.FleetLabel]
			if pinned[string(pod.UID)] || (podFleet == fleet && pod.UID != "") {
				displaced = append(displaced, string(pod.UID))
				continue
			}
			role := pod.Labels[workload.RoleLabel]
			if !labeled || owner == nil || owner.UID != instance.UID || owner.Name != instance.Name || owner.Kind != "OpenSearchServerless" ||
				owner.APIVersion != v1alpha1.GroupVersion.String() || podFleet == "" || podFleet != instance.Status.ServingFleetIntentByRole[role] {
				return nil, fmt.Errorf("cannot classify owned Pod %q during fleet retirement", pod.Name)
			}
		}
		if page.Continue == "" {
			if len(displaced) > 512 {
				return nil, fmt.Errorf("displaced Pod inventory exceeds the reservation limit")
			}
			slices.Sort(displaced)
			return displaced, nil
		}
		options.Continue = page.Continue
	}
	return nil, fmt.Errorf("cannot complete the namespace Pod inventory for retirement")
}
