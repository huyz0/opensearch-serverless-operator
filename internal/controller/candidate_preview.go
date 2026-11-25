package controller

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"reflect"
	"slices"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/huyz0/opensearch-serverless-operator/api/v1alpha1"
	"github.com/huyz0/opensearch-serverless-operator/internal/workload"
)

// reconcileCandidatePreview records candidate state before creating Services.
// Pod provisioning runs separately after both Service UIDs are pinned.
func (r *InstanceReconciler) reconcileCandidatePreview(ctx context.Context, instance *v1alpha1.OpenSearchServerless) (bool, error) {
	var desired *v1alpha1.FleetCandidate
	if instance.Spec.Rollout != nil {
		desired = instance.Spec.Rollout.Candidate
	}
	state := instance.Status.CandidateFleet
	if pending := instance.Status.PendingHTTPTLS; pending != nil && pending.Fleet != "" &&
		(desired == nil || !desired.HTTPRenewFromIssuer || pending.Fleet != desired.Fleet) {
		before := instance.DeepCopy()
		instance.Status.PendingHTTPTLS = nil
		return r.patchPreviewStatus(ctx, before, instance)
	}
	if desired == nil && state == nil {
		return false, nil
	}
	before := instance.DeepCopy()
	switch {
	case state != nil && (desired == nil || state.PreviewRetiring):
		if r.reconcilePreviewRetirement(ctx, instance) {
			return true, nil
		}
	case state == nil:
		err := r.recordPreviewCandidate(ctx, instance, desired)
		if errors.Is(err, errHTTPLeafStaging) {
			return true, nil
		}
		if err != nil {
			return false, err
		}
	case state.Fleet != desired.Fleet || state.Image != desired.Image || !candidateInputsMatch(state, desired):
		setPreviewCondition(instance, metav1.ConditionFalse, "CandidateChanged", "Remove the recorded candidate before staging a different identity")
	case state.Generation != instance.Generation:
		ok, err := r.canRebaseCandidate(ctx, instance, desired)
		switch {
		case err != nil:
			setPreviewCondition(instance, metav1.ConditionFalse, "CandidateRebaseBlocked", err.Error())
		case !ok:
			setPreviewCondition(instance, metav1.ConditionFalse, "CandidateChanged", "The current spec no longer matches the recorded candidate revision")
		default:
			state.Generation = instance.Generation
			setPreviewCondition(instance, metav1.ConditionUnknown, "CandidateRebased", "Unchanged candidate identity was recovered; preview Services must be rechecked")
		}
	default:
		r.observeRecordedPreview(ctx, instance, desired)
	}
	return r.patchPreviewStatus(ctx, before, instance)
}

func setPreviewCondition(instance *v1alpha1.OpenSearchServerless, status metav1.ConditionStatus, reason, message string) {
	meta.SetStatusCondition(&instance.Status.Conditions, metav1.Condition{
		Type: "PreviewServicesConfigured", Status: status, Reason: reason, Message: message, ObservedGeneration: instance.Generation,
	})
}

func (r *InstanceReconciler) patchPreviewStatus(ctx context.Context, before, instance *v1alpha1.OpenSearchServerless) (bool, error) {
	if reflect.DeepEqual(before.Status, instance.Status) {
		return false, nil
	}
	if err := r.Status().Patch(ctx, instance, client.MergeFromWithOptions(before, client.MergeFromWithOptimisticLock{})); err != nil {
		return false, err
	}
	return true, nil
}

// reconcilePreviewRetirement returns true when it already persisted a journal
// update or requested deletion. Otherwise the caller persists the condition.
func (r *InstanceReconciler) reconcilePreviewRetirement(ctx context.Context, instance *v1alpha1.OpenSearchServerless) bool {
	state := instance.Status.CandidateFleet
	if state.WasServing || state.Fleet == instance.Status.ServingFleetIntentByRole["search"] || state.Fleet == instance.Status.ServingFleetIntentByRole["ingest"] {
		if err := r.detachPromotedPreview(ctx, instance); err != nil {
			setPreviewCondition(instance, metav1.ConditionFalse, "PromotedPreviewDetachBlocked", err.Error())
		} else {
			setPreviewCondition(instance, metav1.ConditionFalse, "PromotedPreviewDetached", "Promoted fleet is preserved and its preview aliases are recorded for cleanup")
		}
		return false
	}
	if !state.PreviewRetiring {
		state.PreviewRetiring = true
		setPreviewCondition(instance, metav1.ConditionFalse, "PreviewRetiring", "Candidate preview Service retirement is recorded")
		return false
	}
	changed, err := r.retireCandidatePreviewService(ctx, instance)
	if changed {
		return true
	}
	if err != nil {
		setPreviewCondition(instance, metav1.ConditionFalse, "PreviewRetirementBlocked", err.Error())
		return false
	}
	finalized, err := r.finalizeCandidateRetirement(ctx, instance)
	if finalized {
		return true
	}
	if err != nil {
		setPreviewCondition(instance, metav1.ConditionFalse, "CandidateRetirementBlocked", err.Error())
	} else {
		setPreviewCondition(instance, metav1.ConditionFalse, "PreviewServicesRemoved", "Preview Services are absent; candidate Pods and retained identities still require retirement")
	}
	return false
}

// recordPreviewCandidate qualifies fleet inputs before pinning the template.
// Certificate issuance can persist an intermediate intent before this step.
func (r *InstanceReconciler) recordPreviewCandidate(ctx context.Context, instance *v1alpha1.OpenSearchServerless, desired *v1alpha1.FleetCandidate) error {
	switch {
	case instance.Status.RollbackTarget != nil:
		setPreviewCondition(instance, metav1.ConditionFalse, "RollbackFleetRetained", "Retained rollback capacity must be retired before staging a new candidate")
		return nil
	case slices.ContainsFunc(instance.Status.RetiredCandidateFleets, func(retired v1alpha1.RetiredCandidateFleet) bool {
		return retired.PreviewOnly && retired.Fleet != instance.Status.ServingFleetIntentByRole["search"] && retired.Fleet != instance.Status.ServingFleetIntentByRole["ingest"]
	}):
		setPreviewCondition(instance, metav1.ConditionFalse, "DetachedFleetRetained", "A previously detached fleet must be retired before staging a new candidate")
		return nil
	}
	if _, pinned := instance.Status.HTTPFleetCertificates[desired.Fleet]; pinned {
		setPreviewCondition(instance, metav1.ConditionFalse, "CandidateCertificateAlreadyPinned", "Choose a new fleet identity for the preview certificate")
		return nil
	}
	if _, pinned := instance.Status.PeerTLSByFleet[desired.Fleet]; pinned {
		setPreviewCondition(instance, metav1.ConditionFalse, "CandidateCertificateAlreadyPinned", "Choose a new fleet identity for the peer TLS snapshot")
		return nil
	}
	for _, retired := range instance.Status.RetiredCandidateFleets {
		if retired.Fleet == desired.Fleet {
			setPreviewCondition(instance, metav1.ConditionFalse, "CandidateFleetRetired", "Choose a new fleet identity; this fleet remains fenced against late creates")
			return nil
		}
	}
	if desired.Fleet == instance.Status.ServingFleetIntentByRole["search"] || desired.Fleet == instance.Status.ServingFleetIntentByRole["ingest"] {
		setPreviewCondition(instance, metav1.ConditionFalse, "CandidateMatchesServing", "Candidate fleet must differ from both serving roles")
		return nil
	}
	pluginPins, err := r.previewPluginFleet(ctx, instance, desired)
	if err != nil {
		setPreviewCondition(instance, metav1.ConditionFalse, "PreviewPluginsUnqualified", err.Error())
		return nil
	}
	runtimePins, err := r.previewFleetRuntime(ctx, instance, desired)
	if err != nil {
		setPreviewCondition(instance, metav1.ConditionFalse, "PreviewRuntimeUnqualified", err.Error())
		return nil
	}
	candidateInstance := instance.DeepCopy()
	candidateInstance.Spec.Image = desired.Image
	if set, found := pluginPins[desired.Fleet]; found {
		candidateInstance.Spec.Plugins = set.Plugins
	}
	if config, found := runtimePins[desired.Fleet]; found {
		applyFleetRuntime(candidateInstance, config)
	}
	domain, err := r.peerDNSDomain(instance)
	if err != nil {
		setPreviewCondition(instance, metav1.ConditionFalse, "PeerDNSUnavailable", err.Error())
		return nil
	}
	revision, err := podRevision(candidateInstance, domain)
	if err != nil {
		return err
	}
	peer, err := r.previewPeerTLS(ctx, instance, desired, domain)
	if err != nil {
		setPreviewCondition(instance, metav1.ConditionFalse, "PreviewPeerTLSUnqualified", err.Error())
		return nil
	}
	leaf, err := r.previewHTTPCertificate(ctx, instance, desired, domain)
	if errors.Is(err, errHTTPLeafStaging) {
		return err
	}
	if err != nil {
		setPreviewCondition(instance, metav1.ConditionFalse, "PreviewCertificateUnqualified", err.Error())
		return nil
	}
	if peer != nil {
		if instance.Status.PeerTLSByFleet == nil {
			instance.Status.PeerTLSByFleet = make(map[string]v1alpha1.PeerTLSBaseline)
		}
		instance.Status.PeerTLSByFleet[desired.Fleet] = *peer
	}
	if leaf != nil {
		if desired.HTTPRenewFromIssuer {
			instance.Status.PendingHTTPTLS = nil
		}
		if instance.Status.HTTPFleetCertificates == nil {
			instance.Status.HTTPFleetCertificates = make(map[string]v1alpha1.HTTPFleetCertificate)
		}
		instance.Status.HTTPFleetCertificates[desired.Fleet] = *leaf
		if base := instance.Status.HTTPTLSBaseline; base != nil && leaf.TrustSHA256 != "" && leaf.TrustSHA256 != base.TrustHash {
			instance.Status.HTTPTrustRotationUsed = true
		}
	}
	if len(pluginPins) != 0 {
		if instance.Status.PluginSetsByFleet == nil {
			instance.Status.PluginSetsByFleet = make(map[string]v1alpha1.FleetPluginSet, len(pluginPins))
		}
		maps.Copy(instance.Status.PluginSetsByFleet, pluginPins)
	}
	if len(runtimePins) != 0 {
		if instance.Status.RuntimeByFleet == nil {
			instance.Status.RuntimeByFleet = make(map[string]v1alpha1.FleetRuntime, len(runtimePins))
		}
		maps.Copy(instance.Status.RuntimeByFleet, runtimePins)
	}
	instance.Status.CandidateFleet = &v1alpha1.CandidateFleetStatus{Fleet: desired.Fleet, Image: desired.Image, Revision: revision, Generation: instance.Generation,
		HTTPCertificateRef: candidateHTTPCertificateName(desired), HTTPTrustBundleRef: desired.HTTPTrustBundleRef,
		HTTPRenewFromIssuer: desired.HTTPRenewFromIssuer, PeerTLS: desired.PeerTLS, PluginOverride: desired.PluginOverride, RuntimeOverride: desired.RuntimeOverride}
	setPreviewCondition(instance, metav1.ConditionFalse, "CandidateRecorded", "Candidate identity is recorded; preview Services are not yet observed")
	return nil
}

func (r *InstanceReconciler) observeRecordedPreview(ctx context.Context, instance *v1alpha1.OpenSearchServerless, desired *v1alpha1.FleetCandidate) {
	if peer, pinned := instance.Status.PeerTLSByFleet[desired.Fleet]; pinned {
		domain, err := r.peerDNSDomain(instance)
		if err != nil {
			setPreviewCondition(instance, metav1.ConditionFalse, "PreviewPeerTLSUnqualified", err.Error())
			return
		}
		input := &peerTLSInput{baseline: peer}
		nextPod := &v1alpha1.PodCreationIntent{Name: podNameForSequence(instance, instance.Status.PodCreationSequence+1)}
		if peer.SnapshotUID == "" {
			snapshot, err := r.ensurePeerSnapshot(ctx, instance, nextPod, input, domain)
			if err != nil {
				if r.replanPreviewPeerTLS(ctx, instance, desired, peer, domain) {
					setPreviewCondition(instance, metav1.ConditionFalse, "CandidatePeerTLSReplanned", "Qualified issuer inputs replaced the uncommitted candidate snapshot intent")
					return
				}
				setPreviewCondition(instance, metav1.ConditionFalse, "PreviewPeerTLSUnqualified", err.Error())
				return
			}
			peer.SnapshotUID = string(snapshot.UID)
			instance.Status.PeerTLSByFleet[desired.Fleet] = peer
			setPreviewCondition(instance, metav1.ConditionFalse, "CandidatePeerTLSPinned", "Peer TLS snapshot UID is recorded; preview Services are not yet observed")
			return
		}
		_, err = r.readPeerFleetSnapshot(ctx, instance, &peer, nextPod.Name, domain, r.now())
		if err != nil {
			setPreviewCondition(instance, metav1.ConditionFalse, "PreviewPeerTLSUnqualified", err.Error())
			return
		}
	} else if desired.PeerTLS != nil {
		setPreviewCondition(instance, metav1.ConditionFalse, "PreviewPeerTLSUnqualified", "Candidate peer TLS snapshot is not recorded")
		return
	}
	if _, pinned := instance.Status.HTTPFleetCertificates[desired.Fleet]; pinned || desired.HTTPRenewFromIssuer || instance.Status.CandidateFleet.HTTPCertificateRef != "" || instance.Status.CandidateFleet.HTTPTrustBundleRef != nil {
		domain, err := r.peerDNSDomain(instance)
		if err == nil {
			_, err = r.resolveHTTPTLSForFleet(ctx, instance, domain, desired.Fleet)
		}
		if err != nil {
			setPreviewCondition(instance, metav1.ConditionFalse, "PreviewCertificateUnqualified", err.Error())
			return
		}
	}
	pinned := map[string]types.UID{"search": types.UID(instance.Status.CandidateFleet.SearchServiceUID), "ingest": types.UID(instance.Status.CandidateFleet.IngestServiceUID)}
	reader := r.APIReader
	if reader == nil {
		reader = r.Client
	}
	uids, err := ensurePreviewServices(ctx, reader, r.Core, instance, desired.Fleet, pinned)
	if err != nil {
		setPreviewCondition(instance, metav1.ConditionFalse, "PreviewServiceConflict", err.Error())
		return
	}
	state := instance.Status.CandidateFleet
	state.SearchServiceUID = string(uids["search"])
	state.IngestServiceUID = string(uids["ingest"])
	setPreviewCondition(instance, metav1.ConditionTrue, "ServicesObserved", "Candidate preview Service identities are recorded")
}

// canRebaseCandidate only accepts an unchanged candidate template and no
// in-flight lifecycle work. Existing candidate Pods must still identify that
// exact template; a later reconcile rechecks the pinned preview Services.
func (r *InstanceReconciler) canRebaseCandidate(ctx context.Context, instance *v1alpha1.OpenSearchServerless, desired *v1alpha1.FleetCandidate) (bool, error) {
	if r.Core == nil || instance.Status.PendingPodCreation != nil || instance.Status.PendingGateRelease != nil || instance.Status.ProvisioningAttempt != nil || instance.Status.SearchWarmupSlot != nil || instance.Status.ActiveOperation != nil {
		return false, fmt.Errorf("Pod lifecycle work or an operation is still in progress")
	}
	for _, retired := range instance.Status.RetiredPodCreations {
		if retired.Fleet == desired.Fleet {
			return false, fmt.Errorf("a retired candidate Pod creation may still arrive")
		}
	}
	copy := instance.DeepCopyObject().(*v1alpha1.OpenSearchServerless)
	copy.Spec.Image = desired.Image
	domain, err := r.peerDNSDomain(instance)
	if err != nil {
		return false, err
	}
	revisions, err := podRevisions(copy, domain)
	if err != nil {
		return false, err
	}
	revision := instance.Status.CandidateFleet.Revision
	if !revisions.forFleetTemplate(desired.Fleet).accepts(revision) {
		return false, nil
	}
	selector := labels.SelectorFromSet(labels.Set{workload.InstanceLabel: string(instance.UID), workload.FleetLabel: desired.Fleet}).String()
	pods, err := r.Core.Pods(instance.Namespace).List(ctx, metav1.ListOptions{LabelSelector: selector, Limit: 513})
	if err != nil || pods.Continue != "" || len(pods.Items) > 512 {
		return false, fmt.Errorf("candidate Pod observation is incomplete")
	}
	for _, pod := range pods.Items {
		owner := metav1.GetControllerOf(&pod)
		role, zone := pod.Labels[workload.RoleLabel], pod.Labels[workload.ZoneLabel]
		pool, roleExists := instance.Spec.Pools[role]
		_, zoneExists := pool.Zones[zone]
		imageMatches := false
		for _, container := range pod.Spec.Containers {
			if container.Name == "opensearch" && container.Image == desired.Image {
				imageMatches = true
			}
		}
		if owner == nil || owner.UID != instance.UID || owner.Name != instance.Name || owner.Kind != "OpenSearchServerless" || owner.APIVersion != v1alpha1.GroupVersion.String() || pod.UID == "" || pod.DeletionTimestamp != nil || !roleExists || !zoneExists || !imageMatches || pod.Labels["serverless.opensearch.org/revision"] != revision {
			return false, fmt.Errorf("candidate Pod identity or revision conflicts with the recorded fleet")
		}
	}
	return true, nil
}
