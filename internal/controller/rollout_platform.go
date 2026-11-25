package controller

import (
	"context"
	"crypto/ed25519"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	typedcore "k8s.io/client-go/kubernetes/typed/core/v1"

	"github.com/huyz0/opensearch-serverless-operator/api/v1alpha1"
	"github.com/huyz0/opensearch-serverless-operator/internal/rollout"
	"github.com/huyz0/opensearch-serverless-operator/internal/workload"
)

// signedRolloutPlatformCheck reads short-lived, signed platform observations
// through a namespaced ConfigMap. Only the operator's deployment configuration
// supplies the trusted key. The ConfigMap writer need not be trusted.
func signedRolloutPlatformCheck(core typedcore.CoreV1Interface, trustedKey ed25519.PublicKey) rolloutPlatformCheck {
	key := append(ed25519.PublicKey(nil), trustedKey...)
	return func(ctx context.Context, instance *v1alpha1.OpenSearchServerless, operation *v1alpha1.OpenSearchServerlessOperation, role string, observed candidateRoleObservation) (rolloutPlatformObservation, error) {
		if core == nil || len(key) != ed25519.PublicKeySize || instance == nil || operation == nil ||
			instance.UID == "" || instance.Namespace == "" || operation.Namespace != instance.Namespace || operation.UID == "" ||
			operation.Spec.InstanceRef.UID != string(instance.UID) || operation.Spec.InstanceRef.Name != instance.Name ||
			(role != "search" && role != "ingest") {
			return rolloutPlatformObservation{}, fmt.Errorf("trusted key, namespace and rollout identities are required")
		}
		if journal := operation.Status.WeightedRollout; journal != nil {
			if !weightedClaimMatches(instance, operation) || journal.InstanceGeneration != instance.Generation ||
				journal.Roles[role].Revisions[observed.Fleet] != observed.Revision || journal.Roles[role].BackendUIDs[observed.Fleet] == "" {
				return rolloutPlatformObservation{}, fmt.Errorf("fleet observation is outside the claimed weighted journal")
			}
		} else if observed.Fleet != operation.Spec.TargetFleet {
			return rolloutPlatformObservation{}, fmt.Errorf("fleet observation is outside the operation target")
		}
		name := rollout.FleetPlatformEvidenceName(operation.UID, role, observed.Fleet, operation.Spec.TargetFleet)
		cm, err := core.ConfigMaps(instance.Namespace).Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			return rolloutPlatformObservation{}, fmt.Errorf("read rollout platform evidence: %w", err)
		}
		if cm.UID == "" || cm.ResourceVersion == "" || cm.DeletionTimestamp != nil {
			return rolloutPlatformObservation{}, fmt.Errorf("rollout platform evidence has no live identity or version")
		}
		evidence, err := rollout.ReadSignedPlatformEvidence(cm.Data, key)
		if err != nil {
			return rolloutPlatformObservation{}, err
		}
		now := time.Now()
		if evidence.Namespace != instance.Namespace || evidence.InstanceUID != instance.UID || evidence.InstanceGeneration != instance.Generation ||
			evidence.OperationUID != operation.UID || evidence.Role != role ||
			evidence.Fleet != observed.Fleet || evidence.Revision != observed.Revision || evidence.Revision == "" ||
			evidence.ObservedAt.IsZero() || evidence.ObservedAt.After(now) || now.Sub(evidence.ObservedAt) > rolloutObservationAge ||
			!evidence.PeerQualified || !evidence.SecurityQualified || !evidence.RollbackCompatible {
			return rolloutPlatformObservation{}, fmt.Errorf("rollout platform evidence is stale or does not qualify this role and revision")
		}
		if !platformPodsMatch(evidence.Pods, observed) {
			return rolloutPlatformObservation{}, fmt.Errorf("rollout platform evidence does not match the current Pods, processes and zones")
		}
		if expected := transitionHTTPTrustHash(instance, operation, role); expected != "" && (evidence.HTTPTrustSHA256 != expected || !evidence.HTTPTrustFenced) {
			return rolloutPlatformObservation{}, fmt.Errorf("rollout platform evidence does not qualify the HTTP trust transition")
		}
		return rolloutPlatformObservation{
			observedAt: evidence.ObservedAt,
			unchanged:  configMapVersionCheck(core, cm),
		}, nil
	}
}

func configMapVersionCheck(core typedcore.CoreV1Interface, observed *corev1.ConfigMap) func(context.Context) bool {
	namespace, name, uid, version := observed.Namespace, observed.Name, observed.UID, observed.ResourceVersion
	return func(ctx context.Context) bool {
		live, err := core.ConfigMaps(namespace).Get(ctx, name, metav1.GetOptions{})
		return err == nil && live.DeletionTimestamp == nil && live.UID == uid && live.ResourceVersion == version
	}
}

// Rollback uses the trust bundle already approved for the outbound fleet to
// cover both sets of backends until rollback converges.
func transitionHTTPTrustHash(instance *v1alpha1.OpenSearchServerless, operation *v1alpha1.OpenSearchServerlessOperation, role string) string {
	target := operation.Spec.TargetFleet
	source := operation.Spec.ExpectedServingFleetByRole[role]
	if operation.Spec.Action == "Rollback" && operation.Status.WeightedRollout != nil {
		for fleet := range operation.Status.WeightedRollout.Roles[role].Revisions {
			if fleet != target {
				source = fleet
			}
		}
	}
	old, next := httpTLSBaselineForFleet(instance, source), httpTLSBaselineForFleet(instance, target)
	if old == nil || next == nil || source == "" || target == "" || (!instance.Status.HTTPTrustRotationUsed && old.TrustHash == next.TrustHash) {
		return ""
	}
	if operation.Spec.Action == "Rollback" {
		return old.TrustHash
	}
	return next.TrustHash
}

func platformPodsMatch(attested []rollout.PlatformPod, observed candidateRoleObservation) bool {
	if len(attested) == 0 || len(attested) != len(observed.PodSnapshot) || len(attested) != len(observed.Pods) {
		return false
	}
	processes := make(map[types.UID]string, len(observed.Pods))
	for _, pod := range observed.Pods {
		processes[types.UID(pod.PodUID)] = pod.ProcessIncarnation
	}
	expected := make(map[types.UID]rollout.PlatformPod, len(observed.PodSnapshot))
	for _, pod := range observed.PodSnapshot {
		zone, process := pod.Labels[workload.ZoneLabel], processes[pod.UID]
		if pod.UID == "" || pod.ResourceVersion == "" || pod.Spec.NodeName == "" || zone == "" || process == "" {
			return false
		}
		expected[pod.UID] = rollout.PlatformPod{
			UID: pod.UID, ResourceVersion: pod.ResourceVersion, NodeName: pod.Spec.NodeName,
			Zone: zone, ProcessIncarnation: process,
		}
	}
	for _, pod := range attested {
		if want, exists := expected[pod.UID]; !exists || want != pod {
			return false
		}
		delete(expected, pod.UID)
	}
	return len(expected) == 0
}
