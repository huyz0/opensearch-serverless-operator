package controller

import (
	"context"
	"fmt"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	typedcore "k8s.io/client-go/kubernetes/typed/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/huyz0/opensearch-serverless-operator/api/v1alpha1"
	"github.com/huyz0/opensearch-serverless-operator/internal/workload"
)

// ensurePreviewServices creates or verifies candidate-only Services. The
// caller must persist and qualify the candidate fleet before invoking it. It
// never adopts, rewrites or deletes an existing Service. Its caller must pin
// the returned UIDs before relying on them for preview traffic.
func ensurePreviewServices(ctx context.Context, reader client.Reader, core typedcore.CoreV1Interface, instance *v1alpha1.OpenSearchServerless, fleet string, pinned map[string]types.UID) (map[string]types.UID, error) {
	if core == nil || instance == nil || instance.DeletionTimestamp != nil || instance.UID == "" {
		return nil, fmt.Errorf("live instance and Service client are required")
	}
	for _, role := range []string{"search", "ingest"} {
		if instance.Status.ServingFleetIntentByRole[role] == "" || instance.Status.ServingFleetIntentByRole[role] == fleet {
			return nil, fmt.Errorf("candidate must differ from each recorded serving fleet")
		}
	}
	result := make(map[string]types.UID, 2)
	for _, role := range []string{"search", "ingest"} {
		desired, err := workload.PreviewService(instance, role, fleet)
		if err != nil {
			return nil, err
		}
		services := core.Services(instance.Namespace)
		observed, err := services.Get(ctx, desired.Name, metav1.GetOptions{})
		if apierrors.IsNotFound(err) {
			if pinned[role] != "" {
				return nil, fmt.Errorf("pinned %s preview Service is missing", role)
			}
			if reader != nil {
				var live v1alpha1.OpenSearchServerless
				if err := reader.Get(ctx, client.ObjectKeyFromObject(instance), &live); err != nil {
					return nil, err
				}
				if live.UID != instance.UID || live.ResourceVersion != instance.ResourceVersion || live.DeletionTimestamp != nil || live.Status.ActiveOperation != nil {
					return nil, fmt.Errorf("candidate state changed before creating the %s preview Service", role)
				}
			}
			_, err = services.Create(ctx, desired, metav1.CreateOptions{})
			if err != nil && !apierrors.IsAlreadyExists(err) {
				return nil, fmt.Errorf("create %s preview Service: %w", role, err)
			}
			observed, err = services.Get(ctx, desired.Name, metav1.GetOptions{})
		}
		if err != nil {
			return nil, fmt.Errorf("read %s preview Service: %w", role, err)
		}
		if !matchesFleetService(instance, observed, desired, pinned[role]) {
			return nil, fmt.Errorf("%s preview Service conflicts with candidate routing intent", role)
		}
		result[role] = observed.UID
	}
	return result, nil
}

func markRolloutCandidateServed(instance *v1alpha1.OpenSearchServerless, operation *v1alpha1.OpenSearchServerlessOperation) {
	candidate := instance.Status.CandidateFleet
	if operation.Spec.Action == "Rollback" {
		recordDisplacedRoleRevisions(instance, operation.Spec.TargetFleet)
	}
	if candidate == nil {
		return
	}
	if operation.Spec.Action == "Promote" && operation.Spec.TargetFleet == candidate.Fleet {
		candidate.WasServing = true
	}
	if operation.Spec.Action == "Rollback" {
		for _, role := range []string{"search", "ingest"} {
			if candidate.Fleet == instance.Status.ServingFleetIntentByRole[role] || candidate.Fleet == operation.Status.RolloutRoles[role].SourceFleet || candidate.Fleet == operation.Spec.ExpectedServingFleetByRole[role] {
				candidate.WasServing = true
			}
			if weighted := operation.Status.WeightedRollout; weighted != nil && weighted.Roles[role].InitialWeights[candidate.Fleet] > 0 {
				candidate.WasServing = true
			}
		}
	}
}

// Capture the previous serving pins before the rollback commit replaces them.
func recordDisplacedRoleRevisions(instance *v1alpha1.OpenSearchServerless, restored string) {
	search, ingest := instance.Status.ServingRevisionByRole["search"], instance.Status.ServingRevisionByRole["ingest"]
	if search.Fleet == "" || search.Fleet != ingest.Fleet || search.Fleet == restored || search.Revision == "" || ingest.Revision == "" ||
		instance.Status.ServingFleetIntentByRole["search"] != search.Fleet || instance.Status.ServingFleetIntentByRole["ingest"] != ingest.Fleet {
		return
	}
	roles := v1alpha1.FleetRoleRevisions{Search: search.Revision, Ingest: ingest.Revision}
	if candidate := instance.Status.CandidateFleet; candidate != nil && candidate.Fleet == search.Fleet {
		candidate.RetirementRevisionsByRole = &roles
	}
	for i := range instance.Status.RetiredCandidateFleets {
		retired := &instance.Status.RetiredCandidateFleets[i]
		if retired.PreviewOnly && retired.Fleet == search.Fleet {
			retired.Revision, retired.RevisionsByRole = roles.Fields()
		}
	}
}

// detachPromotedPreview journals alias cleanup without releasing ownership of
// Pods, Service backends, or TLS inputs used by serving and rollback fleets.
func (r *InstanceReconciler) detachPromotedPreview(ctx context.Context, instance *v1alpha1.OpenSearchServerless) error {
	state, target := instance.Status.CandidateFleet, instance.Status.RollbackTarget
	commit := instance.Status.LastRolloutCommit
	if r.Core == nil || instance.Spec.Rollout == nil || instance.Spec.Rollout.Candidate != nil || state == nil || state.Fleet == "" || state.Revision == "" || state.SearchServiceUID == "" || state.IngestServiceUID == "" ||
		commit == nil || commit.UID == "" || instance.Status.ActiveOperation != nil ||
		instance.Status.PendingPodCreation != nil || instance.Status.PendingGateRelease != nil || instance.Status.ProvisioningAttempt != nil || instance.Status.SearchWarmupSlot != nil || len(instance.Status.BlockedProvisioningAttempts) != 0 {
		return fmt.Errorf("preview detach requires a completed rollout and settled Pod lifecycle")
	}
	promoted := commit.Action == "Promote" && target != nil && target.PromotionUID == commit.UID
	rolledBack := commit.Action == "Rollback" && target == nil && state.WasServing
	if !promoted && !rolledBack {
		return fmt.Errorf("promoted preview does not match a completed promotion or rollback")
	}
	if len(instance.Status.RetiredCandidateFleets) >= 64 {
		return fmt.Errorf("retired candidate history is full")
	}
	for _, retired := range instance.Status.RetiredCandidateFleets {
		if retired.Fleet == state.Fleet {
			return fmt.Errorf("promoted candidate identity is already archived")
		}
	}
	for _, role := range []string{"search", "ingest"} {
		serving := instance.Status.ServingRevisionByRole[role]
		if serving.Fleet == "" || instance.Status.ServingFleetIntentByRole[role] != serving.Fleet || serving.Revision == "" {
			return fmt.Errorf("%s serving fleet revision does not match the committed rollout", role)
		}
		var pinnedUID string
		if promoted {
			retained := target.Roles[role]
			if serving.Fleet != state.Fleet || serving.Revision != state.Revision || retained.Fleet == "" || retained.Fleet == state.Fleet || retained.Revision == "" || retained.ServiceUID == "" {
				return fmt.Errorf("%s serving and rollback fleets do not match the committed promotion", role)
			}
			pinnedUID = retained.ServiceUID
		} else if serving.Fleet == state.Fleet {
			return fmt.Errorf("%s still serves the displaced candidate", role)
		}
		wanted, err := workload.RoleService(instance, role, serving.Fleet, 9200)
		if err != nil {
			return err
		}
		actual, err := r.Core.Services(instance.Namespace).Get(ctx, wanted.Name, metav1.GetOptions{})
		if err != nil || !matchesFleetService(instance, actual, wanted, types.UID(pinnedUID)) {
			return fmt.Errorf("%s stable Service does not match the committed rollout", role)
		}
	}
	archived := v1alpha1.RetiredCandidateFleet{Fleet: state.Fleet, Revision: state.Revision, SearchServiceUID: state.SearchServiceUID,
		IngestServiceUID: state.IngestServiceUID, PreviewOnly: true}
	if rolledBack && state.RetirementRevisionsByRole != nil {
		roles, valid := v1alpha1.ResolveFleetRoleRevisions("", state.RetirementRevisionsByRole)
		if !valid {
			return fmt.Errorf("displaced candidate role revisions are incomplete")
		}
		archived.Revision, archived.RevisionsByRole = roles.Fields()
	}
	instance.Status.RetiredCandidateFleets = append(instance.Status.RetiredCandidateFleets, archived)
	instance.Status.CandidateFleet = nil
	return nil
}

// retireCandidatePreviewService pins an unrecorded Service UID before removal,
// then deletes at most one matching Service using UID and version preconditions.
// The retirement marker and pinned identities remain after deletion to catch
// late create requests. It never clears the candidate or removes Pods.
func (r *InstanceReconciler) retireCandidatePreviewService(ctx context.Context, instance *v1alpha1.OpenSearchServerless) (bool, error) {
	state := instance.Status.CandidateFleet
	if r.Core == nil || state == nil || !state.PreviewRetiring || state.Fleet == "" || state.Fleet == instance.Status.ServingFleetIntentByRole["search"] || state.Fleet == instance.Status.ServingFleetIntentByRole["ingest"] {
		return false, fmt.Errorf("candidate preview retirement identity or serving state changed")
	}
	for _, role := range []string{"search", "ingest"} {
		desired, err := workload.PreviewService(instance, role, state.Fleet)
		if err != nil {
			return false, err
		}
		services := r.Core.Services(instance.Namespace)
		service, err := services.Get(ctx, desired.Name, metav1.GetOptions{})
		if apierrors.IsNotFound(err) {
			continue
		}
		if err != nil {
			return false, fmt.Errorf("read preview Service: %w", err)
		}
		pinned := state.SearchServiceUID
		if role == "ingest" {
			pinned = state.IngestServiceUID
		}
		if !matchesFleetService(instance, service, desired, types.UID(pinned)) {
			return false, fmt.Errorf("%s preview Service identity or routing changed", role)
		}
		if pinned == "" {
			before := instance.DeepCopyObject().(*v1alpha1.OpenSearchServerless)
			if role == "search" {
				state.SearchServiceUID = string(service.UID)
			} else {
				state.IngestServiceUID = string(service.UID)
			}
			if err := r.Status().Patch(ctx, instance, client.MergeFromWithOptions(before, client.MergeFromWithOptimisticLock{})); err != nil {
				return false, err
			}
			return true, nil
		}
		uid, version := service.UID, service.ResourceVersion
		if err := services.Delete(ctx, service.Name, metav1.DeleteOptions{Preconditions: &metav1.Preconditions{UID: &uid, ResourceVersion: &version}}); err != nil {
			return false, fmt.Errorf("delete preview Service: %w", err)
		}
		return true, nil
	}
	return false, nil
}
