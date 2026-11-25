package controller

import (
	"context"
	"fmt"
	"slices"
	"sort"
	"time"

	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/huyz0/opensearch-serverless-operator/api/v1alpha1"
	"github.com/huyz0/opensearch-serverless-operator/internal/workload"
)

// finalizeCandidateRetirement moves an empty candidate into retained history.
// Pod creation and PDB identity tombstones remain after cleanup.
func (r *InstanceReconciler) finalizeCandidateRetirement(ctx context.Context, instance *v1alpha1.OpenSearchServerless) (bool, error) {
	state := instance.Status.CandidateFleet
	if state == nil || state.WasServing || !state.PreviewRetiring || instance.Status.ActiveOperation != nil || instance.Status.PendingPodCreation != nil || instance.Status.PendingGateRelease != nil || instance.Status.ProvisioningAttempt != nil || instance.Status.SearchWarmupSlot != nil || len(instance.Status.BlockedProvisioningAttempts) != 0 {
		return false, nil
	}
	if state.Fleet == "" || state.Revision == "" || state.Fleet == instance.Status.ServingFleetIntentByRole["search"] || state.Fleet == instance.Status.ServingFleetIntentByRole["ingest"] {
		return false, fmt.Errorf("candidate retirement conflicts with serving state")
	}
	if len(instance.Status.RetiredCandidateFleets) >= 64 {
		return false, fmt.Errorf("retired candidate history is full")
	}
	for _, retired := range instance.Status.RetiredCandidateFleets {
		if retired.Fleet == state.Fleet {
			return false, fmt.Errorf("candidate fleet identity is already retired")
		}
	}
	selector := labels.SelectorFromSet(labels.Set{workload.InstanceLabel: string(instance.UID), workload.FleetLabel: state.Fleet}).String()
	pods, err := r.Core.Pods(instance.Namespace).List(ctx, metav1.ListOptions{LabelSelector: selector, Limit: 513})
	if err != nil || pods.Continue != "" || len(pods.Items) > 512 {
		return false, fmt.Errorf("candidate Pod observation is incomplete")
	}
	if len(pods.Items) != 0 {
		return false, nil
	}
	before := instance.DeepCopyObject().(*v1alpha1.OpenSearchServerless)
	instance.Status.RetiredCandidateFleets = append(instance.Status.RetiredCandidateFleets, v1alpha1.RetiredCandidateFleet{Fleet: state.Fleet, Revision: state.Revision, SearchServiceUID: state.SearchServiceUID, IngestServiceUID: state.IngestServiceUID})
	instance.Status.CandidateFleet = nil
	retainedForRollback := false
	if target := instance.Status.RollbackTarget; target != nil {
		for _, role := range target.Roles {
			retainedForRollback = retainedForRollback || role.Fleet == state.Fleet
		}
	}
	if !retainedForRollback {
		delete(instance.Status.HTTPFleetCertificates, state.Fleet)
		delete(instance.Status.PeerTLSByFleet, state.Fleet)
		delete(instance.Status.PluginSetsByFleet, state.Fleet)
		delete(instance.Status.RuntimeByFleet, state.Fleet)
	}
	meta.SetStatusCondition(&instance.Status.Conditions, metav1.Condition{Type: "PreviewServicesConfigured", Status: metav1.ConditionFalse, Reason: "CandidateRetired", Message: "Candidate identity is retained to detect late creates", ObservedGeneration: instance.Generation})
	if err := r.Status().Patch(ctx, instance, client.MergeFromWithOptions(before, client.MergeFromWithOptimisticLock{})); err != nil {
		return false, err
	}
	return true, nil
}

// reconcileRetiredCandidateFleets keeps old fleet identities fenced. Unknown
// or released late Pods and replacement Services are held for manual recovery.
func (r *InstanceReconciler) reconcileRetiredCandidateFleets(ctx context.Context, instance *v1alpha1.OpenSearchServerless) (ctrl.Result, bool, error) {
	if len(instance.Status.RetiredCandidateFleets) > 64 {
		return ctrl.Result{}, true, fmt.Errorf("retired candidate history exceeds its limit")
	}
	var pods []corev1.Pod
	var budgets []policyv1.PodDisruptionBudget
	if slices.ContainsFunc(instance.Status.RetiredCandidateFleets, func(retired v1alpha1.RetiredCandidateFleet) bool { return !retired.PreviewOnly }) {
		selector := labels.SelectorFromSet(labels.Set{workload.InstanceLabel: string(instance.UID)}).String()
		page, err := r.Core.Pods(instance.Namespace).List(ctx, metav1.ListOptions{LabelSelector: selector, Limit: 513})
		if err != nil || page.Continue != "" || len(page.Items) > 512 {
			return ctrl.Result{RequeueAfter: time.Minute}, true, fmt.Errorf("retired candidate Pod observation is incomplete")
		}
		if r.Policy == nil {
			return ctrl.Result{RequeueAfter: time.Minute}, true, fmt.Errorf("retired candidate disruption budget client is unavailable")
		}
		continuation := ""
		for {
			page, err := r.Policy.PodDisruptionBudgets(instance.Namespace).List(ctx, metav1.ListOptions{Limit: 256, Continue: continuation})
			if err != nil || len(budgets)+len(page.Items) > 8192 {
				return ctrl.Result{RequeueAfter: time.Minute}, true, fmt.Errorf("retired candidate disruption budget observation is incomplete")
			}
			budgets = append(budgets, page.Items...)
			if page.Continue == "" {
				break
			}
			if page.Continue == continuation {
				return ctrl.Result{RequeueAfter: time.Minute}, true, fmt.Errorf("disruption budget pagination did not advance")
			}
			continuation = page.Continue
		}
		sort.Slice(budgets, func(i, j int) bool { return budgets[i].Name < budgets[j].Name })
		pods = page.Items
		sort.Slice(pods, func(i, j int) bool { return pods[i].Name < pods[j].Name })
	}
	seen := map[string]bool{}
	var previewIssue string
	for i, retired := range instance.Status.RetiredCandidateFleets {
		revisions, valid := v1alpha1.ResolveFleetRoleRevisions(retired.Revision, retired.RevisionsByRole)
		serving := retired.Fleet == instance.Status.ServingFleetIntentByRole["search"] || retired.Fleet == instance.Status.ServingFleetIntentByRole["ingest"]
		if retired.Fleet == "" || !valid || seen[retired.Fleet] || (serving && !retired.PreviewOnly) ||
			(retired.PreviewOnly && len(retired.DisruptionBudgets) != 0) || (instance.Status.CandidateFleet != nil && retired.Fleet == instance.Status.CandidateFleet.Fleet) {
			return ctrl.Result{RequeueAfter: time.Minute}, true, fmt.Errorf("retired candidate identity conflicts with active state")
		}
		seen[retired.Fleet] = true
		for _, pod := range pods {
			if retired.PreviewOnly || pod.Labels[workload.FleetLabel] != retired.Fleet {
				continue
			}
			if !r.DevelopmentGatedPods || pod.Spec.NodeName != "" || !hasPlacementGate(&pod) || pod.DeletionTimestamp != nil {
				return ctrl.Result{RequeueAfter: time.Minute}, true, fmt.Errorf("late retired candidate Pod is not safely gated")
			}
			identity, err := retiredFleetPodIdentity(instance, &pod, retired.Fleet, revisions.ForRole(pod.Labels[workload.RoleLabel]))
			if err != nil {
				return ctrl.Result{RequeueAfter: time.Minute}, true, err
			}
			if err := validateRetiredPod(instance, *identity, &pod); err != nil {
				return ctrl.Result{RequeueAfter: time.Minute}, true, err
			}
			before := instance.DeepCopyObject().(*v1alpha1.OpenSearchServerless)
			if err := appendRetiredCreation(instance, &v1alpha1.PodCreationIntent{Name: identity.Name, Role: identity.Role, Zone: identity.Zone, Fleet: identity.Fleet, Revision: identity.Revision, Sequence: identity.Sequence}); err != nil {
				return ctrl.Result{RequeueAfter: time.Minute}, true, err
			}
			if err := r.Status().Patch(ctx, instance, client.MergeFromWithOptions(before, client.MergeFromWithOptimisticLock{})); err != nil {
				return ctrl.Result{}, true, err
			}
			return ctrl.Result{Requeue: true}, true, nil
		}
		result, changed, err := r.reconcileArchivedPreviewServices(ctx, instance, i)
		if retired.PreviewOnly && err != nil {
			previewIssue = err.Error()
			continue
		}
		if changed || err != nil {
			return result, changed, err
		}
		if retired.PreviewOnly {
			continue
		}
		if len(retired.DisruptionBudgets) > 64 {
			return ctrl.Result{RequeueAfter: time.Minute}, true, fmt.Errorf("retired candidate disruption budget history exceeds its limit")
		}
		pinnedBudgets := map[string]string{}
		for _, pinned := range retired.DisruptionBudgets {
			if pinned.Name == "" || pinned.UID == "" || pinnedBudgets[pinned.Name] != "" {
				return ctrl.Result{RequeueAfter: time.Minute}, true, fmt.Errorf("retired disruption budget identity is incomplete or duplicated")
			}
			pinnedBudgets[pinned.Name] = pinned.UID
		}
		for _, budget := range budgets {
			pinned, previouslyPinned := pinnedBudgets[budget.Name]
			owner := metav1.GetControllerOf(&budget)
			owned := owner != nil && owner.UID == instance.UID && owner.Name == instance.Name && owner.Kind == "OpenSearchServerless" && owner.APIVersion == v1alpha1.GroupVersion.String()
			selector := budget.Spec.Selector
			if !previouslyPinned && (!owned || selector == nil || selector.MatchLabels[workload.FleetLabel] != retired.Fleet) {
				continue
			}
			if selector == nil {
				return ctrl.Result{RequeueAfter: time.Minute}, true, fmt.Errorf("retired disruption budget selector is missing")
			}
			role, zone := selector.MatchLabels[workload.RoleLabel], selector.MatchLabels[workload.ZoneLabel]
			if !owned || budget.UID == "" || budget.ResourceVersion == "" || (previouslyPinned && pinned != string(budget.UID)) || len(selector.MatchLabels) != 4 || len(selector.MatchExpressions) != 0 || selector.MatchLabels[workload.InstanceLabel] != string(instance.UID) || selector.MatchLabels[workload.FleetLabel] != retired.Fleet || (role != "search" && role != "ingest") || zone == "" || budget.Name != disruptionBudgetName(instance, retired.Fleet, role, zone) || budget.Spec.MaxUnavailable != nil || budget.Spec.MinAvailable == nil || budget.Spec.MinAvailable.Type != intstr.Int || budget.Spec.MinAvailable.IntVal < 0 || (budget.Spec.UnhealthyPodEvictionPolicy != nil && *budget.Spec.UnhealthyPodEvictionPolicy != policyv1.IfHealthyBudget) {
				return ctrl.Result{RequeueAfter: time.Minute}, true, fmt.Errorf("retired disruption budget conflicts with recorded identity or selector")
			}
			if budget.DeletionTimestamp != nil {
				return ctrl.Result{RequeueAfter: time.Second}, true, nil
			}
			if !previouslyPinned {
				if len(retired.DisruptionBudgets) >= 64 {
					return ctrl.Result{RequeueAfter: time.Minute}, true, fmt.Errorf("retired disruption budget history is full")
				}
				before := instance.DeepCopyObject().(*v1alpha1.OpenSearchServerless)
				instance.Status.RetiredCandidateFleets[i].DisruptionBudgets = append(instance.Status.RetiredCandidateFleets[i].DisruptionBudgets, v1alpha1.RetiredDisruptionBudget{Name: budget.Name, UID: string(budget.UID)})
				if err := r.Status().Patch(ctx, instance, client.MergeFromWithOptions(before, client.MergeFromWithOptimisticLock{})); err != nil {
					return ctrl.Result{}, true, err
				}
				return ctrl.Result{Requeue: true}, true, nil
			}
			uid, version := budget.UID, budget.ResourceVersion
			if err := r.Policy.PodDisruptionBudgets(instance.Namespace).Delete(ctx, budget.Name, metav1.DeleteOptions{Preconditions: &metav1.Preconditions{UID: &uid, ResourceVersion: &version}}); err != nil {
				return ctrl.Result{}, true, err
			}
			return ctrl.Result{Requeue: true}, true, nil
		}
	}
	if previewIssue != "" || meta.FindStatusCondition(instance.Status.Conditions, "PromotedPreviewAliasesClean") != nil {
		before := instance.DeepCopyObject().(*v1alpha1.OpenSearchServerless)
		condition := metav1.Condition{Type: "PromotedPreviewAliasesClean", Status: metav1.ConditionTrue, Reason: "AliasesAbsent", Message: "Promoted preview aliases are absent", ObservedGeneration: instance.Generation}
		if previewIssue != "" {
			condition.Status, condition.Reason, condition.Message = metav1.ConditionFalse, "AliasCleanupBlocked", previewIssue
		}
		if meta.SetStatusCondition(&instance.Status.Conditions, condition) {
			if err := r.Status().Patch(ctx, instance, client.MergeFromWithOptions(before, client.MergeFromWithOptimisticLock{})); err != nil {
				return ctrl.Result{}, true, err
			}
			return ctrl.Result{Requeue: true}, true, nil
		}
	}
	return ctrl.Result{}, false, nil
}

// reconcileArchivedPreviewServices removes only the fleet's preview aliases.
func (r *InstanceReconciler) reconcileArchivedPreviewServices(ctx context.Context, instance *v1alpha1.OpenSearchServerless, i int) (ctrl.Result, bool, error) {
	retired := instance.Status.RetiredCandidateFleets[i]
	for _, role := range []string{"search", "ingest"} {
		desired, err := workload.PreviewService(instance, role, retired.Fleet)
		if err != nil {
			return ctrl.Result{}, true, err
		}
		services := r.Core.Services(instance.Namespace)
		service, err := services.Get(ctx, desired.Name, metav1.GetOptions{})
		if apierrors.IsNotFound(err) {
			continue
		}
		if err != nil {
			return ctrl.Result{}, true, err
		}
		pinned := retired.SearchServiceUID
		if role == "ingest" {
			pinned = retired.IngestServiceUID
		}
		if !matchesFleetService(instance, service, desired, types.UID(pinned)) {
			return ctrl.Result{RequeueAfter: time.Minute}, true, fmt.Errorf("retired preview Service conflicts with recorded identity")
		}
		if pinned == "" {
			before := instance.DeepCopyObject().(*v1alpha1.OpenSearchServerless)
			if role == "search" {
				instance.Status.RetiredCandidateFleets[i].SearchServiceUID = string(service.UID)
			} else {
				instance.Status.RetiredCandidateFleets[i].IngestServiceUID = string(service.UID)
			}
			if err := r.Status().Patch(ctx, instance, client.MergeFromWithOptions(before, client.MergeFromWithOptimisticLock{})); err != nil {
				return ctrl.Result{}, true, err
			}
			return ctrl.Result{Requeue: true}, true, nil
		}
		uid, version := service.UID, service.ResourceVersion
		if err := services.Delete(ctx, service.Name, metav1.DeleteOptions{Preconditions: &metav1.Preconditions{UID: &uid, ResourceVersion: &version}}); err != nil {
			return ctrl.Result{}, true, err
		}
		return ctrl.Result{Requeue: true}, true, nil
	}
	return ctrl.Result{}, false, nil
}
