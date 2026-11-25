package controller

import (
	"context"
	"maps"
	"slices"

	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"github.com/huyz0/opensearch-serverless-operator/api/v1alpha1"
	"github.com/huyz0/opensearch-serverless-operator/internal/rollout"
)

const configMapReferenceIndex = ".spec.watchedConfigMaps"
const retiredPodNameIndex = ".status.retiredPodCreations.name"

func (r *InstanceReconciler) watchedConfigMapNames(object client.Object) []string {
	instance, ok := object.(*v1alpha1.OpenSearchServerless)
	if !ok {
		return nil
	}
	names := make(map[string]struct{})
	add := func(name string) {
		if name != "" {
			names[name] = struct{}{}
		}
	}
	if repo := instance.Spec.Storage.RepositoryRef; repo != nil {
		add(repo.Name)
	}
	if availability := instance.Spec.Availability; availability != nil && availability.EmergencyCapacity != nil {
		add(availability.EmergencyCapacity.EvidenceRef.Name)
	}
	if report := instance.Status.EmergencyFailbackEvidence; report != nil {
		add(report.Name)
	}
	for _, pool := range instance.Spec.Pools {
		add(pool.CapacityProfileRef)
	}
	if commit := instance.Status.LastRolloutCommit; commit != nil && commit.UID != "" {
		add(rollout.FleetRetirementEvidenceName(types.UID(commit.UID)))
	}
	if len(instance.Spec.Plugins) != 0 || len(instance.Status.PluginSetsByFleet) != 0 {
		add(r.DevelopmentPluginPolicy)
	}
	if rollout := instance.Spec.Rollout; rollout != nil && rollout.Candidate != nil && rollout.Candidate.PluginOverride != nil && len(rollout.Candidate.PluginOverride.Plugins) != 0 {
		add(r.DevelopmentPluginPolicy)
	}
	addPluginInputs := func(input *v1alpha1.PluginInstallIntent) {
		if input != nil {
			add(input.PolicySourceName)
			add(input.RequestName)
			add(input.PolicyName)
		}
	}
	for _, fleet := range instance.Status.PluginSetsByFleet {
		for _, input := range fleet.InputsByRole {
			addPluginInputs(&input)
		}
	}
	if intent := instance.Status.PendingPodCreation; intent != nil {
		addPluginInputs(intent.PluginInstall)
	}
	if intent := instance.Status.PendingGateRelease; intent != nil {
		addPluginInputs(intent.Pod.PluginInstall)
	}
	return slices.Sorted(maps.Keys(names))
}

func (r *InstanceReconciler) instancesForConfigMap(ctx context.Context, object client.Object) []reconcile.Request {
	return r.instancesForIndexedReference(ctx, object, configMapReferenceIndex)
}

func (r *InstanceReconciler) instancesForIndexedReference(ctx context.Context, object client.Object, index string) []reconcile.Request {
	if object.GetNamespace() == "" || object.GetName() == "" {
		return nil
	}
	var instances v1alpha1.OpenSearchServerlessList
	if err := r.List(ctx, &instances, client.InNamespace(object.GetNamespace()), client.MatchingFields{index: object.GetName()}); err != nil {
		ctrl.LoggerFrom(ctx).Error(err, "list instances for watched reference", "index", index, "namespace", object.GetNamespace(), "name", object.GetName())
		return nil
	}
	requests := make([]reconcile.Request, 0, len(instances.Items))
	for _, instance := range instances.Items {
		requests = append(requests, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(&instance)})
	}
	return requests
}

func retiredPodNames(object client.Object) []string {
	instance, ok := object.(*v1alpha1.OpenSearchServerless)
	if !ok {
		return nil
	}
	names := make([]string, 0, len(instance.Status.RetiredPodCreations))
	for _, item := range instance.Status.RetiredPodCreations {
		if item.Name != "" {
			names = append(names, item.Name)
		}
	}
	return names
}

func (r *InstanceReconciler) instancesForRetiredPod(ctx context.Context, object client.Object) []reconcile.Request {
	return r.instancesForIndexedReference(ctx, object, retiredPodNameIndex)
}

// SetupWithManager registers the instance watch.
func (r *InstanceReconciler) SetupWithManager(manager ctrl.Manager) error {
	if err := manager.GetFieldIndexer().IndexField(context.Background(), &v1alpha1.OpenSearchServerless{}, configMapReferenceIndex, r.watchedConfigMapNames); err != nil {
		return err
	}
	builder := ctrl.NewControllerManagedBy(manager).
		For(&v1alpha1.OpenSearchServerless{}).
		Owns(&corev1.Service{}).
		Watches(&corev1.ConfigMap{}, handler.EnqueueRequestsFromMapFunc(r.instancesForConfigMap))
	if r.DevelopmentGatedPods {
		if err := manager.GetFieldIndexer().IndexField(context.Background(), &v1alpha1.OpenSearchServerless{}, retiredPodNameIndex, retiredPodNames); err != nil {
			return err
		}
		builder = builder.Owns(&corev1.Pod{}).Watches(&corev1.Pod{}, handler.EnqueueRequestsFromMapFunc(r.instancesForRetiredPod)).Owns(&policyv1.PodDisruptionBudget{})
	}
	return builder.Complete(r)
}
