package controller

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"

	"github.com/huyz0/opensearch-serverless-operator/api/v1alpha1"
	"github.com/huyz0/opensearch-serverless-operator/internal/runtimeconfig"
	"github.com/huyz0/opensearch-serverless-operator/internal/storage"
	"github.com/huyz0/opensearch-serverless-operator/internal/workload"
)

// applyFleetRuntime replaces only the startup fields. Capacity, stores and
// managed Java settings still come from the instance and its typed inputs.
func applyFleetRuntime(instance *v1alpha1.OpenSearchServerless, config v1alpha1.FleetRuntime) {
	instance.Spec.Runtime = config.Common
	instance.Spec.Pools = maps.Clone(instance.Spec.Pools)
	for role, pool := range instance.Spec.Pools {
		pool.Runtime = nil
		if override, found := config.ByRole[role]; found {
			pool.Runtime = &override
		}
		instance.Spec.Pools[role] = pool
	}
}

func configuredFleetRuntime(instance *v1alpha1.OpenSearchServerless) v1alpha1.FleetRuntime {
	config := v1alpha1.FleetRuntime{Common: instance.Spec.Runtime}
	for role, pool := range instance.Spec.Pools {
		if pool.Runtime != nil {
			if config.ByRole == nil {
				config.ByRole = make(map[string]v1alpha1.RuntimeConfig, len(instance.Spec.Pools))
			}
			config.ByRole[role] = *pool.Runtime
		}
	}
	return config
}

func startupCatalog(image, qualifiedImage, role string) runtimeconfig.Catalog {
	if image != "" && image == qualifiedImage && qualifiedPluginImage.MatchString(image) {
		return runtimeconfig.DevelopmentCatalog(role)
	}
	return runtimeconfig.Catalog{}
}

func checkRuntimeArguments(instance *v1alpha1.OpenSearchServerless, role string, containers []corev1.Container, args []string) error {
	for _, container := range containers {
		if container.Name == "opensearch" && container.Image == instance.Spec.Image && len(container.EnvFrom) == 0 &&
			!slices.ContainsFunc(container.Env, func(env corev1.EnvVar) bool {
				return env.Name == "JDK_JAVA_OPTIONS" || env.Name == "JAVA_TOOL_OPTIONS" || env.Name == "_JAVA_OPTIONS"
			}) &&
			runtimeconfig.MatchesPodRuntime(container.Args, args) &&
			(instance.Status.PrimaryStore == nil || runtimeconfig.MatchesPodPrimaryStore(container.Args, storage.S3(*instance.Status.PrimaryStore))) {
			return nil
		}
	}
	return fmt.Errorf("%s Pod startup arguments or Java image differ from the recorded runtime", role)
}

func checkFleetRuntimePod(instance *v1alpha1.OpenSearchServerless, fleet, role string, pod *corev1.Pod) error {
	config, pinned := instance.Status.RuntimeByFleet[fleet]
	if !pinned {
		return nil
	}
	args, err := runtimeArgs(config, role, runtimeconfig.DevelopmentCatalog(role))
	if err != nil {
		return err
	}
	return checkRuntimeArguments(instance, role, pod.Spec.Containers, args)
}

func runtimeArgs(config v1alpha1.FleetRuntime, role string, catalog runtimeconfig.Catalog) ([]string, error) {
	var roleConfig *v1alpha1.RuntimeConfig
	if value, found := config.ByRole[role]; found {
		roleConfig = &value
	}
	return runtimeconfig.Resolve(config.Common, roleConfig, catalog)
}

func (r *InstanceReconciler) previewFleetRuntime(ctx context.Context, instance *v1alpha1.OpenSearchServerless, candidate *v1alpha1.FleetCandidate) (map[string]v1alpha1.FleetRuntime, error) {
	source := instance.Status.ServingFleetIntentByRole["search"]
	ingest := instance.Status.ServingFleetIntentByRole["ingest"]
	current, pinned := instance.Status.RuntimeByFleet[source]
	_, ingestPinned := instance.Status.RuntimeByFleet[ingest]
	if !pinned {
		current = configuredFleetRuntime(instance)
	}
	configured := current.Common != nil || len(current.ByRole) != 0
	qualified := instance.Spec.Image == r.DevelopmentRuntimeSettingsImage && qualifiedPluginImage.MatchString(instance.Spec.Image)
	if !pinned && !ingestPinned && candidate.RuntimeOverride == nil && !configured && !qualified {
		return nil, nil
	}
	if r.Core == nil || source == "" || source != ingest || candidate.Image != instance.Spec.Image ||
		!qualified {
		return nil, fmt.Errorf("preview runtime settings require both serving roles on the same fleet and the qualified same image")
	}
	if candidate.RuntimeOverride != nil {
		window, err := time.ParseDuration(instance.Spec.Rollout.RollbackWindow)
		if err != nil || window <= 0 {
			return nil, fmt.Errorf("preview runtime settings require a positive rollback window")
		}
	}
	needed := 1
	if !pinned {
		needed++
	}
	if len(instance.Status.RuntimeByFleet)+needed > 8 {
		return nil, fmt.Errorf("runtime fleet history is full")
	}
	next := current
	if candidate.RuntimeOverride != nil {
		next = *candidate.RuntimeOverride
	}
	for _, role := range []string{"search", "ingest"} {
		catalog := startupCatalog(candidate.Image, r.DevelopmentRuntimeSettingsImage, role)
		if _, err := runtimeArgs(next, role, catalog); err != nil {
			return nil, fmt.Errorf("candidate %s runtime settings: %w", role, err)
		}
		args, err := runtimeArgs(current, role, catalog)
		if err != nil {
			return nil, fmt.Errorf("serving %s runtime settings: %w", role, err)
		}
		if err := r.witnessServingRuntime(ctx, instance, source, role, args); err != nil {
			return nil, err
		}
	}
	if !pinned && candidate.RuntimeOverride == nil && !configured {
		return nil, nil
	}
	pins := map[string]v1alpha1.FleetRuntime{candidate.Fleet: next}
	if !pinned {
		pins[source] = current
	}
	return pins, nil
}

func (r *InstanceReconciler) witnessServingRuntime(ctx context.Context, instance *v1alpha1.OpenSearchServerless, fleet, role string, args []string) error {
	selector := labels.SelectorFromSet(labels.Set{workload.InstanceLabel: string(instance.UID), workload.FleetLabel: fleet, workload.RoleLabel: role})
	pods, err := r.Core.Pods(instance.Namespace).List(ctx, metav1.ListOptions{LabelSelector: selector.String(), Limit: 513})
	if err != nil || pods.Continue != "" || len(pods.Items) > 512 {
		return fmt.Errorf("cannot observe the serving %s startup arguments", role)
	}
	ready := false
	for i := range pods.Items {
		pod := &pods.Items[i]
		if pod.DeletionTimestamp != nil || terminalCapacityPod(pod) {
			continue
		}
		owner := metav1.GetControllerOf(pod)
		if pod.UID == "" || owner == nil || owner.UID != instance.UID || owner.Name != instance.Name ||
			owner.Kind != "OpenSearchServerless" || owner.APIVersion != v1alpha1.GroupVersion.String() {
			return fmt.Errorf("serving %s Pod has a conflicting owner", role)
		}
		if err := checkRuntimeArguments(instance, role, pod.Spec.Containers, args); err != nil {
			return err
		}
		ready = ready || readyForPreview(pod)
	}
	if !ready {
		return fmt.Errorf("no Ready serving %s Pod confirms the startup settings", role)
	}
	return nil
}
