package controller

import (
	"context"
	"fmt"
	"reflect"
	"slices"
	"time"

	corev1 "k8s.io/api/core/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"

	"github.com/huyz0/opensearch-serverless-operator/api/v1alpha1"
	"github.com/huyz0/opensearch-serverless-operator/internal/plugins"
	"github.com/huyz0/opensearch-serverless-operator/internal/workload"
)

func (r *InstanceReconciler) pluginsForFleetRole(ctx context.Context, instance *v1alpha1.OpenSearchServerless, fleet, role string) (*pluginInputs, error) {
	set, pinned := instance.Status.PluginSetsByFleet[fleet]
	if !pinned {
		if candidate := instance.Status.CandidateFleet; candidate != nil && candidate.Fleet == fleet && candidate.PluginOverride != nil {
			return nil, fmt.Errorf("candidate plugin set pin is missing")
		}
		return r.pluginsForRole(ctx, instance, role)
	}
	expected, found := set.InputsByRole[role]
	if !found {
		for _, plugin := range set.Plugins {
			if len(plugin.Roles) == 0 || slices.Contains(plugin.Roles, role) {
				return nil, fmt.Errorf("%s plugin inputs for fleet %q are missing", role, fleet)
			}
		}
		return nil, nil
	}
	selected := *instance
	selected.Spec.Plugins = set.Plugins
	inputs, err := r.resolvePluginInputs(ctx, &selected, role, expected.InstallerImage, r.DevelopmentPluginRuntimeImage, expected.PolicySourceName)
	if err != nil {
		return nil, err
	}
	intent := pluginInstallIntent(instance, inputs)
	if intent == nil || *intent != expected {
		return nil, fmt.Errorf("%s plugin inputs for fleet %q differ from the recorded set", role, fleet)
	}
	return inputs, nil
}

func (r *InstanceReconciler) observeServingPluginInputs(ctx context.Context, instance *v1alpha1.OpenSearchServerless, fleet, role string, approved *pluginInputs) (*v1alpha1.PluginInstallIntent, error) {
	selector := labels.SelectorFromSet(labels.Set{workload.InstanceLabel: string(instance.UID), workload.FleetLabel: fleet, workload.RoleLabel: role})
	pods, err := r.Core.Pods(instance.Namespace).List(ctx, metav1.ListOptions{LabelSelector: selector.String(), Limit: 513})
	if err != nil || pods.Continue != "" || len(pods.Items) > 512 {
		return nil, fmt.Errorf("cannot observe the serving %s plugin set", role)
	}
	var expected *v1alpha1.PluginInstallIntent
	ready := false
	for i := range pods.Items {
		pod := &pods.Items[i]
		if pod.DeletionTimestamp != nil || terminalCapacityPod(pod) {
			continue
		}
		owner := metav1.GetControllerOf(pod)
		if pod.UID == "" || owner == nil || owner.UID != instance.UID || owner.Name != instance.Name ||
			owner.Kind != "OpenSearchServerless" || owner.APIVersion != v1alpha1.GroupVersion.String() {
			return nil, fmt.Errorf("serving %s Pod has a conflicting owner", role)
		}
		if !slices.ContainsFunc(pod.Spec.Containers, func(container corev1.Container) bool {
			return container.Name == "opensearch" && container.Image == instance.Spec.Image
		}) {
			return nil, fmt.Errorf("serving %s Pod uses a different Java image", role)
		}
		actual, err := pluginIntentFromPod(pod)
		if err != nil || (actual == nil) != (approved == nil) {
			return nil, fmt.Errorf("serving %s plugin set differs from the configured source", role)
		}
		if actual != nil {
			if actual.RequestUID == "" || actual.PolicyUID == "" || actual.RequestSHA256 != approved.Resolution.RequestSHA256 {
				return nil, fmt.Errorf("serving %s plugin set differs from the configured source", role)
			}
			if expected == nil {
				installed, err := r.resolvePluginInputs(ctx, instance, role, actual.InstallerImage, r.DevelopmentPluginRuntimeImage, actual.PolicySourceName)
				if err != nil {
					return nil, err
				}
				expected = pluginInstallIntent(instance, installed)
			}
			if err := checkRecordedPluginIntent(instance, pod.Name, actual, expected); err != nil {
				return nil, err
			}
		}
		if err := validateRecordedPluginPod(pod, actual); err != nil {
			return nil, fmt.Errorf("serving %s Pod plugin installation differs: %w", role, err)
		}
		ready = ready || readyForPreview(pod)
	}
	if !ready {
		return nil, fmt.Errorf("no Ready serving %s Pod confirms the source plugin set", role)
	}
	return expected, nil
}

func candidateInputsMatch(state *v1alpha1.CandidateFleetStatus, desired *v1alpha1.FleetCandidate) bool {
	if !candidateTLSInputsMatch(state, desired) || !apiequality.Semantic.DeepEqual(state.RuntimeOverride, desired.RuntimeOverride) ||
		(state.PluginOverride == nil) != (desired.PluginOverride == nil) {
		return false
	}
	return state.PluginOverride == nil || slices.EqualFunc(state.PluginOverride.Plugins, desired.PluginOverride.Plugins,
		func(a, b v1alpha1.Plugin) bool {
			return a.Name == b.Name && a.Artifact == b.Artifact &&
				slices.Equal(a.Roles, b.Roles) && reflect.DeepEqual(a.RegistryCredentialsRef, b.RegistryCredentialsRef)
		})
}

func checkFleetPluginPod(instance *v1alpha1.OpenSearchServerless, fleet, role string, pod *corev1.Pod) error {
	set, pinned := instance.Status.PluginSetsByFleet[fleet]
	if !pinned {
		return nil
	}
	actual, err := pluginIntentFromPod(pod)
	if err != nil {
		return err
	}
	var expected *v1alpha1.PluginInstallIntent
	if input, found := set.InputsByRole[role]; found {
		expected = &input
	}
	if err := checkRecordedPluginIntent(instance, pod.Name, actual, expected); err != nil {
		return err
	}
	if actual != nil && (actual.RequestUID == "" || actual.PolicyUID == "") {
		return fmt.Errorf("Pod lacks its pinned plugin snapshot UIDs")
	}
	return validateRecordedPluginPod(pod, actual)
}

func (r *InstanceReconciler) checkObservedPodPlugins(ctx context.Context, instance *v1alpha1.OpenSearchServerless, pod *corev1.Pod, fleet, role string) error {
	if _, pinned := instance.Status.PluginSetsByFleet[fleet]; pinned {
		// A Pod's installed files are local. Validate its fleet assignment
		// without making another fleet's recovery depend on this registry input.
		return checkFleetPluginPod(instance, fleet, role, pod)
	}
	inputs, err := r.pluginsForFleetRole(ctx, instance, fleet, role)
	if err != nil {
		return err
	}
	intent, err := pluginIntentFromPod(pod)
	if err != nil {
		return err
	}
	if err := checkPluginIntent(instance, pod.Name, intent, inputs); err != nil || intent == nil {
		return err
	}
	if intent.RequestUID == "" || intent.PolicyUID == "" {
		return fmt.Errorf("existing Pod lacks pinned plugin snapshot identities")
	}
	_, _, err = r.ensurePluginSnapshots(ctx, instance, intent, inputs)
	return err
}

// previewPluginFleet requires approval of the exact search and ingest sets
// before a changed set can start a JVM on the shared store and peer network.
func (r *InstanceReconciler) previewPluginFleet(ctx context.Context, instance *v1alpha1.OpenSearchServerless, candidate *v1alpha1.FleetCandidate) (map[string]v1alpha1.FleetPluginSet, error) {
	search := instance.Status.ServingFleetIntentByRole["search"]
	ingest := instance.Status.ServingFleetIntentByRole["ingest"]
	inherited, searchPinned := instance.Status.PluginSetsByFleet[search]
	_, ingestPinned := instance.Status.PluginSetsByFleet[ingest]
	if candidate.PluginOverride == nil && !searchPinned && !ingestPinned {
		return nil, nil
	}
	if search == "" || search != ingest || candidate.Image != instance.Spec.Image {
		return nil, fmt.Errorf("preview plugin changes require both serving roles on the same fleet and a same-image candidate")
	}
	window, err := time.ParseDuration(instance.Spec.Rollout.RollbackWindow)
	if err != nil || window <= 0 {
		return nil, fmt.Errorf("preview plugin sets require a positive rollback window")
	}
	needed := 1
	if !searchPinned {
		needed++
	}
	if len(instance.Status.PluginSetsByFleet)+needed > 8 {
		return nil, fmt.Errorf("plugin fleet history is full")
	}
	selected := *instance
	oldView := *instance
	if searchPinned {
		oldView.Spec.Plugins = inherited.Plugins
	}
	if candidate.PluginOverride != nil {
		selected.Spec.Plugins = candidate.PluginOverride.Plugins
	} else {
		selected.Spec.Plugins = inherited.Plugins
	}
	set := v1alpha1.FleetPluginSet{
		PluginSet:    v1alpha1.PluginSet{Plugins: selected.Spec.Plugins},
		InputsByRole: make(map[string]v1alpha1.PluginInstallIntent, 2),
	}
	source := make(map[string]v1alpha1.PluginInstallIntent, 2)
	var policy *pluginInputs
	for _, role := range []string{"ingest", "search"} {
		old, err := r.pluginsForRole(ctx, &oldView, role)
		if err != nil {
			return nil, err
		}
		next, err := r.pluginsForRole(ctx, &selected, role)
		if err != nil {
			return nil, err
		}
		for _, input := range []*pluginInputs{old, next} {
			if input == nil {
				continue
			}
			if policy != nil && (policy.PolicyUID != input.PolicyUID || policy.PolicyVersion != input.PolicyVersion ||
				policy.Resolution.PolicySHA256 != input.Resolution.PolicySHA256) {
				return nil, fmt.Errorf("plugin policy changed while qualifying the fleets")
			}
			policy = input
		}
		witnessed, err := r.observeServingPluginInputs(ctx, &oldView, search, role, old)
		if err != nil {
			return nil, err
		}
		if searchPinned {
			recorded, found := inherited.InputsByRole[role]
			if (witnessed != nil) != found || (found && *witnessed != recorded) {
				return nil, fmt.Errorf("serving %s plugin inputs differ from the fleet pin", role)
			}
		}
		if witnessed != nil {
			source[role] = *witnessed
		}
		if next != nil {
			set.InputsByRole[role] = *pluginInstallIntent(instance, next)
		}
	}
	if policy != nil {
		old := plugins.FleetDigest(source["ingest"].RequestSHA256, source["search"].RequestSHA256)
		next := plugins.FleetDigest(set.InputsByRole["ingest"].RequestSHA256, set.InputsByRole["search"].RequestSHA256)
		if err := plugins.QualifyFleetTransition(policy.PolicyJSON, old, next); err != nil {
			return nil, err
		}
	}
	pins := map[string]v1alpha1.FleetPluginSet{candidate.Fleet: set}
	if !searchPinned {
		if len(instance.Spec.Plugins) > 32 {
			return nil, fmt.Errorf("the serving fleet exceeds the 32-plugin fleet limit")
		}
		pins[search] = v1alpha1.FleetPluginSet{
			PluginSet:    v1alpha1.PluginSet{Plugins: instance.Spec.Plugins},
			InputsByRole: source,
		}
	}
	return pins, nil
}
