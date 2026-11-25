package controller

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"maps"
	"slices"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/selection"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/huyz0/opensearch-serverless-operator/api/v1alpha1"
	"github.com/huyz0/opensearch-serverless-operator/internal/workload"
)

const podRevisionLabel = "serverless.opensearch.org/revision"

type templateRevisions struct {
	current, previous, legacy string
	withAvailability          string
	aliases                   map[string]string
	byFleet                   map[string]templateRevisions
}

func hashPodTemplate(spec v1alpha1.OpenSearchServerlessSpec, domain string) (string, error) {
	encoded, err := json.Marshal(spec)
	if err != nil {
		return "", err
	}
	if domain != "" {
		encoded = append(encoded, []byte("\npeer-dns-preflight=v1\npeer-dns-domain="+domain)...)
	}
	encoded = append(encoded, []byte("\npod-template="+developmentPodTemplateVersion)...)
	return fmt.Sprintf("%x", sha256.Sum256(encoded))[:16], nil
}

func podRevision(instance *v1alpha1.OpenSearchServerless, domain string) (string, error) {
	spec := removeCapacityCounts(instance.Spec)
	spec.Availability = nil
	return hashPodTemplate(spec, domain)
}

func removeCapacityCounts(spec v1alpha1.OpenSearchServerlessSpec) v1alpha1.OpenSearchServerlessSpec {
	spec.Rollout = nil
	spec.Pools = maps.Clone(spec.Pools)
	for role, pool := range spec.Pools {
		// Keep the zone identities. Capacity counts do not affect a Pod's template.
		pool.Zones = maps.Clone(pool.Zones)
		for zone := range pool.Zones {
			pool.Zones[zone] = v1alpha1.Zone{}
		}
		spec.Pools[role] = pool
	}
	return spec
}

func podRevisions(instance *v1alpha1.OpenSearchServerless, domain string) (templateRevisions, error) {
	base, err := revisionsForTemplate(instance, domain)
	if err != nil {
		return templateRevisions{}, err
	}
	fleets := make(map[string]struct{}, len(instance.Status.PluginSetsByFleet)+len(instance.Status.RuntimeByFleet))
	for fleet := range instance.Status.PluginSetsByFleet {
		fleets[fleet] = struct{}{}
	}
	for fleet := range instance.Status.RuntimeByFleet {
		fleets[fleet] = struct{}{}
	}
	if len(fleets) != 0 {
		base.byFleet = make(map[string]templateRevisions, len(fleets))
		for fleet := range fleets {
			view := *instance
			if set, found := instance.Status.PluginSetsByFleet[fleet]; found {
				view.Spec.Plugins = set.Plugins
			}
			if config, found := instance.Status.RuntimeByFleet[fleet]; found {
				applyFleetRuntime(&view, config)
			}
			revisions, err := revisionsForTemplate(&view, domain)
			if err != nil {
				return templateRevisions{}, err
			}
			base.byFleet[fleet] = revisions
		}
	}
	return base, nil
}

func (revisions templateRevisions) forFleetTemplate(fleet string) templateRevisions {
	if scoped, found := revisions.byFleet[fleet]; found {
		return scoped
	}
	return revisions
}

func revisionsForTemplate(instance *v1alpha1.OpenSearchServerless, domain string) (templateRevisions, error) {
	normalized := removeCapacityCounts(instance.Spec)
	canonical := normalized
	canonical.Availability = nil
	current, err := hashPodTemplate(canonical, domain)
	if err != nil {
		return templateRevisions{}, err
	}
	revisions := templateRevisions{current: current, aliases: instance.Status.TemplateRevisionAliases}
	if normalized.Availability != nil {
		revisions.withAvailability, err = hashPodTemplate(normalized, domain)
		if err != nil {
			return templateRevisions{}, err
		}
	}
	spec := instance.Spec
	spec.Rollout = nil
	revisions.previous, err = hashPodTemplate(spec, domain)
	if err != nil {
		return templateRevisions{}, err
	}
	if policy := instance.Spec.Rollout; policy != nil {
		legacyPolicy := *policy
		legacyPolicy.Candidate = nil
		spec := instance.Spec
		spec.Rollout = &legacyPolicy
		revisions.legacy, err = hashPodTemplate(spec, domain)
	}
	return revisions, err
}

func (revisions templateRevisions) accepts(label string) bool {
	if label == "" {
		return false
	}
	if label == revisions.current || label == revisions.previous || label == revisions.legacy || label == revisions.withAvailability {
		return true
	}
	target, exists := revisions.aliases[label]
	return exists && (target == revisions.current || target == revisions.previous || target != "" && target == revisions.withAvailability)
}

func (revisions templateRevisions) compatibleOrCurrent(label string) string {
	if revisions.accepts(label) {
		return label
	}
	return revisions.current
}

// sameServingTemplate requires a recorded alias before treating distinct labels
// as interchangeable. Compatibility of each label alone is insufficient.
func (revisions templateRevisions) sameServingTemplate(first, second string) bool {
	if first == "" || second == "" {
		return false
	}
	if first == second {
		return revisions.accepts(first)
	}
	return revisions.current != "" &&
		(first == revisions.current || revisions.aliases[first] == revisions.current) &&
		(second == revisions.current || revisions.aliases[second] == revisions.current)
}

// forFleet keeps the literal revision on replacements for an existing role.
// A pinned preview keeps its identity even before its first Pod is admitted.
func (revisions templateRevisions) forFleet(instance *v1alpha1.OpenSearchServerless, pods []corev1.Pod, role, fleet string) (string, error) {
	revisions = revisions.forFleetTemplate(fleet)
	revision := ""
	serving := instance.Status.ServingFleetIntentByRole[role] == fleet
	protected := servingRevisionProtected(instance)
	if pin := instance.Status.ServingRevisionByRole[role]; serving && pin.Fleet == fleet && pin.Revision != "" {
		if revisions.accepts(pin.Revision) {
			revision = pin.Revision
		} else if protected {
			return "", fmt.Errorf("%s fleet %q has a serving revision incompatible with the current template", role, fleet)
		}
	}
	if candidate := instance.Status.CandidateFleet; candidate != nil && candidate.Fleet == fleet && revisions.accepts(candidate.Revision) {
		if revision != "" && revision != candidate.Revision {
			return "", fmt.Errorf("%s fleet %q serving and candidate revision pins conflict", role, fleet)
		}
		revision = candidate.Revision
	}
	pinned := revision != ""
	for _, pod := range pods {
		if pod.Labels[workload.RoleLabel] != role || pod.Labels[workload.FleetLabel] != fleet ||
			pod.DeletionTimestamp != nil || terminalCapacityPod(&pod) {
			continue
		}
		label := pod.Labels[podRevisionLabel]
		if !revisions.accepts(label) {
			if protected {
				return "", fmt.Errorf("%s fleet %q has a Pod with an unverified template revision", role, fleet)
			}
			continue
		}
		if revision != "" && revision != label {
			if !serving || !revisions.sameServingTemplate(revision, label) {
				return "", fmt.Errorf("%s fleet %q has conflicting revision labels", role, fleet)
			}
			if !pinned {
				revision = revisions.current
			}
			continue
		}
		revision = label
	}
	return revisions.compatibleOrCurrent(revision), nil
}

func pinServingRevision(instance *v1alpha1.OpenSearchServerless, role, fleet, revision string) {
	if instance.Status.ServingRevisionByRole == nil {
		instance.Status.ServingRevisionByRole = make(map[string]v1alpha1.ServingRoleRevision, 2)
	}
	instance.Status.ServingRevisionByRole[role] = v1alpha1.ServingRoleRevision{Fleet: fleet, Revision: revision}
}

func servingRevisionMatchesIntent(instance *v1alpha1.OpenSearchServerless, intent *v1alpha1.PodCreationIntent) bool {
	pin := instance.Status.ServingRevisionByRole[intent.Role]
	return !servingRevisionProtected(instance) || instance.Status.ServingFleetIntentByRole[intent.Role] != intent.Fleet || pin.Fleet != intent.Fleet || pin.Revision == intent.Revision
}

func servingRevisionProtected(instance *v1alpha1.OpenSearchServerless) bool {
	return instance.Spec.Rollout != nil || instance.Status.CandidateFleet != nil || instance.Status.RollbackTarget != nil
}

// adoptServingRevisionPins backfills an upgraded instance from owned Pods
// with a common template and at least one ready Pod.
func adoptServingRevisionPins(instance *v1alpha1.OpenSearchServerless, pods []corev1.Pod, revisions templateRevisions) bool {
	changed := false
	for _, role := range []string{"search", "ingest"} {
		fleet := instance.Status.ServingFleetIntentByRole[role]
		scoped := revisions.forFleetTemplate(fleet)
		pin := instance.Status.ServingRevisionByRole[role]
		if fleet == "" || (pin.Fleet == fleet && (scoped.accepts(pin.Revision) || servingRevisionProtected(instance))) {
			continue
		}
		var observed string
		hasReadyPod := false
		for i := range pods {
			pod := &pods[i]
			if pod.Labels[workload.RoleLabel] != role || pod.Labels[workload.FleetLabel] != fleet || pod.DeletionTimestamp != nil || terminalCapacityPod(pod) {
				continue
			}
			owner := metav1.GetControllerOf(pod)
			label := pod.Labels[podRevisionLabel]
			if pod.Namespace != instance.Namespace || pod.UID == "" || pod.ResourceVersion == "" || pod.Labels[workload.InstanceLabel] != string(instance.UID) ||
				owner == nil || owner.UID != instance.UID || owner.Name != instance.Name || owner.Kind != "OpenSearchServerless" || owner.APIVersion != v1alpha1.GroupVersion.String() ||
				!scoped.accepts(label) || (observed != "" && !scoped.sameServingTemplate(observed, label)) {
				observed = ""
				break
			}
			if observed == "" {
				observed = label
			} else if observed != label {
				observed = scoped.current
			}
			hasReadyPod = hasReadyPod || (pod.Spec.NodeName != "" && !hasPlacementGate(pod) && podStartupReady(pod))
		}
		if observed != "" && hasReadyPod {
			pinServingRevision(instance, role, fleet, observed)
			changed = true
		}
	}
	return changed
}

// recordLegacyTemplateRevision preserves reproducible old revision identifiers
// before capacity or rollout policy changes make the old hash unreproducible.
func (r *InstanceReconciler) recordLegacyTemplateRevision(ctx context.Context, instance *v1alpha1.OpenSearchServerless) (bool, error) {
	if instance.DeletionTimestamp != nil || instance.UID == "" {
		return false, nil
	}
	domain, err := r.peerDNSDomain(instance)
	if err != nil {
		return false, nil // Normal reconciliation reports the unsupported DNS input.
	}
	revisions, err := podRevisions(instance, domain)
	if err != nil {
		return false, err
	}
	aliases := maps.Clone(instance.Status.TemplateRevisionAliases)
	if aliases == nil {
		aliases = make(map[string]string)
	}
	views := []templateRevisions{revisions}
	for _, scoped := range revisions.byFleet {
		views = append(views, scoped)
	}
	for _, view := range views {
		for label, target := range aliases {
			if target == view.previous || target != "" && target == view.withAvailability {
				aliases[label] = view.current
			}
		}
		var missing []string
		for _, label := range []string{view.previous, view.legacy, view.withAvailability} {
			if label != "" && label != view.current && aliases[label] != view.current && !slices.Contains(missing, label) {
				missing = append(missing, label)
			}
		}
		if len(missing) > 0 {
			if err := r.collectTemplateAliases(ctx, instance, view.current, missing, aliases); err != nil {
				return false, err
			}
		}
	}
	if maps.Equal(aliases, instance.Status.TemplateRevisionAliases) {
		return false, nil
	}
	if len(aliases) > 16 {
		return false, fmt.Errorf("too many recorded legacy template revisions")
	}
	before := instance.DeepCopyObject().(*v1alpha1.OpenSearchServerless)
	instance.Status.TemplateRevisionAliases = aliases
	if err := r.Status().Patch(ctx, instance, client.MergeFromWithOptions(before, client.MergeFromWithOptimisticLock{})); err != nil {
		return false, err
	}
	return true, nil
}

func (r *InstanceReconciler) collectTemplateAliases(ctx context.Context, instance *v1alpha1.OpenSearchServerless, current string, missing []string, aliases map[string]string) error {
	record := func(label string) error {
		if !slices.Contains(missing, label) {
			return nil
		}
		if previous, exists := aliases[label]; exists && previous != current {
			return fmt.Errorf("legacy template alias conflicts with the current template")
		}
		aliases[label] = current
		return nil
	}
	if candidate := instance.Status.CandidateFleet; candidate != nil && candidate.Image == instance.Spec.Image {
		if err := record(candidate.Revision); err != nil {
			return err
		}
	}
	if target := instance.Status.RollbackTarget; target != nil {
		for _, role := range target.Roles {
			if err := record(role.Revision); err != nil {
				return err
			}
		}
	}
	for role, serving := range instance.Status.ServingRevisionByRole {
		if serving.Fleet == instance.Status.ServingFleetIntentByRole[role] {
			if err := record(serving.Revision); err != nil {
				return err
			}
		}
	}
	if pending := instance.Status.PendingPodCreation; pending != nil && pending.Generation == instance.Generation {
		if err := record(pending.Revision); err != nil {
			return err
		}
	}
	if release := instance.Status.PendingGateRelease; release != nil && release.Pod.Generation == instance.Generation {
		if err := record(release.Pod.Revision); err != nil {
			return err
		}
	}
	requirement, err := labels.NewRequirement(podRevisionLabel, selection.In, missing)
	if err != nil {
		return err
	}
	selector := labels.SelectorFromSet(labels.Set{workload.InstanceLabel: string(instance.UID)}).Add(*requirement)
	pods, err := r.Core.Pods(instance.Namespace).List(ctx, metav1.ListOptions{LabelSelector: selector.String(), Limit: 513})
	if err != nil {
		return err
	}
	if pods.Continue != "" || len(pods.Items) > 512 {
		return fmt.Errorf("legacy Pod observation is incomplete")
	}
	for _, pod := range pods.Items {
		owner := metav1.GetControllerOf(&pod)
		if pod.UID != "" && owner != nil && owner.UID == instance.UID && owner.Name == instance.Name &&
			owner.Kind == "OpenSearchServerless" && owner.APIVersion == v1alpha1.GroupVersion.String() {
			if err := record(pod.Labels[podRevisionLabel]); err != nil {
				return err
			}
		}
	}
	return nil
}
