package controller

import (
	"context"
	"fmt"
	"sort"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/huyz0/opensearch-serverless-operator/api/v1alpha1"
	"github.com/huyz0/opensearch-serverless-operator/internal/workload"
)

// candidateRoleObservation is short-lived process-local evidence, not proof
// of actual AZ placement, cache residency, peer compatibility or client routes.
type candidateRoleObservation struct {
	InstanceUID, Fleet, Revision string
	Generation                   int64
	Pods                         []searchPodQualification
	PodSnapshot                  []corev1.Pod
	ObservedAt                   time.Time
}

// sampleCandidateIngest is historical diagnostics only. It must not authorize
// promotion or be used as a substitute for a fresh complete qualification.
func (r *InstanceReconciler) sampleCandidateIngest(ctx context.Context, instance *v1alpha1.OpenSearchServerless) (bool, error) {
	if instance.Status.CandidateFleet == nil || !r.fleetQualification().supportsDevelopmentIngestProbe(instance) {
		return false, nil
	}
	now := r.now()
	state := instance.Status.CandidateFleet
	if last := instance.Status.LastCandidateIngestObservation; last != nil && last.ObservedGeneration == instance.Generation && last.Fleet == state.Fleet && last.Revision == state.Revision && !now.Before(last.ObservedAt.Time) && now.Sub(last.ObservedAt.Time) < time.Minute {
		return false, nil
	}
	status := &v1alpha1.CandidateIngestObservationStatus{ObservedGeneration: instance.Generation, Fleet: state.Fleet, Revision: state.Revision, Result: "Unavailable", ObservedAt: metav1.NewTime(now)}
	observation, err := r.observeCandidateIngestLease(ctx, instance, 30*time.Second)
	if err == nil {
		status.Result = "LeaseObserved"
		status.QualifiedPods = int32(len(observation.Pods))
		if r.candidatePreviewEndpointsObserved(ctx, instance, "ingest", observation) {
			status.Result = "LeaseAndPreviewEndpointsObserved"
		}
	}
	before := instance.DeepCopyObject().(*v1alpha1.OpenSearchServerless)
	instance.Status.LastCandidateIngestObservation = status
	if err := r.Status().Patch(ctx, instance, client.MergeFromWithOptions(before, client.MergeFromWithOptimisticLock{})); err != nil {
		return false, err
	}
	return true, nil
}

func (r *InstanceReconciler) candidatePreviewEndpointsObserved(ctx context.Context, instance *v1alpha1.OpenSearchServerless, role string, observation candidateRoleObservation) bool {
	if !r.DevelopmentObserveEndpoints || r.Discovery == nil || instance.Status.CandidateFleet == nil {
		return false
	}
	state := instance.Status.CandidateFleet
	serviceUID := state.IngestServiceUID
	if role == "search" {
		serviceUID = state.SearchServiceUID
	}
	desired, err := workload.PreviewService(instance, role, state.Fleet)
	if err != nil {
		return false
	}
	minima := map[string]int{}
	for _, policy := range developmentCandidatePolicies(instance) {
		if policy.Group.Role == role {
			minima[policy.Group.Zone] = policy.Desired
		}
	}
	allowed := make(map[types.UID]struct{}, len(observation.Pods))
	for _, pod := range observation.Pods {
		allowed[types.UID(pod.PodUID)] = struct{}{}
	}
	endpointCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	endpoints, err := observeServiceEndpoints(endpointCtx, r.Core, r.Discovery, instance, role, state.Fleet, desired, types.UID(serviceUID), minima, allowed)
	return err == nil && len(endpoints.ReadyPodVersions) == len(allowed)
}

// sampleCandidateSearch is historical diagnostics, not promotion evidence.
func (r *InstanceReconciler) sampleCandidateSearch(ctx context.Context, instance *v1alpha1.OpenSearchServerless) (bool, error) {
	pool, exists := instance.Spec.Pools["search"]
	if !exists || !pool.WarmOnJoin || pool.WarmReadiness != "Strict" || instance.Status.CandidateFleet == nil || !r.fleetQualification().supportsDevelopmentSearchProbe(instance) {
		return false, nil
	}
	now := r.now()
	state := instance.Status.CandidateFleet
	if last := instance.Status.LastCandidateSearchObservation; last != nil && last.ObservedGeneration == instance.Generation && last.Fleet == state.Fleet && last.Revision == state.Revision && !now.Before(last.ObservedAt.Time) && now.Sub(last.ObservedAt.Time) < time.Minute {
		return false, nil
	}
	status := &v1alpha1.CandidateSearchObservationStatus{ObservedGeneration: instance.Generation, Fleet: state.Fleet, Revision: state.Revision, Result: "Unavailable", ObservedAt: metav1.NewTime(now)}
	observation, err := r.observeCandidateSearchWarmth(ctx, instance, 30*time.Second)
	if err == nil {
		status.Result, status.QualifiedPods = "WarmthObserved", int32(len(observation.Pods))
		if r.candidatePreviewEndpointsObserved(ctx, instance, "search", observation) {
			status.Result = "WarmthAndPreviewEndpointsObserved"
		}
	}
	before := instance.DeepCopyObject().(*v1alpha1.OpenSearchServerless)
	instance.Status.LastCandidateSearchObservation = status
	if err := r.Status().Patch(ctx, instance, client.MergeFromWithOptions(before, client.MergeFromWithOptimisticLock{})); err != nil {
		return false, err
	}
	return true, nil
}

// observeCandidateSearchWarmth qualifies every allocated candidate role Pod
// and re-lists the entire search set after probing. The caller must re-read the
// instance and all other eligibility evidence before using this observation.
func (r *InstanceReconciler) observeCandidateSearchWarmth(ctx context.Context, instance *v1alpha1.OpenSearchServerless, maxAge time.Duration) (candidateRoleObservation, error) {
	if instance == nil || !instance.Spec.Pools["search"].WarmOnJoin || instance.Spec.Pools["search"].WarmReadiness != "Strict" {
		return candidateRoleObservation{}, fmt.Errorf("strict candidate search is required")
	}
	return r.observeCandidateRole(ctx, instance, "search", maxAge, r.fleetQualification().qualifyDevelopmentSearchPod)
}

// observeCandidateIngestLease does not establish write success or takeover.
func (r *InstanceReconciler) observeCandidateIngestLease(ctx context.Context, instance *v1alpha1.OpenSearchServerless, maxAge time.Duration) (candidateRoleObservation, error) {
	return r.observeCandidateRole(ctx, instance, "ingest", maxAge, r.fleetQualification().qualifyDevelopmentIngestPod)
}

func (r *InstanceReconciler) observeCandidateRole(ctx context.Context, instance *v1alpha1.OpenSearchServerless, role string, maxAge time.Duration, qualify func(context.Context, *v1alpha1.OpenSearchServerless, string, string, string, string, time.Duration) (searchPodQualification, error)) (candidateRoleObservation, error) {
	var out candidateRoleObservation
	if instance == nil || instance.UID == "" || instance.DeletionTimestamp != nil || instance.Status.CandidateFleet == nil || r.Core == nil || maxAge <= 0 || (role != "search" && role != "ingest") {
		return out, fmt.Errorf("candidate role and a positive observation window are required")
	}
	state := instance.Status.CandidateFleet
	targets := map[string]int{}
	for _, policy := range developmentCandidatePolicies(instance) {
		if policy.Group.Role == role {
			targets[policy.Group.Zone] = policy.Desired
		}
	}
	if len(targets) == 0 {
		return out, fmt.Errorf("candidate role capacity is not allocated")
	}
	return r.fleetQualification().observeFleetRole(ctx, instance, role, state.Fleet, state.Revision, targets, true, maxAge, qualify)
}

// observeFleetRole qualifies every Pod in a role and pins the Kubernetes
// identities used by the probes. A staged preview uses exact allocation
// counts. Rollout requires at least the configured serving floor in each zone.
func (r fleetQualification) observeFleetRole(ctx context.Context, instance *v1alpha1.OpenSearchServerless, role, fleet, revision string, targets map[string]int, exact bool, maxAge time.Duration, qualify func(context.Context, *v1alpha1.OpenSearchServerless, string, string, string, string, time.Duration) (searchPodQualification, error)) (candidateRoleObservation, error) {
	var out candidateRoleObservation
	if instance == nil || instance.UID == "" || instance.DeletionTimestamp != nil || r.core == nil || maxAge <= 0 ||
		fleet == "" || revision == "" || len(targets) == 0 || qualify == nil || (role != "search" && role != "ingest") {
		return out, fmt.Errorf("fleet role, revision, zone requirements and qualification probe are required")
	}
	timeout := maxAge
	if timeout > 30*time.Second {
		timeout = 30 * time.Second
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	selector := labels.SelectorFromSet(labels.Set{workload.InstanceLabel: string(instance.UID), workload.RoleLabel: role, workload.FleetLabel: fleet}).String()
	list := func() ([]corev1.Pod, error) {
		pods, err := r.core.Pods(instance.Namespace).List(ctx, metav1.ListOptions{LabelSelector: selector, Limit: 513})
		if err != nil || pods.Continue != "" || len(pods.Items) > 512 {
			return nil, fmt.Errorf("cannot list complete candidate role Pod set")
		}
		sort.Slice(pods.Items, func(i, j int) bool { return pods.Items[i].Name < pods.Items[j].Name })
		return pods.Items, nil
	}
	first, err := list()
	if err != nil {
		return out, err
	}
	counts := map[string]int{}
	peerTLS := peerTLSBaselineForFleet(instance, fleet)
	for _, pod := range first {
		zone := pod.Labels[workload.ZoneLabel]
		if workload.PodDisruptionTarget(&pod) {
			return out, fmt.Errorf("fleet Pod %s is marked for disruption", pod.Name)
		}
		if !peerTLSPodMatches(&pod, peerTLS) {
			return out, fmt.Errorf("fleet Pod %s does not match its peer TLS snapshot", pod.Name)
		}
		if err := checkFleetRuntimePod(instance, fleet, role, &pod); err != nil {
			return out, err
		}
		if err := checkFleetPluginPod(instance, fleet, role, &pod); err != nil {
			return out, fmt.Errorf("fleet Pod %s does not match its plugin set: %w", pod.Name, err)
		}
		if _, ok := targets[zone]; !ok || pod.UID == "" || pod.ResourceVersion == "" || pod.DeletionTimestamp != nil || pod.Labels["serverless.opensearch.org/revision"] != revision || (exact && role == "search" && pod.Annotations[warmupModeAnnotation] != "Strict") {
			return out, fmt.Errorf("candidate role Pod is outside the allocated immutable revision")
		}
		counts[zone]++
	}
	for zone, desired := range targets {
		if counts[zone] < desired || (exact && counts[zone] != desired) {
			return out, fmt.Errorf("candidate role Pod count differs in zone %q", zone)
		}
	}
	var qualified []searchPodQualification
	for _, pod := range first {
		q, err := qualify(ctx, instance, pod.Name, string(pod.UID), fleet, pod.Labels[workload.ZoneLabel], 5*time.Second)
		if err != nil {
			return out, fmt.Errorf("qualify candidate role Pod %s: %w", pod.Name, err)
		}
		if q.PodUID != string(pod.UID) {
			return out, fmt.Errorf("qualification identity differs for Pod %s", pod.Name)
		}
		qualified = append(qualified, q)
	}
	last, err := list()
	if err != nil || len(last) != len(first) {
		return out, fmt.Errorf("candidate role Pod set changed during observation")
	}
	for i := range first {
		if first[i].Name != last[i].Name || first[i].UID != last[i].UID || first[i].ResourceVersion != last[i].ResourceVersion {
			return out, fmt.Errorf("candidate role Pod changed during observation")
		}
	}
	now := time.Now()
	oldest := now
	for _, q := range qualified {
		if q.ObservedAt.IsZero() || q.ObservedAt.After(now) || now.Sub(q.ObservedAt) > maxAge {
			return out, fmt.Errorf("candidate role observation expired")
		}
		if q.ObservedAt.Before(oldest) {
			oldest = q.ObservedAt
		}
	}
	return candidateRoleObservation{InstanceUID: string(instance.UID), Fleet: fleet, Revision: revision, Generation: instance.Generation, Pods: qualified, PodSnapshot: last, ObservedAt: oldest}, nil
}
