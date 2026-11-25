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
	"github.com/huyz0/opensearch-serverless-operator/pkg/podlifecycle"
)

// servingRoleProtection is ephemeral evidence of local qualification and
// Kubernetes endpoints. It does not establish actual AZ, retained cache,
// peer reachability, ingress health or permission to evict another Pod.
type servingRoleProtection struct {
	InstanceUID, Fleet, ServiceUID, ServiceVersion string
	Generation                                     int64
	Pods                                           []searchPodQualification
	RegularEndpointsByZone                         map[string]int
	EmergencyPods                                  []emergencyFailbackPod
	BootstrapPod                                   *emergencyFailbackPod
	PodSnapshot                                    []corev1.Pod
	Endpoints                                      roleEndpointObservation
	ObservedAt                                     time.Time
}

// satisfiesRegularServingTargets excludes emergency capacity and any bootstrap
// Pod that was exempted from individual qualification by the observer.
func (observed servingRoleProtection) satisfiesRegularServingTargets(instance *v1alpha1.OpenSearchServerless, role string) bool {
	pool, exists := instance.Spec.Pools[role]
	if !exists {
		return false
	}
	regional := 0
	for zone, desired := range pool.Zones {
		regular := observed.RegularEndpointsByZone[zone]
		if int64(regular) < int64(desired.Replicas)+int64(desired.WarmSpare) {
			return false
		}
		regional += regular
	}
	return regional >= servingRegionalFloor(instance, role)
}

// sampleServingSearchProtection stores historical diagnostics only. Failed
// observations replace any previous success and never authorize retirement.
func (r *InstanceReconciler) sampleServingSearchProtection(ctx context.Context, instance *v1alpha1.OpenSearchServerless) (bool, error) {
	pool, exists := instance.Spec.Pools["search"]
	if !exists || !pool.WarmOnJoin || pool.WarmReadiness != "Strict" || !r.DevelopmentObserveEndpoints || !r.fleetQualification().supportsDevelopmentSearchProbe(instance) {
		return false, nil
	}
	now := r.now()
	if last := instance.Status.LastServingSearchProtection; last != nil && last.ObservedGeneration == instance.Generation && !now.Before(last.ObservedAt.Time) && now.Sub(last.ObservedAt.Time) < time.Minute {
		return false, nil
	}
	status := &v1alpha1.ServingSearchProtectionStatus{ObservedGeneration: instance.Generation, Fleet: instance.Status.ServingFleetIntentByRole["search"], Result: "Unavailable", ObservedAt: metav1.NewTime(now)}
	observation, err := r.observeServingSearchProtection(ctx, instance, 30*time.Second)
	if err == nil {
		status.Result = "WarmupAndEndpointsObserved"
		status.QualifiedPods = int32(len(observation.Pods))
		_, status.RegularCapacityRecovered = observation.emergencyFailbackCandidate(instance, "search", time.Now())
		status.ServiceUID, status.ServiceVersion = observation.ServiceUID, observation.ServiceVersion
	}
	before := instance.DeepCopyObject().(*v1alpha1.OpenSearchServerless)
	instance.Status.LastServingSearchProtection = status
	if err := r.Status().Patch(ctx, instance, client.MergeFromWithOptions(before, client.MergeFromWithOptimisticLock{})); err != nil {
		return false, err
	}
	return true, nil
}

// observeServingSearchProtection probes every current serving search Pod,
// checks desired capacity in each configured zone and validates the stable
// Service endpoints against the qualified set. The caller must re-read the
// instance and independently verify the other retirement preconditions.
func (r *InstanceReconciler) observeServingSearchProtection(ctx context.Context, instance *v1alpha1.OpenSearchServerless, maxAge time.Duration) (servingRoleProtection, error) {
	return r.observeSearchProtection(ctx, instance, maxAge, nil)
}

func (r *InstanceReconciler) observeSearchProtection(ctx context.Context, instance *v1alpha1.OpenSearchServerless, maxAge time.Duration, bootstrap *v1alpha1.ColdBootstrapStatus) (servingRoleProtection, error) {
	if instance == nil || !instance.Spec.Pools["search"].WarmOnJoin || instance.Spec.Pools["search"].WarmReadiness != "Strict" {
		return servingRoleProtection{}, fmt.Errorf("strict serving search is required")
	}
	return r.fleetQualification().observeServingRoleProtection(ctx, instance, "search", maxAge, r.fleetQualification().qualifyDevelopmentSearchPod, bootstrap, false)
}

func (r fleetQualification) observeServingRoleProtection(ctx context.Context, instance *v1alpha1.OpenSearchServerless, role string, maxAge time.Duration, qualify func(context.Context, *v1alpha1.OpenSearchServerless, string, string, string, string, time.Duration) (searchPodQualification, error), bootstrap *v1alpha1.ColdBootstrapStatus, includeZeroTargets bool) (servingRoleProtection, error) {
	var result servingRoleProtection
	if instance == nil || instance.UID == "" || instance.DeletionTimestamp != nil || r.core == nil || r.discovery == nil || maxAge <= 0 || (role != "search" && role != "ingest") || qualify == nil {
		return result, fmt.Errorf("live instance, Kubernetes clients and observation window are required")
	}
	pool, ok := instance.Spec.Pools[role]
	fleet := instance.Status.ServingFleetIntentByRole[role]
	if !ok || fleet == "" {
		return result, fmt.Errorf("serving role and its recorded fleet are required")
	}
	minima := map[string]int{}
	positiveTarget := false
	for zone, desired := range pool.Zones {
		total := int64(desired.Replicas) + int64(desired.WarmSpare)
		if zone == "" || total < 0 || total > 512 {
			return result, fmt.Errorf("serving role zone capacity is invalid")
		}
		if total > 0 || includeZeroTargets {
			minima[zone] = int(total)
		}
		positiveTarget = positiveTarget || total > 0
	}
	if !positiveTarget {
		return result, fmt.Errorf("serving role has no required capacity")
	}
	domain, err := r.peerDNSDomain(instance)
	if err != nil {
		return result, err
	}
	revisions, err := podRevisions(instance, domain)
	if err != nil {
		return result, err
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
			return nil, fmt.Errorf("serving role Pod observation is incomplete")
		}
		sort.Slice(pods.Items, func(i, j int) bool { return pods.Items[i].Name < pods.Items[j].Name })
		return pods.Items, nil
	}
	first, err := list()
	if err != nil {
		return result, err
	}
	revision, err := revisions.forFleet(instance, first, role, fleet)
	if err != nil {
		return result, err
	}
	scoped := revisions.forFleetTemplate(fleet)
	counts := map[string]int{}
	for _, pod := range first {
		zone := pod.Labels[workload.ZoneLabel]
		if workload.PodDisruptionTarget(&pod) {
			return result, fmt.Errorf("serving role Pod %s is marked for disruption", pod.Name)
		}
		if _, ok := minima[zone]; !ok || pod.UID == "" || pod.ResourceVersion == "" || pod.DeletionTimestamp != nil || !scoped.sameServingTemplate(revision, pod.Labels[podRevisionLabel]) {
			return result, fmt.Errorf("serving role Pod has an unexpected zone or revision")
		}
		counts[zone]++
	}
	for zone, minimum := range minima {
		if counts[zone] < minimum {
			return result, fmt.Errorf("serving role capacity is short in zone %q", zone)
		}
	}
	qualified := make([]searchPodQualification, 0, len(first))
	allowed := make(map[types.UID]struct{}, len(first))
	var bootstrapPod *emergencyFailbackPod
	for _, pod := range first {
		if bootstrap != nil && pod.Name == bootstrap.Name {
			if bootstrap.Fleet != fleet || !matchesSearchBootstrapPod(instance, &pod) {
				return result, fmt.Errorf("search bootstrap Pod identity changed")
			}
			victim := failbackPodIdentity(instance, &pod)
			bootstrapPod = &victim
			allowed[pod.UID] = struct{}{}
			continue
		}
		observation, err := qualify(ctx, instance, pod.Name, string(pod.UID), fleet, pod.Labels[workload.ZoneLabel], 5*time.Second)
		if err != nil {
			return result, fmt.Errorf("qualify serving role Pod %s: %w", pod.Name, err)
		}
		qualified = append(qualified, observation)
		allowed[pod.UID] = struct{}{}
	}
	desiredService, err := workload.RoleService(instance, role, fleet, 9200)
	if err != nil {
		return result, err
	}
	service, err := r.core.Services(instance.Namespace).Get(ctx, desiredService.Name, metav1.GetOptions{})
	if err != nil || service.UID == "" {
		return result, fmt.Errorf("serving role Service is unavailable")
	}
	endpoints, err := observeRoleEndpoints(ctx, r.core, r.discovery, instance, role, fleet, service.UID, minima, allowed)
	if err != nil {
		return result, err
	}
	last, err := list()
	if err != nil || len(last) != len(first) {
		return result, fmt.Errorf("serving role Pod set changed during observation")
	}
	for i := range first {
		if first[i].Name != last[i].Name || first[i].UID != last[i].UID || first[i].ResourceVersion != last[i].ResourceVersion {
			return result, fmt.Errorf("serving role Pod changed during observation")
		}
	}
	now, oldest := time.Now(), endpoints.ObservedAt
	if oldest.IsZero() || oldest.After(now) || now.Sub(oldest) > maxAge {
		return result, fmt.Errorf("serving role endpoint observation expired")
	}
	for _, pod := range qualified {
		if pod.ObservedAt.IsZero() || pod.ObservedAt.After(now) || now.Sub(pod.ObservedAt) > maxAge {
			return result, fmt.Errorf("serving role qualification expired")
		}
		if pod.ObservedAt.Before(oldest) {
			oldest = pod.ObservedAt
		}
	}
	regularEndpoints := make(map[string]int, len(minima))
	var emergencyPods []emergencyFailbackPod
	for _, pod := range last {
		endpointVersion, included := endpoints.ReadyPodVersions[pod.UID]
		if included && endpointVersion != pod.ResourceVersion {
			return result, fmt.Errorf("serving endpoint differs from the qualified Pod observation")
		}
		if bootstrapPod != nil && bootstrapPod.UID == string(pod.UID) {
			continue
		}
		if pod.Annotations[emergencyCapacityAnnotation] == "true" {
			emergencyPods = append(emergencyPods, failbackPodIdentity(instance, &pod))
		} else if included {
			regularEndpoints[pod.Labels[workload.ZoneLabel]]++
		}
	}
	return servingRoleProtection{
		InstanceUID: string(instance.UID), Fleet: fleet, Generation: instance.Generation,
		ServiceUID: string(service.UID), ServiceVersion: endpoints.ServiceVersion, Pods: qualified, ObservedAt: oldest,
		RegularEndpointsByZone: regularEndpoints, EmergencyPods: emergencyPods, BootstrapPod: bootstrapPod,
		PodSnapshot: last, Endpoints: endpoints,
	}, nil
}

func failbackPodIdentity(instance *v1alpha1.OpenSearchServerless, pod *corev1.Pod) emergencyFailbackPod {
	return emergencyFailbackPod{
		Identity: podlifecycle.Identity{Namespace: pod.Namespace, Name: pod.Name, UID: string(pod.UID), OwnerUID: string(instance.UID)},
		Version:  pod.ResourceVersion, Zone: pod.Labels[workload.ZoneLabel],
	}
}
