package controller

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"sort"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/metrics"

	"github.com/huyz0/opensearch-serverless-operator/api/v1alpha1"
	"github.com/huyz0/opensearch-serverless-operator/internal/workload"
)

const (
	endpointObservationInterval = time.Minute
	unverifiedEndpointRetry     = 10 * time.Second
)

const endpointObservationMetricName = "opensearch_serverless_endpoint_observation_duration_seconds"

var endpointObservationDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
	Name:    endpointObservationMetricName,
	Help:    "Time for an instance endpoint sample, including retries and the status patch.",
	Buckets: []float64{0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 20},
}, []string{"outcome"})

func init() {
	metrics.Registry.MustRegister(endpointObservationDuration)
}

// sampleEndpointObservation checks all serving roles in one bounded window.
// The condition and rotating role detail report Kubernetes coverage only.
func (r *InstanceReconciler) sampleEndpointObservation(ctx context.Context, instance *v1alpha1.OpenSearchServerless) (bool, error) {
	if r.Core == nil || r.Discovery == nil {
		return false, fmt.Errorf("endpoint observation clients are unavailable")
	}
	now := r.now()
	if endpointObservationDelay(instance, now) > 0 {
		return false, nil
	}
	fleets := endpointServingFleets(instance)
	roles := make([]string, 0, len(fleets))
	for name := range fleets {
		roles = append(roles, name)
	}
	if len(roles) == 0 {
		return false, nil
	}
	sort.Strings(roles)
	role := roles[0]
	if last := instance.Status.LastEndpointObservation; last != nil {
		for _, candidate := range roles {
			if candidate > last.Role {
				role = candidate
				break
			}
		}
	}
	started := time.Now()
	probeCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	condition := metav1.Condition{Type: "Available", Status: metav1.ConditionTrue, Reason: "ReadyRoleEndpointsObserved",
		Message: "All configured roles meet their serving floors in Kubernetes EndpointSlices", ObservedGeneration: instance.Generation}
	var detail *v1alpha1.EndpointObservationStatus
	for _, name := range roles {
		status, err := r.observeEndpointCapacity(probeCtx, instance, name, now)
		if errors.Is(err, errEndpointObservationChanged) && probeCtx.Err() == nil {
			status, err = r.observeEndpointCapacity(probeCtx, instance, name, now)
		}
		if name == role {
			detail = status
		}
		if err != nil {
			if condition.Status == metav1.ConditionTrue {
				condition.Status, condition.Reason = metav1.ConditionUnknown, "RoleEndpointsUnverified"
				condition.Message = fmt.Sprintf("Kubernetes endpoint coverage could not be verified for %s", name)
			}
			continue
		}
		if status.Result != "KubernetesEndpointsObserved" && condition.Status != metav1.ConditionFalse {
			condition.Status, condition.Reason = metav1.ConditionFalse, "RoleEndpointFloorShortfall"
			condition.Message = fmt.Sprintf("Kubernetes endpoints for %s are below a serving floor after excluding disruption targets", name)
		}
	}
	before := instance.DeepCopyObject().(*v1alpha1.OpenSearchServerless)
	detail.ObservedServingFleets = fleets
	instance.Status.LastEndpointObservation = detail
	meta.SetStatusCondition(&instance.Status.Conditions, condition)
	if err := r.Status().Patch(ctx, instance, client.MergeFromWithOptions(before, client.MergeFromWithOptimisticLock{})); err != nil {
		endpointObservationDuration.WithLabelValues("StatusPatchFailed").Observe(time.Since(started).Seconds())
		return false, err
	}
	endpointObservationDuration.WithLabelValues(condition.Reason).Observe(time.Since(started).Seconds())
	return true, nil
}

func endpointObservationDelay(instance *v1alpha1.OpenSearchServerless, now time.Time) time.Duration {
	if !endpointAvailabilitySampled(instance) || now.Before(instance.Status.LastEndpointObservation.ObservedAt.Time) {
		return 0
	}
	interval := endpointObservationInterval
	if meta.FindStatusCondition(instance.Status.Conditions, "Available").Reason == "RoleEndpointsUnverified" {
		interval = unverifiedEndpointRetry
	}
	return max(0, interval-now.Sub(instance.Status.LastEndpointObservation.ObservedAt.Time))
}

func endpointServingFleets(instance *v1alpha1.OpenSearchServerless) map[string]string {
	fleets := make(map[string]string, 2)
	for _, role := range []string{"search", "ingest"} {
		for _, zone := range instance.Spec.Pools[role].Zones {
			if int64(zone.Replicas)+int64(zone.WarmSpare) > 0 {
				fleets[role] = instance.Status.ServingFleetIntentByRole[role]
				break
			}
		}
	}
	return fleets
}

func endpointAvailabilitySampled(instance *v1alpha1.OpenSearchServerless) bool {
	condition := meta.FindStatusCondition(instance.Status.Conditions, "Available")
	sample := instance.Status.LastEndpointObservation
	if condition == nil || condition.ObservedGeneration != instance.Generation || sample == nil ||
		sample.ObservedGeneration != instance.Generation || !maps.Equal(sample.ObservedServingFleets, endpointServingFleets(instance)) {
		return false
	}
	switch condition.Reason {
	case "ReadyRoleEndpointsObserved", "RoleEndpointsUnverified", "RoleEndpointFloorShortfall":
		return true
	default:
		return false
	}
}

func (r *InstanceReconciler) observeEndpointCapacity(ctx context.Context, instance *v1alpha1.OpenSearchServerless, role string, now time.Time) (*v1alpha1.EndpointObservationStatus, error) {
	fleet := instance.Status.ServingFleetIntentByRole[role]
	status := &v1alpha1.EndpointObservationStatus{ObservedGeneration: instance.Generation, Role: role, Fleet: fleet, Result: "Unavailable", ObservedAt: metav1.NewTime(now)}
	desired, err := workload.RoleService(instance, role, fleet, 9200)
	if err != nil {
		return status, err
	}
	service, err := r.Core.Services(instance.Namespace).Get(ctx, desired.Name, metav1.GetOptions{})
	if err != nil {
		return status, err
	}
	selector := labels.SelectorFromSet(labels.Set{workload.InstanceLabel: string(instance.UID), workload.RoleLabel: role, workload.FleetLabel: fleet}).String()
	pods, err := r.Core.Pods(instance.Namespace).List(ctx, metav1.ListOptions{LabelSelector: selector, Limit: 513})
	if err != nil {
		return status, err
	}
	if pods.Continue != "" || len(pods.Items) > 512 {
		return status, fmt.Errorf("complete role Pod list is required")
	}
	status.ServiceUID = string(service.UID)
	allowed := make(map[types.UID]struct{}, len(pods.Items))
	for _, pod := range pods.Items {
		if pod.UID != "" {
			allowed[pod.UID] = struct{}{}
		}
	}
	minima := servingZoneMinima(instance, role)
	zones := minima
	if role == "search" && r.DevelopmentSearchScaleDown {
		zones = maps.Clone(minima)
		for zone := range instance.Spec.Pools[role].Zones {
			if _, exists := zones[zone]; !exists {
				zones[zone] = 0
			}
		}
	}
	observed, err := inspectServiceEndpoints(ctx, r.Core, r.Discovery, instance, role, fleet, desired, service.UID, zones, allowed)
	if err != nil {
		return status, err
	}
	for zone := range zones {
		minima[zone] = int(instance.Spec.Pools[role].Zones[zone].MinServing)
	}
	status.Result = endpointCapacityResult(instance, role, minima, observed.UndisruptedReadyByZone)
	status.ReadyByZone = make(map[string]int32, len(observed.ReadyByZone))
	for zone, count := range observed.ReadyByZone {
		status.ReadyByZone[zone] = int32(count)
	}
	return status, nil
}

func endpointCapacityResult(instance *v1alpha1.OpenSearchServerless, role string, minima, counts map[string]int) string {
	belowZone, belowRegional := servingFloorShortfalls(instance, role, minima, counts)
	switch {
	case belowRegional:
		return "KubernetesEndpointsBelowRegionalFloor"
	case belowZone:
		return "KubernetesEndpointsBelowZoneFloor"
	default:
		return "KubernetesEndpointsObserved"
	}
}

func servingFloorShortfalls(instance *v1alpha1.OpenSearchServerless, role string, minima, counts map[string]int) (belowZone, belowRegional bool) {
	total := 0
	for zone, minReady := range minima {
		total += counts[zone]
		belowZone = belowZone || counts[zone] < minReady
	}
	return belowZone, total < servingRegionalFloor(instance, role)
}

func servingRegionalFloor(instance *v1alpha1.OpenSearchServerless, role string) int {
	if instance.Spec.Availability == nil {
		return 1
	}
	return max(1, int(instance.Spec.Availability.MinServingTotal[role]))
}

// roleEndpointObservation is point-in-time Kubernetes routing evidence, not a
// health, application warmth, actual AZ, edge convergence or traffic probe.
