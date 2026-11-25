package controller

import (
	"context"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/huyz0/opensearch-serverless-operator/api/v1alpha1"
	"github.com/huyz0/opensearch-serverless-operator/internal/java"
)

func (r fleetQualification) supportsDevelopmentIngestProbe(instance *v1alpha1.OpenSearchServerless) bool {
	return instance != nil && instance.Spec.Image == r.serviceProbeImage && qualifiedRoleProbeImage.MatchString(r.serviceProbeImage) && r.supportsDevelopmentPodHTTP(instance)
}

// qualifyDevelopmentIngestPod observes a specific Pod over its approved HTTP channel.
// Readiness and lease presence are not proof of successful writes or takeover.
func (r fleetQualification) qualifyDevelopmentIngestPod(ctx context.Context, instance *v1alpha1.OpenSearchServerless, name, uid, fleet, zone string, minimumLease time.Duration) (searchPodQualification, error) {
	if !r.supportsDevelopmentIngestProbe(instance) {
		return searchPodQualification{}, fmt.Errorf("ingest probe requires the qualified development image and a supported authentication mode")
	}
	probe := r.observeIngest
	if probe == nil {
		probe = func(ctx context.Context, pod *corev1.Pod) (java.IngestQualification, error) {
			client, endpoint, err := r.developmentPodHTTP(ctx, instance, pod, 9200)
			if err != nil {
				return java.IngestQualification{}, err
			}
			defer client.CloseIdleConnections()
			qualification, err := java.QualifyIngest(ctx, client, endpoint, pod.Name, minimumLease)
			if err == nil && hasManagedHTTPS(instance) && !qualification.StatsProcessVerified {
				return java.IngestQualification{}, fmt.Errorf("managed HTTPS qualification requires the local ingest stats process identity")
			}
			return qualification, err
		}
	}
	return qualifyRolePod(ctx, r.core, instance, name, uid, fleet, zone, "ingest", probe)
}

func (r fleetQualification) observeServingIngestProtection(ctx context.Context, instance *v1alpha1.OpenSearchServerless, maxAge time.Duration) (servingRoleProtection, error) {
	return r.observeServingRoleProtection(ctx, instance, "ingest", maxAge, r.qualifyDevelopmentIngestPod, nil, false)
}

// sampleServingIngestProtection is diagnostic only and cannot authorize a
// disruptive action or claim shard takeover capability.
func (r *InstanceReconciler) sampleServingIngestProtection(ctx context.Context, instance *v1alpha1.OpenSearchServerless) (bool, error) {
	if _, exists := instance.Spec.Pools["ingest"]; !exists || !r.DevelopmentObserveEndpoints || !r.fleetQualification().supportsDevelopmentIngestProbe(instance) {
		return false, nil
	}
	now := r.now()
	if last := instance.Status.LastServingIngestProtection; last != nil && last.ObservedGeneration == instance.Generation && !now.Before(last.ObservedAt.Time) && now.Sub(last.ObservedAt.Time) < time.Minute {
		return false, nil
	}
	status := &v1alpha1.ServingIngestProtectionStatus{ObservedGeneration: instance.Generation, Fleet: instance.Status.ServingFleetIntentByRole["ingest"], Result: "Unavailable", ObservedAt: metav1.NewTime(now)}
	observation, err := r.fleetQualification().observeServingIngestProtection(ctx, instance, 30*time.Second)
	if err == nil {
		status.Result = "LeaseAndEndpointsObserved"
		status.QualifiedPods = int32(len(observation.Pods))
		_, status.RegularCapacityRecovered = observation.emergencyFailbackCandidate(instance, "ingest", time.Now())
		status.ServiceUID, status.ServiceVersion = observation.ServiceUID, observation.ServiceVersion
	}
	before := instance.DeepCopyObject().(*v1alpha1.OpenSearchServerless)
	instance.Status.LastServingIngestProtection = status
	if err := r.Status().Patch(ctx, instance, client.MergeFromWithOptions(before, client.MergeFromWithOptimisticLock{})); err != nil {
		return false, err
	}
	return true, nil
}
