package controller

import (
	"context"

	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/client-go/kubernetes/typed/discovery/v1"

	"github.com/huyz0/opensearch-serverless-operator/internal/java"
)

type fleetQualification struct {
	podHTTPAccess
	discovery                      discoveryv1.DiscoveryV1Interface
	warmupImage, serviceProbeImage string
	observeSearch                  func(context.Context, *corev1.Pod) (java.SearchQualification, error)
	observeIngest                  func(context.Context, *corev1.Pod) (java.IngestQualification, error)
}

func (r *InstanceReconciler) fleetQualification() fleetQualification {
	return fleetQualification{
		podHTTPAccess: r.podHTTP(), discovery: r.Discovery,
		warmupImage: r.DevelopmentWarmupImage, serviceProbeImage: r.DevelopmentServiceProbeImage,
		observeSearch: r.ObserveSearchQualification, observeIngest: r.ObserveIngestQualification,
	}
}

func (r fleetQualification) roleProbe(role string) rolloutRoleProbe {
	switch role {
	case "search":
		return r.qualifyDevelopmentSearchPod
	case "ingest":
		return r.qualifyDevelopmentIngestPod
	default:
		return nil
	}
}
