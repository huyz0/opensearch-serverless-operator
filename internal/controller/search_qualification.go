package controller

import (
	"context"
	"fmt"
	"net"
	"time"

	"github.com/google/uuid"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	typedcorev1 "k8s.io/client-go/kubernetes/typed/core/v1"

	"github.com/huyz0/opensearch-serverless-operator/api/v1alpha1"
	"github.com/huyz0/opensearch-serverless-operator/internal/java"
	"github.com/huyz0/opensearch-serverless-operator/internal/workload"
)

// searchPodQualification is ephemeral and must not be reused after its
// observations age or any Pod, process, route or serving state changes.
type searchPodQualification struct {
	PodUID, ContainerID, ProcessIncarnation string
	RestartCount                            int32
	ObservedAt                              time.Time
	StatsProcessVerified                    bool
	LeaseValidUntil                         time.Time
	LocalSearchActivity                     *java.LocalSearchActivity
}

func (r fleetQualification) supportsDevelopmentSearchProbe(instance *v1alpha1.OpenSearchServerless) bool {
	return instance != nil && r.warmupImage != "" && instance.Spec.Image == r.warmupImage &&
		r.supportsDevelopmentPodHTTP(instance)
}

// qualifyDevelopmentSearchPod checks the selected development image through
// the instance's pinned HTTP channel.
func (r fleetQualification) qualifyDevelopmentSearchPod(ctx context.Context, instance *v1alpha1.OpenSearchServerless, name, uid, fleet, zone string, minimumLease time.Duration) (searchPodQualification, error) {
	if !r.supportsDevelopmentSearchProbe(instance) {
		return searchPodQualification{}, fmt.Errorf("search probe requires the qualified development image and a supported authentication mode")
	}
	probe := r.observeSearch
	if probe == nil {
		probe = r.developmentSearchProbe(instance, 9200, minimumLease)
	}
	return qualifySearchPod(ctx, r.core, instance, name, uid, fleet, zone, probe)
}

func (r fleetQualification) developmentSearchProbe(instance *v1alpha1.OpenSearchServerless, port int, minimumLease time.Duration) func(context.Context, *corev1.Pod) (java.SearchQualification, error) {
	return func(ctx context.Context, pod *corev1.Pod) (java.SearchQualification, error) {
		if minimumLease < 0 {
			return java.SearchQualification{}, fmt.Errorf("a nonnegative lease margin is required")
		}
		ctx, cancel := context.WithTimeout(ctx, 8*time.Second)
		defer cancel()
		client, endpoint, err := r.developmentPodHTTP(ctx, instance, pod, port)
		if err != nil {
			return java.SearchQualification{}, err
		}
		defer client.CloseIdleConnections()
		qualification, err := java.QualifySearch(ctx, client, endpoint, pod.Name, minimumLease)
		if err == nil && hasManagedHTTPS(instance) && !qualification.StatsProcessVerified {
			return java.SearchQualification{}, fmt.Errorf("managed HTTPS qualification requires the local stats process identity")
		}
		return qualification, err
	}
}

// qualifySearchPod reads from the API server both before and after an
// application probe. The probe must contact the provided Pod IP directly,
// authenticate the endpoint as appropriate, and enforce its own deadline.
// No value returned here is a claim of current cache residency or routing.
func qualifySearchPod(ctx context.Context, core typedcorev1.CoreV1Interface, instance *v1alpha1.OpenSearchServerless, name, uid, fleet, zone string, probe func(context.Context, *corev1.Pod) (java.SearchQualification, error)) (searchPodQualification, error) {
	if probe == nil {
		return searchPodQualification{}, fmt.Errorf("search probe is required")
	}
	return qualifyRolePod(ctx, core, instance, name, uid, fleet, zone, "search", probe)
}

// qualifyRolePod binds a direct process observation to an unchanged Pod and
// container. The probe owns role-specific application checks.
func qualifyRolePod(ctx context.Context, core typedcorev1.CoreV1Interface, instance *v1alpha1.OpenSearchServerless, name, uid, fleet, zone, role string, probe func(context.Context, *corev1.Pod) (java.RoleQualification, error)) (searchPodQualification, error) {
	if core == nil || instance == nil || instance.Namespace == "" || instance.Name == "" || instance.UID == "" || instance.DeletionTimestamp != nil || name == "" || uid == "" || fleet == "" || zone == "" || (role != "search" && role != "ingest") || probe == nil {
		return searchPodQualification{}, fmt.Errorf("instance, exact Pod, group and probe are required")
	}
	pods := core.Pods(instance.Namespace)
	first, err := pods.Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return searchPodQualification{}, fmt.Errorf("read role Pod: %w", err)
	}
	validate := func(pod *corev1.Pod) (*corev1.ContainerStatus, error) {
		owner := metav1.GetControllerOf(pod)
		if pod.Namespace != instance.Namespace || pod.Name != name || string(pod.UID) != uid || pod.ResourceVersion == "" || owner == nil || owner.UID != instance.UID || owner.Name != instance.Name || owner.Kind != "OpenSearchServerless" || owner.APIVersion != v1alpha1.GroupVersion.String() || pod.DeletionTimestamp != nil || pod.Status.Phase != corev1.PodRunning || pod.Spec.NodeName == "" || net.ParseIP(pod.Status.PodIP) == nil || pod.Labels[workload.InstanceLabel] != string(instance.UID) || pod.Labels[workload.RoleLabel] != role || pod.Labels[workload.FleetLabel] != fleet || pod.Labels[workload.ZoneLabel] != zone {
			return nil, fmt.Errorf("role Pod identity, group or running state changed")
		}
		ready := false
		for _, c := range pod.Status.Conditions {
			if c.Type == corev1.PodReady && c.Status == corev1.ConditionTrue {
				ready = true
			}
		}
		container := runningOpenSearch(pod)
		if !ready || container == nil || !container.Ready {
			return nil, fmt.Errorf("role Pod or container is not ready")
		}
		return container, nil
	}
	firstContainer, err := validate(first)
	if err != nil {
		return searchPodQualification{}, err
	}
	started := time.Now()
	evidence, err := probe(ctx, first.DeepCopy())
	if err != nil {
		return searchPodQualification{}, fmt.Errorf("qualify Java role process: %w", err)
	}
	incarnation, err := uuid.Parse(evidence.ProcessIncarnation)
	if err != nil || incarnation == uuid.Nil || incarnation.Version() != 4 {
		return searchPodQualification{}, fmt.Errorf("invalid Java process incarnation")
	}
	last, err := pods.Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return searchPodQualification{}, fmt.Errorf("re-read role Pod: %w", err)
	}
	lastContainer, err := validate(last)
	if err != nil {
		return searchPodQualification{}, err
	}
	if first.ResourceVersion != last.ResourceVersion || first.Status.PodIP != last.Status.PodIP || first.Spec.NodeName != last.Spec.NodeName || firstContainer.ContainerID != lastContainer.ContainerID || firstContainer.RestartCount != lastContainer.RestartCount {
		return searchPodQualification{}, fmt.Errorf("role Pod changed during qualification")
	}
	return searchPodQualification{
		PodUID: uid, ContainerID: firstContainer.ContainerID, RestartCount: firstContainer.RestartCount,
		ProcessIncarnation: evidence.ProcessIncarnation, ObservedAt: started, StatsProcessVerified: evidence.StatsProcessVerified, LeaseValidUntil: evidence.LeaseValidUntil,
		LocalSearchActivity: evidence.LocalSearchActivity,
	}, nil
}
