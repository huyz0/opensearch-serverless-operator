package controller

import (
	"context"
	"fmt"
	"net/netip"

	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	"github.com/huyz0/opensearch-serverless-operator/api/v1alpha1"
	"github.com/huyz0/opensearch-serverless-operator/internal/workload"
	"github.com/huyz0/opensearch-serverless-operator/pkg/podlifecycle"
)

// observeWriterTrafficWithdrawal samples Kubernetes routing for the stable index Service.
// Callers must also bind the Java reservation and verify the active proxy before closing admission.
func (r writerRuntime) observeWriterTrafficWithdrawal(ctx context.Context, instance *v1alpha1.OpenSearchServerless, victim podlifecycle.Identity, serviceUID types.UID, minima map[string]int, survivors map[types.UID]struct{}) (roleEndpointObservation, error) {
	if instance == nil || instance.UID == "" || r.core == nil || r.discovery == nil ||
		victim.Namespace != instance.Namespace || victim.Name == "" || victim.UID == "" || victim.OwnerUID != string(instance.UID) {
		return roleEndpointObservation{}, fmt.Errorf("an owned writer Pod and Kubernetes clients are required")
	}
	if _, found := survivors[types.UID(victim.UID)]; found {
		return roleEndpointObservation{}, fmt.Errorf("withdrawn Pod cannot count toward surviving endpoints")
	}
	fleet := instance.Status.ServingFleetIntentByRole["ingest"]
	desired, err := workload.RoleService(instance, "ingest", fleet, 9200)
	if err != nil {
		return roleEndpointObservation{}, err
	}
	pods := r.core.Pods(instance.Namespace)
	before, err := pods.Get(ctx, victim.Name, metav1.GetOptions{})
	if err != nil {
		return roleEndpointObservation{}, err
	}
	owner := metav1.GetControllerOf(before)
	if before.UID != types.UID(victim.UID) || before.ResourceVersion == "" || before.DeletionTimestamp != nil ||
		before.Status.Phase != corev1.PodRunning || before.Spec.NodeName == "" || owner == nil || owner.UID != instance.UID ||
		owner.Kind != "OpenSearchServerless" || owner.APIVersion != v1alpha1.GroupVersion.String() || owner.Name != instance.Name ||
		before.Labels[workload.InstanceLabel] != string(instance.UID) || before.Labels[workload.RoleLabel] != "ingest" ||
		before.Labels[workload.FleetLabel] != fleet || workload.PodDisruptionTarget(before) {
		return roleEndpointObservation{}, fmt.Errorf("writer Pod ownership, serving fleet or running state changed")
	}
	if _, err := netip.ParseAddr(before.Status.PodIP); err != nil {
		return roleEndpointObservation{}, fmt.Errorf("writer Pod has no valid address")
	}
	notReady := false
	for _, condition := range before.Status.Conditions {
		if condition.Type == corev1.PodReady {
			notReady = condition.Status == corev1.ConditionFalse
			break
		}
	}
	if !notReady {
		return roleEndpointObservation{}, fmt.Errorf("writer Pod withdrawal has not reached Kubernetes readiness")
	}
	observed, err := observeLiveServiceEndpoints(ctx, r.core, r.discovery, instance, "ingest", fleet, desired, serviceUID, minima, survivors, before)
	if err != nil {
		return roleEndpointObservation{}, err
	}
	if _, belowRegional := servingFloorShortfalls(instance, "ingest", minima, observed.ReadyByZone); belowRegional {
		return roleEndpointObservation{}, fmt.Errorf("surviving index endpoints are below the regional floor")
	}
	after, err := pods.Get(ctx, victim.Name, metav1.GetOptions{})
	if err != nil || after.UID != before.UID || after.ResourceVersion != before.ResourceVersion {
		return roleEndpointObservation{}, fmt.Errorf("writer Pod changed during withdrawal observation")
	}
	return observed, nil
}

func endpointMayStillServePod(endpoint discoveryv1.Endpoint, pod *corev1.Pod) bool {
	if endpoint.Conditions.Ready != nil && !*endpoint.Conditions.Ready && endpoint.Conditions.Serving != nil && !*endpoint.Conditions.Serving {
		return false
	}
	if ref := endpoint.TargetRef; ref != nil && (ref.UID == pod.UID || (ref.Name == pod.Name && ref.Namespace == pod.Namespace)) {
		return true
	}
	podIPs := append([]corev1.PodIP{{IP: pod.Status.PodIP}}, pod.Status.PodIPs...)
	for _, addr := range endpoint.Addresses {
		candidate, err := netip.ParseAddr(addr)
		if err != nil {
			continue
		}
		for _, podIP := range podIPs {
			observed, err := netip.ParseAddr(podIP.IP)
			if err == nil && observed.Unmap() == candidate.Unmap() {
				return true
			}
		}
	}
	return false
}
