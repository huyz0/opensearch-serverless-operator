package controller

import (
	"context"
	"errors"
	"fmt"
	"net/netip"
	"time"

	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	typedcore "k8s.io/client-go/kubernetes/typed/core/v1"
	typeddiscovery "k8s.io/client-go/kubernetes/typed/discovery/v1"

	"github.com/huyz0/opensearch-serverless-operator/api/v1alpha1"
	"github.com/huyz0/opensearch-serverless-operator/internal/workload"
)

var errEndpointObservationChanged = errors.New("routing changed during endpoint observation")

type roleEndpointObservation struct {
	InstanceUID            types.UID
	Role                   string
	Fleet                  string
	ServiceUID             types.UID
	ServiceName            string
	ServiceVersion         string
	ReadyByZone            map[string]int
	UndisruptedReadyByZone map[string]int
	ReadyPodVersions       map[types.UID]string
	DisruptingReadyPods    int
	SliceVersions          map[string]endpointSliceVersion
	ObservedAt             time.Time
}

type endpointSliceVersion struct {
	uid     types.UID
	version string
}

// observeRoleEndpoints fails closed on unknown or foreign ready endpoints. The
// caller supplies the allowed Pod UIDs, establishes actual placement and process
// qualification, imposes an evidence lifetime, and re-observes before a disruptive action. The
// individual reads are not an atomic snapshot and a successful observation
// cannot guarantee future traffic behavior.
func observeRoleEndpoints(ctx context.Context, core typedcore.CoreV1Interface, discovery typeddiscovery.DiscoveryV1Interface, instance *v1alpha1.OpenSearchServerless, role, fleet string, serviceUID types.UID, minimumByZone map[string]int, qualifiedPods map[types.UID]struct{}) (roleEndpointObservation, error) {
	desired, err := workload.RoleService(instance, role, fleet, 9200)
	if err != nil {
		return roleEndpointObservation{}, err
	}
	return observeServiceEndpoints(ctx, core, discovery, instance, role, fleet, desired, serviceUID, minimumByZone, qualifiedPods)
}

func observeServiceEndpoints(ctx context.Context, core typedcore.CoreV1Interface, discovery typeddiscovery.DiscoveryV1Interface, instance *v1alpha1.OpenSearchServerless, role, fleet string, desired *corev1.Service, serviceUID types.UID, minimumByZone map[string]int, qualifiedPods map[types.UID]struct{}) (roleEndpointObservation, error) {
	return observeLiveServiceEndpoints(ctx, core, discovery, instance, role, fleet, desired, serviceUID, minimumByZone, qualifiedPods, nil)
}

func observeLiveServiceEndpoints(ctx context.Context, core typedcore.CoreV1Interface, discovery typeddiscovery.DiscoveryV1Interface, instance *v1alpha1.OpenSearchServerless, role, fleet string, desired *corev1.Service, serviceUID types.UID, minimumByZone map[string]int, qualifiedPods map[types.UID]struct{}, withdrawn *corev1.Pod) (roleEndpointObservation, error) {
	observed, err := inspectLiveServiceEndpoints(ctx, core, discovery, instance, role, fleet, desired, serviceUID, minimumByZone, qualifiedPods, withdrawn)
	if err != nil {
		return roleEndpointObservation{}, err
	}
	if observed.DisruptingReadyPods != 0 {
		return roleEndpointObservation{}, fmt.Errorf("%d ready endpoint Pods are marked for disruption", observed.DisruptingReadyPods)
	}
	for zone, minimum := range minimumByZone {
		if observed.ReadyByZone[zone] < minimum {
			return roleEndpointObservation{}, fmt.Errorf("zone %q has %d ready endpoints, requires %d", zone, observed.ReadyByZone[zone], minimum)
		}
	}
	return observed, nil
}

// inspectServiceEndpoints preserves verified zero counts for diagnostics.
// Traffic qualification still uses observeServiceEndpoints and its floors.
func inspectServiceEndpoints(ctx context.Context, core typedcore.CoreV1Interface, discovery typeddiscovery.DiscoveryV1Interface, instance *v1alpha1.OpenSearchServerless, role, fleet string, desired *corev1.Service, serviceUID types.UID, minimumByZone map[string]int, qualifiedPods map[types.UID]struct{}) (roleEndpointObservation, error) {
	return inspectLiveServiceEndpoints(ctx, core, discovery, instance, role, fleet, desired, serviceUID, minimumByZone, qualifiedPods, nil)
}

func inspectLiveServiceEndpoints(ctx context.Context, core typedcore.CoreV1Interface, discovery typeddiscovery.DiscoveryV1Interface, instance *v1alpha1.OpenSearchServerless, role, fleet string, desired *corev1.Service, serviceUID types.UID, minimumByZone map[string]int, qualifiedPods map[types.UID]struct{}, withdrawn *corev1.Pod) (roleEndpointObservation, error) {
	if core == nil || discovery == nil || instance == nil || serviceUID == "" || len(minimumByZone) == 0 {
		return roleEndpointObservation{}, fmt.Errorf("clients, instance, Service UID and zone requirements are required")
	}
	positiveMinimum := false
	for zone, minimum := range minimumByZone {
		if zone == "" || minimum < 0 {
			return roleEndpointObservation{}, fmt.Errorf("each named zone requires a nonnegative endpoint minimum")
		}
		positiveMinimum = positiveMinimum || minimum > 0
	}
	if !positiveMinimum && withdrawn == nil {
		return roleEndpointObservation{}, fmt.Errorf("at least one zone requires a positive endpoint minimum")
	}
	if desired == nil || desired.Namespace != instance.Namespace || desired.Name == "" {
		return roleEndpointObservation{}, fmt.Errorf("expected Service identity is required")
	}
	started := time.Now()
	services := core.Services(instance.Namespace)
	service, err := services.Get(ctx, desired.Name, metav1.GetOptions{})
	if err != nil {
		return roleEndpointObservation{}, fmt.Errorf("read role Service: %w", err)
	}
	if !matchesFleetService(instance, service, desired, serviceUID) {
		return roleEndpointObservation{}, fmt.Errorf("role Service identity or selector differs")
	}
	selector := labels.SelectorFromSet(labels.Set{discoveryv1.LabelServiceName: service.Name}).String()
	slices, err := discovery.EndpointSlices(instance.Namespace).List(ctx, metav1.ListOptions{LabelSelector: selector, Limit: 129})
	if err != nil {
		return roleEndpointObservation{}, fmt.Errorf("list EndpointSlices: %w", err)
	}
	if len(slices.Items) == 0 {
		return roleEndpointObservation{}, fmt.Errorf("role Service has no EndpointSlices")
	}
	if slices.Continue != "" || len(slices.Items) > 128 {
		return roleEndpointObservation{}, fmt.Errorf("too many EndpointSlices for one observation")
	}
	counts := make(map[string]int, len(minimumByZone))
	undisrupted := make(map[string]int, len(minimumByZone))
	for zone := range minimumByZone {
		counts[zone] = 0
	}
	type podVersion struct{ name, version, zone string }
	versions := make(map[types.UID]podVersion)
	addressOwners := make(map[netip.Addr]types.UID)
	sliceVersions := make(map[string]endpointSliceVersion, len(slices.Items))
	endpointCount := 0
	disruptingReadyPods := 0
	for _, slice := range slices.Items {
		if slice.Name == "" || slice.UID == "" || slice.ResourceVersion == "" {
			return roleEndpointObservation{}, fmt.Errorf("EndpointSlice identity or version is missing")
		}
		if _, exists := sliceVersions[slice.Name]; exists {
			return roleEndpointObservation{}, fmt.Errorf("duplicate EndpointSlice identity")
		}
		sliceVersions[slice.Name] = endpointSliceVersion{uid: slice.UID, version: slice.ResourceVersion}
		endpointCount += len(slice.Endpoints)
		if endpointCount > 512 {
			return roleEndpointObservation{}, fmt.Errorf("too many endpoints for one observation")
		}
		if err := validateRoleEndpointSlice(&slice, service); err != nil {
			return roleEndpointObservation{}, err
		}
		for _, endpoint := range slice.Endpoints {
			if endpoint.Conditions.Ready == nil {
				return roleEndpointObservation{}, fmt.Errorf("endpoint readiness is unknown")
			}
			if withdrawn != nil && endpointMayStillServePod(endpoint, withdrawn) {
				return roleEndpointObservation{}, fmt.Errorf("withdrawn Pod is still eligible or ambiguous in an EndpointSlice")
			}
			if endpoint.Conditions.Terminating != nil && *endpoint.Conditions.Terminating && (endpoint.Conditions.Serving == nil || *endpoint.Conditions.Serving) {
				return roleEndpointObservation{}, fmt.Errorf("a terminating endpoint may still serve traffic")
			}
			if !*endpoint.Conditions.Ready {
				continue
			}
			if err := validateReadyEndpointReference(endpoint, instance.Namespace); err != nil {
				return roleEndpointObservation{}, err
			}
			if _, ok := qualifiedPods[endpoint.TargetRef.UID]; !ok {
				return roleEndpointObservation{}, fmt.Errorf("ready endpoint does not belong to the qualified Pod set")
			}
			pod, err := core.Pods(instance.Namespace).Get(ctx, endpoint.TargetRef.Name, metav1.GetOptions{})
			if err != nil {
				return roleEndpointObservation{}, fmt.Errorf("read endpoint Pod: %w", err)
			}
			zone := pod.Labels[workload.ZoneLabel]
			_, allowedZone := minimumByZone[zone]
			if !allowedZone {
				return roleEndpointObservation{}, fmt.Errorf("ready endpoint Pod is outside the observed zones")
			}
			if err := validateReadyEndpointPod(instance, pod, role, fleet, endpoint, zone); err != nil {
				return roleEndpointObservation{}, err
			}
			if err := recordReadyEndpointAddresses(addressOwners, endpoint, slice.AddressType, pod); err != nil {
				return roleEndpointObservation{}, err
			}
			version := podVersion{name: pod.Name, version: pod.ResourceVersion, zone: zone}
			if prior, ok := versions[pod.UID]; ok {
				if prior != version {
					return roleEndpointObservation{}, fmt.Errorf("%w: a Pod reference changed", errEndpointObservationChanged)
				}
				continue
			}
			versions[pod.UID] = version
			if workload.PodDisruptionTarget(pod) {
				disruptingReadyPods++
			} else {
				undisrupted[zone]++
			}
			counts[zone]++
		}
	}
	for uid, version := range versions {
		pod, err := core.Pods(instance.Namespace).Get(ctx, version.name, metav1.GetOptions{})
		if err != nil {
			return roleEndpointObservation{}, fmt.Errorf("re-read endpoint Pod: %w", err)
		}
		if pod.UID != uid || pod.ResourceVersion != version.version {
			return roleEndpointObservation{}, fmt.Errorf("%w: endpoint Pod changed", errEndpointObservationChanged)
		}
	}
	if err := recheckEndpointSlices(ctx, discovery.EndpointSlices(instance.Namespace), service.Name, sliceVersions); err != nil {
		return roleEndpointObservation{}, err
	}
	last, err := services.Get(ctx, desired.Name, metav1.GetOptions{})
	if err != nil {
		return roleEndpointObservation{}, fmt.Errorf("re-read role Service: %w", err)
	}
	if !matchesFleetService(instance, last, desired, serviceUID) || last.ResourceVersion != service.ResourceVersion {
		return roleEndpointObservation{}, fmt.Errorf("%w: role Service changed", errEndpointObservationChanged)
	}
	readyPods := make(map[types.UID]string, len(versions))
	for uid, pod := range versions {
		readyPods[uid] = pod.version
	}
	return roleEndpointObservation{
		InstanceUID: instance.UID, Role: role, Fleet: fleet, ServiceUID: serviceUID, ServiceName: service.Name,
		ServiceVersion: service.ResourceVersion, ReadyByZone: counts, UndisruptedReadyByZone: undisrupted, ReadyPodVersions: readyPods, DisruptingReadyPods: disruptingReadyPods,
		SliceVersions: sliceVersions, ObservedAt: started,
	}, nil
}

func validateRoleEndpointSlice(slice *discoveryv1.EndpointSlice, service *corev1.Service) error {
	owner := metav1.GetControllerOf(slice)
	if slice.Namespace != service.Namespace || slice.DeletionTimestamp != nil || owner == nil || owner.APIVersion != "v1" || owner.Kind != "Service" ||
		owner.Name != service.Name || owner.UID != service.UID || slice.Labels[discoveryv1.LabelServiceName] != service.Name ||
		slice.Labels[discoveryv1.LabelManagedBy] != "endpointslice-controller.k8s.io" {
		return fmt.Errorf("EndpointSlice ownership differs")
	}
	if slice.AddressType != discoveryv1.AddressTypeIPv4 && slice.AddressType != discoveryv1.AddressTypeIPv6 {
		return fmt.Errorf("EndpointSlice address type is unsupported")
	}
	if len(slice.Ports) != 1 {
		return fmt.Errorf("EndpointSlice must have one role Service port")
	}
	port := slice.Ports[0]
	if port.Name == nil || *port.Name != "http" || port.Port == nil || *port.Port != 9200 || port.Protocol == nil || *port.Protocol != corev1.ProtocolTCP {
		return fmt.Errorf("EndpointSlice port differs from the role Service")
	}
	return nil
}

func validateReadyEndpointReference(endpoint discoveryv1.Endpoint, namespace string) error {
	// The Kubernetes EndpointSlice controller omits apiVersion from Pod references.
	target := endpoint.TargetRef
	if (endpoint.Conditions.Terminating != nil && *endpoint.Conditions.Terminating) || (endpoint.Conditions.Serving != nil && !*endpoint.Conditions.Serving) ||
		target == nil || (target.APIVersion != "" && target.APIVersion != "v1") || target.Kind != "Pod" ||
		target.Namespace != namespace || target.Name == "" || target.UID == "" || len(endpoint.Addresses) == 0 {
		return fmt.Errorf("ready endpoint identity or condition is invalid")
	}
	return nil
}

func validateReadyEndpointPod(instance *v1alpha1.OpenSearchServerless, pod *corev1.Pod, role, fleet string, endpoint discoveryv1.Endpoint, zone string) error {
	owner := metav1.GetControllerOf(pod)
	if pod.UID != endpoint.TargetRef.UID || pod.Name != endpoint.TargetRef.Name || pod.Namespace != instance.Namespace || pod.ResourceVersion == "" ||
		owner == nil || owner.UID != instance.UID || owner.Name != instance.Name || owner.Kind != "OpenSearchServerless" || owner.APIVersion != v1alpha1.GroupVersion.String() ||
		pod.Labels[workload.InstanceLabel] != string(instance.UID) || pod.Labels[workload.RoleLabel] != role || pod.Labels[workload.FleetLabel] != fleet {
		return fmt.Errorf("ready endpoint Pod ownership or fleet differs")
	}
	if pod.DeletionTimestamp != nil || pod.Spec.NodeName == "" || pod.Spec.HostNetwork || hasPlacementGate(pod) || !podStartupReady(pod) {
		return fmt.Errorf("endpoint Pod is not a ready, released Pod on the Pod network")
	}
	if endpoint.NodeName != nil && *endpoint.NodeName != pod.Spec.NodeName {
		return fmt.Errorf("endpoint node does not match the Pod")
	}
	if endpoint.Zone != nil && *endpoint.Zone != zone {
		return fmt.Errorf("endpoint zone does not match the Pod placement")
	}
	return nil
}

func recordReadyEndpointAddresses(owners map[netip.Addr]types.UID, endpoint discoveryv1.Endpoint, addressType discoveryv1.AddressType, pod *corev1.Pod) error {
	ips := make(map[netip.Addr]bool, len(pod.Status.PodIPs)+1)
	for _, value := range append([]corev1.PodIP{{IP: pod.Status.PodIP}}, pod.Status.PodIPs...) {
		if ip, err := netip.ParseAddr(value.IP); err == nil && ip.Zone() == "" {
			ips[ip.Unmap()] = true
		}
	}
	for _, value := range endpoint.Addresses {
		ip, err := netip.ParseAddr(value)
		if err != nil || ip.Zone() != "" || !ips[ip.Unmap()] || (addressType == discoveryv1.AddressTypeIPv4) != ip.Unmap().Is4() {
			return fmt.Errorf("endpoint address does not match the Pod")
		}
		key := ip.Unmap()
		if owner, exists := owners[key]; exists && owner != pod.UID {
			return fmt.Errorf("ready endpoint address refers to multiple Pods")
		}
		owners[key] = pod.UID
	}
	return nil
}

func recheckEndpointSlices(ctx context.Context, slices typeddiscovery.EndpointSliceInterface, serviceName string, expected map[string]endpointSliceVersion) error {
	selector := labels.Set{discoveryv1.LabelServiceName: serviceName}.String()
	current, err := slices.List(ctx, metav1.ListOptions{LabelSelector: selector, Limit: 129})
	if err != nil {
		return fmt.Errorf("re-list EndpointSlices: %w", err)
	}
	if current.Continue != "" || len(current.Items) != len(expected) {
		return fmt.Errorf("%w: EndpointSlice set changed", errEndpointObservationChanged)
	}
	seen := make(map[string]bool, len(expected))
	for _, slice := range current.Items {
		if version, ok := expected[slice.Name]; !ok || seen[slice.Name] || version.uid != slice.UID || version.version != slice.ResourceVersion {
			return fmt.Errorf("%w: EndpointSlice version or identity changed", errEndpointObservationChanged)
		}
		seen[slice.Name] = true
	}
	return nil
}
