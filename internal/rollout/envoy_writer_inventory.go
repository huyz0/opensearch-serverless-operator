package rollout

import (
	"bytes"
	"context"
	"fmt"
	"maps"
	"net/netip"
	"net/url"
	"slices"
	"time"

	"golang.org/x/sync/errgroup"
	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/huyz0/opensearch-serverless-operator/api/v1alpha1"
	"github.com/huyz0/opensearch-serverless-operator/internal/java"
	"github.com/huyz0/opensearch-serverless-operator/internal/workload"
)

// KubernetesEnvoyWriterInventory derives selector-route evidence inputs from
// Kubernetes and direct Java stats. Reader should be an uncached API reader.
// Tunnels must be independently tied to the named Envoy Pod UIDs.
type KubernetesEnvoyWriterInventory struct {
	Reader                    client.Reader
	Kube                      kubernetes.Interface
	Namespace, Operation      string
	InstanceName, RouteName   string
	RouteURL, RouteAuthSecret string
	ReaderSecret              string
	Gateway, GatewayService   client.ObjectKey
	GatewayListener           string
	Tunnels                   map[string]EnvoyTunnel
	ProbeLease                func(context.Context, *v1alpha1.OpenSearchServerless, *corev1.Pod, *RouteCredential) (java.Lease, error)
}

func (s KubernetesEnvoyWriterInventory) Read(ctx context.Context) (EnvoyWriterWithdrawalInventory, error) {
	var result EnvoyWriterWithdrawalInventory
	if s.Reader == nil || s.Kube == nil || s.ProbeLease == nil || s.Namespace == "" || s.Operation == "" ||
		s.InstanceName == "" || s.RouteName == "" || s.Gateway.Namespace == "" || s.Gateway.Name == "" ||
		s.GatewayService.Namespace == "" || s.GatewayService.Name == "" || s.GatewayListener == "" ||
		s.RouteAuthSecret == "" || s.ReaderSecret == "" || s.ReaderSecret == s.RouteAuthSecret {
		return result, fmt.Errorf("exact writer, gateway, and independent authenticated probe configuration are required")
	}

	var op v1alpha1.OpenSearchServerlessOperation
	if err := s.Reader.Get(ctx, client.ObjectKey{Namespace: s.Namespace, Name: s.Operation}, &op); err != nil {
		return result, err
	}
	if err := op.Spec.Validate(); err != nil {
		return result, err
	}
	journal := op.Status.WriterRetirement
	if op.Spec.Action != "RetireWriter" || op.Spec.InstanceRef.Name != s.InstanceName || op.UID == "" || op.Generation <= 0 ||
		op.DeletionTimestamp != nil || journal == nil || !journal.WithdrawalArmed || journal.VictimGone ||
		journal.WithdrawnAt == nil || journal.WithdrawnAt.IsZero() || time.Now().Before(journal.WithdrawnAt.Time) ||
		journal.BindingDigest == "" || journal.ContainerID == "" || journal.NodeID == "" || journal.NodeEphemeralID == "" {
		return result, fmt.Errorf("operation does not have a live confirmed writer withdrawal")
	}
	if !slices.Contains(op.Finalizers, "serverless.opensearch.org/writer-retirement") {
		return result, fmt.Errorf("the writer retirement journal must be protected by its finalizer")
	}
	var instance v1alpha1.OpenSearchServerless
	if err := s.Reader.Get(ctx, client.ObjectKey{Namespace: s.Namespace, Name: s.InstanceName}, &instance); err != nil {
		return result, err
	}
	claim := v1alpha1.OperationClaim{Name: op.Name, UID: string(op.UID), Action: op.Spec.Action, Generation: op.Generation}
	fleet := instance.Status.ServingFleetIntentByRole["ingest"]
	if string(instance.UID) != op.Spec.InstanceRef.UID || instance.Generation != journal.InstanceGeneration ||
		instance.DeletionTimestamp != nil || instance.Status.ActiveOperation == nil || *instance.Status.ActiveOperation != claim ||
		fleet == "" || fleet != journal.ServingFleet || instance.Status.BasicAuthBaseline == nil || instance.Status.BasicAuthBaseline.SecretUID == "" {
		return result, fmt.Errorf("instance, writer claim, serving fleet or Basic Auth pin changed")
	}
	core := s.Kube.CoreV1()
	desired, err := workload.RoleService(&instance, "ingest", fleet, 9200)
	if err != nil {
		return result, err
	}
	service, err := core.Services(s.Namespace).Get(ctx, desired.Name, metav1.GetOptions{})
	if err != nil {
		return result, err
	}
	if journal.ServiceUID == "" || string(service.UID) != journal.ServiceUID || service.ResourceVersion == "" ||
		service.DeletionTimestamp != nil || !writerInventoryOwnedBy(service, &instance) ||
		service.Spec.Type != corev1.ServiceTypeClusterIP || service.Spec.ClusterIP == "" || service.Spec.ClusterIP == corev1.ClusterIPNone ||
		service.Spec.PublishNotReadyAddresses || (service.Spec.InternalTrafficPolicy != nil && *service.Spec.InternalTrafficPolicy != corev1.ServiceInternalTrafficPolicyCluster) ||
		len(service.Spec.Ports) != 1 || service.Spec.Ports[0] != desired.Spec.Ports[0] || !maps.Equal(service.Spec.Selector, desired.Spec.Selector) {
		return result, fmt.Errorf("pinned stable index Service changed or can publish unready backends")
	}

	route, gateway, replicas, err := s.readWriterEdge(ctx, service, journal)
	if err != nil {
		return result, err
	}
	proxies := make(map[string]EnvoyTunnel, len(replicas.UIDs))
	for uid := range replicas.UIDs {
		tunnel, exists := s.Tunnels[uid]
		if !exists {
			return result, fmt.Errorf("serving Envoy Pod %q has no verified tunnel", uid)
		}
		proxies[uid] = tunnel
	}
	edge, err := ReadRouteCredential(ctx, core, &instance, s.RouteAuthSecret)
	if err != nil || edge == nil {
		return result, fmt.Errorf("writer evidence requires an immutable authenticated edge credential")
	}
	edge.AuthProbeURL, err = BindAuthenticatedProbeURL(s.RouteURL, route)
	if err != nil {
		return result, err
	}
	base, err := url.Parse(edge.AuthProbeURL)
	if err != nil || base.Path != "" {
		return result, fmt.Errorf("writer evidence requires an index hostname routed at the root")
	}
	reader, err := ReadRouteCredential(ctx, core, &instance, s.ReaderSecret)
	if err != nil || reader == nil || reader.Username == edge.Username || bytes.Equal(reader.Password, edge.Password) {
		return result, fmt.Errorf("writer evidence requires an independent immutable Java reader credential")
	}

	target := op.Spec.WriterRetirement
	victim, err := core.Pods(s.Namespace).Get(ctx, target.PodName, metav1.GetOptions{})
	if err != nil {
		return result, err
	}
	if !validWriterInventoryPod(victim, &instance, fleet) || string(victim.UID) != target.PodUID ||
		writerInventoryContainerID(victim) != journal.ContainerID || !writerInventoryPodCondition(victim, corev1.PodReady, corev1.ConditionFalse) {
		return result, fmt.Errorf("exact withdrawn writer Pod or recorded container changed")
	}
	victimIPs, err := writerInventoryAddresses(victim)
	if err != nil {
		return result, err
	}
	backends, versions, err := s.readWriterSurvivors(ctx, &instance, service, fleet, victim, victimIPs, reader, journal)
	if err != nil {
		return result, err
	}

	currentService, err := core.Services(s.Namespace).Get(ctx, service.Name, metav1.GetOptions{})
	if err != nil || currentService.UID != service.UID || currentService.ResourceVersion != service.ResourceVersion ||
		!edge.Unchanged(ctx, core, s.Namespace) {
		return result, fmt.Errorf("stable index Service or edge credential changed while reading inventory")
	}
	var currentOp v1alpha1.OpenSearchServerlessOperation
	var currentInstance v1alpha1.OpenSearchServerless
	if err := s.Reader.Get(ctx, client.ObjectKeyFromObject(&op), &currentOp); err != nil {
		return result, err
	}
	if err := s.Reader.Get(ctx, client.ObjectKeyFromObject(&instance), &currentInstance); err != nil {
		return result, err
	}
	if currentOp.UID != op.UID || currentOp.ResourceVersion != op.ResourceVersion ||
		currentInstance.UID != instance.UID || currentInstance.ResourceVersion != instance.ResourceVersion {
		return result, fmt.Errorf("writer operation or instance changed while reading inventory")
	}

	result = EnvoyWriterWithdrawalInventory{
		Scope: WriterWithdrawalScope{
			RouteAuthIdentity: edge.RouteAuthIdentity,
			Namespace:         s.Namespace, InstanceUID: instance.UID, InstanceGeneration: instance.Generation, OperationUID: op.UID,
			VictimPodUID: victim.UID, VictimPodVersion: victim.ResourceVersion, Writer: java.WriterProcess{NodeID: journal.NodeID, EphemeralID: journal.NodeEphemeralID},
			ServiceName: service.Name, ServiceUID: service.UID, ServiceVersion: service.ResourceVersion,
			RouteName: route.GetName(), RouteUID: route.GetUID(), RouteVersion: route.GetResourceVersion(),
			GatewayNamespace: gateway.GetNamespace(), GatewayName: gateway.GetName(), GatewayUID: gateway.GetUID(), GatewayVersion: gateway.GetResourceVersion(),
			GatewayListener: s.GatewayListener, GatewayServiceUID: types.UID(journal.GatewayServiceUID),
		},
		WithdrawnAt: journal.WithdrawnAt.Time, Fleet: fleet, VictimAddresses: sortedWriterIPStrings(victimIPs),
		EndpointVersions: versions, Pods: backends, Proxies: proxies, GatewayVersions: replicas.Versions, Credential: edge,
	}
	return result, nil
}

func (s KubernetesEnvoyWriterInventory) readWriterEdge(ctx context.Context, service *corev1.Service, journal *v1alpha1.WriterRetirementStatus) (*unstructured.Unstructured, *unstructured.Unstructured, GatewayReplicaObservation, error) {
	var replicas GatewayReplicaObservation
	read := func(kind string, key client.ObjectKey, uid string) (*unstructured.Unstructured, error) {
		object := &unstructured.Unstructured{}
		object.SetGroupVersionKind(schema.GroupVersionKind{Group: "gateway.networking.k8s.io", Version: "v1", Kind: kind})
		if err := s.Reader.Get(ctx, key, object); err != nil {
			return nil, err
		}
		if uid == "" || string(object.GetUID()) != uid || object.GetResourceVersion() == "" || object.GetDeletionTimestamp() != nil {
			return nil, fmt.Errorf("pinned %s identity changed", kind)
		}
		return object, nil
	}
	route, err := read("HTTPRoute", client.ObjectKey{Namespace: s.Namespace, Name: s.RouteName}, journal.RouteUID)
	if err != nil {
		return nil, nil, replicas, err
	}
	parents, _, parentErr := unstructured.NestedSlice(route.Object, "spec", "parentRefs")
	if parentErr != nil || len(parents) != 1 || !WriterRouteUsesService(route, service.Name) {
		return nil, nil, replicas, fmt.Errorf("writer route requires one Gateway parent and selector rule")
	}
	parent, _ := parents[0].(map[string]any)
	parentNS, found, err := unstructured.NestedString(parent, "namespace")
	if !found {
		parentNS = s.Namespace
	}
	if err != nil || parentNS != s.Gateway.Namespace || parent["name"] != s.Gateway.Name || parent["sectionName"] != s.GatewayListener ||
		(parent["group"] != nil && parent["group"] != "gateway.networking.k8s.io") || (parent["kind"] != nil && parent["kind"] != "Gateway") ||
		(parent["port"] != nil && parent["port"] != int64(443)) {
		return nil, nil, replicas, fmt.Errorf("writer route does not bind the configured HTTPS parent")
	}
	gateway, err := read("Gateway", s.Gateway, journal.GatewayUID)
	if err != nil {
		return nil, nil, replicas, err
	}
	listeners, _, err := unstructured.NestedSlice(gateway.Object, "spec", "listeners")
	if err != nil || !slices.ContainsFunc(listeners, func(raw any) bool {
		listener, _ := raw.(map[string]any)
		return listener["name"] == s.GatewayListener && listener["protocol"] == "HTTPS" && listener["port"] == int64(443)
	}) {
		return nil, nil, replicas, fmt.Errorf("approved Gateway listener must use HTTPS on port 443")
	}
	core := s.Kube.CoreV1()
	frontend, err := core.Services(s.GatewayService.Namespace).Get(ctx, s.GatewayService.Name, metav1.GetOptions{})
	if err != nil {
		return nil, nil, replicas, err
	}
	if journal.GatewayServiceUID == "" || string(frontend.UID) != journal.GatewayServiceUID ||
		frontend.Labels["gateway.envoyproxy.io/owning-gateway-name"] != gateway.GetName() ||
		frontend.Labels["gateway.envoyproxy.io/owning-gateway-namespace"] != gateway.GetNamespace() {
		return nil, nil, replicas, fmt.Errorf("frontend Service does not belong to the pinned Envoy Gateway")
	}
	replicas, err = ObserveEnvoyGatewayEndpoints(ctx, core, s.Kube.DiscoveryV1(), frontend.Namespace, frontend.Name, frontend.UID)
	if err != nil {
		return nil, nil, replicas, err
	}
	if len(replicas.UIDs) == 0 || replicas.Versions["service/"+string(frontend.UID)] != frontend.ResourceVersion {
		return nil, nil, replicas, fmt.Errorf("pinned Envoy frontend Service changed or has no serving replicas")
	}
	return route, gateway, replicas, nil
}

func writerInventoryOwnedBy(object metav1.Object, instance *v1alpha1.OpenSearchServerless) bool {
	owner := metav1.GetControllerOf(object)
	return owner != nil && owner.UID == instance.UID && owner.Name == instance.Name && owner.Kind == "OpenSearchServerless" &&
		owner.APIVersion == v1alpha1.GroupVersion.String()
}

func validWriterInventoryPod(pod *corev1.Pod, instance *v1alpha1.OpenSearchServerless, fleet string) bool {
	zone, exists := instance.Spec.Pools["ingest"].Zones[pod.Labels[workload.ZoneLabel]]
	return pod.UID != "" && pod.ResourceVersion != "" && pod.DeletionTimestamp == nil && pod.Spec.NodeName != "" &&
		pod.Status.Phase == corev1.PodRunning && writerInventoryOwnedBy(pod, instance) && !workload.PodDisruptionTarget(pod) &&
		pod.Labels[workload.InstanceLabel] == string(instance.UID) && pod.Labels[workload.RoleLabel] == "ingest" &&
		pod.Labels[workload.FleetLabel] == fleet && exists && int64(zone.Replicas)+int64(zone.WarmSpare) > 0 &&
		pod.Annotations["serverless.opensearch.org/emergency-capacity"] != "true" &&
		pod.Annotations["serverless.opensearch.org/keystore-uid"] == instance.Status.BasicAuthBaseline.SecretUID &&
		writerInventoryContainerID(pod) != ""
}

func writerInventoryContainerID(pod *corev1.Pod) string {
	for _, container := range pod.Status.ContainerStatuses {
		if container.Name == "opensearch" && container.State.Running != nil {
			return container.ContainerID
		}
	}
	return ""
}

func writerInventoryPodCondition(pod *corev1.Pod, typ corev1.PodConditionType, status corev1.ConditionStatus) bool {
	for _, condition := range pod.Status.Conditions {
		if condition.Type == typ {
			return condition.Status == status
		}
	}
	return false
}

func writerInventoryAddresses(pod *corev1.Pod) (map[netip.Addr]bool, error) {
	ips := make(map[netip.Addr]bool)
	values := append([]corev1.PodIP{{IP: pod.Status.PodIP}}, pod.Status.PodIPs...)
	for _, value := range values {
		ip, err := netip.ParseAddr(value.IP)
		if err != nil || !ip.IsGlobalUnicast() || ip.Zone() != "" {
			return nil, fmt.Errorf("Pod %q has an invalid address", pod.Name)
		}
		ips[ip.Unmap()] = true
	}
	return ips, nil
}

func sortedWriterIPStrings(ips map[netip.Addr]bool) []string {
	values := make([]string, 0, len(ips))
	for ip := range ips {
		values = append(values, ip.String())
	}
	slices.Sort(values)
	return values
}

type writerSurvivor struct {
	pod *corev1.Pod
	ips map[netip.Addr]bool
}

func (s KubernetesEnvoyWriterInventory) readWriterSurvivors(ctx context.Context, instance *v1alpha1.OpenSearchServerless, service *corev1.Service, fleet string, victim *corev1.Pod, victimIPs map[netip.Addr]bool, credential *RouteCredential, journal *v1alpha1.WriterRetirementStatus) (map[string]EnvoyBackend, map[types.UID]string, error) {
	options := metav1.ListOptions{LabelSelector: labels.Set{discoveryv1.LabelServiceName: service.Name}.String(), Limit: 129}
	discovery := s.Kube.DiscoveryV1().EndpointSlices(s.Namespace)
	inventory, err := discovery.List(ctx, options)
	if err != nil {
		return nil, nil, err
	}
	if inventory.Continue != "" || len(inventory.Items) == 0 || len(inventory.Items) > 128 {
		return nil, nil, fmt.Errorf("index EndpointSlice inventory is incomplete")
	}
	pods := s.Kube.CoreV1().Pods(s.Namespace)
	survivors := make(map[string]writerSurvivor)
	versions := make(map[types.UID]string, len(inventory.Items))
	endpointCount := 0
	for _, slice := range inventory.Items {
		owner := metav1.GetControllerOf(&slice)
		if slice.UID == "" || slice.ResourceVersion == "" || versions[slice.UID] != "" || slice.DeletionTimestamp != nil ||
			owner == nil || owner.UID != service.UID || owner.Name != service.Name || owner.Kind != "Service" || owner.APIVersion != "v1" ||
			slice.Labels[discoveryv1.LabelServiceName] != service.Name || slice.Labels[discoveryv1.LabelManagedBy] != "endpointslice-controller.k8s.io" ||
			(slice.AddressType != discoveryv1.AddressTypeIPv4 && slice.AddressType != discoveryv1.AddressTypeIPv6) ||
			len(slice.Ports) != 1 || slice.Ports[0].Name == nil || *slice.Ports[0].Name != "http" || slice.Ports[0].Port == nil ||
			*slice.Ports[0].Port != 9200 || slice.Ports[0].Protocol == nil || *slice.Ports[0].Protocol != corev1.ProtocolTCP {
			return nil, nil, fmt.Errorf("invalid index EndpointSlice identity, owner or port")
		}
		versions[slice.UID] = slice.ResourceVersion
		endpointCount += len(slice.Endpoints)
		if endpointCount > 512 {
			return nil, nil, fmt.Errorf("too many index endpoints for one observation")
		}
		for _, endpoint := range slice.Endpoints {
			conditions := endpoint.Conditions
			if conditions.Ready == nil || (conditions.Terminating != nil && *conditions.Terminating &&
				(conditions.Serving == nil || *conditions.Serving)) {
				return nil, nil, fmt.Errorf("index endpoint has unknown readiness or can still serve while terminating")
			}
			if !*conditions.Ready && conditions.Serving != nil && !*conditions.Serving {
				continue
			}
			ref := endpoint.TargetRef
			if ref != nil && (ref.UID == victim.UID || (ref.Name == victim.Name && ref.Namespace == victim.Namespace)) {
				return nil, nil, fmt.Errorf("withdrawn writer remains eligible in an EndpointSlice")
			}
			addresses := make(map[netip.Addr]bool, len(endpoint.Addresses))
			for _, value := range endpoint.Addresses {
				ip, err := netip.ParseAddr(value)
				if err != nil || !ip.IsGlobalUnicast() || ip.Zone() != "" || (slice.AddressType == discoveryv1.AddressTypeIPv4) != ip.Is4() || victimIPs[ip.Unmap()] {
					return nil, nil, fmt.Errorf("index endpoint address is invalid or still belongs to the withdrawn writer")
				}
				addresses[ip.Unmap()] = true
			}
			if !*conditions.Ready {
				continue
			}
			if (conditions.Terminating != nil && *conditions.Terminating) || (conditions.Serving != nil && !*conditions.Serving) ||
				ref == nil || ref.Kind != "Pod" || (ref.APIVersion != "" && ref.APIVersion != "v1") ||
				ref.Namespace != s.Namespace || ref.Name == "" || ref.UID == "" || len(addresses) == 0 {
				return nil, nil, fmt.Errorf("ready index endpoint does not identify one live Pod")
			}
			survivor, found := survivors[ref.Name]
			if !found {
				pod, err := pods.Get(ctx, ref.Name, metav1.GetOptions{})
				if err != nil {
					return nil, nil, err
				}
				survivor = writerSurvivor{pod: pod, ips: make(map[netip.Addr]bool)}
			}
			pod := survivor.pod
			if pod.UID != ref.UID || !validWriterInventoryPod(pod, instance, fleet) ||
				!writerInventoryPodCondition(pod, corev1.PodReady, corev1.ConditionTrue) ||
				(endpoint.NodeName != nil && *endpoint.NodeName != pod.Spec.NodeName) ||
				(endpoint.Zone != nil && *endpoint.Zone != pod.Labels[workload.ZoneLabel]) {
				return nil, nil, fmt.Errorf("ready index endpoint Pod identity, role or placement changed")
			}
			podIPs, err := writerInventoryAddresses(pod)
			if err != nil {
				return nil, nil, err
			}
			for address := range addresses {
				if !podIPs[address] {
					return nil, nil, fmt.Errorf("ready index endpoint IP does not belong to its target Pod")
				}
				survivor.ips[address] = true
			}
			survivors[ref.Name] = survivor
		}
	}
	if len(survivors) == 0 {
		return nil, nil, fmt.Errorf("no ready regular index survivors remain")
	}

	targets := make([]*corev1.Pod, 0, 1+len(survivors))
	targets = append(targets, victim)
	for _, survivor := range survivors {
		targets = append(targets, survivor.pod)
	}
	leases := make([]java.Lease, len(targets))
	group, work := errgroup.WithContext(ctx)
	group.SetLimit(8)
	for i, pod := range targets {
		group.Go(func() error {
			started := time.Now()
			lease, err := s.ProbeLease(work, instance, pod, credential)
			if err != nil {
				return fmt.Errorf("direct Java stats on Pod %q: %w", pod.Name, err)
			}
			if lease.NodeName != pod.Name || lease.NodeID == "" || lease.NodeEphemeralID == "" || lease.ProcessIncarnation == "" ||
				len(lease.Roles) != 1 || !lease.HasRole("ingest") || !time.Now().Before(lease.EvidenceValidUntil(started, time.Second)) {
				return fmt.Errorf("Pod %q did not report a live exact ingest JVM", pod.Name)
			}
			current, err := pods.Get(work, pod.Name, metav1.GetOptions{})
			if err != nil || current.UID != pod.UID || current.ResourceVersion != pod.ResourceVersion {
				return fmt.Errorf("Pod %q changed while probing its Java process", pod.Name)
			}
			leases[i] = lease
			return nil
		})
	}
	if err := group.Wait(); err != nil {
		return nil, nil, err
	}
	if leases[0].NodeID != journal.NodeID || leases[0].NodeEphemeralID != journal.NodeEphemeralID {
		return nil, nil, fmt.Errorf("live withdrawn Java process differs from the operation journal")
	}
	backends := make(map[string]EnvoyBackend, len(survivors))
	seenNodes := map[string]bool{leases[0].NodeID: true}
	seenPods := map[types.UID]bool{victim.UID: true}
	for i, pod := range targets[1:] {
		lease := leases[i+1]
		if seenNodes[lease.NodeID] || seenPods[pod.UID] {
			return nil, nil, fmt.Errorf("index endpoints do not identify distinct Pods and Java nodes")
		}
		seenNodes[lease.NodeID] = true
		seenPods[pod.UID] = true
		backends[pod.Name] = EnvoyBackend{
			UID: string(pod.UID), ResourceVersion: pod.ResourceVersion, Fleet: fleet,
			ProcessIncarnation: lease.ProcessIncarnation, Addresses: sortedWriterIPStrings(survivors[pod.Name].ips),
		}
	}
	current, err := discovery.List(ctx, options)
	if err != nil || current.Continue != "" || len(current.Items) != len(versions) {
		return nil, nil, fmt.Errorf("index EndpointSlice set changed during Java probing")
	}
	latest := make(map[types.UID]string, len(current.Items))
	for _, slice := range current.Items {
		latest[slice.UID] = slice.ResourceVersion
	}
	if !maps.Equal(latest, versions) || !credential.Unchanged(ctx, s.Kube.CoreV1(), s.Namespace) {
		return nil, nil, fmt.Errorf("index EndpointSlices or Java reader credential changed during probing")
	}
	return backends, versions, nil
}
