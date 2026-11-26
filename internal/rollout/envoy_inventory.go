package rollout

import (
	"context"
	"crypto/ed25519"
	"fmt"
	"slices"
	"time"

	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	typedcore "k8s.io/client-go/kubernetes/typed/core/v1"
	typeddiscovery "k8s.io/client-go/kubernetes/typed/discovery/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/huyz0/opensearch-serverless-operator/api/v1alpha1"
	"github.com/huyz0/opensearch-serverless-operator/internal/workload"
)

// KubernetesEnvoyInventory binds the collector to a claimed role, approved
// application identities and the Gateway's serving Service membership.
type KubernetesEnvoyInventory struct {
	Reader                     client.Reader
	Kube                       kubernetes.Interface
	Namespace, Operation, Role string
	RouteURL, RouteAuthSecret  string
	GatewayService             client.ObjectKey
	GatewayServiceUID          types.UID
	PlatformKey                ed25519.PublicKey
	Tunnels                    map[string]EnvoyTunnel
}

func (s KubernetesEnvoyInventory) Read(ctx context.Context) (EnvoyInventory, error) {
	if s.Reader == nil || s.Kube == nil || s.Namespace == "" || s.Operation == "" ||
		(s.Role != "search" && s.Role != "ingest") || len(s.PlatformKey) != ed25519.PublicKeySize ||
		s.GatewayService.Namespace == "" || s.GatewayService.Name == "" || s.GatewayServiceUID == "" {
		return EnvoyInventory{}, fmt.Errorf("operation, role, Gateway Service and trusted platform key are required")
	}
	var op v1alpha1.OpenSearchServerlessOperation
	if err := s.Reader.Get(ctx, client.ObjectKey{Namespace: s.Namespace, Name: s.Operation}, &op); err != nil {
		return EnvoyInventory{}, err
	}
	if err := op.Spec.Validate(); err != nil {
		return EnvoyInventory{}, err
	}
	journal := op.Status.WeightedRollout
	if journal == nil || op.DeletionTimestamp != nil {
		return EnvoyInventory{}, fmt.Errorf("operation does not have a live weighted journal")
	}
	part, found := journal.Roles[s.Role]
	if !found || part.RouteName == "" || part.RouteUID == "" {
		return EnvoyInventory{}, fmt.Errorf("journal does not bind this role")
	}
	var instance v1alpha1.OpenSearchServerless
	if err := s.Reader.Get(ctx, client.ObjectKey{Namespace: s.Namespace, Name: op.Spec.InstanceRef.Name}, &instance); err != nil {
		return EnvoyInventory{}, err
	}
	claim := v1alpha1.OperationClaim{Name: op.Name, UID: string(op.UID), Action: op.Spec.Action, Generation: op.Generation}
	if string(instance.UID) != op.Spec.InstanceRef.UID || instance.Generation != journal.InstanceGeneration ||
		instance.DeletionTimestamp != nil || instance.Spec.Rollout == nil || !instance.Spec.Rollout.GatewayBackends ||
		instance.Status.ActiveOperation == nil || *instance.Status.ActiveOperation != claim {
		return EnvoyInventory{}, fmt.Errorf("instance or claim changed")
	}
	route := &unstructured.Unstructured{}
	route.SetGroupVersionKind(schema.GroupVersionKind{Group: "gateway.networking.k8s.io", Version: "v1", Kind: "HTTPRoute"})
	if err := s.Reader.Get(ctx, client.ObjectKey{Namespace: s.Namespace, Name: part.RouteName}, route); err != nil {
		return EnvoyInventory{}, err
	}
	if string(route.GetUID()) != part.RouteUID || route.GetDeletionTimestamp() != nil || route.GetResourceVersion() == "" {
		return EnvoyInventory{}, fmt.Errorf("weighted HTTPRoute identity changed")
	}
	weights, err := s.readRouteWeights(route, &instance, part)
	if err != nil {
		return EnvoyInventory{}, err
	}
	credential, err := ReadRouteCredential(ctx, s.Kube.CoreV1(), &instance, s.RouteAuthSecret)
	if err != nil {
		return EnvoyInventory{}, err
	}
	inventory := EnvoyInventory{
		WeightEvidenceScope: WeightEvidenceScope{
			Namespace: s.Namespace, InstanceUID: instance.UID, InstanceGeneration: instance.Generation,
			OperationUID: op.UID, Role: s.Role, RouteName: route.GetName(), RouteUID: route.GetUID(), RouteVersion: route.GetResourceVersion(),
		},
		Weights: weights, Pods: make(map[string]EnvoyBackend), routeCredential: credential,
	}
	if credential != nil {
		credential.AuthProbeURL, err = BindAuthenticatedProbeURL(s.RouteURL, route)
		if err != nil {
			return EnvoyInventory{}, err
		}
		inventory.RouteAuthIdentity = credential.RouteAuthIdentity
	}
	if err := s.captureServingTunnels(ctx, &inventory); err != nil {
		return EnvoyInventory{}, err
	}
	for fleet, weight := range weights {
		if weight > 0 {
			if err := s.addQualifiedFleet(ctx, &instance, &op, fleet, part.Revisions[fleet], inventory.Pods); err != nil {
				return EnvoyInventory{}, err
			}
		}
	}
	return inventory, nil
}

func (s KubernetesEnvoyInventory) readRouteWeights(route *unstructured.Unstructured, instance *v1alpha1.OpenSearchServerless, part v1alpha1.WeightedRoleStatus) (map[string]int, error) {
	rules, _, _ := unstructured.NestedSlice(route.Object, "spec", "rules")
	if len(rules) != 1 {
		return nil, fmt.Errorf("weighted HTTPRoute must have one role rule")
	}
	rule, _ := rules[0].(map[string]any)
	refs, _ := rule["backendRefs"].([]any)
	if len(refs) != len(part.BackendUIDs) {
		return nil, fmt.Errorf("weighted HTTPRoute must reference its pinned fleet Services")
	}
	fleets := make(map[string]string, len(refs))
	for fleet, uid := range part.BackendUIDs {
		pin := instance.Status.GatewayBackends[s.Role+"/"+fleet]
		if uid == "" || pin.ServiceUID != uid || pin.Name == "" || pin.Role != s.Role || pin.Fleet != fleet || fleets[pin.Name] != "" {
			return nil, fmt.Errorf("pinned fleet Service identity changed")
		}
		fleets[pin.Name] = fleet
	}
	weights := make(map[string]int, len(refs))
	for _, item := range refs {
		ref, _ := item.(map[string]any)
		name, _ := ref["name"].(string)
		fleet := fleets[name]
		_, duplicate := weights[fleet]
		port, _ := ref["port"].(int64)
		weight, explicit := ref["weight"].(int64)
		if fleet == "" || duplicate || port != 9200 || !explicit || weight < 0 || weight > 100 ||
			(ref["group"] != nil && ref["group"] != "") || (ref["kind"] != nil && ref["kind"] != "Service") ||
			(ref["namespace"] != nil && ref["namespace"] != s.Namespace) {
			return nil, fmt.Errorf("HTTPRoute refers to an unexpected weighted backend")
		}
		weights[fleet] = int(weight)
	}
	if !validWeights(weights) {
		return nil, fmt.Errorf("HTTPRoute weights must add to 100")
	}
	return weights, nil
}

func (s KubernetesEnvoyInventory) captureServingTunnels(ctx context.Context, inventory *EnvoyInventory) error {
	observation, err := ObserveEnvoyGatewayEndpoints(ctx, s.Kube.CoreV1(), s.Kube.DiscoveryV1(), s.GatewayService.Namespace, s.GatewayService.Name, s.GatewayServiceUID)
	if err != nil {
		return err
	}
	inventory.Proxies = make(map[string]EnvoyTunnel, len(observation.UIDs))
	inventory.GatewayVersions = observation.Versions
	for uid := range observation.UIDs {
		tunnel, found := s.Tunnels[uid]
		if !found {
			return fmt.Errorf("serving Gateway Pod UID %q has no verified tunnel", uid)
		}
		inventory.Proxies[uid] = tunnel
	}
	return nil
}

type GatewayReplicaObservation struct {
	UIDs     map[string]bool
	Versions map[string]string
	PodNames map[string]string
}

// ObserveEnvoyGatewayEndpoints independently reads every currently serving
// replica of the pinned frontend Service.
func ObserveEnvoyGatewayEndpoints(ctx context.Context, core typedcore.CoreV1Interface, discovery typeddiscovery.DiscoveryV1Interface, namespace, name string, uid types.UID) (GatewayReplicaObservation, error) {
	if core == nil || discovery == nil || namespace == "" || name == "" || uid == "" {
		return GatewayReplicaObservation{}, fmt.Errorf("pinned Gateway Service and clients are required")
	}
	service, err := core.Services(namespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return GatewayReplicaObservation{}, err
	}
	if service.UID != uid || service.ResourceVersion == "" || service.DeletionTimestamp != nil ||
		service.Spec.PublishNotReadyAddresses {
		return GatewayReplicaObservation{}, fmt.Errorf("Gateway Service changed or does not expose only ready endpoints")
	}
	list, err := discovery.EndpointSlices(service.Namespace).List(ctx, metav1.ListOptions{
		LabelSelector: labels.Set{discoveryv1.LabelServiceName: service.Name}.String(), Limit: 129,
	})
	if err != nil {
		return GatewayReplicaObservation{}, err
	}
	if len(list.Items) == 0 || len(list.Items) > 128 || list.Continue != "" {
		return GatewayReplicaObservation{}, fmt.Errorf("Gateway Service endpoint inventory is incomplete")
	}
	observation := GatewayReplicaObservation{
		UIDs: make(map[string]bool), PodNames: make(map[string]string),
		Versions: map[string]string{"service/" + string(service.UID): service.ResourceVersion},
	}
	for _, slice := range list.Items {
		owner := metav1.GetControllerOf(&slice)
		if owner == nil || owner.UID != service.UID || owner.Name != service.Name || owner.Kind != "Service" ||
			owner.APIVersion != "v1" || slice.DeletionTimestamp != nil || slice.UID == "" || slice.ResourceVersion == "" ||
			slice.Labels[discoveryv1.LabelManagedBy] != "endpointslice-controller.k8s.io" {
			return GatewayReplicaObservation{}, fmt.Errorf("Gateway EndpointSlice is outside the pinned Service")
		}
		observation.Versions["slice/"+string(slice.UID)] = slice.ResourceVersion
		for _, endpoint := range slice.Endpoints {
			if endpoint.Conditions.Ready == nil || (endpoint.Conditions.Terminating != nil && *endpoint.Conditions.Terminating &&
				(endpoint.Conditions.Serving == nil || *endpoint.Conditions.Serving)) {
				return GatewayReplicaObservation{}, fmt.Errorf("Gateway replica has unknown or draining serving state")
			}
			if !*endpoint.Conditions.Ready {
				continue
			}
			ref := endpoint.TargetRef
			if ref == nil || ref.Kind != "Pod" || ref.Namespace != service.Namespace || ref.Name == "" || ref.UID == "" {
				return GatewayReplicaObservation{}, fmt.Errorf("ready Gateway endpoint has no Pod identity")
			}
			observation.UIDs[string(ref.UID)] = true
			observation.PodNames[string(ref.UID)] = ref.Name
		}
	}
	return observation, nil
}

func (s KubernetesEnvoyInventory) addQualifiedFleet(ctx context.Context, instance *v1alpha1.OpenSearchServerless, op *v1alpha1.OpenSearchServerlessOperation, fleet, revision string, result map[string]EnvoyBackend) error {
	core := s.Kube.CoreV1()
	name := FleetPlatformEvidenceName(op.UID, s.Role, fleet, op.Spec.TargetFleet)
	cm, err := core.ConfigMaps(s.Namespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return err
	}
	approval, err := ReadSignedPlatformEvidence(cm.Data, s.PlatformKey)
	if err != nil || cm.DeletionTimestamp != nil || revision == "" ||
		approval.Namespace != s.Namespace || approval.InstanceUID != instance.UID || approval.InstanceGeneration != instance.Generation ||
		approval.OperationUID != op.UID || approval.Fleet != fleet || approval.Revision != revision || approval.Role != s.Role ||
		!approval.PeerQualified || !approval.SecurityQualified || !approval.RollbackCompatible || !freshWeightObservation(time.Now(), approval.ObservedAt) {
		return fmt.Errorf("fleet %q has no current qualified platform statement", fleet)
	}
	approved := make(map[types.UID]PlatformPod, len(approval.Pods))
	for _, pod := range approval.Pods {
		if pod.UID == "" || approved[pod.UID].UID != "" || pod.ResourceVersion == "" || pod.ProcessIncarnation == "" {
			return fmt.Errorf("fleet %q contains an incomplete or duplicate Pod approval", fleet)
		}
		approved[pod.UID] = pod
	}
	selector := labels.Set{workload.InstanceLabel: string(instance.UID), workload.RoleLabel: s.Role, workload.FleetLabel: fleet}.String()
	pods, err := core.Pods(s.Namespace).List(ctx, metav1.ListOptions{LabelSelector: selector, Limit: 513})
	if err != nil {
		return err
	}
	if pods.Continue != "" || len(pods.Items) > 512 || len(pods.Items) == 0 || len(pods.Items) != len(approved) {
		return fmt.Errorf("fleet %q Pod inventory differs from its approval", fleet)
	}
	for _, pod := range pods.Items {
		pin := approved[pod.UID]
		owner := metav1.GetControllerOf(&pod)
		if pin.UID == "" || pin.ResourceVersion != pod.ResourceVersion || owner == nil || owner.UID != instance.UID ||
			owner.Name != instance.Name || owner.Kind != "OpenSearchServerless" || owner.APIVersion != v1alpha1.GroupVersion.String() ||
			pod.Status.Phase != corev1.PodRunning || pod.DeletionTimestamp != nil ||
			pod.Spec.NodeName == "" || pod.Spec.NodeName != pin.NodeName || pod.Labels[workload.ZoneLabel] != pin.Zone ||
			pod.Labels["serverless.opensearch.org/revision"] != revision || result[pod.Name].UID != "" {
			return fmt.Errorf("fleet %q Pod %q changed or is unavailable", fleet, pod.Name)
		}
		var addresses []string
		for _, ip := range pod.Status.PodIPs {
			addresses = append(addresses, ip.IP)
		}
		if len(addresses) == 0 && pod.Status.PodIP != "" {
			addresses = append(addresses, pod.Status.PodIP)
		}
		slices.Sort(addresses)
		result[pod.Name] = EnvoyBackend{UID: string(pod.UID), ResourceVersion: pod.ResourceVersion, Fleet: fleet, ProcessIncarnation: pin.ProcessIncarnation, Addresses: addresses}
	}
	return nil
}
