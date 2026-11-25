package controller

import (
	"context"
	"fmt"
	"maps"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/huyz0/opensearch-serverless-operator/api/v1alpha1"
	"github.com/huyz0/opensearch-serverless-operator/internal/rollout"
)

type writerEnvoyObservation struct {
	validUntil time.Time
	unchanged  func(context.Context) error
}

func (r writerRuntime) observeWriterEnvoyWithdrawal(ctx context.Context, instance *v1alpha1.OpenSearchServerless, intent writerPreparationIntent, access writerPreparationAccess, victimVersion string, observed roleEndpointObservation) (writerEnvoyObservation, error) {
	var result writerEnvoyObservation
	route, err := readWriterGatewayObject(ctx, r.reader, "HTTPRoute", client.ObjectKey{Namespace: instance.Namespace, Name: intent.RouteName})
	if err != nil {
		return result, err
	}
	if route.GetUID() != intent.RouteUID || route.GetDeletionTimestamp() != nil || !rollout.WriterRouteUsesService(route, observed.ServiceName) {
		return result, fmt.Errorf("approved index route no longer targets the stable Service")
	}
	gateway, err := r.readWriterGateway(ctx, intent, route)
	if err != nil {
		return result, err
	}
	replicas, err := rollout.ObserveEnvoyGatewayEndpoints(ctx, r.core, r.discovery, intent.GatewayService.Namespace, intent.GatewayService.Name, intent.GatewayServiceUID)
	if err != nil {
		return result, err
	}
	credential, err := rollout.ReadRouteCredential(ctx, r.core, instance, access.RouteAuthSecret)
	if err != nil || credential == nil {
		return result, fmt.Errorf("qualified index route authentication is required")
	}
	credential.AuthProbeURL, err = rollout.BindAuthenticatedProbeURL(access.RouteURL, route)
	if err != nil {
		return result, err
	}
	cm, err := r.core.ConfigMaps(instance.Namespace).Get(ctx, rollout.WriterWithdrawalEvidenceName(intent.OperationUID), metav1.GetOptions{})
	if err != nil {
		return result, err
	}
	if cm.UID == "" || cm.ResourceVersion == "" || cm.DeletionTimestamp != nil || !credential.Unchanged(ctx, r.core, instance.Namespace) {
		return result, fmt.Errorf("Envoy evidence or its route credential changed")
	}
	endpoints := make(map[types.UID]string, len(observed.SliceVersions))
	for _, version := range observed.SliceVersions {
		endpoints[version.uid] = version.version
	}
	scope := rollout.WriterWithdrawalScope{
		RouteAuthIdentity: credential.RouteAuthIdentity,
		Namespace:         instance.Namespace, InstanceUID: instance.UID, InstanceGeneration: instance.Generation, OperationUID: intent.OperationUID,
		VictimPodUID: types.UID(intent.Victim.UID), VictimPodVersion: victimVersion, Writer: intent.Process,
		ServiceName: observed.ServiceName, ServiceUID: observed.ServiceUID, ServiceVersion: observed.ServiceVersion,
		RouteName: route.GetName(), RouteUID: route.GetUID(), RouteVersion: route.GetResourceVersion(),
		GatewayNamespace: gateway.GetNamespace(), GatewayName: gateway.GetName(), GatewayUID: gateway.GetUID(), GatewayVersion: gateway.GetResourceVersion(),
		GatewayListener: intent.GatewayListener, GatewayServiceUID: intent.GatewayServiceUID,
	}
	result.validUntil, err = rollout.ObserveSignedWriterWithdrawal(cm.Data, access.PlatformKey, scope, endpoints, observed.ReadyPodVersions, replicas, intent.WithdrawnAt, time.Now())
	if err != nil {
		return result, err
	}
	reportUnchanged := configMapVersionCheck(r.core, cm)
	result.unchanged = func(ctx context.Context) error {
		for _, object := range []*unstructured.Unstructured{route, gateway} {
			live, err := readWriterGatewayObject(ctx, r.reader, object.GetKind(), client.ObjectKeyFromObject(object))
			if err != nil || live.GetDeletionTimestamp() != nil || live.GetUID() != object.GetUID() || live.GetResourceVersion() != object.GetResourceVersion() {
				return fmt.Errorf("Envoy %s changed during writer eviction", object.GetKind())
			}
		}
		live, err := rollout.ObserveEnvoyGatewayEndpoints(ctx, r.core, r.discovery, intent.GatewayService.Namespace, intent.GatewayService.Name, intent.GatewayServiceUID)
		if err != nil || !maps.Equal(live.UIDs, replicas.UIDs) || !maps.Equal(live.Versions, replicas.Versions) {
			return fmt.Errorf("Envoy Gateway endpoints changed during writer eviction")
		}
		if !reportUnchanged(ctx) || !credential.Unchanged(ctx, r.core, instance.Namespace) {
			return fmt.Errorf("Envoy evidence or its route credential changed during writer eviction")
		}
		return nil
	}
	return result, nil
}

func (r writerRuntime) readWriterGateway(ctx context.Context, intent writerPreparationIntent, route *unstructured.Unstructured) (*unstructured.Unstructured, error) {
	refs, _, _ := unstructured.NestedSlice(route.Object, "spec", "parentRefs")
	if len(refs) != 1 || intent.Gateway.Namespace == "" || intent.Gateway.Name == "" || intent.GatewayUID == "" || intent.GatewayListener == "" ||
		intent.GatewayService.Name == "" || intent.GatewayServiceUID == "" {
		return nil, fmt.Errorf("one pinned Envoy HTTPS parent and frontend Service are required")
	}
	ref, _ := refs[0].(map[string]any)
	namespace, found, err := unstructured.NestedString(ref, "namespace")
	if !found {
		namespace = route.GetNamespace()
	}
	if err != nil || namespace != intent.Gateway.Namespace || ref["name"] != intent.Gateway.Name || ref["sectionName"] != intent.GatewayListener ||
		(ref["group"] != nil && ref["group"] != "gateway.networking.k8s.io") || (ref["kind"] != nil && ref["kind"] != "Gateway") ||
		(ref["port"] != nil && ref["port"] != int64(443)) {
		return nil, fmt.Errorf("index HTTPRoute does not bind the pinned HTTPS parent")
	}
	gateway, err := readWriterGatewayObject(ctx, r.reader, "Gateway", intent.Gateway)
	if err != nil {
		return nil, err
	}
	if gateway.GetUID() != intent.GatewayUID || gateway.GetDeletionTimestamp() != nil {
		return nil, fmt.Errorf("pinned Gateway changed")
	}
	listeners, _, _ := unstructured.NestedSlice(gateway.Object, "spec", "listeners")
	for _, raw := range listeners {
		listener, _ := raw.(map[string]any)
		if listener["name"] == intent.GatewayListener && listener["protocol"] == "HTTPS" && listener["port"] == int64(443) {
			service, err := r.core.Services(intent.GatewayService.Namespace).Get(ctx, intent.GatewayService.Name, metav1.GetOptions{})
			if err != nil {
				return nil, err
			}
			if service.UID == intent.GatewayServiceUID && service.Labels["gateway.envoyproxy.io/owning-gateway-name"] == intent.Gateway.Name &&
				service.Labels["gateway.envoyproxy.io/owning-gateway-namespace"] == intent.Gateway.Namespace {
				return gateway, nil
			}
		}
	}
	return nil, fmt.Errorf("frontend Service or HTTPS listener does not match the pinned Envoy Gateway")
}
