package controller

import (
	"context"
	"fmt"
	"time"

	"k8s.io/apimachinery/pkg/types"

	"github.com/huyz0/opensearch-serverless-operator/api/v1alpha1"
	"github.com/huyz0/opensearch-serverless-operator/internal/rollout"
)

// externalRouteObservation must come from an independently qualified probe of
// the configured production client path. A ClusterIP probe is not sufficient
// for an ingress or load-balancer path.
type externalRouteObservation struct {
	InstanceUID, OperationUID, Role, Fleet string
	ServiceUID, ServiceVersion             string
	RouteID                                string
	RespondingPodUID                       types.UID
	RespondingPodVersion                   string
	ObservedAt                             time.Time
	ValidUntil                             time.Time
}

type rolloutRouteObservation struct {
	*rollout.RouteEvidence
	Endpoints roleEndpointObservation
}

// probeAndObserveRolloutRoute obtains both observations for one applied role.
// The caller supplies the route configuration and directly qualified processes.
func (r *OperationReconciler) probeAndObserveRolloutRoute(ctx context.Context, instance *v1alpha1.OpenSearchServerless, operation *v1alpha1.OpenSearchServerlessOperation, role string, route rolloutRouteConfig, minima map[string]int, qualifiedProcesses map[types.UID]string, minLease, maxAge time.Duration) (*rolloutRouteObservation, error) {
	if maxAge <= 0 {
		return nil, fmt.Errorf("a positive route observation window is required")
	}
	roles, err := rolloutProgress(instance, operation)
	if err != nil {
		return nil, err
	}
	for _, progress := range roles {
		if progress.Intent.Role == role {
			ctx, cancel := context.WithTimeout(ctx, maxAge)
			defer cancel()
			edge, err := probeRolloutEdge(ctx, r.Core, instance, progress.Intent, route, qualifiedProcesses, minLease)
			if err != nil {
				return nil, err
			}
			qualifiedPods := make(map[types.UID]struct{}, len(qualifiedProcesses))
			for uid := range qualifiedProcesses {
				qualifiedPods[uid] = struct{}{}
			}
			return r.observeRolloutRoute(ctx, instance, operation, role, minima, qualifiedPods, edge, maxAge)
		}
	}
	return nil, fmt.Errorf("requested role is not part of the operation")
}

// observeRolloutRoute combines a fresh EndpointSlice observation with a
// separate qualified edge probe. The caller must establish the qualified Pod
// set, actual zones and authenticity of the external probe independently.
func (r *OperationReconciler) observeRolloutRoute(ctx context.Context, instance *v1alpha1.OpenSearchServerless, operation *v1alpha1.OpenSearchServerlessOperation, role string, minima map[string]int, qualifiedPods map[types.UID]struct{}, edge externalRouteObservation, maxAge time.Duration) (*rolloutRouteObservation, error) {
	roles, err := rolloutProgress(instance, operation)
	if err != nil {
		return nil, err
	}
	var intent rollout.SwitchIntent
	for _, progress := range roles {
		if progress.Intent.Role == role {
			intent = progress.Intent
		}
	}
	if intent.Role == "" || intent.Phase != rollout.Applied || maxAge <= 0 || r.Core == nil || r.Discovery == nil {
		return nil, fmt.Errorf("applied role and endpoint clients are required")
	}
	endpoints, err := observeRoleEndpoints(ctx, r.Core, r.Discovery, instance, role, intent.TargetFleet, types.UID(intent.ServiceUID), minima, qualifiedPods)
	if err != nil {
		return nil, err
	}
	if len(endpoints.ReadyPodVersions) < servingRegionalFloor(instance, role) {
		return nil, fmt.Errorf("%s route is below the regional serving floor", role)
	}
	now := time.Now()
	if r.Now != nil {
		now = r.Now()
	}
	_, respondingPodQualified := qualifiedPods[edge.RespondingPodUID]
	respondingPodVersion := endpoints.ReadyPodVersions[edge.RespondingPodUID]
	if edge.RespondingPodVersion == "" || edge.RespondingPodVersion != respondingPodVersion {
		return nil, fmt.Errorf("responding Pod changed or left the ready endpoints after the production probe")
	}
	if edge.InstanceUID != intent.InstanceUID || edge.OperationUID != intent.OperationUID || edge.Role != role || edge.Fleet != intent.TargetFleet || edge.ServiceUID != intent.ServiceUID || edge.ServiceVersion != endpoints.ServiceVersion || edge.RouteID == "" || edge.RespondingPodUID == "" || !respondingPodQualified || endpoints.InstanceUID != instance.UID || endpoints.Role != role || endpoints.Fleet != intent.TargetFleet || string(endpoints.ServiceUID) != intent.ServiceUID || endpoints.ObservedAt.IsZero() || edge.ObservedAt.IsZero() || endpoints.ObservedAt.Before(intent.AppliedAt) || edge.ObservedAt.Before(intent.AppliedAt) || endpoints.ObservedAt.After(now) || edge.ObservedAt.After(now) || now.Sub(endpoints.ObservedAt) > maxAge || now.Sub(edge.ObservedAt) > maxAge {
		return nil, fmt.Errorf("endpoint and production route evidence do not match the applied role")
	}
	observedAt := endpoints.ObservedAt
	if edge.ObservedAt.Before(observedAt) {
		observedAt = edge.ObservedAt
	}
	return &rolloutRouteObservation{
		RouteEvidence: &rollout.RouteEvidence{InstanceUID: intent.InstanceUID, OperationUID: intent.OperationUID, Role: role, Fleet: intent.TargetFleet, ServiceUID: intent.ServiceUID, ServiceVersion: endpoints.ServiceVersion, EndpointsConverged: true, ClientProbePassed: true, ObservedAt: observedAt, ValidUntil: edge.ValidUntil},
		Endpoints:     endpoints,
	}, nil
}
