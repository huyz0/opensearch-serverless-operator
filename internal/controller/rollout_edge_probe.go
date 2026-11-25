package controller

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation"
	typedcore "k8s.io/client-go/kubernetes/typed/core/v1"

	"github.com/huyz0/opensearch-serverless-operator/api/v1alpha1"
	"github.com/huyz0/opensearch-serverless-operator/internal/java"
	"github.com/huyz0/opensearch-serverless-operator/internal/rollout"
	"github.com/huyz0/opensearch-serverless-operator/internal/workload"
)

// probeRolloutEdge observes one response through the explicitly configured
// HTTPS route. The caller supplies a trusted, authenticated transport, ensures
// the route is the production client path, and qualifies the Pod identities.
// A single response is not evidence of fleet-wide or future edge behavior.
func probeRolloutEdge(ctx context.Context, core typedcore.CoreV1Interface, instance *v1alpha1.OpenSearchServerless, intent rollout.SwitchIntent, route rolloutRouteConfig, qualifiedProcesses map[types.UID]string, minLease time.Duration) (externalRouteObservation, error) {
	var result externalRouteObservation
	routeURL, routeID, httpClient := route.URL, route.ID, route.Client
	parsed, err := url.Parse(routeURL)
	if err != nil || parsed == nil || parsed.Scheme != "https" || parsed.Host == "" || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" || parsed.Opaque != "" || routeID == "" || core == nil || instance == nil || instance.UID == "" || string(instance.UID) != intent.InstanceUID || intent.OperationUID == "" || intent.ServiceUID == "" || intent.Phase != rollout.Applied || intent.AppliedAt.IsZero() || len(qualifiedProcesses) == 0 || httpClient == nil || minLease < 0 {
		return result, fmt.Errorf("applied role, HTTPS route and qualified client and Pods are required")
	}
	if (instance.Spec.Security != nil || instance.Status.BasicAuthBaseline != nil) != (route.credential != nil) {
		return result, fmt.Errorf("edge credential must match the qualified instance authentication mode")
	}
	transport, ok := httpClient.Transport.(*http.Transport)
	if !ok || transport == nil || transport.Proxy != nil || transport.DialTLSContext != nil || transport.DialTLS != nil || (transport.TLSClientConfig != nil && transport.TLSClientConfig.InsecureSkipVerify) {
		return result, fmt.Errorf("an explicit certificate-verifying transport without a proxy or custom TLS dialer is required")
	}
	desired, err := workload.RoleService(instance, intent.Role, intent.TargetFleet, 9200)
	if err != nil {
		return result, err
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	services := core.Services(instance.Namespace)
	before, err := services.Get(ctx, desired.Name, metav1.GetOptions{})
	if err != nil {
		return result, fmt.Errorf("read role Service: %w", err)
	}
	if !matchesFleetService(instance, before, desired, types.UID(intent.ServiceUID)) {
		return result, fmt.Errorf("role Service identity or selector changed")
	}
	start := time.Now()
	if start.Before(intent.AppliedAt) {
		return result, fmt.Errorf("route probe started before switch observation")
	}
	probeClient := *httpClient
	probeClient.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	probeClient.Jar = nil
	var lease java.Lease
	if credential := route.credential; credential != nil {
		if err := java.RequireRouteStatsAuthentication(ctx, &probeClient, routeURL); err != nil {
			return result, fmt.Errorf("verify anonymous edge denial: %w", err)
		}
		lease, err = java.GetLeaseWithBasicAuth(ctx, &probeClient, routeURL, credential.Username, credential.Password)
	} else {
		lease, err = java.GetLease(ctx, &probeClient, routeURL)
	}
	if err != nil {
		return result, fmt.Errorf("probe production route: %w", err)
	}
	if len(validation.IsDNS1123Subdomain(lease.NodeName)) != 0 || !lease.Valid || lease.ExpiresInMillis <= minLease.Milliseconds() || !lease.HasRole(intent.Role) {
		return result, fmt.Errorf("responding process has no sufficiently valid lease")
	}
	pods := core.Pods(instance.Namespace)
	pod, err := pods.Get(ctx, lease.NodeName, metav1.GetOptions{})
	if err != nil {
		return result, fmt.Errorf("read responding Pod: %w", err)
	}
	if !matchesRoleProbePod(instance, pod, intent.Role, intent.TargetFleet) {
		return result, fmt.Errorf("responding Pod does not match the qualified fleet")
	}
	if lease.ProcessIncarnation == "" || lease.ProcessIncarnation != qualifiedProcesses[pod.UID] {
		return result, fmt.Errorf("responding JVM does not match the qualified process")
	}
	again, err := pods.Get(ctx, pod.Name, metav1.GetOptions{})
	if err != nil || again.UID != pod.UID || again.ResourceVersion != pod.ResourceVersion {
		return result, fmt.Errorf("responding Pod changed during route probe")
	}
	after, err := services.Get(ctx, desired.Name, metav1.GetOptions{})
	if err != nil || !matchesFleetService(instance, after, desired, before.UID) || after.ResourceVersion != before.ResourceVersion {
		return result, fmt.Errorf("role Service changed during route probe")
	}
	validUntil := lease.EvidenceValidUntil(start, minLease)
	if !time.Now().Before(validUntil) {
		return result, fmt.Errorf("responding lease expired during route probe")
	}
	return externalRouteObservation{InstanceUID: intent.InstanceUID, OperationUID: intent.OperationUID, Role: intent.Role, Fleet: intent.TargetFleet, ServiceUID: intent.ServiceUID, ServiceVersion: before.ResourceVersion, RouteID: routeID, RespondingPodUID: pod.UID, RespondingPodVersion: pod.ResourceVersion, ObservedAt: start, ValidUntil: validUntil}, nil
}
