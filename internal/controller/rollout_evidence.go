package controller

import (
	"context"
	"crypto/ed25519"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net/http"
	"net/url"
	"slices"
	"time"

	"github.com/google/uuid"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation"
	typedcore "k8s.io/client-go/kubernetes/typed/core/v1"

	"github.com/huyz0/opensearch-serverless-operator/api/v1alpha1"
	"github.com/huyz0/opensearch-serverless-operator/internal/java"
	"github.com/huyz0/opensearch-serverless-operator/internal/rollout"
	"github.com/huyz0/opensearch-serverless-operator/internal/workload"
)

type rolloutRoleProbe func(context.Context, *v1alpha1.OpenSearchServerless, string, string, string, string, time.Duration) (searchPodQualification, error)

type rolloutRouteConfig struct {
	URL, ID, AuthSecret string
	Client              *http.Client
	credential          *rollout.RouteCredential
}

// rolloutPlatformCheck verifies actual zone placement, peer and security
// readiness, and rollback compatibility for these Pod and process identities.
type rolloutPlatformCheck func(context.Context, *v1alpha1.OpenSearchServerless, *v1alpha1.OpenSearchServerlessOperation, string, candidateRoleObservation) (rolloutPlatformObservation, error)

type rolloutPlatformObservation struct {
	observedAt time.Time
	unchanged  func(context.Context) bool
}

// ConfigureDevelopmentRollout enables one named instance with qualified
// direct development probes, a stable HTTPS proxy and signed evidence.
func (r *OperationReconciler) ConfigureDevelopmentRollout(observer *InstanceReconciler, instanceName, searchURL, ingestURL string, key ed25519.PublicKey, routeRoots *x509.CertPool, searchAuthSecret, ingestAuthSecret string) error {
	if r.Core == nil || r.Discovery == nil || r.Policy == nil || observer == nil || observer.Core == nil ||
		!observer.DevelopmentGatedPods || !observer.DevelopmentReleaseGates || len(key) != ed25519.PublicKeySize ||
		!qualifiedRoleProbeImage.MatchString(observer.DevelopmentWarmupImage) ||
		observer.DevelopmentWarmupImage != observer.DevelopmentServiceProbeImage || len(validation.IsDNS1123Subdomain(instanceName)) != 0 {
		return fmt.Errorf("rollout requires a named instance, enabled Pod provisioning, a matching qualified image and an Ed25519 public key")
	}
	if searchURL == ingestURL {
		return fmt.Errorf("search and ingest must have distinct HTTPS routes")
	}
	if (searchAuthSecret == "") != (ingestAuthSecret == "") {
		return fmt.Errorf("authenticated rollout requires credentials for both roles")
	}
	authSecrets := map[string]string{"search": searchAuthSecret, "ingest": ingestAuthSecret}
	client := &http.Client{Transport: &http.Transport{TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: routeRoots}}}
	routes := make(map[string]rolloutRouteConfig, 2)
	for role, address := range map[string]string{"search": searchURL, "ingest": ingestURL} {
		parsed, err := url.Parse(address)
		if err != nil || parsed.Scheme != "https" || parsed.Hostname() == "" || parsed.User != nil ||
			parsed.RawQuery != "" || parsed.Fragment != "" || parsed.Opaque != "" || parsed.ForceQuery || parsed.RawPath != "" {
			return fmt.Errorf("%s rollout route must be an HTTPS base URL", role)
		}
		secret := authSecrets[role]
		if secret != "" && len(validation.IsDNS1123Subdomain(secret)) != 0 {
			return fmt.Errorf("%s rollout credential reference must be a Secret name", role)
		}
		routes[role] = rolloutRouteConfig{URL: address, ID: role, Client: client, AuthSecret: secret}
	}
	qualification := observer.fleetQualification()
	tls := rolloutTLSQualification{http: observer.qualifyHTTPRotation, peer: observer.qualifyPeerRotation}
	r.RolloutInstanceName = instanceName
	r.rolloutAuthSecrets = authSecrets
	r.rolloutRouteURLs = map[string]string{"search": searchURL, "ingest": ingestURL}
	r.RolloutEvidence = r.liveRolloutEvidence(qualification, tls, map[string]rolloutRoleProbe{"search": qualification.qualifyDevelopmentSearchPod, "ingest": qualification.qualifyDevelopmentIngestPod}, routes, signedRolloutPlatformCheck(r.Core, key))
	r.RolloutPodDomain = qualification.peerDNSDomain
	r.rolloutSourceProbe = func(ctx context.Context, instance *v1alpha1.OpenSearchServerless, pod *corev1.Pod, role string) error {
		if instance.Spec.Image != qualification.warmupImage || !qualification.supportsDevelopmentPodHTTP(instance) {
			return fmt.Errorf("source Pod requires the qualified development image and supported authentication mode")
		}
		directClient, endpoint, err := qualification.developmentPodHTTP(ctx, instance, pod, 9200)
		if err != nil {
			return err
		}
		defer directClient.CloseIdleConnections()
		return observeRollbackProcess(ctx, directClient, endpoint, pod.Name, role)
	}
	return nil
}

func observeRollbackProcess(ctx context.Context, client *http.Client, endpoint, name, role string) error {
	start := time.Now()
	lease, err := java.GetLease(ctx, client, endpoint)
	if err != nil {
		return err
	}
	incarnation, err := uuid.Parse(lease.ProcessIncarnation)
	if err != nil || incarnation.Version() != 4 || lease.NodeName != name || !lease.HasRole(role) ||
		!time.Now().Before(lease.EvidenceValidUntil(start, 5*time.Second)) {
		return fmt.Errorf("source process identity or lease is unavailable")
	}
	return nil
}

type rolloutTLSQualification struct {
	http, peer func(context.Context, *v1alpha1.OpenSearchServerless, string, string, bool) error
}

func (r *InstanceReconciler) ConfigureDevelopmentFleetRetirement(instanceName string, key ed25519.PublicKey) {
	r.retirementInstanceName = instanceName
	r.retirementEvidenceKey = append(ed25519.PublicKey(nil), key...)
}

// liveRolloutEvidence joins application probes, platform evidence and route
// observations. Omitting an unavailable role still permits switch read-back.
func (r *OperationReconciler) liveRolloutEvidence(observer fleetQualification, tls rolloutTLSQualification, probes map[string]rolloutRoleProbe, routes map[string]rolloutRouteConfig, verify rolloutPlatformCheck) rolloutEvidenceReader {
	return func(ctx context.Context, instance *v1alpha1.OpenSearchServerless, operation *v1alpha1.OpenSearchServerlessOperation) (rolloutEvidence, error) {
		if observer.core == nil || verify == nil || instance == nil || operation == nil {
			return rolloutEvidence{}, fmt.Errorf("direct Pod observer and independent rollout qualification are required")
		}
		for _, role := range []string{"search", "ingest"} {
			route := routes[role]
			if probes[role] == nil || route.URL == "" || route.ID == "" || route.Client == nil {
				return rolloutEvidence{}, fmt.Errorf("direct Pod probe and HTTPS route are required for %s", role)
			}
		}
		if routes["search"].URL == routes["ingest"].URL || routes["search"].ID == routes["ingest"].ID {
			return rolloutEvidence{}, fmt.Errorf("search and ingest must have distinct production routes")
		}
		ctx, cancel := context.WithTimeout(ctx, rolloutObservationAge)
		defer cancel()
		candidates := make(map[string]*rollout.CandidateEvidence, 2)
		candidateObservations := make(map[string]candidateRoleObservation, 2)
		platformObservations := make(map[string]rolloutPlatformObservation, 2)
		routeObservations := make(map[string]*rolloutRouteObservation, 2)
		resolvedRoutes := make(map[string]rolloutRouteConfig, 2)
		for role, route := range routes {
			credential, err := rollout.ReadRouteCredential(ctx, observer.core, instance, route.AuthSecret)
			if err != nil {
				continue
			}
			route.credential = credential
			resolvedRoutes[role] = route
		}
		for _, role := range []string{"search", "ingest"} {
			route, configured := resolvedRoutes[role]
			if !configured {
				continue
			}
			var state v1alpha1.RolloutRoleStatus
			if operation.Status.RolloutRoles == nil {
				candidate := currentPromotionCandidate(instance, operation.Spec.TargetFleet)
				if operation.Spec.Action != "Promote" || candidate == nil {
					return rolloutEvidence{}, fmt.Errorf("rollout requires a staged promotion or prepared role intents")
				}
				state.TargetFleet, state.CandidateRevision = candidate.Fleet, candidate.Revision
			} else {
				state = operation.Status.RolloutRoles[role]
			}
			if err := tls.http(ctx, instance, state.TargetFleet, operation.Spec.ExpectedServingFleetByRole[role], operation.Spec.Action == "Promote"); err != nil {
				return rolloutEvidence{}, err
			}
			peerSource := operation.Spec.ExpectedServingFleetByRole[role]
			if operation.Spec.Action == "Rollback" && peerSource == state.TargetFleet && instance.Status.CandidateFleet != nil {
				peerSource = instance.Status.CandidateFleet.Fleet
			}
			if err := tls.peer(ctx, instance, state.TargetFleet, peerSource, operation.Spec.Action == "Promote"); err != nil {
				return rolloutEvidence{}, err
			}
			minima := servingZoneMinima(instance, role)
			observed, err := observer.observeFleetRole(ctx, instance, role, state.TargetFleet, state.CandidateRevision, minima, false, rolloutObservationAge, probes[role])
			if err != nil || len(observed.Pods) == 0 || len(observed.PodSnapshot) < servingRegionalFloor(instance, role) ||
				slices.ContainsFunc(observed.Pods, func(pod searchPodQualification) bool {
					return !pod.StatsProcessVerified || pod.LeaseValidUntil.IsZero()
				}) {
				continue
			}
			var credentialValidUntil time.Time
			if route.credential != nil {
				credentialValidUntil = observer.podHTTPAccess.verifyRolloutCredential(ctx, instance, role, observed, route.credential)
				if credentialValidUntil.IsZero() {
					continue
				}
			}
			platform, err := verify(ctx, instance, operation, role, observed)
			now := time.Now()
			if err != nil || platform.unchanged == nil || platform.observedAt.IsZero() || platform.observedAt.After(now) || now.Sub(platform.observedAt) > rolloutObservationAge {
				continue
			}
			if state.Phase == string(rollout.Applied) {
				qualified := make(map[types.UID]string, len(observed.Pods))
				for _, pod := range observed.Pods {
					qualified[types.UID(pod.PodUID)] = pod.ProcessIncarnation
				}
				routeObservations[role], _ = r.probeAndObserveRolloutRoute(ctx, instance, operation, role, route, minima, qualified, 5*time.Second, rolloutObservationAge)
			}
			candidateObservations[role] = observed
			platformObservations[role] = platform
			validUntil := observed.Pods[0].LeaseValidUntil
			for _, pod := range observed.Pods[1:] {
				validUntil = minTime(validUntil, pod.LeaseValidUntil)
			}
			if !credentialValidUntil.IsZero() {
				validUntil = minTime(validUntil, credentialValidUntil)
			}
			candidates[role] = &rollout.CandidateEvidence{
				InstanceUID: observed.InstanceUID, OperationUID: string(operation.UID), Role: role, Fleet: observed.Fleet, Revision: observed.Revision,
				CapacityQualified: true, PeerQualified: true, SecurityQualified: true, RollbackCompatible: true,
				ObservedAt: minTime(platform.observedAt, observed.ObservedAt),
				ValidUntil: validUntil,
			}
		}
		evidence := rolloutEvidence{Candidates: candidates, Routes: make(map[string]*rollout.RouteEvidence, 2)}
		for role, route := range routeObservations {
			if route != nil {
				evidence.Routes[role] = route.RouteEvidence
			}
		}
		evidence.revalidate = func(ctx context.Context, evidence *rolloutEvidence) {
			ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
			defer cancel()
			for _, role := range []string{"search", "ingest"} {
				observed, ok := candidateObservations[role]
				if !ok {
					continue
				}
				if !rolloutPodsUnchanged(ctx, observer, instance, role, observed) || !resolvedRoutes[role].credential.Unchanged(ctx, observer.core, instance.Namespace) ||
					!platformObservations[role].unchanged(ctx) {
					delete(evidence.Candidates, role)
					delete(evidence.Routes, role)
					continue
				}
				if route := routeObservations[role]; route != nil {
					if err := recheckEndpointSlices(ctx, r.Discovery.EndpointSlices(instance.Namespace), route.Endpoints.ServiceName, route.Endpoints.SliceVersions); err != nil {
						delete(evidence.Routes, role)
					}
				}
			}
		}
		return evidence, nil
	}
}

func servingZoneMinima(instance *v1alpha1.OpenSearchServerless, role string) map[string]int {
	minima := make(map[string]int)
	for zone, config := range instance.Spec.Pools[role].Zones {
		if int64(config.Replicas)+int64(config.WarmSpare) > 0 {
			minima[zone] = max(1, int(config.MinServing))
		}
	}
	return minima
}

func rolloutPodsUnchanged(ctx context.Context, observer fleetQualification, instance *v1alpha1.OpenSearchServerless, role string, observed candidateRoleObservation) bool {
	return fleetPodsUnchanged(ctx, observer.core.Pods(instance.Namespace), observed.InstanceUID, role, observed.Fleet, observed.PodSnapshot)
}

func fleetPodsUnchanged(ctx context.Context, podsAPI typedcore.PodInterface, instanceUID, role, fleet string, snapshot []corev1.Pod) bool {
	selector := labels.SelectorFromSet(labels.Set{workload.InstanceLabel: instanceUID, workload.RoleLabel: role, workload.FleetLabel: fleet}).String()
	pods, err := podsAPI.List(ctx, metav1.ListOptions{LabelSelector: selector, Limit: 513})
	if err != nil || pods.Continue != "" || len(pods.Items) != len(snapshot) {
		return false
	}
	versions := make(map[types.UID]string, len(pods.Items))
	for _, pod := range snapshot {
		versions[pod.UID] = pod.ResourceVersion
	}
	for _, pod := range pods.Items {
		if versions[pod.UID] != pod.ResourceVersion || pod.ResourceVersion == "" {
			return false
		}
		delete(versions, pod.UID)
	}
	return len(versions) == 0
}

func minTime(first, second time.Time) time.Time {
	if first.Before(second) {
		return first
	}
	return second
}
