package controller

import (
	"context"
	"crypto/ed25519"
	"fmt"
	"maps"
	"slices"
	"time"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/huyz0/opensearch-serverless-operator/api/v1alpha1"
	"github.com/huyz0/opensearch-serverless-operator/internal/rollout"
)

const weightedAdmission = 1

// ConfigureDevelopmentWeightedRollout restricts weighted writes to two
// deployment-approved role routes on the configured development instance.
func (r *OperationReconciler) ConfigureDevelopmentWeightedRollout(observer *InstanceReconciler, searchRoute, ingestRoute string, key ed25519.PublicKey) error {
	if r.RolloutInstanceName == "" || r.RolloutEvidence == nil || observer == nil || len(key) != ed25519.PublicKeySize ||
		searchRoute == ingestRoute || len(validation.IsDNS1123Subdomain(searchRoute)) != 0 || len(validation.IsDNS1123Subdomain(ingestRoute)) != 0 {
		return fmt.Errorf("weighted rollout requires development rollout and two distinct HTTPRoute names")
	}
	r.weightedRoutes = map[string]string{"search": searchRoute, "ingest": ingestRoute}
	qualification := observer.fleetQualification()
	r.weightedRuntimeSupported = qualification.supportsDevelopmentPodHTTP
	tls := rolloutTLSQualification{http: observer.qualifyHTTPRotation, peer: observer.qualifyPeerRotation}
	r.weightedEvidence = r.liveWeightedRolloutEvidence(qualification, tls, signedRolloutPlatformCheck(r.Core, key), key)
	return nil
}

func (r *OperationReconciler) supportsWeightedRuntime(instance *v1alpha1.OpenSearchServerless) bool {
	if !hasManagedHTTPS(instance) && instance.Spec.Security == nil && instance.Status.BasicAuthBaseline == nil {
		return true
	}
	if r.weightedRuntimeSupported == nil || !r.weightedRuntimeSupported(instance) {
		return false
	}
	return instance.Status.BasicAuthBaseline == nil || (r.rolloutAuthSecrets["search"] != "" && r.rolloutAuthSecrets["ingest"] != "")
}

// liveWeightedRolloutEvidence combines application and platform qualification
// for each fleet that will still receive traffic with independent proxy
// observations. A fleet being removed may be unhealthy.
func (r *OperationReconciler) liveWeightedRolloutEvidence(observer fleetQualification, tls rolloutTLSQualification, verify rolloutPlatformCheck, key ed25519.PublicKey) weightStepEvidence {
	trustedKey := append(ed25519.PublicKey(nil), key...)
	return func(ctx context.Context, instance *v1alpha1.OpenSearchServerless, operation *v1alpha1.OpenSearchServerlessOperation, intent rollout.WeightIntent, route observedWeightRoute) (weightedRolloutEvidence, error) {
		if observer.core == nil || instance == nil || verify == nil || observer.roleProbe(intent.Role) == nil || len(trustedKey) != ed25519.PublicKeySize {
			return weightedRolloutEvidence{}, fmt.Errorf("weighted rollout requires a qualified application probe and platform key")
		}
		if !r.supportsWeightedRuntime(instance) {
			return weightedRolloutEvidence{}, fmt.Errorf("weighted rollout requires the approved runtime and authentication mode")
		}
		ctx, cancel := context.WithTimeout(ctx, rolloutObservationAge)
		defer cancel()
		snapshot := weightedRolloutEvidence{WeightSnapshot: rollout.WeightSnapshot{
			MaxAge: rolloutObservationAge, MinimumSamples: 500, TolerancePoints: 5,
			Qualifications: make(map[string]rollout.FleetWeightQualification),
		}}
		snapshot.Now = r.weightNow()
		credential, err := rollout.ReadRouteCredential(ctx, r.Core, instance, r.rolloutAuthSecrets[intent.Role])
		if err != nil {
			return snapshot, nil
		}
		var auth rollout.RouteAuthIdentity
		if credential != nil {
			credential.AuthProbeURL, err = rollout.BindAuthenticatedProbeURL(r.rolloutRouteURLs[intent.Role], route.Object)
			if err != nil {
				return snapshot, err
			}
			auth = credential.RouteAuthIdentity
		}
		proxyUnchanged, err := readWeightProxyEvidence(ctx, r.Core, trustedKey, instance, operation, route, auth, &snapshot.WeightSnapshot)
		if err != nil {
			// Missing or stale evidence leaves the planner on Hold. A step
			// already applied is handled by the journal's read-back path.
			return snapshot, nil
		}
		if err := tls.http(ctx, instance, operation.Spec.TargetFleet, operation.Spec.ExpectedServingFleetByRole[intent.Role], operation.Spec.Action == "Promote"); err != nil {
			return snapshot, err
		}
		peerSource := operation.Spec.ExpectedServingFleetByRole[intent.Role]
		if operation.Spec.Action == "Rollback" {
			for fleet := range intent.Revisions {
				if fleet != operation.Spec.TargetFleet {
					peerSource = fleet
				}
			}
		}
		if err := tls.peer(ctx, instance, operation.Spec.TargetFleet, peerSource, operation.Spec.Action == "Promote"); err != nil {
			return snapshot, err
		}
		type fleetObservation struct {
			pods     candidateRoleObservation
			platform rolloutPlatformObservation
		}
		observations := make(map[string]fleetObservation)
		for _, fleet := range slices.Sorted(maps.Keys(intent.To)) {
			if intent.To[fleet] == 0 {
				continue
			}
			protected, err := r.observeRolloutBudgets(ctx, instance, fleet)
			if err != nil || !protected {
				continue
			}
			observed, err := observer.observeFleetRole(ctx, instance, intent.Role, fleet, intent.Revisions[fleet],
				servingZoneMinima(instance, intent.Role), false, rolloutObservationAge, observer.roleProbe(intent.Role))
			if err != nil || len(observed.Pods) == 0 || len(observed.PodSnapshot) < servingRegionalFloor(instance, intent.Role) {
				continue
			}
			processes := make(map[string]string, len(observed.Pods))
			validUntil := observed.Pods[0].LeaseValidUntil
			for _, pod := range observed.Pods {
				if pod.StatsProcessVerified && pod.ProcessIncarnation != "" && !pod.LeaseValidUntil.IsZero() {
					processes[pod.PodUID] = pod.ProcessIncarnation
					validUntil = minTime(validUntil, pod.LeaseValidUntil)
				}
			}
			if len(processes) != len(observed.Pods) {
				continue
			}
			if credential != nil {
				deadline := observer.podHTTPAccess.verifyRolloutCredential(ctx, instance, intent.Role, observed, credential)
				if deadline.IsZero() {
					continue
				}
				validUntil = minTime(validUntil, deadline)
			}
			platform, err := verify(ctx, instance, operation, intent.Role, observed)
			if err != nil || platform.unchanged == nil || platform.observedAt.IsZero() {
				continue
			}
			snapshot.Qualifications[fleet] = rollout.FleetWeightQualification{
				CandidateEvidence: rollout.CandidateEvidence{
					InstanceUID: observed.InstanceUID, OperationUID: string(operation.UID), Role: intent.Role, Fleet: fleet,
					Revision: observed.Revision, CapacityQualified: true, PeerQualified: true,
					SecurityQualified: true, RollbackCompatible: true,
					ObservedAt: minTime(platform.observedAt, observed.ObservedAt), ValidUntil: validUntil,
				},
				Processes: processes,
			}
			observations[fleet] = fleetObservation{pods: observed, platform: platform}
		}
		snapshot.unchanged = func(ctx context.Context) bool {
			ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
			defer cancel()
			for _, observed := range observations {
				if !rolloutPodsUnchanged(ctx, observer, instance, intent.Role, observed.pods) || !observed.platform.unchanged(ctx) {
					return false
				}
			}
			return proxyUnchanged(ctx)
		}
		if !credential.Unchanged(ctx, r.Core, instance.Namespace) {
			clear(snapshot.Qualifications)
			snapshot.ExpectedProxyUIDs, snapshot.Proxies = nil, nil
		}
		return snapshot, nil
	}
}

// buildWeightedPromotionJournal derives a promotion schedule from
// deployment-approved route names, fleet revisions and observed backend pins.
// The caller validates and persists the resulting journal with the claim held.
func (r *OperationReconciler) buildWeightedPromotionJournal(ctx context.Context, reader client.Reader, instance *v1alpha1.OpenSearchServerless, op *v1alpha1.OpenSearchServerlessOperation, names map[string]string) (*v1alpha1.WeightedRolloutStatus, error) {
	if op.Spec.Action != "Promote" || !weightedClaimMatches(instance, op) ||
		names["search"] == "" || names["ingest"] == "" || names["search"] == names["ingest"] {
		return nil, fmt.Errorf("a claimed promotion and distinct deployment-approved role routes are required")
	}
	if err := op.Spec.Validate(); err != nil {
		return nil, err
	}
	candidate := currentPromotionCandidate(instance, op.Spec.TargetFleet)
	if candidate == nil {
		return nil, fmt.Errorf("weighted promotion requires a current pinned candidate")
	}
	preview := map[string]types.UID{"search": types.UID(candidate.SearchServiceUID), "ingest": types.UID(candidate.IngestServiceUID)}
	if _, err := ensurePreviewServices(ctx, nil, r.Core, instance, candidate.Fleet, preview); err != nil {
		return nil, fmt.Errorf("verify weighted promotion preview: %w", err)
	}
	journal := &v1alpha1.WeightedRolloutStatus{
		InstanceGeneration: instance.Generation, AdmissionWeight: weightedAdmission,
		Roles: make(map[string]v1alpha1.WeightedRoleStatus, len(weightedRoleOrder)),
	}
	for _, role := range weightedRoleOrder {
		source, target := op.Spec.ExpectedServingFleetByRole[role], candidate.Fleet
		if source == "" || source == target || instance.Status.ServingFleetIntentByRole[role] != source {
			return nil, fmt.Errorf("%s source fleet differs from serving intent", role)
		}
		sourceRevision, err := r.observeSourceFleetRevision(ctx, instance, role, source)
		if err != nil {
			return nil, err
		}
		service, err := r.observeRoleSwitch(ctx, instance, op, rollout.SwitchIntent{Role: role, SourceFleet: source, TargetFleet: target}, nil, nil, rolloutObservationAge)
		if err != nil {
			return nil, err
		}
		if !service.Service.Owned || !service.Service.RoutingShapeValid || service.Service.ServiceUID == "" ||
			service.Service.ResourceVersion == "" || service.Service.Fleet != source {
			return nil, fmt.Errorf("%s stable Service differs from the committed serving fleet", role)
		}
		part := v1alpha1.WeightedRoleStatus{
			StableServiceUID: service.Service.ServiceUID,
			RouteName:        names[role], BackendUIDs: make(map[string]string, 2),
			Revisions:      map[string]string{source: sourceRevision, target: candidate.Revision},
			InitialWeights: map[string]int{source: 100, target: 0},
			Targets:        weightedPromotionTargets(source, target, op.Spec.WeightedTraffic.ForRole(role)),
		}
		binding := weightRouteBinding{
			Key: client.ObjectKey{Namespace: instance.Namespace, Name: part.RouteName}, InstanceUID: string(instance.UID),
			Role: role, ServiceNames: make(map[string]string, 2), ServicePort: 9200,
		}
		for _, fleet := range []string{source, target} {
			pin := instance.Status.GatewayBackends[role+"/"+fleet]
			if pin.Role != role || pin.Fleet != fleet || pin.ServiceUID == "" {
				return nil, fmt.Errorf("%s backend for fleet %q is not pinned", role, fleet)
			}
			part.BackendUIDs[fleet], binding.ServiceNames[fleet] = pin.ServiceUID, pin.Name
		}
		route := &unstructured.Unstructured{}
		route.SetGroupVersionKind(httpRouteGVK)
		if err := reader.Get(ctx, binding.Key, route); err != nil {
			return nil, err
		}
		binding.UID = route.GetUID()
		live, digest, err := inspectWeightRoute(route, binding)
		if err != nil {
			return nil, err
		}
		if !maps.Equal(live.Weights, part.InitialWeights) {
			return nil, fmt.Errorf("%s weighted route must start at the committed serving fleet", role)
		}
		if instance.Status.BasicAuthBaseline != nil {
			if _, err := rollout.BindAuthenticatedProbeURL(r.rolloutRouteURLs[role], route); err != nil {
				return nil, fmt.Errorf("%s weighted route authentication: %w", role, err)
			}
		}
		part.RouteUID, part.RouteSpecDigest = string(route.GetUID()), digest
		journal.Roles[role] = part
	}
	return journal, nil
}

func weightedPromotionTargets(source, target string, intermediate []int) []v1alpha1.WeightedTarget {
	targets := make([]v1alpha1.WeightedTarget, 0, len(intermediate)+2)
	targets = append(targets, v1alpha1.WeightedTarget{Weights: map[string]int{source: 100 - weightedAdmission, target: weightedAdmission}})
	for _, weight := range intermediate {
		targets = append(targets, v1alpha1.WeightedTarget{Weights: map[string]int{source: 100 - weight, target: weight}})
	}
	return append(targets, v1alpha1.WeightedTarget{Weights: map[string]int{source: 0, target: 100}})
}
