package rollout

import (
	"crypto/ed25519"
	"crypto/sha256"
	"fmt"
	"maps"
	"time"

	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation"
)

const weightEvidenceDomain = "opensearch-serverless-weighted-route-evidence/v1\n"

type WeightEvidenceScope struct {
	RouteAuthIdentity
	Namespace          string    `json:"namespace"`
	InstanceUID        types.UID `json:"instanceUID"`
	InstanceGeneration int64     `json:"instanceGeneration"`
	OperationUID       types.UID `json:"operationUID"`
	Role               string    `json:"role"`
	RouteName          string    `json:"routeName"`
	RouteUID           types.UID `json:"routeUID"`
	RouteVersion       string    `json:"routeVersion"`
}

type WeightEvidence struct {
	WeightEvidenceScope
	Weights map[string]int `json:"weights"`
	// Inventory comes from the serving Gateway replica inventory, independently
	// of which replicas happened to answer the probes.
	InventoryObservedAt time.Time           `json:"inventoryObservedAt"`
	ServingProxyUIDs    []string            `json:"servingProxyUIDs"`
	Proxies             []WeightProxyReport `json:"proxies"`
}

type WeightProxyReport struct {
	ProxyUID          string                       `json:"proxyUID"`
	ObservedAt        time.Time                    `json:"observedAt"`
	RouteVersion      string                       `json:"routeVersion"`
	Healthy           map[string]map[string]string `json:"healthy"`
	ConfiguredWeights map[string]int               `json:"configuredWeights,omitempty"`
	Requests          map[string]int               `json:"requests"`
	Failures          int                          `json:"failures"`
}

func WeightEvidenceName(uid types.UID, role string) string {
	digest := sha256.Sum256([]byte(uid))
	return fmt.Sprintf("weight-evidence-%x-%s", digest[:12], role)
}

func ParseWeightEvidence(raw string) (WeightEvidence, error) {
	return parseEvidence[WeightEvidence](raw)
}

func readSignedWeightEvidence(data map[string]string, key ed25519.PublicKey) (WeightEvidence, error) {
	return readSignedEvidence[WeightEvidence](weightEvidenceDomain, data, key)
}

// ObserveSignedWeights adapts authenticated edge evidence. The expected scope
// and weights must come from the live bound route and operation, and the
// caller still needs independent Pod and fleet qualification.
func ObserveSignedWeights(data map[string]string, key ed25519.PublicKey, scope WeightEvidenceScope, weights map[string]int, now time.Time) ([]string, []ProxyWeightObservation, error) {
	evidence, err := readSignedWeightEvidence(data, key)
	if err != nil {
		return nil, nil, err
	}
	if evidence.WeightEvidenceScope != scope || !maps.Equal(evidence.Weights, weights) {
		return nil, nil, fmt.Errorf("signed evidence differs from the current weighted route")
	}
	if err := evidence.validate(now); err != nil {
		return nil, nil, err
	}
	deadline := evidence.InventoryObservedAt.Add(PlatformEvidenceMaxAge)
	reports := make([]ProxyWeightObservation, 0, len(evidence.Proxies))
	for _, report := range evidence.Proxies {
		reports = append(reports, ProxyWeightObservation{
			ProxyUID: report.ProxyUID, InstanceUID: string(scope.InstanceUID), OperationUID: string(scope.OperationUID),
			Role: scope.Role, RouteUID: string(scope.RouteUID), RouteVersion: report.RouteVersion,
			ObservedAt: report.ObservedAt, ValidUntil: deadline,
			Healthy: report.Healthy, ConfiguredWeights: report.ConfiguredWeights, Requests: report.Requests, Failures: report.Failures,
		})
	}
	return evidence.ServingProxyUIDs, reports, nil
}

func (e WeightEvidence) validate(now time.Time) error {
	if e.Namespace == "" || e.InstanceUID == "" || e.OperationUID == "" || e.InstanceGeneration <= 0 ||
		(e.Role != "search" && e.Role != "ingest") || e.RouteName == "" || len(validation.IsDNS1123Subdomain(e.RouteName)) != 0 ||
		!e.RouteAuthIdentity.valid() || (e.RouteAuthIdentity != (RouteAuthIdentity{}) && e.AuthProbeURL == "") ||
		e.RouteUID == "" || e.RouteVersion == "" || !validWeights(e.Weights) ||
		!freshWeightObservation(now, e.InventoryObservedAt) ||
		len(e.ServingProxyUIDs) == 0 || len(e.ServingProxyUIDs) != len(e.Proxies) {
		return fmt.Errorf("complete current weighted route and proxy inventory are required")
	}
	expected := make(map[string]bool, len(e.ServingProxyUIDs))
	for _, uid := range e.ServingProxyUIDs {
		if uid == "" || expected[uid] {
			return fmt.Errorf("serving proxy inventory has a missing or duplicate UID")
		}
		expected[uid] = true
	}
	for _, report := range e.Proxies {
		if !expected[report.ProxyUID] || report.RouteVersion != e.RouteVersion || report.ObservedAt.Before(e.InventoryObservedAt) ||
			!freshWeightObservation(now, report.ObservedAt) {
			return fmt.Errorf("proxy report is stale or does not match this route version and inventory")
		}
		if report.ConfiguredWeights != nil && !maps.Equal(report.ConfiguredWeights, e.Weights) {
			return fmt.Errorf("proxy's active weights differ from the route")
		}
		delete(expected, report.ProxyUID)
	}
	return nil
}

func freshWeightObservation(now, observed time.Time) bool {
	return fresh(now, observed, PlatformEvidenceMaxAge) && now.Before(observed.Add(PlatformEvidenceMaxAge))
}
