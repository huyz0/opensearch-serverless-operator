package rollout

import (
	"crypto/ed25519"
	"crypto/sha256"
	"fmt"
	"maps"
	"time"

	"k8s.io/apimachinery/pkg/types"

	"github.com/huyz0/opensearch-serverless-operator/internal/java"
)

const writerWithdrawalDomain = "opensearch-serverless-writer-withdrawal/v1\n"

// WriterWithdrawalScope binds an Envoy sample to the exact operation,
// withdrawing Java process, Service and approved index HTTPRoute.
type WriterWithdrawalScope struct {
	RouteAuthIdentity
	Namespace          string             `json:"namespace"`
	InstanceUID        types.UID          `json:"instanceUID"`
	InstanceGeneration int64              `json:"instanceGeneration"`
	OperationUID       types.UID          `json:"operationUID"`
	VictimPodUID       types.UID          `json:"victimPodUID"`
	VictimPodVersion   string             `json:"victimPodVersion"`
	Writer             java.WriterProcess `json:"writer"`
	ServiceName        string             `json:"serviceName"`
	ServiceUID         types.UID          `json:"serviceUID"`
	ServiceVersion     string             `json:"serviceVersion"`
	RouteName          string             `json:"routeName"`
	RouteUID           types.UID          `json:"routeUID"`
	RouteVersion       string             `json:"routeVersion"`
	GatewayNamespace   string             `json:"gatewayNamespace"`
	GatewayName        string             `json:"gatewayName"`
	GatewayUID         types.UID          `json:"gatewayUID"`
	GatewayVersion     string             `json:"gatewayVersion"`
	GatewayListener    string             `json:"gatewayListener"`
	GatewayServiceUID  types.UID          `json:"gatewayServiceUID"`
}

type WriterWithdrawalEvidence struct {
	WriterWithdrawalScope
	ObservedAt       time.Time            `json:"observedAt"`
	EndpointVersions map[types.UID]string `json:"endpointVersions"`
	GatewayVersions  map[string]string    `json:"gatewayVersions"`
	ServingProxyUIDs []string             `json:"servingProxyUIDs"`
	Proxies          []WriterProxyReport  `json:"proxies"`
}

type WriterProxyReport struct {
	ProxyUID     string               `json:"proxyUID"`
	ObservedAt   time.Time            `json:"observedAt"`
	LiveBackends map[types.UID]string `json:"liveBackends"`
}

func WriterWithdrawalEvidenceName(operationUID types.UID) string {
	digest := sha256.Sum256([]byte(operationUID))
	return fmt.Sprintf("writer-withdrawal-%x", digest[:12])
}

func SignWriterWithdrawalEvidence(evidence WriterWithdrawalEvidence, signer ed25519.PrivateKey) (map[string]string, error) {
	return signEvidence(writerWithdrawalDomain, evidence, signer)
}

func ParseWriterWithdrawalEvidence(raw string) (WriterWithdrawalEvidence, error) {
	return parseEvidence[WriterWithdrawalEvidence](raw)
}

// ObserveSignedWriterWithdrawal verifies every serving Envoy report against
// the live Service and the currently qualified survivor endpoint set.
func ObserveSignedWriterWithdrawal(data map[string]string, key ed25519.PublicKey, scope WriterWithdrawalScope, endpoints, survivors map[types.UID]string, gateway GatewayReplicaObservation, notBefore, now time.Time) (time.Time, error) {
	evidence, err := readSignedEvidence[WriterWithdrawalEvidence](writerWithdrawalDomain, data, key)
	if err != nil {
		return time.Time{}, err
	}
	return evidence.verify(scope, endpoints, survivors, gateway, notBefore, now)
}

func (e WriterWithdrawalEvidence) verify(scope WriterWithdrawalScope, endpoints, survivors map[types.UID]string, gateway GatewayReplicaObservation, notBefore, now time.Time) (time.Time, error) {
	if e.WriterWithdrawalScope != scope || scope.Namespace == "" || scope.InstanceUID == "" || scope.InstanceGeneration <= 0 ||
		scope.OperationUID == "" || scope.VictimPodUID == "" || scope.VictimPodVersion == "" || scope.Writer.NodeID == "" ||
		scope.Writer.EphemeralID == "" || scope.ServiceName == "" || scope.ServiceUID == "" || scope.ServiceVersion == "" ||
		scope.RouteName == "" || scope.RouteUID == "" || scope.RouteVersion == "" || notBefore.IsZero() ||
		!scope.RouteAuthIdentity.valid() || scope.AuthProbeURL == "" ||
		scope.GatewayNamespace == "" || scope.GatewayName == "" || scope.GatewayUID == "" || scope.GatewayVersion == "" ||
		scope.GatewayListener == "" || scope.GatewayServiceUID == "" ||
		e.ObservedAt.Before(notBefore) || !freshWeightObservation(now, e.ObservedAt) ||
		len(endpoints) == 0 || !maps.Equal(e.EndpointVersions, endpoints) || len(survivors) == 0 {
		return time.Time{}, fmt.Errorf("writer withdrawal evidence differs from the current operation, route or EndpointSlices")
	}
	if _, victim := survivors[scope.VictimPodUID]; victim {
		return time.Time{}, fmt.Errorf("retiring writer cannot count as a surviving backend")
	}
	for uid, version := range endpoints {
		if uid == "" || version == "" {
			return time.Time{}, fmt.Errorf("EndpointSlice identity or version is missing")
		}
	}
	for uid, version := range survivors {
		if uid == "" || version == "" {
			return time.Time{}, fmt.Errorf("surviving backend identity or version is missing")
		}
	}
	if len(gateway.Versions) == 0 || !maps.Equal(e.GatewayVersions, gateway.Versions) ||
		len(e.ServingProxyUIDs) == 0 || len(e.ServingProxyUIDs) != len(e.Proxies) || len(e.ServingProxyUIDs) != len(gateway.UIDs) {
		return time.Time{}, fmt.Errorf("writer withdrawal needs all serving Envoy replicas")
	}
	expected := make(map[string]bool, len(e.ServingProxyUIDs))
	for _, uid := range e.ServingProxyUIDs {
		if uid == "" || expected[uid] || !gateway.UIDs[uid] {
			return time.Time{}, fmt.Errorf("duplicate or missing serving Envoy UID")
		}
		expected[uid] = true
	}
	for _, report := range e.Proxies {
		if !expected[report.ProxyUID] || report.ObservedAt.Before(e.ObservedAt) ||
			!freshWeightObservation(now, report.ObservedAt) || !maps.Equal(report.LiveBackends, survivors) {
			return time.Time{}, fmt.Errorf("Envoy replica evidence is stale or still differs from the surviving backend set")
		}
		delete(expected, report.ProxyUID)
	}
	return e.ObservedAt.Add(PlatformEvidenceMaxAge), nil
}
