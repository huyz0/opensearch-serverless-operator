package rollout

import (
	"crypto/ed25519"
	"testing"
	"time"
)

func TestObserveSignedWeights(t *testing.T) {
	key, signer, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 11, 24, 5, 0, 0, 0, time.UTC)
	scope := WeightEvidenceScope{
		Namespace: "tenant", InstanceUID: "instance", InstanceGeneration: 3,
		OperationUID: "operation", Role: "search", RouteName: "search", RouteUID: "route", RouteVersion: "23",
	}
	weights := map[string]int{"a": 99, "b": 1}
	for _, test := range []struct {
		name   string
		edit   func(*WeightEvidence)
		domain string
	}{
		{name: "complete current signed reports"},
		{name: "missing replica observation", edit: func(e *WeightEvidence) { e.Proxies = e.Proxies[:1] }},
		{name: "duplicate replica observation", edit: func(e *WeightEvidence) { e.Proxies[1].ProxyUID = "p1" }},
		{name: "stale inventory", edit: func(e *WeightEvidence) {
			e.InventoryObservedAt = now.Add(-PlatformEvidenceMaxAge)
		}},
		{name: "stale replica health and samples", edit: func(e *WeightEvidence) {
			e.Proxies[1].ObservedAt = now.Add(-PlatformEvidenceMaxAge)
		}},
		{name: "replica predates the inventory", edit: func(e *WeightEvidence) {
			e.Proxies[1].ObservedAt = e.InventoryObservedAt.Add(-time.Millisecond)
		}},
		{name: "different configuration on one replica", edit: func(e *WeightEvidence) { e.Proxies[1].RouteVersion = "22" }},
		{name: "wrong role", edit: func(e *WeightEvidence) { e.Role = "ingest" }},
		{name: "different route UID", edit: func(e *WeightEvidence) { e.RouteUID = "other" }},
		{name: "credential from another scope", edit: func(e *WeightEvidence) {
			e.RouteAuthIdentity = RouteAuthIdentity{AuthSecretName: "route-probe", AuthSecretUID: "secret", AuthSecretVersion: "4", AuthProbeURL: "https://search.example.com"}
		}},
		{name: "different weights", edit: func(e *WeightEvidence) { e.Weights = map[string]int{"a": 80, "b": 20} }},
		{name: "proxy is on older configured weights despite the current route version", edit: func(e *WeightEvidence) {
			e.Proxies[0].ConfiguredWeights = map[string]int{"a": 98, "b": 2}
		}},
		{name: "signature for selector evidence domain", domain: PlatformEvidenceDomain},
	} {
		t.Run(test.name, func(t *testing.T) {
			evidence := WeightEvidence{
				WeightEvidenceScope: scope, Weights: weights, InventoryObservedAt: now.Add(-time.Second),
				ServingProxyUIDs: []string{"p1", "p2"},
				Proxies: []WeightProxyReport{
					{ProxyUID: "p1", ObservedAt: now, RouteVersion: scope.RouteVersion, ConfiguredWeights: weights},
					{ProxyUID: "p2", ObservedAt: now, RouteVersion: scope.RouteVersion},
				},
			}
			domain := weightEvidenceDomain
			if test.domain != "" {
				domain = test.domain
			}
			if test.edit != nil {
				test.edit(&evidence)
			}
			data, err := signEvidence(domain, evidence, signer)
			if err != nil {
				t.Fatal(err)
			}
			ids, reports, err := ObserveSignedWeights(data, key, scope, weights, now)
			if test.edit != nil || test.domain != "" {
				if err == nil || ids != nil || reports != nil {
					t.Fatalf("invalid evidence was accepted: %v %v %v", ids, reports, err)
				}
				return
			}
			if err != nil || len(ids) != 2 || len(reports) != 2 || reports[0].ConfiguredWeights["b"] != 1 ||
				!reports[0].ValidUntil.Equal(evidence.InventoryObservedAt.Add(PlatformEvidenceMaxAge)) {
				t.Fatalf("current evidence was rejected or lost inventory expiry: %v %v %v", ids, reports, err)
			}
			data["evidence.json"] += " "
			if _, _, err := ObserveSignedWeights(data, key, scope, weights, now); err == nil {
				t.Fatal("modified evidence retained signature trust")
			}
		})
	}
}
