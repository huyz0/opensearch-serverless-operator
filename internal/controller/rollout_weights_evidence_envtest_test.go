//go:build envtest

package controller

import (
	"context"
	"crypto/ed25519"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	typedcore "k8s.io/client-go/kubernetes/typed/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/huyz0/opensearch-serverless-operator/api/v1alpha1"
	"github.com/huyz0/opensearch-serverless-operator/internal/rollout"
)

func exerciseSignedWeightEvidence(t *testing.T, c client.Client, core typedcore.CoreV1Interface, namespace string) {
	ctx := context.Background()
	instance := &v1alpha1.OpenSearchServerless{
		ObjectMeta: metav1.ObjectMeta{Name: "weighted", Namespace: namespace},
		Spec: v1alpha1.OpenSearchServerlessSpec{
			Image: "example/image@sha256:" + strings.Repeat("a", 64), ServiceAccountName: "runtime",
			Storage: v1alpha1.Storage{Type: "s3", Bucket: "bucket", Region: "region"},
			Pools:   map[string]v1alpha1.Pool{"search": {Zones: map[string]v1alpha1.Zone{"az1": {Replicas: 1}}}},
			Rollout: &v1alpha1.Rollout{Strategy: "BlueGreen", GatewayBackends: true},
		},
	}
	if err := c.Create(ctx, instance); err != nil {
		t.Fatal(err)
	}
	operation := &v1alpha1.OpenSearchServerlessOperation{
		ObjectMeta: metav1.ObjectMeta{Name: "promote-weighted", Namespace: namespace},
		Spec: v1alpha1.OperationSpec{
			InstanceRef: v1alpha1.InstanceReference{Name: instance.Name, UID: string(instance.UID)},
			Action:      "Promote", TargetFleet: "green", ExpectedServingFleetByRole: map[string]string{"search": "blue", "ingest": "blue"},
		},
	}
	if err := c.Create(ctx, operation); err != nil {
		t.Fatal(err)
	}
	route, binding := weightedHTTPRouteFixture(namespace)
	route.SetName("weighted-search")
	binding.Key, binding.InstanceUID = client.ObjectKeyFromObject(route), string(instance.UID)
	if err := c.Create(ctx, route); err != nil {
		t.Fatal(err)
	}
	binding.UID = route.GetUID()
	_, digest, err := inspectWeightRoute(route, binding)
	if err != nil {
		t.Fatal(err)
	}
	binding.SpecWithoutWeights = digest
	observed, err := readWeightRoute(ctx, c, binding, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	key, signer, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatal(err)
	}
	action := rollout.WeightAction{
		Kind: rollout.PatchRouteWeights, RouteUID: observed.RouteUID, ResourceVersion: observed.ResourceVersion,
		From: observed.Weights, To: map[string]int{"blue": 99, "green": 1},
	}
	from := observed.Weights
	appliedAt := time.Now()
	if err := applyWeightRoute(ctx, c, c, binding, action, time.Now, nil); err != nil {
		t.Fatal(err)
	}
	observed, err = readWeightRoute(ctx, c, binding, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	proxyObservedAt := time.Now()
	healthy := map[string]map[string]string{
		"blue": {"blue-pod": "blue-jvm"}, "green": {"green-pod": "green-jvm"},
	}
	evidence := rollout.WeightEvidence{
		WeightEvidenceScope: rollout.WeightEvidenceScope{
			Namespace: namespace, InstanceUID: instance.UID, InstanceGeneration: instance.Generation,
			OperationUID: operation.UID, Role: binding.Role, RouteName: route.GetName(),
			RouteUID: route.GetUID(), RouteVersion: observed.ResourceVersion,
		},
		Weights: observed.Weights, InventoryObservedAt: proxyObservedAt,
		ServingProxyUIDs: []string{"proxy-1", "proxy-2"},
		Proxies: []rollout.WeightProxyReport{
			{ProxyUID: "proxy-1", RouteVersion: observed.ResourceVersion, ObservedAt: proxyObservedAt, Healthy: healthy, Requests: observed.Weights},
			{ProxyUID: "proxy-2", RouteVersion: observed.ResourceVersion, ObservedAt: proxyObservedAt, Healthy: healthy, Requests: observed.Weights},
		},
	}
	if err := rollout.PublishWeightEvidence(ctx, c, core, namespace, operation.Name, evidence, signer); err == nil {
		t.Fatal("publication succeeded before the operation claimed the instance")
	}
	claim := operationClaimFor(operation)
	instance.Status.ActiveOperation = &claim
	if err := c.Status().Update(ctx, instance); err != nil {
		t.Fatal(err)
	}
	if err := rollout.PublishWeightEvidence(ctx, c, core, namespace, operation.Name, evidence, signer); err != nil {
		t.Fatal(err)
	}
	intent := rollout.WeightIntent{
		InstanceUID: string(instance.UID), OperationUID: string(operation.UID), Role: binding.Role, RouteUID: observed.RouteUID,
		From: from, To: observed.Weights, Revisions: map[string]string{"blue": "blue-revision", "green": "green-revision"},
		AdmissionWeight: 1, Phase: rollout.Applied, AppliedAt: appliedAt,
	}
	snapshot := rollout.WeightSnapshot{
		Now: time.Now(), MaxAge: rollout.PlatformEvidenceMaxAge, ClaimInstanceUID: intent.InstanceUID,
		ClaimOperationUID: intent.OperationUID, Route: observed.WeightRouteObservation, MinimumSamples: 100, TolerancePoints: 3,
		Qualifications: make(map[string]rollout.FleetWeightQualification),
	}
	for fleet, revision := range intent.Revisions {
		snapshot.Qualifications[fleet] = rollout.FleetWeightQualification{
			CandidateEvidence: rollout.CandidateEvidence{
				InstanceUID: intent.InstanceUID, OperationUID: intent.OperationUID, Role: intent.Role, Fleet: fleet, Revision: revision,
				CapacityQualified: true, PeerQualified: true, SecurityQualified: true, RollbackCompatible: true, ObservedAt: proxyObservedAt,
			},
			Processes: healthy[fleet],
		}
	}
	reportUnchanged, err := readWeightProxyEvidence(ctx, core, key, instance, operation, observed, rollout.RouteAuthIdentity{}, &snapshot)
	if err != nil || !reportUnchanged(ctx) {
		t.Fatal(err)
	}
	result, err := rollout.PlanWeightStep(intent, snapshot)
	if err != nil || result.Kind != rollout.RecordConverged {
		t.Fatalf("expected verified cutover convergence, got %#v, %v", result, err)
	}
	reports := core.ConfigMaps(namespace)
	report, err := reports.Get(ctx, rollout.WeightEvidenceName(operation.UID, binding.Role), metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	report.Annotations = map[string]string{"fixture": "updated-after-read"}
	if _, err := reports.Update(ctx, report, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	if reportUnchanged(ctx) {
		t.Fatal("weighted proxy evidence stayed current across a ConfigMap version change")
	}
	action.ResourceVersion = observed.ResourceVersion
	action.From, action.To = observed.Weights, map[string]int{"blue": 0, "green": 100}
	if err := applyWeightRoute(ctx, c, c, binding, action, time.Now, nil); err != nil {
		t.Fatal(err)
	}
	observed, err = readWeightRoute(ctx, c, binding, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	if err := rollout.PublishWeightEvidence(ctx, c, core, namespace, operation.Name, evidence, signer); err == nil {
		t.Fatal("stale evidence was published after a route version change")
	}
	if _, err := readWeightProxyEvidence(ctx, core, key, instance, operation, observed, rollout.RouteAuthIdentity{}, &snapshot); err == nil ||
		len(snapshot.Proxies) != 0 || len(snapshot.ExpectedProxyUIDs) != 0 {
		t.Fatal("stale signed evidence survived a route version change")
	}

	instance.Spec.Security = &v1alpha1.SecurityConfig{Provider: serverlessBasicAuth,
		AuthenticationSecretRef: &v1alpha1.SecretKeyReference{Name: "bootstrap", Key: "password"}}
	if err := c.Update(ctx, instance); err != nil {
		t.Fatal(err)
	}
	instance.Status.BasicAuthBaseline = &v1alpha1.BasicAuthBaseline{SecretName: "bootstrap", SecretUID: "bootstrap-uid", Key: "password", BundleSHA256: strings.Repeat("a", 64)}
	if err := c.Status().Update(ctx, instance); err != nil {
		t.Fatal(err)
	}
	evidence.InstanceGeneration = instance.Generation
	evidence.RouteVersion, evidence.Weights = observed.ResourceVersion, observed.Weights
	evidence.InventoryObservedAt = time.Now()
	for i := range evidence.Proxies {
		evidence.Proxies[i].RouteVersion = observed.ResourceVersion
		evidence.Proxies[i].ObservedAt = evidence.InventoryObservedAt
	}
	if err := rollout.PublishWeightEvidence(ctx, c, core, namespace, operation.Name, evidence, signer); err == nil {
		t.Fatal("published an anonymous observation for a protected instance")
	}
	immutable := true
	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "route-probe", Namespace: namespace}, Type: rollout.RouteCredentialSecretType,
		Immutable: &immutable, Data: map[string][]byte{"username": []byte("proxy-probe"), "password": []byte("fixture-password")}}
	secrets := core.Secrets(namespace)
	secret, err = secrets.Create(ctx, secret, metav1.CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	auth := rollout.RouteAuthIdentity{AuthSecretName: secret.Name, AuthSecretUID: secret.UID, AuthSecretVersion: secret.ResourceVersion, AuthProbeURL: "https://search.example.com"}
	evidence.RouteAuthIdentity = auth
	evidence.AuthProbeURL = "https://another.example.com"
	if err := rollout.PublishWeightEvidence(ctx, c, core, namespace, operation.Name, evidence, signer); err == nil {
		t.Fatal("published authenticated evidence from another virtual host")
	}
	evidence.AuthProbeURL = auth.AuthProbeURL
	if err := rollout.PublishWeightEvidence(ctx, c, core, namespace, operation.Name, evidence, signer); err != nil {
		t.Fatal(err)
	}
	snapshot.Now = time.Now()
	if _, err := readWeightProxyEvidence(ctx, core, key, instance, operation, observed, auth, &snapshot); err != nil || len(snapshot.Proxies) != 2 {
		t.Fatalf("current authentication evidence was not accepted: %v", err)
	}
	if err := secrets.Delete(ctx, secret.Name, metav1.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	secret.UID, secret.ResourceVersion = "", ""
	secret, err = secrets.Create(ctx, secret, metav1.CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := rollout.PublishWeightEvidence(ctx, c, core, namespace, operation.Name, evidence, signer); err == nil {
		t.Fatal("published evidence for a replaced credential Secret")
	}
	newAuth := auth
	newAuth.AuthSecretUID, newAuth.AuthSecretVersion = secret.UID, secret.ResourceVersion
	if _, err := readWeightProxyEvidence(ctx, core, key, instance, operation, observed, newAuth, &snapshot); err == nil || len(snapshot.Proxies) != 0 {
		t.Fatal("old signed evidence qualified a replacement credential Secret")
	}
}
