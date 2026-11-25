package controller

import (
	"context"
	"crypto/ed25519"
	"fmt"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	typedcore "k8s.io/client-go/kubernetes/typed/core/v1"

	"github.com/huyz0/opensearch-serverless-operator/api/v1alpha1"
	"github.com/huyz0/opensearch-serverless-operator/internal/rollout"
)

// readWeightProxyEvidence fills the planner snapshot from namespaced signed
// evidence. The caller supplies the already verified route and trusted key.
func readWeightProxyEvidence(ctx context.Context, core typedcore.CoreV1Interface, key ed25519.PublicKey, instance *v1alpha1.OpenSearchServerless, operation *v1alpha1.OpenSearchServerlessOperation, route observedWeightRoute, auth rollout.RouteAuthIdentity, snapshot *rollout.WeightSnapshot) (func(context.Context) bool, error) {
	if snapshot == nil {
		return nil, fmt.Errorf("weighted rollout snapshot is required")
	}
	snapshot.ExpectedProxyUIDs, snapshot.Proxies = nil, nil
	snapshot.RouteAuthIdentity = rollout.RouteAuthIdentity{}
	if core == nil || instance == nil || operation == nil || snapshot.Now.IsZero() || len(key) != ed25519.PublicKeySize ||
		instance.DeletionTimestamp != nil || operation.DeletionTimestamp != nil ||
		instance.Namespace == "" || instance.Namespace != operation.Namespace ||
		instance.Status.ActiveOperation == nil || *instance.Status.ActiveOperation != operationClaimFor(operation) ||
		operation.Spec.InstanceRef.UID != string(instance.UID) || operation.Spec.InstanceRef.Name != instance.Name ||
		!route.ScopeVerified || route.Object == nil || route.Object.GetNamespace() != instance.Namespace ||
		route.InstanceUID != string(instance.UID) || route.RouteUID != string(route.Object.GetUID()) ||
		route.ResourceVersion != route.Object.GetResourceVersion() {
		return nil, fmt.Errorf("live route, operation claim and platform key are required for weighted proxy evidence")
	}
	cm, err := core.ConfigMaps(instance.Namespace).Get(ctx, rollout.WeightEvidenceName(operation.UID, route.Role), metav1.GetOptions{})
	if err != nil {
		return nil, fmt.Errorf("read weighted proxy evidence: %w", err)
	}
	if cm.UID == "" || cm.ResourceVersion == "" || cm.DeletionTimestamp != nil {
		return nil, fmt.Errorf("weighted proxy evidence has no live ConfigMap identity or version")
	}
	scope := rollout.WeightEvidenceScope{
		RouteAuthIdentity: auth,
		Namespace:         instance.Namespace, InstanceUID: instance.UID, InstanceGeneration: instance.Generation,
		OperationUID: operation.UID, Role: route.Role, RouteName: route.Object.GetName(),
		RouteUID: route.Object.GetUID(), RouteVersion: route.ResourceVersion,
	}
	snapshot.ExpectedProxyUIDs, snapshot.Proxies, err = rollout.ObserveSignedWeights(cm.Data, key, scope, route.Weights, snapshot.Now)
	if err != nil {
		return nil, err
	}
	snapshot.RouteAuthIdentity = auth
	return configMapVersionCheck(core, cm), nil
}
