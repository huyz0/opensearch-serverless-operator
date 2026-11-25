package rollout

import (
	"context"
	"crypto/ed25519"
	"fmt"
	"time"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	typedcore "k8s.io/client-go/kubernetes/typed/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/huyz0/opensearch-serverless-operator/api/v1alpha1"
)

// PublishWeightEvidence signs a trusted platform verifier's replica inventory,
// health and traffic reports after checking their live Kubernetes scope.
func PublishWeightEvidence(ctx context.Context, reader client.Reader, core typedcore.CoreV1Interface, namespace, operationName string, evidence WeightEvidence, signer ed25519.PrivateKey) error {
	if reader == nil || core == nil || namespace == "" || namespace != evidence.Namespace || operationName == "" ||
		len(signer) != ed25519.PrivateKeySize {
		return fmt.Errorf("namespace, operation, clients and Ed25519 key are required")
	}
	if err := evidence.validate(time.Now()); err != nil {
		return err
	}
	var operation v1alpha1.OpenSearchServerlessOperation
	if err := reader.Get(ctx, client.ObjectKey{Namespace: namespace, Name: operationName}, &operation); err != nil {
		return err
	}
	if err := operation.Spec.Validate(); err != nil {
		return err
	}
	if operation.UID != evidence.OperationUID || operation.DeletionTimestamp != nil ||
		(operation.Spec.Action != "Promote" && operation.Spec.Action != "Rollback") ||
		operation.Spec.InstanceRef.UID != string(evidence.InstanceUID) {
		return fmt.Errorf("operation differs from the weighted route evidence")
	}
	var instance v1alpha1.OpenSearchServerless
	if err := reader.Get(ctx, client.ObjectKey{Namespace: namespace, Name: operation.Spec.InstanceRef.Name}, &instance); err != nil {
		return err
	}
	claim := v1alpha1.OperationClaim{
		Name: operation.Name, UID: string(operation.UID), Action: operation.Spec.Action, Generation: operation.Generation,
	}
	if instance.UID != evidence.InstanceUID || instance.Generation != evidence.InstanceGeneration ||
		instance.DeletionTimestamp != nil || instance.Spec.Rollout == nil || !instance.Spec.Rollout.GatewayBackends ||
		instance.Status.ActiveOperation == nil || *instance.Status.ActiveOperation != claim {
		return fmt.Errorf("instance or active operation differs from the weighted route evidence")
	}
	authenticated := instance.Spec.Security != nil || instance.Status.BasicAuthBaseline != nil
	if authenticated != (evidence.RouteAuthIdentity != (RouteAuthIdentity{})) {
		return fmt.Errorf("proxy authentication observation does not match the instance security mode")
	}
	route := &unstructured.Unstructured{}
	route.SetGroupVersionKind(schema.GroupVersionKind{Group: "gateway.networking.k8s.io", Version: "v1", Kind: "HTTPRoute"})
	if err := reader.Get(ctx, client.ObjectKey{Namespace: namespace, Name: evidence.RouteName}, route); err != nil {
		return err
	}
	if route.GetUID() != evidence.RouteUID || route.GetResourceVersion() != evidence.RouteVersion || route.GetDeletionTimestamp() != nil {
		return fmt.Errorf("HTTPRoute changed since the platform observation")
	}
	if authenticated {
		canonical, err := BindAuthenticatedProbeURL(evidence.AuthProbeURL, route)
		if err != nil || canonical != evidence.AuthProbeURL {
			return fmt.Errorf("authenticated probe URL differs from the bound weighted route")
		}
		credential, err := ReadRouteCredential(ctx, core, &instance, evidence.AuthSecretName)
		if err != nil {
			return err
		}
		credential.AuthProbeURL = canonical
		if credential.RouteAuthIdentity != evidence.RouteAuthIdentity {
			return fmt.Errorf("rollout client credential changed since the proxy observation")
		}
	}
	if err := evidence.validate(time.Now()); err != nil {
		return err
	}
	data, err := signEvidence(weightEvidenceDomain, evidence, signer)
	if err != nil {
		return err
	}
	name := WeightEvidenceName(evidence.OperationUID, evidence.Role)
	key := signer.Public().(ed25519.PublicKey)
	return publishEvidenceConfigMap(ctx, core.ConfigMaps(namespace), namespace, name, data, func(current map[string]string) bool {
		previous, err := readSignedWeightEvidence(current, key)
		if err != nil || previous.validate(time.Now()) != nil ||
			previous.OperationUID != evidence.OperationUID || previous.Role != evidence.Role ||
			previous.Namespace != evidence.Namespace || previous.InstanceUID != evidence.InstanceUID {
			return false
		}
		// One ConfigMap spans every route version for this operation and role.
		// A delayed publisher must preserve a newer observation of another version.
		order := previous.InventoryObservedAt.Compare(evidence.InventoryObservedAt)
		return order > 0 || (order == 0 && previous.WeightEvidenceScope == evidence.WeightEvidenceScope)
	})
}
