package controller

import (
	"context"
	"fmt"

	"github.com/huyz0/opensearch-serverless-operator/api/v1alpha1"
)

const serverlessBasicAuth = "ServerlessBasicAuth"

func basicAuthReference(instance *v1alpha1.OpenSearchServerless) (*v1alpha1.SecretKeyReference, error) {
	security := instance.Spec.Security
	if security == nil {
		if instance.Status.BasicAuthBaseline != nil {
			return nil, fmt.Errorf("startup authentication is pinned for this instance")
		}
		return nil, nil
	}
	ref := security.AuthenticationSecretRef
	credentials := instance.Spec.Storage.Credentials
	if security.Provider != serverlessBasicAuth || security.AuthorizationConfigMapRef != nil || security.Reload != nil ||
		ref == nil || ref.Name == "" || ref.Key == "" || ref.Key == "opensearch.keystore" ||
		credentials == nil || credentials.Type != "Keystore" || credentials.KeystoreSecretRef == nil || credentials.KeystoreSecretRef.Name != ref.Name {
		return nil, fmt.Errorf("development Basic Auth requires a password key in the referenced S3 keystore Secret and startup-only authentication")
	}
	if pins := instance.Status.BasicAuthBaseline; pins != nil && (pins.SecretName != ref.Name || pins.Key != ref.Key || pins.SecretUID == "" || pins.BundleSHA256 == "") {
		return nil, fmt.Errorf("startup authentication reference differs from the pinned baseline")
	}
	return ref, nil
}

func (r *InstanceReconciler) resolveBasicAuth(ctx context.Context, instance *v1alpha1.OpenSearchServerless) (*v1alpha1.BasicAuthBaseline, []byte, error) {
	return r.podHTTP().resolveBasicAuth(ctx, instance)
}
