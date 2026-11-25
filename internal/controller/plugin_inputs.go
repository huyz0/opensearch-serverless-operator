package controller

import (
	"context"
	"crypto/x509"
	"fmt"
	"regexp"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/validation"

	"github.com/huyz0/opensearch-serverless-operator/api/v1alpha1"
	"github.com/huyz0/opensearch-serverless-operator/internal/plugins"
)

var qualifiedPluginImage = regexp.MustCompile(`^.+@sha256:[a-f0-9]{64}$`)

// pluginInputs are observations only. The controller must persist their
// identities, stage immutable copies and pass their hashes to the installer
// before creating a Pod; this helper does not authorize or install a plugin.
type pluginInputs struct {
	Resolution                           *plugins.Resolution
	PolicyName, PolicyUID, PolicyVersion string
	PolicyJSON                           []byte
	InstallerImage                       string
	CredentialsUID                       string
	CredentialsHasCA                     bool
}

func (r *InstanceReconciler) pluginsForRole(ctx context.Context, instance *v1alpha1.OpenSearchServerless, role string) (*pluginInputs, error) {
	if len(instance.Spec.Plugins) == 0 {
		return nil, nil
	}
	return r.resolvePluginInputs(ctx, instance, role, r.DevelopmentPluginInstallerImage, r.DevelopmentPluginRuntimeImage, r.DevelopmentPluginPolicy)
}

// resolvePluginInputs accepts an independently controlled immutable policy and
// one immutable credentials Secret for a role. The configured runtime image is
// an operator-admin qualification that it is compatible with the installer and
// does not require filesystem plugins hidden by the installer mount.
func (r *InstanceReconciler) resolvePluginInputs(ctx context.Context, instance *v1alpha1.OpenSearchServerless, role, installerImage, qualifiedRuntimeImage, policyName string) (*pluginInputs, error) {
	if r.Core == nil || instance == nil || instance.Namespace == "" || instance.UID == "" || !qualifiedPluginImage.MatchString(installerImage) || !qualifiedPluginImage.MatchString(qualifiedRuntimeImage) || instance.Spec.Image != qualifiedRuntimeImage || policyName == "" || len(validation.IsDNS1123Subdomain(policyName)) != 0 {
		return nil, fmt.Errorf("qualified runtime and installer images, namespace and policy are required")
	}
	policy, err := r.Core.ConfigMaps(instance.Namespace).Get(ctx, policyName, metav1.GetOptions{})
	if err != nil {
		return nil, fmt.Errorf("read plugin policy: %w", err)
	}
	if policy.UID == "" || policy.ResourceVersion == "" || policy.DeletionTimestamp != nil || policy.Immutable == nil || !*policy.Immutable || len(policy.Data) != 1 || len(policy.BinaryData) != 0 || policy.Data["policy.json"] == "" {
		return nil, fmt.Errorf("plugin policy must be a live immutable ConfigMap containing only policy.json")
	}
	raw := []byte(policy.Data["policy.json"])
	resolved, err := plugins.ResolveRole(instance.Spec.Plugins, role, raw)
	if err != nil {
		return nil, fmt.Errorf("resolve plugin request: %w", err)
	}
	if resolved == nil {
		return nil, nil
	}
	inputs := &pluginInputs{Resolution: resolved, PolicyName: policy.Name, PolicyUID: string(policy.UID), PolicyVersion: policy.ResourceVersion, PolicyJSON: raw, InstallerImage: installerImage}
	if resolved.CredentialsSecret != "" {
		secret, err := r.Core.Secrets(instance.Namespace).Get(ctx, resolved.CredentialsSecret, metav1.GetOptions{})
		if err != nil {
			return nil, fmt.Errorf("read plugin credentials identity: %w", err)
		}
		if secret.UID == "" || secret.DeletionTimestamp != nil || secret.Immutable == nil || !*secret.Immutable || len(secret.Data["credentials.json"]) == 0 || len(secret.Data["credentials.json"]) > 1<<20 {
			return nil, fmt.Errorf("plugin credentials must be a live immutable Secret with bounded credentials.json")
		}
		inputs.CredentialsUID = string(secret.UID)
		if ca, present := secret.Data["ca.crt"]; present {
			pool := x509.NewCertPool()
			if len(ca) == 0 || len(ca) > 1<<20 || !pool.AppendCertsFromPEM(ca) {
				return nil, fmt.Errorf("plugin registry CA bundle is invalid or too large")
			}
			inputs.CredentialsHasCA = true
		}
	}
	return inputs, nil
}
