package rollout

import (
	"context"
	"fmt"
	"net/url"
	"path"
	"regexp"
	"slices"
	"strings"
	"unicode/utf8"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation"
	typedcore "k8s.io/client-go/kubernetes/typed/core/v1"

	"github.com/huyz0/opensearch-serverless-operator/api/v1alpha1"
	"github.com/huyz0/opensearch-serverless-operator/internal/java"
)

const RouteCredentialSecretType = "serverless.opensearch.org/rollout-probe"

var routeUsername = regexp.MustCompile(`^[A-Za-z0-9_.@-]{1,64}$`)

// RouteAuthIdentity binds an authentication observation to the immutable
// Secret used to make it. The password is never part of the evidence.
type RouteAuthIdentity struct {
	AuthSecretName    string    `json:"authSecretName,omitempty"`
	AuthSecretUID     types.UID `json:"authSecretUID,omitempty"`
	AuthSecretVersion string    `json:"authSecretVersion,omitempty"`
	AuthProbeURL      string    `json:"authProbeURL,omitempty"`
}

func (id RouteAuthIdentity) valid() bool {
	return id == (RouteAuthIdentity{}) || (id.AuthSecretName != "" && len(validation.IsDNS1123Subdomain(id.AuthSecretName)) == 0 && id.AuthSecretUID != "" && id.AuthSecretVersion != "")
}

func canonicalAuthProbeURL(raw string) (string, error) {
	base, err := url.Parse(raw)
	if err != nil || base.Scheme != "https" || len(validation.IsDNS1123Subdomain(strings.ToLower(base.Hostname()))) != 0 ||
		base.User != nil || base.RawQuery != "" || base.ForceQuery || base.Fragment != "" || base.RawPath != "" || base.Opaque != "" {
		return "", fmt.Errorf("authenticated probe requires an HTTPS base URL with a literal DNS hostname")
	}
	base.Path = strings.TrimRight(base.Path, "/")
	if base.Path != "" && path.Clean(base.Path) != base.Path {
		return "", fmt.Errorf("authenticated probe requires a clean base path")
	}
	base.Host = strings.ToLower(base.Host)
	return base.String(), nil
}

// BindAuthenticatedProbeURL ensures the credential will be sent to the role
// route's literal virtual host and its single weighted rule.
func BindAuthenticatedProbeURL(raw string, route *unstructured.Unstructured) (string, error) {
	if route == nil {
		return "", fmt.Errorf("authenticated probe requires a weighted HTTPRoute")
	}
	canonical, err := canonicalAuthProbeURL(raw)
	if err != nil {
		return "", err
	}
	base, _ := url.Parse(canonical)
	hostnames, _, _ := unstructured.NestedStringSlice(route.Object, "spec", "hostnames")
	if !slices.Contains(hostnames, base.Hostname()) {
		return "", fmt.Errorf("authenticated probe hostname must be listed on its weighted HTTPRoute")
	}
	rules, _, _ := unstructured.NestedSlice(route.Object, "spec", "rules")
	if len(rules) != 1 {
		return "", fmt.Errorf("authenticated probe requires one weighted HTTPRoute rule")
	}
	rule, _ := rules[0].(map[string]any)
	matches, found, err := unstructured.NestedSlice(rule, "matches")
	if err == nil && (!found || len(matches) == 0) {
		return canonical, nil
	}
	statsPath := base.Path + "/_serverless/stats"
	for _, item := range matches {
		match, ok := item.(map[string]any)
		if ok && matchesAuthenticatedProbe(match, statsPath) {
			return canonical, nil
		}
	}
	return "", fmt.Errorf("weighted HTTPRoute does not match the authenticated GET stats probe")
}

func matchesAuthenticatedProbe(match map[string]any, statsPath string) bool {
	if method := match["method"]; (method != nil && method != "GET") || match["headers"] != nil || match["queryParams"] != nil {
		return false
	}
	if match["path"] == nil {
		return true
	}
	value, _, _ := unstructured.NestedString(match, "path", "value")
	if !strings.HasPrefix(value, "/") {
		return false
	}
	kind, _, _ := unstructured.NestedString(match, "path", "type")
	switch kind {
	case "Exact":
		return statsPath == value
	case "", "PathPrefix":
		prefix := strings.TrimRight(value, "/")
		return statsPath == prefix || strings.HasPrefix(statsPath, prefix+"/")
	default:
		return false
	}
}

type RouteCredential struct {
	RouteAuthIdentity
	Username string
	Password []byte
}

func (credential *RouteCredential) Unchanged(ctx context.Context, core typedcore.CoreV1Interface, namespace string) bool {
	if credential == nil {
		return true
	}
	return credential.RouteAuthIdentity.Unchanged(ctx, core, namespace)
}

func (id RouteAuthIdentity) Unchanged(ctx context.Context, core typedcore.CoreV1Interface, namespace string) bool {
	if id == (RouteAuthIdentity{}) {
		return true
	}
	if core == nil || !id.valid() {
		return false
	}
	secret, err := core.Secrets(namespace).Get(ctx, id.AuthSecretName, metav1.GetOptions{})
	return err == nil && secret.UID == id.AuthSecretUID && secret.ResourceVersion == id.AuthSecretVersion && secret.DeletionTimestamp == nil
}

// ReadRouteCredential resolves only an ordinary account dedicated to rollout probes.
func ReadRouteCredential(ctx context.Context, core typedcore.CoreV1Interface, instance *v1alpha1.OpenSearchServerless, name string) (*RouteCredential, error) {
	if instance.Spec.Security == nil && instance.Status.BasicAuthBaseline == nil && name == "" {
		return nil, nil
	}
	if instance.Spec.Security == nil || instance.Spec.Security.Provider != "ServerlessBasicAuth" || instance.Status.BasicAuthBaseline == nil || name == "" ||
		name == instance.Status.BasicAuthBaseline.SecretName || core == nil {
		return nil, fmt.Errorf("managed Basic Auth rollout requires a dedicated client credential for each role")
	}
	secret, err := core.Secrets(instance.Namespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil || secret.UID == "" || secret.ResourceVersion == "" || secret.DeletionTimestamp != nil || secret.Immutable == nil || !*secret.Immutable || secret.Type != RouteCredentialSecretType {
		return nil, fmt.Errorf("rollout client credential must be a live, immutable rollout-probe Secret")
	}
	username, password := string(secret.Data["username"]), secret.Data["password"]
	if !routeUsername.MatchString(username) || username == "admin" || username == "_version" ||
		len(password) == 0 || len(password) > java.MaxHealthPasswordBytes || !utf8.Valid(password) {
		return nil, fmt.Errorf("rollout client credential requires an ordinary account and valid password")
	}
	return &RouteCredential{RouteAuthIdentity: RouteAuthIdentity{AuthSecretName: name, AuthSecretUID: secret.UID, AuthSecretVersion: secret.ResourceVersion}, Username: username, Password: password}, nil
}
