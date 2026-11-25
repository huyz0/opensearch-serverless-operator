package runtimeconfig

import (
	"slices"
	"strconv"
	"strings"

	"github.com/huyz0/opensearch-serverless-operator/internal/storage"
)

// PodIPReference is the Pod-IP field reference the workload may expand.
const PodIPReference = "$(POD_IP)"

// ManagedPodArguments renders the launcher arguments for a Kubernetes Pod.
// Only the operator's exact network.host reference is expanded by kubelet.
func ManagedPodArguments(input ManagedInputs, custom []string) ([]string, error) {
	args, err := ManagedArguments(input, custom)
	if err != nil {
		return nil, err
	}
	for i, arg := range args {
		if arg != "-Dnetwork.host="+PodIPReference {
			args[i] = escapePodArgument(arg)
		}
	}
	return args, nil
}

func literalPodArguments(args []string) []string {
	result := make([]string, len(args))
	for i, arg := range args {
		result[i] = escapePodArgument(arg)
	}
	return result
}

func escapePodArgument(arg string) string {
	return strings.ReplaceAll(arg, "$", "$$")
}

// MatchesPodRuntime checks the launcher envelope and the custom runtime tokens
// pinned for a fleet. Template admission verifies the managed property values.
func MatchesPodRuntime(actual, custom []string) bool {
	firstManaged := 2 + len(custom)
	if len(actual) < firstManaged || !strings.HasPrefix(actual[0], "-Xms") ||
		!strings.HasPrefix(actual[1], "-Xmx") || actual[0][4:] != actual[1][4:] {
		return false
	}
	heap, err := strconv.ParseInt(actual[0][4:], 10, 64)
	if err != nil || heap <= 0 || strconv.FormatInt(heap, 10) != actual[0][4:] ||
		!slices.Equal(actual[2:firstManaged], literalPodArguments(custom)) {
		return false
	}
	// ManagedArguments emits sorted, unique -D keys after the custom arguments.
	previous := ""
	required := len(requiredPodProperties)
	for _, arg := range actual[firstManaged:] {
		if !strings.HasPrefix(arg, "-D") {
			return false
		}
		key, value, hasValue := strings.Cut(arg[2:], "=")
		if !hasValue || key <= previous || !managedPodProperty(key) {
			return false
		}
		if !(key == "network.host" && value == PodIPReference) && strings.Contains(strings.ReplaceAll(value, "$$", ""), "$") {
			return false
		}
		if requiredPodProperties[key] {
			required--
		}
		previous = key
	}
	return required == 0
}

// MatchesPodPrimaryStore checks the Pod's encoded store arguments against
// the pinned location, including omission of an unconfigured endpoint.
func MatchesPodPrimaryStore(args []string, store storage.S3) bool {
	expected := store.JavaSettings()
	for _, arg := range args {
		if !strings.HasPrefix(arg, "-Dserverless.store.") {
			continue
		}
		key, value, ok := strings.Cut(arg[2:], "=")
		pinned, found := expected[key]
		if !ok || !found || value != escapePodArgument(pinned) {
			return false
		}
		delete(expected, key)
	}
	return len(expected) == 0
}

var requiredPodProperties = map[string]bool{
	"node.name": true, "cluster.name": true, "serverless.roles": true, "node.attr.zone": true,
	"path.home": true, "opensearch.path.conf": true, "network.host": true,
	"serverless.store.type": true, "serverless.store.bucket": true, "serverless.store.region": true,
	"serverless.store.path_style_access": true, "serverless.search.warm_on_join": true,
}

func managedPodProperty(key string) bool {
	if requiredPodProperties[key] {
		return true
	}
	switch key {
	case "serverless.store.endpoint", "s3.client.default.identity_token_file",
		"serverless.search.require_warmup_completion",
		"serverless.plugins", "serverless.transport.advertise_host", "serverless.transport.require_secure",
		"serverless.peer_tls.certificate", "serverless.peer_tls.private_key", "serverless.peer_tls.trust",
		"serverless.peer_tls.expected_leaf_sha256", "serverless.peer_tls.expected_trust_sha256",
		"serverless.http_tls.certificate", "serverless.http_tls.private_key", "serverless.http_tls.expected_certificate_sha256",
		"serverless.auth.bootstrap.username":
		return true
	}
	return false
}
