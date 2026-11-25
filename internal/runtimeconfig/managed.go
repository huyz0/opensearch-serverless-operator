package runtimeconfig

import (
	"fmt"
	"sort"
	"strings"

	"github.com/huyz0/opensearch-serverless-operator/api/v1alpha1"

	"github.com/huyz0/opensearch-serverless-operator/internal/storage"
)

const S3WebIdentityTokenPath = "/var/run/serverless/s3-identity/token"

const (
	HTTPServerCertDir  = "/var/run/serverless/http-cert"
	HTTPServerTrustDir = "/var/run/serverless/http-trust"
	BasicAuthUser      = "admin"
	BasicAuthFile      = "probe-password"
	BasicAuthSourceDir = "/var/run/serverless/startup-credentials"
	BasicAuthBundleEnv = "SERVERLESS_STARTUP_BUNDLE_SHA256"
)

// ManagedInputs contains operator-owned process identity and storage settings.
type ManagedInputs struct {
	NodeName                string
	ClusterName             string
	Role                    string
	Zone                    string
	Home                    string
	ConfigPath              string
	NetworkHost             string
	PeerAdvertiseHost       string
	PeerTLS                 bool
	PeerTLSLeafSHA256       string
	PeerTLSTrustSHA256      string
	HTTPTLSCertificateHash  string
	BasicAuth               bool
	Store                   storage.S3
	JVMHeap                 string
	WarmOnJoin              bool
	RequireWarmupCompletion bool
	S3WebIdentity           bool
}

// ManagedArguments builds literal JVM tokens for the pinned shell. The caller
// must qualify network addresses, credentials, TLS and the runtime image before
// using them. User-provided args cannot override operator-managed keys.
func ManagedArguments(input ManagedInputs, userArgs []string) ([]string, error) {
	if input.NodeName == "" || input.ClusterName == "" || input.Zone == "" || input.Home == "" || input.ConfigPath == "" || input.NetworkHost == "" || input.Store.Bucket == "" || input.Store.Region == "" {
		return nil, fmt.Errorf("node, cluster, zone, paths, network and store identity are required")
	}
	if input.Role != "search" && input.Role != "ingest" {
		return nil, fmt.Errorf("unsupported role %q", input.Role)
	}
	if input.Role != "search" && input.WarmOnJoin {
		return nil, fmt.Errorf("warm-on-join is only valid for search")
	}
	if input.RequireWarmupCompletion && (input.Role != "search" || !input.WarmOnJoin) {
		return nil, fmt.Errorf("warm-up completion requires search and warm-on-join")
	}
	bytes, err := v1alpha1.ParseJVMHeapBytes(input.JVMHeap)
	if err != nil {
		return nil, err
	}
	settings := input.Store.JavaSettings()
	if input.S3WebIdentity {
		settings["s3.client.default.identity_token_file"] = S3WebIdentityTokenPath
	}
	settings["node.name"] = input.NodeName
	settings["cluster.name"] = input.ClusterName
	settings["serverless.roles"] = input.Role
	settings["node.attr.zone"] = input.Zone
	settings["path.home"] = input.Home
	settings["opensearch.path.conf"] = input.ConfigPath
	settings["network.host"] = input.NetworkHost
	settings["serverless.search.warm_on_join"] = fmt.Sprintf("%t", input.WarmOnJoin)
	if input.RequireWarmupCompletion {
		settings["serverless.search.require_warmup_completion"] = "true"
	}
	if input.PeerAdvertiseHost != "" {
		settings["serverless.transport.advertise_host"] = input.PeerAdvertiseHost
	}
	if input.PeerTLS {
		if input.PeerAdvertiseHost == "" || !isSHA256(input.PeerTLSLeafSHA256) || !isSHA256(input.PeerTLSTrustSHA256) {
			return nil, fmt.Errorf("peer TLS requires an advertised DNS identity and public material hashes")
		}
		settings["serverless.plugins"] = "org.opensearch.serverless.operator.PeerTlsPlugin"
		settings["serverless.transport.require_secure"] = "true"
		settings["serverless.peer_tls.certificate"] = "/var/run/serverless/peer-cert/tls.crt"
		settings["serverless.peer_tls.private_key"] = "/var/run/serverless/peer-cert/tls.key"
		settings["serverless.peer_tls.trust"] = "/var/run/serverless/peer-trust/ca.crt"
		settings["serverless.peer_tls.expected_leaf_sha256"] = input.PeerTLSLeafSHA256
		settings["serverless.peer_tls.expected_trust_sha256"] = input.PeerTLSTrustSHA256
	} else if input.PeerTLSLeafSHA256 != "" || input.PeerTLSTrustSHA256 != "" {
		return nil, fmt.Errorf("peer TLS hashes require peer TLS")
	}
	if input.HTTPTLSCertificateHash != "" {
		if !isSHA256(input.HTTPTLSCertificateHash) {
			return nil, fmt.Errorf("HTTP TLS requires a valid certificate file hash")
		}
		const httpPlugin = "org.opensearch.serverless.operator.HttpTlsPlugin"
		if previous := settings["serverless.plugins"]; previous != "" {
			settings["serverless.plugins"] = previous + "," + httpPlugin
		} else {
			settings["serverless.plugins"] = httpPlugin
		}
		settings["serverless.http_tls.certificate"] = HTTPServerCertDir + "/tls.crt"
		settings["serverless.http_tls.private_key"] = HTTPServerCertDir + "/tls.key"
		settings["serverless.http_tls.expected_certificate_sha256"] = input.HTTPTLSCertificateHash
	}
	if input.BasicAuth {
		if input.HTTPTLSCertificateHash == "" {
			return nil, fmt.Errorf("managed Basic Auth requires HTTP TLS")
		}
		settings["serverless.plugins"] += ",org.opensearch.serverless.auth.ServerlessAuthPlugin"
		settings["serverless.auth.bootstrap.username"] = BasicAuthUser
	}
	var prepareWriter, persistReceipt bool
	for _, argument := range userArgs {
		if strings.HasPrefix(argument, "-Xms") || strings.HasPrefix(argument, "-Xmx") || strings.HasPrefix(argument, "-XX:InitialHeapSize=") || strings.HasPrefix(argument, "-XX:MaxHeapSize=") {
			return nil, fmt.Errorf("user argument overrides managed JVM heap")
		}
		if strings.HasPrefix(argument, "-D") {
			key, value, _ := strings.Cut(strings.TrimPrefix(argument, "-D"), "=")
			if key == writerHandoffReader && !input.BasicAuth {
				return nil, fmt.Errorf("writer handoff receipt readback requires managed Basic Auth and HTTP TLS")
			}
			if key == writerRetirementPreparer {
				prepareWriter = true
			}
			if key == persistWriterHandoff {
				persistReceipt = value == "true"
			}
			if input.BasicAuth && strings.HasPrefix(key, "serverless.auth.bootstrap.") {
				return nil, fmt.Errorf("user argument overrides a managed bootstrap setting")
			}
			if input.HTTPTLSCertificateHash != "" && key == "http.type" {
				return nil, fmt.Errorf("user argument overrides the managed HTTP TLS transport")
			}
			if managedPodProperty(key) || strings.HasPrefix(key, "serverless.store.") || strings.HasPrefix(key, "serverless.peer_tls.") || strings.HasPrefix(key, "serverless.http_tls.") {
				return nil, fmt.Errorf("user argument overrides managed setting %q", key)
			}
		}
	}
	if prepareWriter && (!input.BasicAuth || input.Role != "ingest" || !persistReceipt) {
		return nil, fmt.Errorf("writer retirement preparation requires an ingest role, durable receipts, managed Basic Auth and HTTP TLS")
	}
	keys := make([]string, 0, len(settings))
	for key := range settings {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	args := append([]string{fmt.Sprintf("-Xms%d", bytes), fmt.Sprintf("-Xmx%d", bytes)}, userArgs...)
	for _, key := range keys {
		args = append(args, "-D"+key+"="+settings[key])
	}
	return args, nil
}

func isSHA256(value string) bool {
	if len(value) != 64 {
		return false
	}
	for _, c := range value {
		if !(c >= '0' && c <= '9') && !(c >= 'a' && c <= 'f') {
			return false
		}
	}
	return true
}
