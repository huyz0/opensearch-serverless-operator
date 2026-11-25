package v1alpha1

import (
	"fmt"
	"math"
	"regexp"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/util/validation"
)

var digestImage = regexp.MustCompile(`^.+@sha256:[a-f0-9]{64}$`)
var awsRoleARN = regexp.MustCompile(`^arn:[a-z0-9-]+:iam::[0-9]{12}:role/[A-Za-z0-9+=,.@_/-]+$`)
var pluginName = regexp.MustCompile(`^[a-z0-9][a-z0-9._-]*$`)

// ValidAWSRoleARN checks the public role identifier used by S3 web identity.
func ValidAWSRoleARN(value string) bool { return len(value) <= 512 && awsRoleARN.MatchString(value) }

// ValidPluginName checks a name for an installable filesystem plugin.
// repository-s3 is bundled by the runtime and configured through storage.
func ValidPluginName(name string) bool {
	return pluginName.MatchString(name) && name != "repository-s3"
}

// Validate checks the subset of fields currently represented by the API.
// Resolving references and authorizing their use require separate checks.
func (spec OpenSearchServerlessSpec) Validate() error {
	if !digestImage.MatchString(spec.Image) || spec.ServiceAccountName == "" {
		return fmt.Errorf("a digest-pinned image and service account are required")
	}
	if err := validateStorage(spec.Storage); err != nil {
		return err
	}
	if len(spec.Pools) == 0 {
		return fmt.Errorf("at least one role pool is required")
	}
	if spec.Availability != nil {
		if err := validateAvailability(spec.Availability, spec.Pools); err != nil {
			return err
		}
	}
	if spec.Rollout != nil {
		if err := validateRollout(spec.Rollout, spec.TLS, spec.Pools); err != nil {
			return err
		}
	}
	if spec.Security != nil {
		if err := validateSecurity(spec.Security); err != nil {
			return err
		}
	}
	if spec.TLS != nil && (!validTLSChannel(spec.TLS.HTTP) || !validTLSChannel(spec.TLS.Transport)) {
		return fmt.Errorf("TLS requires a supported source, certificate reference and trust bundle key")
	}
	if err := validatePlugins(spec.Plugins); err != nil {
		return err
	}
	for role, pool := range spec.Pools {
		if err := validatePool(role, pool); err != nil {
			return err
		}
	}
	return nil
}

func validateStorage(storage Storage) error {
	if storage.Type != "s3" {
		return fmt.Errorf("only s3 storage is currently represented")
	}
	if credentials := storage.Credentials; credentials != nil {
		switch credentials.Type {
		case "WorkloadIdentity":
			if credentials.KeystoreSecretRef != nil || !ValidAWSRoleARN(credentials.AWSRoleARN) {
				return fmt.Errorf("workload identity requires an AWS role ARN and cannot include a keystore reference")
			}
		case "Keystore":
			if credentials.KeystoreSecretRef == nil || credentials.KeystoreSecretRef.Name == "" || credentials.AWSRoleARN != "" {
				return fmt.Errorf("keystore credentials require a Secret reference")
			}
		default:
			return fmt.Errorf("unsupported storage credential type %q", credentials.Type)
		}
	}
	if storage.RepositoryRef != nil {
		if storage.RepositoryRef.Name == "" || storage.Bucket != "" || storage.Region != "" || storage.Endpoint != "" || storage.PathStyleAccess != nil {
			return fmt.Errorf("repository reference must have a name and cannot be combined with inline storage settings")
		}
	} else if storage.Bucket == "" || storage.Region == "" {
		return fmt.Errorf("inline s3 storage requires bucket and region")
	}
	return nil
}

func validateAvailability(availability *Availability, pools map[string]Pool) error {
	if availability.MaxConcurrentRetirements < 0 || availability.MaxRollingSurgePerGroup < 0 || availability.MaxPendingProvisionablePods < 0 || availability.MaxConcurrentSearchWarmups < 0 {
		return fmt.Errorf("availability budgets must be nonnegative")
	}
	for role, floor := range availability.MinServingTotal {
		pool, exists := pools[role]
		if !exists || floor < 0 {
			return fmt.Errorf("invalid serving floor for role %q", role)
		}
		var total int64
		for _, zone := range pool.Zones {
			total += int64(zone.Replicas) + int64(zone.WarmSpare)
		}
		if int64(floor) > total {
			return fmt.Errorf("serving floor exceeds configured replicas for role %q", role)
		}
	}
	if emergency := availability.EmergencyCapacity; emergency != nil {
		if len(validation.IsDNS1123Subdomain(emergency.EvidenceRef.Name)) != 0 || emergency.MaxTemporaryPerZone <= 0 ||
			len(emergency.MaxTemporaryByRole) == 0 || availability.MaxPendingProvisionablePods == 0 {
			return fmt.Errorf("emergency capacity requires a namespaced evidence reference and positive Pod limits")
		}
		for role, limit := range emergency.MaxTemporaryByRole {
			pool, exists := pools[role]
			if !exists || pool.CapacityProfileRef == "" || availability.MinServingTotal[role] <= 0 || limit <= 0 {
				return fmt.Errorf("emergency capacity for role %q requires a capacity profile, regional serving floor and positive limit", role)
			}
		}
	}
	return nil
}

func validateRollout(rollout *Rollout, tls *TLSConfig, pools map[string]Pool) error {
	if (rollout.Strategy != "Rolling" && rollout.Strategy != "BlueGreen") || rollout.MaxPreviewReplicas < 0 {
		return fmt.Errorf("invalid rollout strategy or preview limit")
	}
	if candidate := rollout.Candidate; candidate != nil && (rollout.Strategy != "BlueGreen" || candidate.Fleet == "" || len(validation.IsValidLabelValue(candidate.Fleet)) != 0 || !digestImage.MatchString(candidate.Image)) {
		return fmt.Errorf("candidate requires BlueGreen, a valid fleet label and a digest-pinned image")
	}
	if candidate := rollout.Candidate; candidate != nil && candidate.PeerTLS != nil {
		if tls == nil || tls.Transport == nil || !validTLSChannel(candidate.PeerTLS) {
			return fmt.Errorf("candidate peer TLS requires configured transport TLS and complete source references")
		}
		if rollout.RollbackWindow == "" {
			return fmt.Errorf("candidate peer TLS requires a finite rollback window")
		}
	}
	if candidate := rollout.Candidate; candidate != nil && candidate.HTTPRenewFromIssuer {
		if tls == nil || tls.HTTP == nil || tls.HTTP.Source != "CertManager" ||
			candidate.HTTPCertificateRef != nil || rollout.RollbackWindow == "" {
			return fmt.Errorf("candidate HTTP issuer renewal requires cert-manager HTTP TLS, a finite rollback window and no explicit leaf Secret")
		}
	}
	if candidate := rollout.Candidate; candidate != nil && candidate.PluginOverride != nil {
		duration, err := time.ParseDuration(rollout.RollbackWindow)
		if err != nil || duration <= 0 {
			return fmt.Errorf("candidate plugin override requires a positive rollback window")
		}
		if len(candidate.PluginOverride.Plugins) > 32 {
			return fmt.Errorf("candidate plugin override supports up to 32 plugins")
		}
		if err := validatePlugins(candidate.PluginOverride.Plugins); err != nil {
			return err
		}
	}
	if candidate := rollout.Candidate; candidate != nil && candidate.RuntimeOverride != nil {
		duration, err := time.ParseDuration(rollout.RollbackWindow)
		if err != nil || duration <= 0 {
			return fmt.Errorf("candidate runtime override requires a positive rollback window")
		}
		for role := range candidate.RuntimeOverride.ByRole {
			if _, exists := pools[role]; !exists {
				return fmt.Errorf("candidate runtime override has no pool for role %q", role)
			}
		}
	}
	if rollout.RollbackWindow != "" {
		duration, err := time.ParseDuration(rollout.RollbackWindow)
		if err != nil || duration < 0 {
			return fmt.Errorf("invalid rollback window")
		}
	}
	return nil
}

func validateSecurity(security *SecurityConfig) error {
	if security.Provider == "" || (security.Reload != nil && security.Reload.Mode != "HotReload" && security.Reload.Mode != "RollingReplacement") {
		return fmt.Errorf("security requires a provider and supported reload mode")
	}
	if ref := security.AuthenticationSecretRef; ref != nil && (ref.Name == "" || ref.Key == "") {
		return fmt.Errorf("authentication Secret reference requires a name and key")
	}
	if ref := security.AuthorizationConfigMapRef; ref != nil && (ref.Name == "" || ref.Key == "") {
		return fmt.Errorf("authorization ConfigMap reference requires a name and key")
	}
	return nil
}

func validatePool(role string, pool Pool) error {
	if role != "search" && role != "ingest" {
		return fmt.Errorf("unsupported pool role %q", role)
	}
	if pool.WarmReadiness != "" && pool.WarmReadiness != "Strict" && pool.WarmReadiness != "ServingReady" {
		return fmt.Errorf("pool %q has an unsupported warm readiness mode", role)
	}
	if role != "search" && (pool.WarmOnJoin || pool.WarmReadiness != "" || pool.ColdStartPolicy != "") {
		return fmt.Errorf("warm search readiness is only available on search pools")
	}
	if pool.WarmReadiness == "Strict" && !pool.WarmOnJoin {
		return fmt.Errorf("strict warm readiness requires warm on join")
	}
	if pool.ColdStartPolicy != "" && (pool.WarmReadiness != "Strict" || pool.ColdStartPolicy != "AllowOneDegraded") {
		return fmt.Errorf("cold start policy requires strict search readiness and a supported policy")
	}
	if len(pool.Zones) == 0 {
		return fmt.Errorf("pool %q must specify at least one zone", role)
	}
	containers := map[string]bool{"opensearch": true, "plugin-installer": true, "peer-dns-preflight": true}
	volumes := map[string]bool{}
	for _, volume := range pool.SidecarVolumes {
		if len(validation.IsDNS1123Label(volume.Name)) != 0 || strings.HasPrefix(volume.Name, "serverless-") || volumes[volume.Name] {
			return fmt.Errorf("pool %q has an invalid, reserved or duplicate sidecar volume", role)
		}
		volumes[volume.Name] = true
		sources := 0
		if volume.SecretRef != nil {
			sources++
			if len(validation.IsDNS1123Subdomain(volume.SecretRef.Name)) != 0 {
				return fmt.Errorf("pool %q has an invalid sidecar Secret reference", role)
			}
		}
		if volume.ConfigMapRef != nil {
			sources++
			if len(validation.IsDNS1123Subdomain(volume.ConfigMapRef.Name)) != 0 {
				return fmt.Errorf("pool %q has an invalid sidecar ConfigMap reference", role)
			}
		}
		if volume.EmptyDir {
			sources++
		}
		if sources != 1 {
			return fmt.Errorf("pool %q sidecar volume requires exactly one source", role)
		}
		if volume.SizeLimit != "" {
			size, err := resource.ParseQuantity(volume.SizeLimit)
			if !volume.EmptyDir || err != nil || size.Sign() <= 0 {
				return fmt.Errorf("pool %q sidecar volume has an invalid emptyDir size limit", role)
			}
		}
	}
	for _, sidecar := range pool.Sidecars {
		if sidecar.Name == "" || sidecar.Image == "" || containers[sidecar.Name] || sidecar.RestartPolicy != nil {
			return fmt.Errorf("pool %q has an invalid or duplicate sidecar name or image", role)
		}
		containers[sidecar.Name] = true
	}
	for _, sidecar := range pool.NativeSidecars {
		if sidecar.Name == "" || sidecar.Image == "" || containers[sidecar.Name] || (sidecar.RestartPolicy != nil && *sidecar.RestartPolicy != corev1.ContainerRestartPolicyAlways) {
			return fmt.Errorf("pool %q has an invalid or duplicate native sidecar name, image or restart policy", role)
		}
		containers[sidecar.Name] = true
	}
	for name, request := range pool.Resources.Requests {
		if request.Sign() < 0 {
			return fmt.Errorf("pool %q has a negative %s request", role, name)
		}
		if limit, ok := pool.Resources.Limits[name]; ok && request.Cmp(limit) > 0 {
			return fmt.Errorf("pool %q has a %s request above its limit", role, name)
		}
	}
	for name, limit := range pool.Resources.Limits {
		if limit.Sign() < 0 {
			return fmt.Errorf("pool %q has a negative %s limit", role, name)
		}
	}
	if pool.JVMHeap != "" {
		heap, err := ParseJVMHeapBytes(pool.JVMHeap)
		if err != nil {
			return fmt.Errorf("pool %q: %w", role, err)
		}
		if limit, ok := pool.Resources.Limits[corev1.ResourceMemory]; ok && limit.CmpInt64(heap) <= 0 {
			return fmt.Errorf("pool %q JVM heap must be below the memory limit", role)
		}
	}
	for name, zone := range pool.Zones {
		total := int64(zone.Replicas) + int64(zone.WarmSpare)
		if name == "" || zone.Replicas < 0 || zone.WarmSpare < 0 || zone.MinServing < 0 || int64(zone.MinServing) > total || total > math.MaxInt32 {
			return fmt.Errorf("invalid replica counts for pool %q zone %q", role, name)
		}
	}
	return nil
}

// ParseJVMHeapBytes resolves a positive Kubernetes quantity to exact bytes.
func ParseJVMHeapBytes(value string) (int64, error) {
	heap, err := resource.ParseQuantity(value)
	if err != nil {
		return 0, fmt.Errorf("invalid JVM heap quantity")
	}
	bytes := heap.Value()
	if bytes <= 0 || heap.CmpInt64(bytes) != 0 {
		return 0, fmt.Errorf("JVM heap must be a positive whole number of bytes")
	}
	return bytes, nil
}

func validatePlugins(config []Plugin) error {
	plugins := make(map[string]struct{}, len(config))
	for _, plugin := range config {
		if !ValidPluginName(plugin.Name) {
			return fmt.Errorf("invalid or reserved plugin name %q", plugin.Name)
		}
		if !digestImage.MatchString(plugin.Artifact) {
			return fmt.Errorf("plugin %q requires a digest-pinned artifact", plugin.Name)
		}
		if _, exists := plugins[plugin.Name]; exists {
			return fmt.Errorf("duplicate plugin name %q", plugin.Name)
		}
		plugins[plugin.Name] = struct{}{}
		if plugin.RegistryCredentialsRef != nil && plugin.RegistryCredentialsRef.Name == "" {
			return fmt.Errorf("plugin %q registry credential reference is empty", plugin.Name)
		}
		roles := map[string]bool{}
		for _, role := range plugin.Roles {
			if (role != "search" && role != "ingest") || roles[role] {
				return fmt.Errorf("plugin %q has invalid or duplicate role %q", plugin.Name, role)
			}
			roles[role] = true
		}
	}
	return nil
}

func validTLSChannel(channel *TLSChannel) bool {
	if channel == nil {
		return true
	}
	if channel.CertificateRef.Name == "" || channel.TrustBundleRef.Name == "" || channel.TrustBundleRef.Key == "" {
		return false
	}
	switch channel.Source {
	case "Secret":
		return channel.IssuerRef == nil
	case "CertManager":
		return channel.IssuerRef != nil && len(validation.IsDNS1123Subdomain(channel.IssuerRef.Name)) == 0 &&
			len(validation.IsDNS1123Subdomain(channel.CertificateRef.Name)) == 0
	default:
		return false
	}
}
