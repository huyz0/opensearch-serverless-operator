package v1alpha1_test

import (
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/huyz0/opensearch-serverless-operator/api/v1alpha1"
)

func TestValidateInstance(t *testing.T) {
	valid := v1alpha1.OpenSearchServerlessSpec{
		Image: "registry.example/image@sha256:" + strings.Repeat("a", 64), ServiceAccountName: "runtime",
		Storage: v1alpha1.Storage{Type: "s3", RepositoryRef: &v1alpha1.LocalReference{Name: "store"}},
		Pools:   map[string]v1alpha1.Pool{"search": {Zones: map[string]v1alpha1.Zone{"a": {Replicas: 2, MinServing: 1}}}},
	}
	if err := valid.Validate(); err != nil {
		t.Fatal(err)
	}
	invalid := valid
	invalid.Storage.Bucket = "other"
	if err := invalid.Validate(); err == nil {
		t.Fatal("mixed storage sources must fail")
	}
	invalid = valid
	invalid.Pools = map[string]v1alpha1.Pool{"search": {Zones: map[string]v1alpha1.Zone{"a": {Replicas: 1, MinServing: 2}}}}
	if err := invalid.Validate(); err == nil {
		t.Fatal("serving floor above replicas must fail")
	}
}

func TestValidateHeapAndResourceLimits(t *testing.T) {
	base := v1alpha1.OpenSearchServerlessSpec{
		Image: "repo/image@sha256:" + strings.Repeat("a", 64), ServiceAccountName: "runtime",
		Storage: v1alpha1.Storage{Type: "s3", Bucket: "bucket", Region: "region"},
	}
	for _, test := range []struct {
		heap    string
		request string
		valid   bool
	}{
		{heap: "2Gi", request: "3Gi", valid: true},
		{heap: "1.5Gi", request: "3Gi", valid: true},
		{heap: "4Gi", request: "3Gi", valid: false},
		{heap: "bad", request: "3Gi", valid: false},
		{heap: "1.5", request: "3Gi", valid: false},
		{heap: "2Gi", request: "5Gi", valid: false},
	} {
		base.Pools = map[string]v1alpha1.Pool{"search": {
			Zones: map[string]v1alpha1.Zone{"a": {Replicas: 1}}, JVMHeap: test.heap,
			Resources: corev1.ResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceMemory: resource.MustParse(test.request)}, Limits: corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("4Gi")}},
		}}
		if err := base.Validate(); (err == nil) != test.valid {
			t.Errorf("heap=%s request=%s valid=%v: %v", test.heap, test.request, test.valid, err)
		}
	}
}

func TestValidatePlugins(t *testing.T) {
	spec := v1alpha1.OpenSearchServerlessSpec{
		Image: "repo/image@sha256:" + strings.Repeat("a", 64), ServiceAccountName: "runtime",
		Storage: v1alpha1.Storage{Type: "s3", Bucket: "bucket", Region: "region"},
		Pools:   map[string]v1alpha1.Pool{"search": {Zones: map[string]v1alpha1.Zone{"a": {Replicas: 1}}}},
		Plugins: []v1alpha1.Plugin{{Name: "analysis", Artifact: "repo/plugin@sha256:" + strings.Repeat("b", 64), Roles: []string{"search"}}},
	}
	if err := spec.Validate(); err != nil {
		t.Fatal(err)
	}
	spec.Plugins = append(spec.Plugins, spec.Plugins[0])
	if err := spec.Validate(); err == nil {
		t.Fatal("duplicate plugin names must be rejected")
	}
	spec.Plugins = spec.Plugins[:1]
	spec.Plugins[0].Name = "repository-s3"
	if err := spec.Validate(); err == nil {
		t.Fatal("bundled repository-s3 must be rejected as a filesystem plugin")
	}
	spec.Plugins[0].Name = "analysis"
	spec.Plugins[0].Artifact = "repo/plugin:latest"
	if err := spec.Validate(); err == nil {
		t.Fatal("mutable plugin tag must be rejected")
	}
}

func TestValidateStorageCredentials(t *testing.T) {
	spec := v1alpha1.OpenSearchServerlessSpec{
		Image: "repo/image@sha256:" + strings.Repeat("a", 64), ServiceAccountName: "runtime",
		Storage: v1alpha1.Storage{Type: "s3", Bucket: "bucket", Region: "region", Credentials: &v1alpha1.StorageCredentials{Type: "WorkloadIdentity", AWSRoleARN: "arn:aws:iam::123456789012:role/store"}},
		Pools:   map[string]v1alpha1.Pool{"search": {Zones: map[string]v1alpha1.Zone{"a": {Replicas: 1}}}},
	}
	if err := spec.Validate(); err != nil {
		t.Fatal(err)
	}
	spec.Storage.Credentials.Type = "Keystore"
	if err := spec.Validate(); err == nil {
		t.Fatal("keystore must reference a Secret")
	}
	spec.Storage.Credentials.KeystoreSecretRef = &v1alpha1.LocalReference{Name: "store-keystore"}
	spec.Storage.Credentials.AWSRoleARN = ""
	if err := spec.Validate(); err != nil {
		t.Fatal(err)
	}
}

func TestValidateTLSReferences(t *testing.T) {
	spec := v1alpha1.OpenSearchServerlessSpec{
		Image: "repo/image@sha256:" + strings.Repeat("a", 64), ServiceAccountName: "runtime",
		Storage: v1alpha1.Storage{Type: "s3", Bucket: "bucket", Region: "region"},
		Pools:   map[string]v1alpha1.Pool{"search": {Zones: map[string]v1alpha1.Zone{"a": {Replicas: 1}}}},
		TLS:     &v1alpha1.TLSConfig{Transport: &v1alpha1.TLSChannel{Source: "Secret", CertificateRef: v1alpha1.LocalReference{Name: "leaf"}, TrustBundleRef: v1alpha1.SecretKeyReference{Name: "trust", Key: "ca.crt"}}},
	}
	if err := spec.Validate(); err != nil {
		t.Fatal(err)
	}
	spec.TLS.Transport.TrustBundleRef.Key = ""
	if err := spec.Validate(); err == nil {
		t.Fatal("missing trust bundle key must fail")
	}
}

func TestValidateManagedHTTPTLSRenewal(t *testing.T) {
	const image = "repo/image@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	spec := v1alpha1.OpenSearchServerlessSpec{
		Image: image, ServiceAccountName: "runtime",
		Storage: v1alpha1.Storage{Type: "s3", Bucket: "bucket", Region: "region"},
		Pools:   map[string]v1alpha1.Pool{"search": {Zones: map[string]v1alpha1.Zone{"a": {Replicas: 1}}}},
		TLS: &v1alpha1.TLSConfig{HTTP: &v1alpha1.TLSChannel{
			Source: "CertManager", CertificateRef: v1alpha1.LocalReference{Name: "http-primary"},
			IssuerRef:      &v1alpha1.LocalReference{Name: "http-ca"},
			TrustBundleRef: v1alpha1.SecretKeyReference{Name: "http-roots", Key: "ca.crt"},
		}},
		Rollout: &v1alpha1.Rollout{Strategy: "BlueGreen", MaxPreviewReplicas: 1, RollbackWindow: "150s",
			Candidate: &v1alpha1.FleetCandidate{Fleet: "green", Image: image, HTTPRenewFromIssuer: true}},
	}
	if err := spec.Validate(); err != nil {
		t.Fatal(err)
	}
	spec.Rollout.RollbackWindow = ""
	if err := spec.Validate(); err == nil {
		t.Fatal("renewal accepted without a rollback window")
	}
	spec.Rollout.RollbackWindow = "150s"
	spec.Rollout.Candidate.HTTPCertificateRef = &v1alpha1.LocalReference{Name: "other"}
	if err := spec.Validate(); err == nil {
		t.Fatal("renewal accepted with an explicit leaf")
	}
	spec.Rollout.Candidate.HTTPCertificateRef = nil
	spec.TLS.HTTP.Source, spec.TLS.HTTP.IssuerRef = "Secret", nil
	if err := spec.Validate(); err == nil {
		t.Fatal("renewal accepted with a Secret source")
	}
}

func TestValidateSecurityProvider(t *testing.T) {
	spec := v1alpha1.OpenSearchServerlessSpec{
		Image: "repo/image@sha256:" + strings.Repeat("a", 64), ServiceAccountName: "runtime",
		Storage:  v1alpha1.Storage{Type: "s3", Bucket: "bucket", Region: "region"},
		Pools:    map[string]v1alpha1.Pool{"search": {Zones: map[string]v1alpha1.Zone{"a": {Replicas: 1}}}},
		Security: &v1alpha1.SecurityConfig{Provider: "example", Reload: &v1alpha1.SecurityReload{Mode: "HotReload"}, AuthenticationSecretRef: &v1alpha1.SecretKeyReference{Name: "credentials", Key: "auth.yaml"}},
	}
	if err := spec.Validate(); err != nil {
		t.Fatal(err)
	}
	spec.Security.Reload.Mode = "Unknown"
	if err := spec.Validate(); err == nil {
		t.Fatal("unsupported reload mode must fail")
	}
}

func TestValidateAvailabilityAndRollout(t *testing.T) {
	spec := v1alpha1.OpenSearchServerlessSpec{
		Image: "repo/image@sha256:" + strings.Repeat("a", 64), ServiceAccountName: "runtime",
		Storage:      v1alpha1.Storage{Type: "s3", Bucket: "bucket", Region: "region"},
		Pools:        map[string]v1alpha1.Pool{"search": {Zones: map[string]v1alpha1.Zone{"a": {Replicas: 2}}}},
		Availability: &v1alpha1.Availability{MinServingTotal: map[string]int32{"search": 2}},
		Rollout:      &v1alpha1.Rollout{Strategy: "BlueGreen", RollbackWindow: "30m"},
	}
	if err := spec.Validate(); err != nil {
		t.Fatal(err)
	}
	spec.Rollout.Candidate = &v1alpha1.FleetCandidate{Fleet: "green", Image: "repo/image@sha256:" + strings.Repeat("b", 64)}
	if err := spec.Validate(); err != nil {
		t.Fatal(err)
	}
	spec.Rollout.Candidate.Fleet = "bad fleet"
	if err := spec.Validate(); err == nil {
		t.Fatal("invalid candidate fleet accepted")
	}
	spec.Rollout.Candidate.Fleet = "green"
	spec.Rollout.Strategy = "Rolling"
	if err := spec.Validate(); err == nil {
		t.Fatal("candidate accepted for rolling strategy")
	}
	spec.Rollout.Strategy = "BlueGreen"
	spec.Rollout.Candidate = nil
	spec.Availability.MinServingTotal["search"] = 3
	if err := spec.Validate(); err == nil {
		t.Fatal("floor above configured capacity must fail")
	}
	spec.Availability.MinServingTotal["search"] = 2
	spec.Rollout.RollbackWindow = "invalid"
	if err := spec.Validate(); err == nil {
		t.Fatal("invalid rollback duration must fail")
	}
}

func TestValidateSidecars(t *testing.T) {
	spec := v1alpha1.OpenSearchServerlessSpec{
		Image: "repo/image@sha256:" + strings.Repeat("a", 64), ServiceAccountName: "runtime",
		Storage: v1alpha1.Storage{Type: "s3", Bucket: "bucket", Region: "region"},
		Pools:   map[string]v1alpha1.Pool{"search": {Zones: map[string]v1alpha1.Zone{"a": {Replicas: 1}}, Sidecars: []corev1.Container{{Name: "metrics", Image: "example/metrics"}}}},
	}
	if err := spec.Validate(); err != nil {
		t.Fatal(err)
	}
	pool := spec.Pools["search"]
	pool.Sidecars = append(pool.Sidecars, corev1.Container{Name: "opensearch", Image: "example/other"})
	spec.Pools["search"] = pool
	if err := spec.Validate(); err == nil {
		t.Fatal("reserved container name must be rejected")
	}
}

func TestSidecarVolumeValidation(t *testing.T) {
	valid := v1alpha1.OpenSearchServerlessSpec{Image: "example/image@sha256:" + strings.Repeat("a", 64), ServiceAccountName: "runtime", Storage: v1alpha1.Storage{Type: "s3", Bucket: "bucket", Region: "region"}, Pools: map[string]v1alpha1.Pool{"search": {Zones: map[string]v1alpha1.Zone{"a": {Replicas: 1}}, SidecarVolumes: []v1alpha1.SidecarVolume{{Name: "metrics-config", ConfigMapRef: &v1alpha1.LocalReference{Name: "metrics"}}}}}}
	if err := valid.Validate(); err != nil {
		t.Fatal(err)
	}
	for _, volume := range []v1alpha1.SidecarVolume{{Name: "serverless-data", EmptyDir: true}, {Name: "metrics-config"}, {Name: "metrics-config", EmptyDir: true, SecretRef: &v1alpha1.LocalReference{Name: "secret"}}, {Name: "metrics-config", SecretRef: &v1alpha1.LocalReference{Name: "bad name"}}, {Name: "metrics-config", EmptyDir: true, SizeLimit: "0"}, {Name: "metrics-config", EmptyDir: true, SizeLimit: "garbage"}, {Name: "metrics-config", SecretRef: &v1alpha1.LocalReference{Name: "secret"}, SizeLimit: "1Gi"}} {
		copy := valid
		copy.Pools = map[string]v1alpha1.Pool{"search": {Zones: map[string]v1alpha1.Zone{"a": {Replicas: 1}}, SidecarVolumes: []v1alpha1.SidecarVolume{volume}}}
		if err := copy.Validate(); err == nil {
			t.Fatalf("invalid sidecar volume accepted: %+v", volume)
		}
	}
}

func TestNativeSidecarValidation(t *testing.T) {
	spec := v1alpha1.OpenSearchServerlessSpec{Image: "example/image@sha256:" + strings.Repeat("a", 64), ServiceAccountName: "runtime", Storage: v1alpha1.Storage{Type: "s3", Bucket: "bucket", Region: "region"}, Pools: map[string]v1alpha1.Pool{"search": {Zones: map[string]v1alpha1.Zone{"a": {Replicas: 1}}, NativeSidecars: []corev1.Container{{Name: "metrics", Image: "metrics"}}}}}
	if err := spec.Validate(); err != nil {
		t.Fatal(err)
	}
	pool := spec.Pools["search"]
	pool.Sidecars = []corev1.Container{{Name: "metrics", Image: "other"}}
	spec.Pools["search"] = pool
	if err := spec.Validate(); err == nil {
		t.Fatal("duplicate regular and native sidecar accepted")
	}
}

func TestStrictWarmReadinessRequiresWarmOnJoin(t *testing.T) {
	spec := v1alpha1.OpenSearchServerlessSpec{
		Image: "repo/image@sha256:" + strings.Repeat("a", 64), ServiceAccountName: "runtime",
		Storage: v1alpha1.Storage{Type: "s3", Bucket: "bucket", Region: "region"},
		Pools:   map[string]v1alpha1.Pool{"search": {Zones: map[string]v1alpha1.Zone{"a": {Replicas: 1}}, WarmReadiness: "Strict"}},
	}
	if err := spec.Validate(); err == nil {
		t.Fatal("strict readiness must require warm on join")
	}
	pool := spec.Pools["search"]
	pool.WarmOnJoin = true
	spec.Pools["search"] = pool
	if err := spec.Validate(); err != nil {
		t.Fatal(err)
	}
}
