// Package v1alpha1 defines the initial operator API types.
// +kubebuilder:object:generate=true
// +groupName=serverless.opensearch.org
package v1alpha1

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

var GroupVersion = schema.GroupVersion{Group: "serverless.opensearch.org", Version: "v1alpha1"}

// AddToScheme registers the instance types with a Kubernetes scheme.
func AddToScheme(scheme *runtime.Scheme) error {
	scheme.AddKnownTypes(GroupVersion, &OpenSearchServerless{}, &OpenSearchServerlessList{}, &OpenSearchServerlessOperation{}, &OpenSearchServerlessOperationList{})
	metav1.AddToGroupVersion(scheme, GroupVersion)
	return nil
}

// LocalReference names an object in the instance namespace.
type LocalReference struct {
	// +kubebuilder:validation:MinLength=1
	Name string `json:"name"`
}

// Storage configures the supplied primary object store.
// +kubebuilder:validation:XValidation:rule="has(self.repositoryRef) ? !has(self.bucket) && !has(self.region) && !has(self.endpoint) && !has(self.pathStyleAccess) : has(self.bucket) && has(self.region)",message="select a repository reference or inline bucket and region"
// +kubebuilder:validation:XValidation:rule="(oldSelf.hasValue() && self == oldSelf.value()) || (has(self.repositoryRef) || (has(self.bucket) && size(self.bucket) > 0 && has(self.region) && size(self.region) > 0))",optionalOldSelf=true,message="inline storage requires a nonempty bucket and region"
type Storage struct {
	// Type is s3. The bucket must already exist and be dedicated to this instance.
	// +kubebuilder:validation:Enum=s3
	Type string `json:"type"`
	// RepositoryRef names a ConfigMap in this namespace with bucket and region,
	// and optional endpoint and pathStyleAccess. Use it instead of inline store fields.
	RepositoryRef   *LocalReference     `json:"repositoryRef,omitempty"`
	Bucket          string              `json:"bucket,omitempty"`
	Region          string              `json:"region,omitempty"`
	Endpoint        string              `json:"endpoint,omitempty"`
	PathStyleAccess *bool               `json:"pathStyleAccess,omitempty"`
	Credentials     *StorageCredentials `json:"credentials,omitempty"`
}

// StorageCredentials selects a workload identity or an existing keystore.
// +kubebuilder:validation:XValidation:rule="(oldSelf.hasValue() && self == oldSelf.value()) || (self.type == 'WorkloadIdentity' ? has(self.awsRoleArn) && size(self.awsRoleArn) > 0 && !has(self.keystoreSecretRef) : has(self.keystoreSecretRef) && (!has(self.awsRoleArn) || size(self.awsRoleArn) == 0))",optionalOldSelf=true,message="select an AWS role ARN for WorkloadIdentity or a Secret reference for Keystore"
type StorageCredentials struct {
	// +kubebuilder:validation:Enum=Keystore;WorkloadIdentity
	Type              string          `json:"type"`
	KeystoreSecretRef *LocalReference `json:"keystoreSecretRef,omitempty"`
	// +kubebuilder:validation:MaxLength=512
	// +kubebuilder:validation:Pattern=`^arn:[a-z0-9-]+:iam::[0-9]{12}:role/[A-Za-z0-9+=,.@_/-]+$`
	AWSRoleARN string `json:"awsRoleArn,omitempty"`
}

// Zone specifies the desired and minimum serving counts in one zone.
// +kubebuilder:validation:XValidation:rule="self.minServing <= self.replicas + (has(self.warmSpare) ? self.warmSpare : 0)",message="minServing cannot exceed replicas plus warmSpare"
// +kubebuilder:validation:XValidation:rule="self.replicas + (has(self.warmSpare) ? self.warmSpare : 0) <= 2147483647",message="total desired replicas exceeds int32"
type Zone struct {
	// Replicas is the base desired Pod count for this role in this zone.
	// +kubebuilder:validation:Minimum=0
	Replicas int32 `json:"replicas"`
	// WarmSpare adds running Pods to this role and zone. It does not certify cache warmth.
	// +kubebuilder:validation:Minimum=0
	WarmSpare int32 `json:"warmSpare,omitempty"`
	// MinServing is the zonal serving floor for guarded disruption. It cannot exceed replicas plus warmSpare.
	// +kubebuilder:validation:Minimum=0
	MinServing int32 `json:"minServing"`
}

// Pool configures one workload role.
// +kubebuilder:validation:XValidation:rule="(oldSelf.hasValue() && self == oldSelf.value()) || ((!has(self.warmReadiness) || self.warmReadiness != 'Strict' || (has(self.warmOnJoin) && self.warmOnJoin)) && (!has(self.coldStartPolicy) || (has(self.warmReadiness) && self.warmReadiness == 'Strict')))",optionalOldSelf=true,message="strict warm readiness requires warmOnJoin and is required for coldStartPolicy"
type Pool struct {
	// Zones is keyed by the exact topology.kubernetes.io/zone values on eligible Nodes.
	// +kubebuilder:validation:MinProperties=1
	Zones              map[string]Zone             `json:"zones"`
	CapacityProfileRef string                      `json:"capacityProfileRef,omitempty"`
	Runtime            *RuntimeConfig              `json:"runtime,omitempty"`
	Resources          corev1.ResourceRequirements `json:"resources,omitempty"`
	// JVMHeap sets the fixed Java heap for this role, for example 256Mi. Leave room for non-heap memory.
	JVMHeap string `json:"jvmHeap,omitempty"`
	// Sidecars are regular containers alongside the OpenSearch container in each Pod of this role.
	Sidecars []corev1.Container `json:"sidecars,omitempty"`
	// NativeSidecars are restartable init containers that stay running alongside OpenSearch.
	NativeSidecars []corev1.Container `json:"nativeSidecars,omitempty"`
	// +listType=map
	// +listMapKey=name
	SidecarVolumes []SidecarVolume `json:"sidecarVolumes,omitempty"`
	WarmOnJoin     bool            `json:"warmOnJoin,omitempty"`
	// +kubebuilder:validation:Enum=Strict;ServingReady
	WarmReadiness string `json:"warmReadiness,omitempty"`
	// +kubebuilder:validation:Enum=AllowOneDegraded
	ColdStartPolicy      string `json:"coldStartPolicy,omitempty"`
	ProtectFromScaleDown bool   `json:"protectFromScaleDown,omitempty"`
	// ReplaceOnExit gives each OpenSearch process a distinct Pod lifetime.
	ReplaceOnExit bool `json:"replaceOnExit,omitempty"`
}

// SidecarVolume names a role-scoped volume for sidecar containers.
// +kubebuilder:validation:XValidation:rule="(has(self.secretRef) ? 1 : 0) + (has(self.configMapRef) ? 1 : 0) + (has(self.emptyDir) && self.emptyDir ? 1 : 0) == 1",message="choose exactly one sidecar volume source"
// +kubebuilder:validation:XValidation:rule="!self.name.startsWith('serverless-')",message="sidecar volume name uses a reserved prefix"
// +kubebuilder:validation:XValidation:rule="!has(self.sizeLimit) || (has(self.emptyDir) && self.emptyDir)",message="sizeLimit requires emptyDir"
type SidecarVolume struct {
	// +kubebuilder:validation:MaxLength=63
	// +kubebuilder:validation:Pattern=`^[a-z0-9]([-a-z0-9]*[a-z0-9])?$`
	Name         string          `json:"name"`
	SecretRef    *LocalReference `json:"secretRef,omitempty"`
	ConfigMapRef *LocalReference `json:"configMapRef,omitempty"`
	EmptyDir     bool            `json:"emptyDir,omitempty"`
	SizeLimit    string          `json:"sizeLimit,omitempty"`
}

// RuntimeConfig contains startup settings and JVM arguments.
type RuntimeConfig struct {
	// Settings supplies OpenSearch startup settings. Use a separate operation for supported live cluster settings.
	Settings map[string]string `json:"settings,omitempty"`
	// JVM supplies additional startup arguments and system properties. Set the managed heap with the pool's jvmHeap field.
	JVM *JVMConfig `json:"jvm,omitempty"`
}

// FleetRuntime replaces the common and role-specific startup settings.
type FleetRuntime struct {
	Common *RuntimeConfig `json:"common,omitempty"`
	// +kubebuilder:validation:MaxProperties=2
	ByRole map[string]RuntimeConfig `json:"byRole,omitempty"`
}

// JVMConfig contains ordered JVM flags and system properties.
type JVMConfig struct {
	Options          *[]string         `json:"options,omitempty"`
	SystemProperties map[string]string `json:"systemProperties,omitempty"`
}

// Plugin identifies a plugin artifact to install before Java starts.
type Plugin struct {
	// +kubebuilder:validation:Pattern=`^[a-z0-9][a-z0-9._-]*$`
	Name string `json:"name"`
	// +kubebuilder:validation:Pattern=`^.+@sha256:[a-f0-9]{64}$`
	Artifact               string          `json:"artifact"`
	RegistryCredentialsRef *LocalReference `json:"registryCredentialsRef,omitempty"`
	// +kubebuilder:validation:items:Enum=search;ingest
	Roles []string `json:"roles,omitempty"`
}

// PluginSet replaces all filesystem plugin selections for the candidate.
type PluginSet struct {
	// +kubebuilder:validation:MaxItems=32
	Plugins []Plugin `json:"plugins,omitempty"`
}

// FleetPluginSet retains the exact selections and observed role inputs.
type FleetPluginSet struct {
	PluginSet `json:",inline"`
	// +kubebuilder:validation:MaxProperties=2
	InputsByRole map[string]PluginInstallIntent `json:"inputsByRole,omitempty"`
}

// SecretKeyReference identifies a key in a same-namespace Secret.
type SecretKeyReference struct {
	// +kubebuilder:validation:MinLength=1
	Name string `json:"name"`
	// +kubebuilder:validation:MinLength=1
	Key string `json:"key"`
}

// TLSChannel references a leaf certificate and trust bundle.
// +kubebuilder:validation:XValidation:rule="(self.source == 'CertManager') == has(self.issuerRef)",message="issuerRef is required only for CertManager"
type TLSChannel struct {
	// Source is Secret for supplied certificates or CertManager for an approved namespaced Issuer.
	// +kubebuilder:validation:Enum=Secret;CertManager
	Source string `json:"source"`
	// CertificateRef names a Secret or a logical issuance slot for CertManager.
	CertificateRef LocalReference `json:"certificateRef"`
	// TrustBundleRef names a key containing the PEM CA bundle in a same-namespace Secret.
	TrustBundleRef SecretKeyReference `json:"trustBundleRef"`
	// IssuerRef is required when source is CertManager.
	IssuerRef *LocalReference `json:"issuerRef,omitempty"`
}

// TLSConfig keeps HTTP and peer transport trust boundaries distinct.
type TLSConfig struct {
	// HTTP supplies TLS for search and index connections to the Java Pods.
	HTTP *TLSChannel `json:"http,omitempty"`
	// Transport supplies mutual TLS for peer connections between Java Pods.
	Transport *TLSChannel `json:"transport,omitempty"`
}

// ConfigKeyReference identifies a key in a same-namespace ConfigMap.
type ConfigKeyReference struct {
	// +kubebuilder:validation:MinLength=1
	Name string `json:"name"`
	// +kubebuilder:validation:MinLength=1
	Key string `json:"key"`
}

// SecurityConfig selects the provider and its credential and policy inputs.
type SecurityConfig struct {
	// +kubebuilder:validation:MinLength=1
	Provider                  string              `json:"provider"`
	AuthenticationSecretRef   *SecretKeyReference `json:"authenticationSecretRef,omitempty"`
	AuthorizationConfigMapRef *ConfigKeyReference `json:"authorizationConfigMapRef,omitempty"`
	Reload                    *SecurityReload     `json:"reload,omitempty"`
}

// SecurityReload selects a qualified activation mechanism.
type SecurityReload struct {
	// +kubebuilder:validation:Enum=HotReload;RollingReplacement
	Mode string `json:"mode"`
}

// Availability controls bounded voluntary work and pending demand.
type Availability struct {
	MinServingTotal map[string]int32 `json:"minServingTotal,omitempty"`
	// +kubebuilder:validation:Minimum=0
	MaxConcurrentRetirements int32 `json:"maxConcurrentRetirements"`
	// +kubebuilder:validation:Minimum=0
	MaxRollingSurgePerGroup int32 `json:"maxRollingSurgePerGroup"`
	// +kubebuilder:validation:Minimum=0
	MaxPendingProvisionablePods int32 `json:"maxPendingProvisionablePods"`
	// +kubebuilder:validation:Minimum=0
	MaxConcurrentSearchWarmups int32                    `json:"maxConcurrentSearchWarmups"`
	EmergencyCapacity          *EmergencyCapacityPolicy `json:"emergencyCapacity,omitempty"`
}

type EmergencyCapacityPolicy struct {
	EvidenceRef LocalReference `json:"evidenceRef"`
	// +kubebuilder:validation:MinProperties=1
	MaxTemporaryByRole map[string]int32 `json:"maxTemporaryByRole"`
	// +kubebuilder:validation:Minimum=1
	MaxTemporaryPerZone int32 `json:"maxTemporaryPerZone"`
}

type EmergencyCapacityProposal struct {
	Role            string      `json:"role"`
	Zone            string      `json:"zone"`
	Fleet           string      `json:"fleet"`
	EvidenceUID     string      `json:"evidenceUID"`
	EvidenceVersion string      `json:"evidenceVersion"`
	ExpiresAt       metav1.Time `json:"expiresAt"`
}

// EmergencyFailbackEvidence identifies the report used to admit temporary capacity.
type EmergencyFailbackEvidence struct {
	LocalReference `json:",inline"`
	// +kubebuilder:validation:MinLength=1
	UID string `json:"uid"`
}

// Rollout selects the fleet strategy and preview budget.
// +kubebuilder:validation:XValidation:rule="(oldSelf.hasValue() && self == oldSelf.value()) || (!has(self.candidate) || self.strategy == 'BlueGreen')",optionalOldSelf=true,message="candidate requires the BlueGreen strategy"
type Rollout struct {
	// +kubebuilder:validation:Enum=Rolling;BlueGreen
	Strategy string `json:"strategy"`
	// MaxPreviewReplicas bounds the candidate Pods across both roles and all zones.
	// +kubebuilder:validation:Minimum=0
	MaxPreviewReplicas int32 `json:"maxPreviewReplicas,omitempty"`
	// RollbackWindow sets the intended retention period for the former serving fleet, for example 30m.
	RollbackWindow string `json:"rollbackWindow,omitempty"`
	// Candidate stages a second fleet. A separate Promote operation requests the traffic switch.
	Candidate *FleetCandidate `json:"candidate,omitempty"`
	// GatewayBackends provisions stable ClusterIP Services for fleet routing.
	GatewayBackends bool `json:"gatewayBackends,omitempty"`
}

// FleetCandidate requests a second fleet using the current shared settings.
type FleetCandidate struct {
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=63
	// +kubebuilder:validation:Pattern=`^[A-Za-z0-9]([A-Za-z0-9_.-]*[A-Za-z0-9])?$`
	Fleet string `json:"fleet"`
	// +kubebuilder:validation:Pattern=`^.+@sha256:[a-f0-9]{64}$`
	Image string `json:"image"`
	// HTTPCertificateRef supplies an immutable replacement HTTP leaf for this fleet.
	HTTPCertificateRef *LocalReference `json:"httpCertificateRef,omitempty"`
	// HTTPTrustBundleRef pins a replacement HTTP CA bundle for this fleet.
	HTTPTrustBundleRef *SecretKeyReference `json:"httpTrustBundleRef,omitempty"`
	// HTTPRenewFromIssuer stages the current output of the configured HTTP Issuer.
	HTTPRenewFromIssuer bool `json:"httpRenewFromIssuer,omitempty"`
	// PeerTLS stages a new peer certificate and trust pair for this fleet.
	PeerTLS         *TLSChannel   `json:"peerTLS,omitempty"`
	PluginOverride  *PluginSet    `json:"pluginOverride,omitempty"`
	RuntimeOverride *FleetRuntime `json:"runtimeOverride,omitempty"`
}

// OpenSearchServerlessSpec configures the Java runtime, its primary store,
// and the workload pools associated with this instance.
// +kubebuilder:validation:XValidation:rule="!has(self.rollout) || !has(self.rollout.candidate) || !has(self.rollout.candidate.peerTLS) || (has(self.tls) && has(self.tls.transport) && has(self.rollout.rollbackWindow) && size(self.rollout.rollbackWindow) > 0 && size(self.rollout.candidate.peerTLS.certificateRef.name) > 0 && size(self.rollout.candidate.peerTLS.trustBundleRef.name) > 0 && size(self.rollout.candidate.peerTLS.trustBundleRef.key) > 0)",message="candidate peer TLS requires transport TLS, source references and a finite rollback window"
// +kubebuilder:validation:XValidation:rule="!has(self.rollout) || !has(self.rollout.candidate) || !has(self.rollout.candidate.httpRenewFromIssuer) || !self.rollout.candidate.httpRenewFromIssuer || (has(self.tls) && has(self.tls.http) && self.tls.http.source == 'CertManager' && has(self.rollout.rollbackWindow) && size(self.rollout.rollbackWindow) > 0 && !has(self.rollout.candidate.httpCertificateRef))",message="HTTP issuer renewal requires cert-manager HTTP TLS, a finite rollback window and no explicit leaf Secret"
// +kubebuilder:validation:XValidation:rule="!has(self.rollout) || !has(self.rollout.candidate) || !has(self.rollout.candidate.pluginOverride) || (self.rollout.strategy == 'BlueGreen' && has(self.rollout.rollbackWindow) && size(self.rollout.rollbackWindow) > 0)",message="candidate plugin override requires blue/green and a finite rollback window"
// +kubebuilder:validation:XValidation:rule="!has(self.rollout) || !has(self.rollout.candidate) || !has(self.rollout.candidate.runtimeOverride) || (self.rollout.strategy == 'BlueGreen' && has(self.rollout.rollbackWindow) && size(self.rollout.rollbackWindow) > 0)",message="candidate runtime override requires blue/green and a finite rollback window"
// +kubebuilder:validation:XValidation:rule="(oldSelf.hasValue() && self.pools == oldSelf.value().pools) || (size(self.pools) == (('search' in self.pools ? 1 : 0) + ('ingest' in self.pools ? 1 : 0)) && (!('ingest' in self.pools) || ((!has(self.pools['ingest'].warmOnJoin) || !self.pools['ingest'].warmOnJoin) && !has(self.pools['ingest'].warmReadiness) && !has(self.pools['ingest'].coldStartPolicy))))",optionalOldSelf=true,message="pools support only search and ingest, and warm readiness belongs to search"
type OpenSearchServerlessSpec struct {
	// Image is the Java runtime image pinned by SHA-256 digest.
	// +kubebuilder:validation:Pattern=`^.+@sha256:[a-f0-9]{64}$`
	Image string `json:"image"`
	// +kubebuilder:validation:MinLength=1
	ServiceAccountName string  `json:"serviceAccountName"`
	Storage            Storage `json:"storage"`
	// Pools configures independent search and ingest roles. The ingest role serves the index route.
	// +kubebuilder:validation:MinProperties=1
	Pools        map[string]Pool `json:"pools"`
	Runtime      *RuntimeConfig  `json:"runtime,omitempty"`
	Plugins      []Plugin        `json:"plugins,omitempty"`
	TLS          *TLSConfig      `json:"tls,omitempty"`
	Security     *SecurityConfig `json:"security,omitempty"`
	Availability *Availability   `json:"availability,omitempty"`
	Rollout      *Rollout        `json:"rollout,omitempty"`
}

// OpenSearchServerlessStatus reports the evaluated generation and conditions.
type OpenSearchServerlessStatus struct {
	ObservedGeneration int64               `json:"observedGeneration,omitempty"`
	Conditions         []metav1.Condition  `json:"conditions,omitempty"`
	PrimaryStore       *PrimaryStoreStatus `json:"primaryStore,omitempty"`
	PeerTLSBaseline    *PeerTLSBaseline    `json:"peerTLSBaseline,omitempty"`
	HTTPTLSBaseline    *HTTPTLSBaseline    `json:"httpTLSBaseline,omitempty"`
	PendingHTTPTLS     *HTTPLeafSnapshot   `json:"pendingHTTPTLS,omitempty"`
	// +kubebuilder:validation:MaxItems=64
	HTTPManagedSnapshots []string `json:"httpManagedSnapshots,omitempty"`
	// +kubebuilder:validation:MaxProperties=64
	HTTPFleetCertificates map[string]HTTPFleetCertificate `json:"httpFleetCertificates,omitempty"`
	// +kubebuilder:validation:MaxProperties=64
	PeerTLSByFleet map[string]PeerTLSBaseline `json:"peerTLSByFleet,omitempty"`
	// +kubebuilder:validation:MaxProperties=8
	PluginSetsByFleet map[string]FleetPluginSet `json:"pluginSetsByFleet,omitempty"`
	// +kubebuilder:validation:MaxProperties=8
	RuntimeByFleet map[string]FleetRuntime `json:"runtimeByFleet,omitempty"`
	// HTTPTrustRotationUsed keeps signed backend trust evidence mandatory after a CA change.
	HTTPTrustRotationUsed    bool               `json:"httpTrustRotationUsed,omitempty"`
	BasicAuthBaseline        *BasicAuthBaseline `json:"basicAuthBaseline,omitempty"`
	ActiveSecurityRevision   string             `json:"activeSecurityRevision,omitempty"`
	ActiveOperation          *OperationClaim    `json:"activeOperation,omitempty"`
	ServingFleetIntentByRole map[string]string  `json:"servingFleetIntentByRole,omitempty"`
	// ServingRevisionByRole retains the literal revision of each serving fleet across Pod loss.
	// +kubebuilder:validation:MaxProperties=2
	ServingRevisionByRole map[string]ServingRoleRevision `json:"servingRevisionByRole,omitempty"`
	// TemplateRevisionAliases maps verified pre-normalization labels to runtime digests.
	// +kubebuilder:validation:MaxProperties=16
	TemplateRevisionAliases map[string]string `json:"templateRevisionAliases,omitempty"`
	// GatewayBackends records Service intents and pins UIDs after creation.
	// +kubebuilder:validation:MaxProperties=128
	GatewayBackends map[string]GatewayBackendStatus `json:"gatewayBackends,omitempty"`
	// LastRolloutCommit identifies the operation that committed the serving mapping.
	LastRolloutCommit         *OperationClaim             `json:"lastRolloutCommit,omitempty"`
	RollbackTarget            *RollbackTarget             `json:"rollbackTarget,omitempty"`
	FleetRetirement           *FleetRetirementReservation `json:"fleetRetirement,omitempty"`
	CandidateFleet            *CandidateFleetStatus       `json:"candidateFleet,omitempty"`
	EmergencyCapacityProposal *EmergencyCapacityProposal  `json:"emergencyCapacityProposal,omitempty"`
	EmergencyFailbackEvidence *EmergencyFailbackEvidence  `json:"emergencyFailbackEvidence,omitempty"`
	EmergencyRetirement       *EmergencyRetirementIntent  `json:"emergencyRetirement,omitempty"`
	ScaleDownRetirement       *ScaleDownRetirementIntent  `json:"scaleDownRetirement,omitempty"`
	ScaleDownRetryAfter       *metav1.Time                `json:"scaleDownRetryAfter,omitempty"`
	// +kubebuilder:validation:MaxItems=64
	RetiredCandidateFleets []RetiredCandidateFleet `json:"retiredCandidateFleets,omitempty"`
	// +kubebuilder:validation:MaxItems=256
	RecentPodReleases   []PodReleaseRecord `json:"recentPodReleases,omitempty"`
	PodCreationSequence int64              `json:"podCreationSequence,omitempty"`
	PendingPodCreation  *PodCreationIntent `json:"pendingPodCreation,omitempty"`
	PendingGateRelease  *GateReleaseIntent `json:"pendingGateRelease,omitempty"`
	// +kubebuilder:validation:MaxItems=64
	RetiredPodCreations []RetiredPodCreation `json:"retiredPodCreations,omitempty"`
	// +kubebuilder:validation:MaxItems=64
	BlockedProvisioningAttempts    []ProvisioningAttempt             `json:"blockedProvisioningAttempts,omitempty"`
	ProvisioningAttempt            *ProvisioningAttempt              `json:"provisioningAttempt,omitempty"`
	DisruptionBudgetIntent         *DisruptionBudgetIntent           `json:"disruptionBudgetIntent,omitempty"`
	SearchWarmupSlot               *SearchWarmupSlot                 `json:"searchWarmupSlot,omitempty"`
	LastSearchWarmup               *SearchWarmupObservation          `json:"lastSearchWarmup,omitempty"`
	LastSearchQualification        *SearchQualificationObservation   `json:"lastSearchQualification,omitempty"`
	LastServingSearchProtection    *ServingSearchProtectionStatus    `json:"lastServingSearchProtection,omitempty"`
	LastServingIngestProtection    *ServingIngestProtectionStatus    `json:"lastServingIngestProtection,omitempty"`
	LastCandidateIngestObservation *CandidateIngestObservationStatus `json:"lastCandidateIngestObservation,omitempty"`
	LastCandidateSearchObservation *CandidateSearchObservationStatus `json:"lastCandidateSearchObservation,omitempty"`
	LastServiceProbe               *ServiceProbeStatus               `json:"lastServiceProbe,omitempty"`
	LastEndpointObservation        *EndpointObservationStatus        `json:"lastEndpointObservation,omitempty"`
	ColdBootstrap                  *ColdBootstrapStatus              `json:"coldBootstrap,omitempty"`
	ZoneCapacity                   []ZoneCapacityStatus              `json:"zoneCapacity,omitempty"`
	ZoneCapacityObservedAt         *metav1.Time                      `json:"zoneCapacityObservedAt,omitempty"`
	ZoneCapacityObservedGeneration int64                             `json:"zoneCapacityObservedGeneration,omitempty"`
	LastProvisioningRetry          *ProvisioningRetryRecord          `json:"lastProvisioningRetry,omitempty"`
}

// EmergencyRetirementIntent serializes a bootstrap or temporary Pod retirement during failback.
type EmergencyRetirementIntent struct {
	Generation int64  `json:"generation"`
	Role       string `json:"role"`
	Fleet      string `json:"fleet"`
	Zone       string `json:"zone"`
	Name       string `json:"name"`
	PodUID     string `json:"podUID"`
}

// ScaleDownRetirementIntent pins the search Pod selected for steady scale-down.
type ScaleDownRetirementIntent EmergencyRetirementIntent

// CandidateIngestObservationStatus is historical lease evidence, not proof of
// writes, takeover, peer compatibility or permission to promote.
type CandidateIngestObservationStatus struct {
	ObservedGeneration int64       `json:"observedGeneration"`
	Fleet              string      `json:"fleet"`
	Revision           string      `json:"revision"`
	Result             string      `json:"result"`
	QualifiedPods      int32       `json:"qualifiedPods,omitempty"`
	ObservedAt         metav1.Time `json:"observedAt"`
}

// CandidateSearchObservationStatus is historical evidence, not a promotion gate.
type CandidateSearchObservationStatus struct {
	ObservedGeneration int64       `json:"observedGeneration"`
	Fleet              string      `json:"fleet"`
	Revision           string      `json:"revision"`
	Result             string      `json:"result"`
	QualifiedPods      int32       `json:"qualifiedPods,omitempty"`
	ObservedAt         metav1.Time `json:"observedAt"`
}

// ServingSearchProtectionStatus is a historical development observation, not
// current cache residency or permission for a disruptive action.
type ServingSearchProtectionStatus struct {
	ObservedGeneration int64  `json:"observedGeneration"`
	Fleet              string `json:"fleet"`
	Result             string `json:"result"`
	QualifiedPods      int32  `json:"qualifiedPods,omitempty"`
	// RegularCapacityRecovered records that regular endpoints met configured targets while emergency Pods were present.
	RegularCapacityRecovered bool        `json:"regularCapacityRecovered,omitempty"`
	ServiceUID               string      `json:"serviceUID,omitempty"`
	ServiceVersion           string      `json:"serviceVersion,omitempty"`
	ObservedAt               metav1.Time `json:"observedAt"`
}

// ServingIngestProtectionStatus is historical process and endpoint evidence,
// not proof of successful writes, shard ownership or takeover.
type ServingIngestProtectionStatus struct {
	ObservedGeneration int64  `json:"observedGeneration"`
	Fleet              string `json:"fleet"`
	Result             string `json:"result"`
	QualifiedPods      int32  `json:"qualifiedPods,omitempty"`
	// RegularCapacityRecovered records that regular endpoints met configured targets while emergency Pods were present.
	RegularCapacityRecovered bool        `json:"regularCapacityRecovered,omitempty"`
	ServiceUID               string      `json:"serviceUID,omitempty"`
	ServiceVersion           string      `json:"serviceVersion,omitempty"`
	ObservedAt               metav1.Time `json:"observedAt"`
}

// RetiredCandidateFleet retains preview identities so late creates cannot be
// mistaken for a new candidate after the active record is cleared.
type RetiredCandidateFleet struct {
	Fleet            string              `json:"fleet"`
	Revision         string              `json:"revision,omitempty"`
	RevisionsByRole  *FleetRoleRevisions `json:"revisionsByRole,omitempty"`
	SearchServiceUID string              `json:"searchServiceUID,omitempty"`
	IngestServiceUID string              `json:"ingestServiceUID,omitempty"`
	// PreviewOnly fences aliases of a promoted fleet whose workloads are still managed.
	PreviewOnly bool `json:"previewOnly,omitempty"`
	// +kubebuilder:validation:MaxItems=64
	DisruptionBudgets []RetiredDisruptionBudget `json:"disruptionBudgets,omitempty"`
}

type GatewayBackendStatus struct {
	Role       string `json:"role"`
	Fleet      string `json:"fleet"`
	Name       string `json:"name"`
	ServiceUID string `json:"serviceUID,omitempty"`
}

type RetiredDisruptionBudget struct {
	Name string `json:"name"`
	UID  string `json:"uid"`
}

// CandidateFleetStatus pins the candidate before creating preview Services.
type CandidateFleetStatus struct {
	PluginOverride   *PluginSet    `json:"pluginOverride,omitempty"`
	RuntimeOverride  *FleetRuntime `json:"runtimeOverride,omitempty"`
	Fleet            string        `json:"fleet"`
	Image            string        `json:"image"`
	Revision         string        `json:"revision"`
	Generation       int64         `json:"generation"`
	SearchServiceUID string        `json:"searchServiceUID,omitempty"`
	IngestServiceUID string        `json:"ingestServiceUID,omitempty"`
	PreviewRetiring  bool          `json:"previewRetiring,omitempty"`
	// WasServing keeps a formerly promoted candidate on the signed retirement path.
	WasServing                bool                    `json:"wasServing,omitempty"`
	RetirementRevisionsByRole *FleetRoleRevisions     `json:"retirementRevisionsByRole,omitempty"`
	HTTPCertificateRef        string                  `json:"httpCertificateRef,omitempty"`
	HTTPTrustBundleRef        *SecretKeyReference     `json:"httpTrustBundleRef,omitempty"`
	HTTPRenewFromIssuer       bool                    `json:"httpRenewFromIssuer,omitempty"`
	PeerTLS                   *TLSChannel             `json:"peerTLS,omitempty"`
	Capacity                  []CandidateZoneCapacity `json:"capacity,omitempty"`
}

// CandidateZoneCapacity is historical Kubernetes capacity, not warm or
// application-qualified capacity. Its timestamp is ZoneCapacityObservedAt.
type CandidateZoneCapacity struct {
	Role            string `json:"role"`
	Zone            string `json:"zone"`
	DesiredKnown    bool   `json:"desiredKnown"`
	DesiredPods     int32  `json:"desiredPods,omitempty"`
	ObservedPods    int32  `json:"observedPods,omitempty"`
	UnboundPods     int32  `json:"unboundPods,omitempty"`
	KubernetesReady int32  `json:"kubernetesReady,omitempty"`
	TerminatingPods int32  `json:"terminatingPods,omitempty"`
	TerminalPods    int32  `json:"terminalPods,omitempty"`
	Warmth          string `json:"warmth"`
}

// RetiredPodCreation fences a stale name that a delayed create may still use.
type RetiredPodCreation struct {
	Released bool   `json:"released,omitempty"`
	Name     string `json:"name"`
	Sequence int64  `json:"sequence"`
	Role     string `json:"role"`
	Zone     string `json:"zone"`
	Fleet    string `json:"fleet"`
	Revision string `json:"revision"`
	PodUID   string `json:"podUID,omitempty"`
}

// PodReleaseRecord reserves one scheduling attempt before removing its gate.
type PodReleaseRecord struct {
	PodUID    string      `json:"podUID"`
	Role      string      `json:"role"`
	Zone      string      `json:"zone"`
	StartedAt metav1.Time `json:"startedAt"`
}

// ServiceProbeStatus is a historical unauthenticated development sample.
type ServiceProbeStatus struct {
	ObservedGeneration int64       `json:"observedGeneration"`
	Role               string      `json:"role"`
	Fleet              string      `json:"fleet"`
	Result             string      `json:"result"`
	ServiceUID         string      `json:"serviceUID,omitempty"`
	ServiceVersion     string      `json:"serviceVersion,omitempty"`
	PodName            string      `json:"podName,omitempty"`
	PodUID             string      `json:"podUID,omitempty"`
	ObservedAt         metav1.Time `json:"observedAt"`
}

// EndpointObservationStatus is a historical Kubernetes-only route sample.
type EndpointObservationStatus struct {
	ObservedGeneration int64            `json:"observedGeneration"`
	Role               string           `json:"role"`
	Fleet              string           `json:"fleet"`
	ServiceUID         string           `json:"serviceUID,omitempty"`
	Result             string           `json:"result"`
	ReadyByZone        map[string]int32 `json:"readyByZone,omitempty"`
	// ObservedServingFleets pins the role-to-fleet mapping of the availability sample.
	ObservedServingFleets map[string]string `json:"observedServingFleets,omitempty"`
	ObservedAt            metav1.Time       `json:"observedAt"`
}

// ColdBootstrapStatus reserves the single explicitly degraded search Pod.
type ColdBootstrapStatus struct {
	Name     string `json:"name"`
	PodUID   string `json:"podUID,omitempty"`
	Fleet    string `json:"fleet"`
	Sequence int64  `json:"sequence"`
}

// SearchQualificationObservation is a historical development probe, not
// proof of current cache residency or authorization for a disruptive action.
type SearchQualificationObservation struct {
	ObservedGeneration  int64                `json:"observedGeneration"`
	PodName             string               `json:"podName"`
	PodUID              string               `json:"podUID"`
	Fleet               string               `json:"fleet"`
	Zone                string               `json:"zone"`
	Result              string               `json:"result"`
	ContainerID         string               `json:"containerID,omitempty"`
	ProcessIncarnation  string               `json:"processIncarnation,omitempty"`
	ObservedAt          metav1.Time          `json:"observedAt"`
	LocalSearchActivity *LocalSearchActivity `json:"localSearchActivity,omitempty"`
}

// LocalSearchActivity reports observed local shard readers and cumulative
// JVM block cache activity. An absent observation means the data is unknown.
type LocalSearchActivity struct {
	ShardReaders      int64 `json:"shardReaders"`
	BlockCacheHits    int64 `json:"blockCacheHits"`
	BlockCacheMisses  int64 `json:"blockCacheMisses"`
	BlockBytesFetched int64 `json:"blockBytesFetched"`
}

// ProvisioningRetryRecord records the operation that cleared one hold.
type ProvisioningRetryRecord struct {
	OperationUID string `json:"operationUID"`
	PodUID       string `json:"podUID"`
}

// ZoneCapacityStatus distinguishes a Kubernetes capacity shortfall from unverified warmth.
type ZoneCapacityStatus struct {
	Role            string `json:"role"`
	Zone            string `json:"zone"`
	Fleet           string `json:"fleet"`
	Replicas        int32  `json:"replicas"`
	WarmSpare       int32  `json:"warmSpare"`
	MinServing      int32  `json:"minServing"`
	ObservedPods    int32  `json:"observedPods"`
	UnboundPods     int32  `json:"unboundPods"`
	TerminatingPods int32  `json:"terminatingPods"`
	// DisruptionTargetPods are nonterminal, nonterminating Pods marked for disruption.
	DisruptionTargetPods int32 `json:"disruptionTargetPods,omitempty"`
	// ReadyDisruptionTargetPods are disruption targets that still report Ready.
	ReadyDisruptionTargetPods int32 `json:"readyDisruptionTargetPods,omitempty"`
	// BelowMinServingAfterDisruption excludes Ready Pods marked for disruption.
	BelowMinServingAfterDisruption bool   `json:"belowMinServingAfterDisruption,omitempty"`
	TerminalPods                   int32  `json:"terminalPods"`
	KubernetesReady                int32  `json:"kubernetesReady"`
	EmergencyPods                  int32  `json:"emergencyPods,omitempty"`
	EmergencyReady                 int32  `json:"emergencyReady,omitempty"`
	ReadyShortfall                 int32  `json:"readyShortfall"`
	BelowMinServing                bool   `json:"belowMinServing"`
	WarmReserveStatus              string `json:"warmReserveStatus"`
	// WarmReserveReadyShortfall is the number of missing Ready reserve Pods
	// after allocating regular, disruption-free Ready capacity to replicas.
	WarmReserveReadyShortfall int32 `json:"warmReserveReadyShortfall,omitempty"`
}

// SearchWarmupObservation is historical worker completion, not current warmth.
type SearchWarmupObservation struct {
	Name               string      `json:"name"`
	PodUID             string      `json:"podUID"`
	ContainerID        string      `json:"containerID"`
	ProcessIncarnation string      `json:"processIncarnation"`
	Result             string      `json:"result"`
	TimedOut           bool        `json:"timedOut,omitempty"`
	ObservedAt         metav1.Time `json:"observedAt"`
}

// SearchWarmupSlot tracks one operator-started search warm-up by Pod and JVM.
type SearchWarmupSlot struct {
	Name               string `json:"name"`
	PodUID             string `json:"podUID"`
	ContainerID        string `json:"containerID,omitempty"`
	RestartCount       int32  `json:"restartCount,omitempty"`
	ProcessIncarnation string `json:"processIncarnation,omitempty"`
	Result             string `json:"result,omitempty"`
	TimedOut           bool   `json:"timedOut,omitempty"`
}

// DisruptionBudgetIntent pins the group floors before budget mutations.
type DisruptionBudgetIntent struct {
	Generation int64  `json:"generation"`
	Revision   string `json:"revision"`
}

// GateReleaseIntent binds a release decision to the exact Pod and template.
type GateReleaseIntent struct {
	PodUID              string            `json:"podUID"`
	Pod                 PodCreationIntent `json:"pod"`
	StartedAt           metav1.Time       `json:"startedAt"`
	WithdrawalRequested bool              `json:"withdrawalRequested,omitempty"`
}

// ProvisioningAttempt persists the bounded lifetime of released demand.
type ProvisioningAttempt struct {
	Role          string               `json:"role,omitempty"`
	Zone          string               `json:"zone,omitempty"`
	Name          string               `json:"name"`
	PodUID        string               `json:"podUID"`
	StartedAt     metav1.Time          `json:"startedAt"`
	Phase         string               `json:"phase"`
	Reason        string               `json:"reason,omitempty"`
	PeerDNS       bool                 `json:"peerDNS,omitempty"`
	PluginInstall *PluginInstallIntent `json:"pluginInstall,omitempty"`
	Cancellation  *RetiredPodCreation  `json:"cancellation,omitempty"`
}

// ServingRoleRevision ties a revision to the role's serving fleet.
type ServingRoleRevision struct {
	Fleet    string `json:"fleet"`
	Revision string `json:"revision"`
}

// PodCreationIntent identifies one gated Pod before its create request.
type PodCreationIntent struct {
	Name                     string               `json:"name"`
	Role                     string               `json:"role"`
	Zone                     string               `json:"zone"`
	Fleet                    string               `json:"fleet"`
	Revision                 string               `json:"revision"`
	Generation               int64                `json:"generation"`
	Sequence                 int64                `json:"sequence"`
	WarmupMode               string               `json:"warmupMode,omitempty"`
	PluginInstall            *PluginInstallIntent `json:"pluginInstall,omitempty"`
	KeystoreUID              string               `json:"keystoreUID,omitempty"`
	CapacityProfileUID       string               `json:"capacityProfileUID,omitempty"`
	CapacityProfileVersion   string               `json:"capacityProfileVersion,omitempty"`
	EmergencyEvidenceUID     string               `json:"emergencyEvidenceUID,omitempty"`
	EmergencyEvidenceVersion string               `json:"emergencyEvidenceVersion,omitempty"`
	BootstrapReplacementUID  string               `json:"bootstrapReplacementUID,omitempty"`
	PeerDNS                  bool                 `json:"peerDNS,omitempty"`
	PeerTLSLeafUID           string               `json:"peerTLSLeafUID,omitempty"`
	PeerTLSTrustUID          string               `json:"peerTLSTrustUID,omitempty"`
	PeerTLSLeafSHA256        string               `json:"peerTLSLeafSHA256,omitempty"`
	PeerTLSTrustSHA256       string               `json:"peerTLSTrustSHA256,omitempty"`
	PeerTLSSnapshotName      string               `json:"peerTLSSnapshotName,omitempty"`
	PeerTLSSnapshotUID       string               `json:"peerTLSSnapshotUID,omitempty"`
}

// PluginInstallIntent pins public installer inputs and credential identity.
type PluginInstallIntent struct {
	InstallerImage      string `json:"installerImage"`
	PolicySourceName    string `json:"policySourceName"`
	PolicySourceUID     string `json:"policySourceUID"`
	PolicySourceVersion string `json:"policySourceVersion"`
	RequestName         string `json:"requestName"`
	RequestUID          string `json:"requestUID,omitempty"`
	RequestSHA256       string `json:"requestSHA256"`
	PolicyName          string `json:"policyName"`
	PolicyUID           string `json:"policyUID,omitempty"`
	PolicySHA256        string `json:"policySHA256"`
	CredentialsName     string `json:"credentialsName,omitempty"`
	CredentialsUID      string `json:"credentialsUID,omitempty"`
	CredentialsHasCA    bool   `json:"credentialsHasCA,omitempty"`
}

// BasicAuthBaseline pins the startup credential source without exposing its contents.
type BasicAuthBaseline struct {
	SecretName   string `json:"secretName"`
	SecretUID    string `json:"secretUID"`
	Key          string `json:"key"`
	BundleSHA256 string `json:"bundleSHA256"`
}

// HTTPTLSBaseline pins immutable server-side HTTP TLS inputs across Pod creations.
type HTTPTLSBaseline struct {
	CertificateName string `json:"certificateName"`
	CertificateUID  string `json:"certificateUID"`
	CertificateHash string `json:"certificateHash"`
	LeafSHA256      string `json:"leafSHA256,omitempty"`
	TrustName       string `json:"trustName"`
	TrustUID        string `json:"trustUID"`
	TrustKey        string `json:"trustKey"`
	TrustHash       string `json:"trustHash"`
	Domain          string `json:"domain"`
	IssuanceSlot    string `json:"issuanceSlot,omitempty"`
	IssuerName      string `json:"issuerName,omitempty"`
}

// HTTPLeafSnapshot records the public identity of an issued leaf before copying its key.
type HTTPLeafSnapshot struct {
	Fleet         string      `json:"fleet"`
	Generation    int64       `json:"generation"`
	Domain        string      `json:"domain"`
	Name          string      `json:"name"`
	SourceName    string      `json:"sourceName"`
	SourceUID     string      `json:"sourceUID"`
	SourceVersion string      `json:"sourceVersion"`
	LeafSHA256    string      `json:"leafSHA256"`
	PEMSHA256     string      `json:"pemSHA256"`
	TrustName     string      `json:"trustName"`
	TrustUID      string      `json:"trustUID"`
	TrustVersion  string      `json:"trustVersion"`
	TrustKey      string      `json:"trustKey"`
	TrustSHA256   string      `json:"trustSHA256"`
	ObservedAt    metav1.Time `json:"observedAt,omitempty"`
}

// HTTPFleetCertificate pins one fleet's leaf and its negotiated certificate identity.
type HTTPFleetCertificate struct {
	Name        string `json:"name"`
	UID         string `json:"uid"`
	PEMSHA256   string `json:"pemSHA256"`
	LeafSHA256  string `json:"leafSHA256"`
	TrustName   string `json:"trustName,omitempty"`
	TrustKey    string `json:"trustKey,omitempty"`
	TrustUID    string `json:"trustUID,omitempty"`
	TrustSHA256 string `json:"trustSHA256,omitempty"`
}

// PeerTLSBaseline pins immutable development peer TLS inputs across Pod creations.
type PeerTLSBaseline struct {
	LeafName             string `json:"leafName"`
	LeafUID              string `json:"leafUID"`
	LeafResourceVersion  string `json:"leafResourceVersion,omitempty"`
	TrustName            string `json:"trustName"`
	TrustUID             string `json:"trustUID"`
	TrustResourceVersion string `json:"trustResourceVersion,omitempty"`
	TrustKey             string `json:"trustKey"`
	LeafSHA256           string `json:"leafSHA256,omitempty"`
	// LeafPEMSHA256 pins the complete certificate chain, including intermediates.
	LeafPEMSHA256 string `json:"leafPEMSHA256,omitempty"`
	TrustSHA256   string `json:"trustSHA256,omitempty"`
	SnapshotName  string `json:"snapshotName,omitempty"`
	SnapshotUID   string `json:"snapshotUID,omitempty"`
	IssuanceSlot  string `json:"issuanceSlot,omitempty"`
	IssuerName    string `json:"issuerName,omitempty"`
}

// OperationClaim is the durable identity of the single active operation.
// The immutable operation object owns the full action-specific input.
type OperationClaim struct {
	Name       string `json:"name"`
	UID        string `json:"uid"`
	Action     string `json:"action"`
	Generation int64  `json:"generation"`
}

// RollbackTarget pins the source fleet identities when promotion commits.
// RetainUntil limits new rollback handoffs when a window was configured.
type RollbackTarget struct {
	Weighted     bool                          `json:"weighted,omitempty"`
	PromotionUID string                        `json:"promotionUID"`
	Roles        map[string]RollbackRoleTarget `json:"roles"`
	RetainUntil  *metav1.MicroTime             `json:"retainUntil,omitempty"`
}

// FleetRetirementReservation excludes new operations before platform drain.
type FleetRetirementReservation struct {
	UID             string              `json:"uid"`
	CommitUID       string              `json:"commitUID"`
	Fleet           string              `json:"fleet"`
	Revision        string              `json:"revision,omitempty"`
	RevisionsByRole *FleetRoleRevisions `json:"revisionsByRole,omitempty"`
	ReservedAt      metav1.MicroTime    `json:"reservedAt"`
	// +kubebuilder:validation:MaxItems=512
	PodUIDs []string `json:"podUIDs,omitempty"`
}

// FleetRoleRevisions pins the literal Pod revision separately for each role.
type FleetRoleRevisions struct {
	// +kubebuilder:validation:MinLength=1
	Search string `json:"search"`
	// +kubebuilder:validation:MinLength=1
	Ingest string `json:"ingest"`
}

// ResolveFleetRoleRevisions accepts one representation of the fleet revisions.
func ResolveFleetRoleRevisions(shared string, byRole *FleetRoleRevisions) (FleetRoleRevisions, bool) {
	if byRole == nil {
		return FleetRoleRevisions{Search: shared, Ingest: shared}, shared != ""
	}
	return *byRole, shared == "" && byRole.Search != "" && byRole.Ingest != ""
}

func (r FleetRoleRevisions) ForRole(role string) string {
	switch role {
	case "search":
		return r.Search
	case "ingest":
		return r.Ingest
	}
	return ""
}

// Fields preserves the shared revision format for homogeneous fleets.
func (r FleetRoleRevisions) Fields() (string, *FleetRoleRevisions) {
	if r.Search == r.Ingest {
		return r.Search, nil
	}
	return "", &r
}

type RollbackRoleTarget struct {
	Fleet      string `json:"fleet"`
	Revision   string `json:"revision"`
	ServiceUID string `json:"serviceUID"`
	// Route pins are set for weighted rollback even if the promotion has been deleted.
	RouteName          string `json:"routeName,omitempty"`
	RouteUID           string `json:"routeUID,omitempty"`
	RouteSpecDigest    string `json:"routeSpecDigest,omitempty"`
	RetainedBackendUID string `json:"retainedBackendUID,omitempty"`
	ServingBackendUID  string `json:"servingBackendUID,omitempty"`
	ServingRevision    string `json:"servingRevision,omitempty"`
}

// PrimaryStoreStatus pins the resolved, non-secret S3 location.
type PrimaryStoreStatus struct {
	Bucket          string `json:"bucket"`
	Region          string `json:"region"`
	Endpoint        string `json:"endpoint,omitempty"`
	PathStyleAccess bool   `json:"pathStyleAccess,omitempty"`
}

// OpenSearchServerless is a namespaced instance.
// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:validation:XValidation:rule="!has(oldSelf.status) || !has(oldSelf.status.podCreationSequence) || oldSelf.status.podCreationSequence == 0 || ((has(self.spec.tls) && has(self.spec.tls.http)) == (has(oldSelf.spec.tls) && has(oldSelf.spec.tls.http)))",message="HTTP TLS mode cannot change after Pod creation starts"
// +kubebuilder:validation:XValidation:rule="!has(oldSelf.status) || ((!has(oldSelf.status.basicAuthBaseline) && (!has(oldSelf.status.podCreationSequence) || oldSelf.status.podCreationSequence == 0)) || ((has(self.spec.security) && self.spec.security.provider == 'ServerlessBasicAuth') == (has(oldSelf.spec.security) && oldSelf.spec.security.provider == 'ServerlessBasicAuth')))",message="Managed Basic Auth mode cannot change after Pod creation starts or credentials are pinned"
// +kubebuilder:resource:scope=Namespaced
// +kubebuilder:printcolumn:name="Available",type=string,JSONPath=".status.conditions[?(@.type=='Available')].status",description="Last recorded Kubernetes endpoint availability condition"
// +kubebuilder:printcolumn:name="Search Intent",type=string,JSONPath=".status.servingFleetIntentByRole.search",description="Recorded serving fleet intent for search"
// +kubebuilder:printcolumn:name="Index Intent",type=string,JSONPath=".status.servingFleetIntentByRole.ingest",description="Recorded serving fleet intent for indexing"
// +kubebuilder:printcolumn:name="Operation",type=string,JSONPath=".status.activeOperation.name",description="Current claimed operation"
// +kubebuilder:printcolumn:name="Reason",type=string,JSONPath=".status.conditions[?(@.type=='Available')].reason",priority=1,description="Reason for the recorded availability condition"
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=".metadata.creationTimestamp"
type OpenSearchServerless struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
	Spec              OpenSearchServerlessSpec   `json:"spec"`
	Status            OpenSearchServerlessStatus `json:"status,omitempty"`
}

// OpenSearchServerlessList is a list of instances.
// +kubebuilder:object:root=true
type OpenSearchServerlessList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []OpenSearchServerless `json:"items"`
}
