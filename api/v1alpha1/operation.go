package v1alpha1

import (
	"fmt"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// Validate checks the operation shapes currently implemented by this API.
// Authorization, target existence and fleet health require separate checks.
func (spec OperationSpec) Validate() error {
	if spec.InstanceRef.Name == "" || spec.InstanceRef.UID == "" {
		return fmt.Errorf("instance name and UID are required")
	}
	if spec.WriterRetirement != nil && spec.Action != "RetireWriter" {
		return fmt.Errorf("writer retirement payload requires a RetireWriter operation")
	}
	if spec.RoutingMode != "" && (spec.RoutingMode != "WeightedGateway" || (spec.Action != "Promote" && spec.Action != "Rollback")) {
		return fmt.Errorf("WeightedGateway routing mode requires a promotion or rollback")
	}
	if schedule := spec.WeightedTraffic; schedule != nil {
		if spec.Action != "Promote" || spec.RoutingMode != "WeightedGateway" {
			return fmt.Errorf("weighted traffic steps require a WeightedGateway promotion")
		}
		for _, role := range []string{"search", "ingest"} {
			weights := schedule.ForRole(role)
			if len(weights) > 30 {
				return fmt.Errorf("%s accepts at most 30 intermediate candidate weights", role)
			}
			previous := 1
			for _, weight := range weights {
				if weight <= previous || weight >= 100 {
					return fmt.Errorf("%s intermediate candidate weights must increase strictly between 2 and 99", role)
				}
				previous = weight
			}
		}
	}
	switch spec.Action {
	case "Promote", "Rollback":
		if spec.ClusterSettings != nil || spec.SecurityConfiguration != nil || spec.ProvisioningRetry != nil {
			return fmt.Errorf("fleet operations cannot contain settings, security or provisioning retry payloads")
		}
		if spec.TargetFleet == "" || len(spec.ExpectedServingFleetByRole) != 2 || spec.ExpectedServingFleetByRole["search"] == "" || spec.ExpectedServingFleetByRole["ingest"] == "" {
			return fmt.Errorf("promotion and rollback require a target fleet and expected search and ingest assignments")
		}
		if spec.Action == "Promote" && spec.SupersedesOperationUID != "" {
			return fmt.Errorf("only rollback can supersede an operation")
		}
	case "Decommission":
		if spec.TargetFleet != "" || len(spec.ExpectedServingFleetByRole) != 0 || spec.SupersedesOperationUID != "" || spec.ClusterSettings != nil || spec.SecurityConfiguration != nil || spec.ProvisioningRetry != nil {
			return fmt.Errorf("decommission cannot contain fleet assignments")
		}
	case "ApplyClusterSettings":
		if spec.ClusterSettings == nil || spec.SecurityConfiguration != nil || spec.ProvisioningRetry != nil || spec.TargetFleet != "" || len(spec.ExpectedServingFleetByRole) != 0 || spec.SupersedesOperationUID != "" {
			return fmt.Errorf("cluster settings operation requires a patch and no fleet assignments")
		}
		if len(spec.ClusterSettings.Set) == 0 && len(spec.ClusterSettings.Remove) == 0 {
			return fmt.Errorf("cluster settings patch must not be empty")
		}
		removed := map[string]bool{}
		for key := range spec.ClusterSettings.Set {
			if key == "" {
				return fmt.Errorf("cluster setting key cannot be empty")
			}
		}
		for _, key := range spec.ClusterSettings.Remove {
			if key == "" || removed[key] {
				return fmt.Errorf("cluster setting removals must contain unique nonempty keys")
			}
			if _, exists := spec.ClusterSettings.Set[key]; exists {
				return fmt.Errorf("cluster setting %q cannot be set and removed", key)
			}
			removed[key] = true
		}
	case "ApplySecurityConfiguration":
		if spec.SecurityConfiguration == nil || spec.SecurityConfiguration.CandidateRevision == "" || spec.SecurityConfiguration.ExpectedActiveRevision == "" || spec.ClusterSettings != nil || spec.ProvisioningRetry != nil || spec.TargetFleet != "" || len(spec.ExpectedServingFleetByRole) != 0 || spec.SupersedesOperationUID != "" {
			return fmt.Errorf("security operation requires candidate and expected active revisions without other action payloads")
		}
	case "RetryProvisioning":
		if spec.ProvisioningRetry == nil || spec.ProvisioningRetry.PodUID == "" || !spec.ProvisioningRetry.AcknowledgeNewAttempt || spec.TargetFleet != "" || len(spec.ExpectedServingFleetByRole) != 0 || spec.SupersedesOperationUID != "" || spec.ClusterSettings != nil || spec.SecurityConfiguration != nil {
			return fmt.Errorf("provisioning retry requires the blocked Pod UID and acknowledgement without other action payloads")
		}
	case "RetireWriter":
		if spec.WriterRetirement == nil || spec.WriterRetirement.PodName == "" || spec.WriterRetirement.PodUID == "" ||
			spec.TargetFleet != "" || len(spec.ExpectedServingFleetByRole) != 0 || spec.SupersedesOperationUID != "" ||
			spec.ClusterSettings != nil || spec.SecurityConfiguration != nil || spec.ProvisioningRetry != nil {
			return fmt.Errorf("writer retirement requires one exact Pod and no other action payloads")
		}
	default:
		return fmt.Errorf("operation action %q is not yet supported", spec.Action)
	}
	return nil
}

// InstanceReference identifies the instance targeted by an operation.
type InstanceReference struct {
	// +kubebuilder:validation:MinLength=1
	Name string `json:"name"`
	// +kubebuilder:validation:MinLength=1
	UID string `json:"uid"`
}

// OperationSpec records immutable intent. The controller verifies the
// referenced instance, the operation claim and action-specific prerequisites
// before issuing effects. New action checks run at creation so operations
// stored by older releases can still update status and clear finalizers.
// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="operation spec is immutable"
// +kubebuilder:validation:XValidation:rule="!has(self.weightedTraffic) || (self.action == 'Promote' && has(self.routingMode) && self.routingMode == 'WeightedGateway')",message="weighted traffic requires a WeightedGateway promotion"
// +kubebuilder:validation:XValidation:rule="has(self.writerRetirement) == (self.action == 'RetireWriter')",message="RetireWriter requires a writer retirement payload"
// +kubebuilder:validation:XValidation:rule="oldSelf.hasValue() || (has(self.clusterSettings) == (self.action == 'ApplyClusterSettings'))",optionalOldSelf=true,message="clusterSettings is required only for ApplyClusterSettings"
// +kubebuilder:validation:XValidation:rule="oldSelf.hasValue() || (has(self.securityConfiguration) == (self.action == 'ApplySecurityConfiguration'))",optionalOldSelf=true,message="securityConfiguration is required only for ApplySecurityConfiguration"
// +kubebuilder:validation:XValidation:rule="oldSelf.hasValue() || (has(self.provisioningRetry) == (self.action == 'RetryProvisioning'))",optionalOldSelf=true,message="provisioningRetry is required only for RetryProvisioning"
// +kubebuilder:validation:XValidation:rule="oldSelf.hasValue() || (!has(self.routingMode) || self.action in ['Promote', 'Rollback'])",optionalOldSelf=true,message="routingMode requires a promotion or rollback"
// +kubebuilder:validation:XValidation:rule="oldSelf.hasValue() || (!has(self.supersedesOperationUID) || size(self.supersedesOperationUID) == 0 || self.action == 'Rollback')",optionalOldSelf=true,message="only rollback can supersede an operation"
// +kubebuilder:validation:XValidation:rule="oldSelf.hasValue() || (self.action in ['Promote', 'Rollback'] ? has(self.targetFleet) && size(self.targetFleet) > 0 && has(self.expectedServingFleetByRole) && size(self.expectedServingFleetByRole) == 2 && 'search' in self.expectedServingFleetByRole && size(self.expectedServingFleetByRole['search']) > 0 && 'ingest' in self.expectedServingFleetByRole && size(self.expectedServingFleetByRole['ingest']) > 0 : (!has(self.targetFleet) || size(self.targetFleet) == 0) && (!has(self.expectedServingFleetByRole) || size(self.expectedServingFleetByRole) == 0))",optionalOldSelf=true,message="only promotion and rollback require a target fleet and both expected role assignments"
type OperationSpec struct {
	InstanceRef InstanceReference `json:"instanceRef"`
	// +kubebuilder:validation:Enum=Promote;Rollback;Decommission;ApplyClusterSettings;ApplySecurityConfiguration;RetryProvisioning;RetireWriter
	Action string `json:"action"`
	// +kubebuilder:validation:Enum=WeightedGateway
	RoutingMode                string                       `json:"routingMode,omitempty"`
	WeightedTraffic            *WeightedPromotionTraffic    `json:"weightedTraffic,omitempty"`
	TargetFleet                string                       `json:"targetFleet,omitempty"`
	ExpectedServingFleetByRole map[string]string            `json:"expectedServingFleetByRole,omitempty"`
	SupersedesOperationUID     string                       `json:"supersedesOperationUID,omitempty"`
	ClusterSettings            *ClusterSettingsPatch        `json:"clusterSettings,omitempty"`
	SecurityConfiguration      *SecurityConfigurationChange `json:"securityConfiguration,omitempty"`
	ProvisioningRetry          *ProvisioningRetry           `json:"provisioningRetry,omitempty"`
	WriterRetirement           *WriterRetirementTarget      `json:"writerRetirement,omitempty"`
}

// WriterRetirementTarget selects one serving index Pod by Kubernetes identity.
type WriterRetirementTarget struct {
	// +kubebuilder:validation:MinLength=1
	PodName string `json:"podName"`
	// +kubebuilder:validation:MinLength=1
	PodUID string `json:"podUID"`
}

// WeightedPromotionTraffic sets candidate percentages between admission at
// 1% and full cutover at 100%. Each role advances in the supplied order.
type WeightedPromotionTraffic struct {
	// +kubebuilder:validation:MaxItems=30
	// +kubebuilder:validation:items:Minimum=2
	// +kubebuilder:validation:items:Maximum=99
	Search []int `json:"search,omitempty"`
	// +kubebuilder:validation:MaxItems=30
	// +kubebuilder:validation:items:Minimum=2
	// +kubebuilder:validation:items:Maximum=99
	Ingest []int `json:"ingest,omitempty"`
}

func (schedule *WeightedPromotionTraffic) ForRole(role string) []int {
	if schedule == nil {
		return nil
	}
	switch role {
	case "search":
		return schedule.Search
	case "ingest":
		return schedule.Ingest
	default:
		return nil
	}
}

// ProvisioningRetry authorizes one new attempt after resolving a blocked Pod.
type ProvisioningRetry struct {
	// +kubebuilder:validation:MinLength=1
	PodUID string `json:"podUID"`
	// +kubebuilder:validation:XValidation:rule="self == true",message="a new provisioning attempt must be acknowledged"
	AcknowledgeNewAttempt bool `json:"acknowledgeNewAttempt"`
}

// SecurityConfigurationChange selects a staged revision and expected current revision.
type SecurityConfigurationChange struct {
	// +kubebuilder:validation:MinLength=1
	CandidateRevision string `json:"candidateRevision"`
	// +kubebuilder:validation:MinLength=1
	ExpectedActiveRevision string                 `json:"expectedActiveRevision"`
	Account                *SecurityAccountChange `json:"account,omitempty"`
}

// SecurityAccountChange carries one account transition. An absent previous
// reference creates the account. An absent desired reference removes it.
// +kubebuilder:validation:XValidation:rule="has(self.previousPasswordRef) || has(self.desiredPasswordRef)",message="an account change needs at least one password reference"
type SecurityAccountChange struct {
	// +kubebuilder:validation:Pattern=`^[A-Za-z0-9_.@-]{1,64}$`
	Username            string              `json:"username"`
	PreviousPasswordRef *SecretKeyReference `json:"previousPasswordRef,omitempty"`
	DesiredPasswordRef  *SecretKeyReference `json:"desiredPasswordRef,omitempty"`
}

// SecurityAccountSources pins the immutable inputs for a single dispatch.
type SecurityAccountSources struct {
	PreviousSecretUID string `json:"previousSecretUID,omitempty"`
	DesiredSecretUID  string `json:"desiredSecretUID,omitempty"`
	// +kubebuilder:validation:MaxProperties=512
	Consumers map[string]string `json:"consumers,omitempty"`
}

// ClusterSettingsPatch represents an explicit persistent settings change.
// +kubebuilder:validation:XValidation:rule="oldSelf.hasValue() || ((has(self.set) && size(self.set) > 0) || (has(self.remove) && size(self.remove) > 0))",optionalOldSelf=true,message="cluster settings patch must not be empty"
type ClusterSettingsPatch struct {
	Set    map[string]string `json:"set,omitempty"`
	Remove []string          `json:"remove,omitempty"`
}

// OperationStatus records observed progress independently of operation intent.
type OperationStatus struct {
	ObservedGeneration     int64                        `json:"observedGeneration,omitempty"`
	Phase                  string                       `json:"phase,omitempty"`
	Conditions             []metav1.Condition           `json:"conditions,omitempty"`
	SettingsBase           map[string]SettingValue      `json:"settingsBase,omitempty"`
	SettingsServiceUID     string                       `json:"settingsServiceUID,omitempty"`
	SecurityAccountSources *SecurityAccountSources      `json:"securityAccountSources,omitempty"`
	RolloutRoles           map[string]RolloutRoleStatus `json:"rolloutRoles,omitempty"`
	WeightedRollout        *WeightedRolloutStatus       `json:"weightedRollout,omitempty"`
	WriterRetirement       *WriterRetirementStatus      `json:"writerRetirement,omitempty"`
}

// WriterRetirementStatus pins the runtime and routing identities before withdrawal.
// It remains reserved through admission closure and successor verification.
type WriterRetirementStatus struct {
	InstanceGeneration int64                       `json:"instanceGeneration"`
	ContainerID        string                      `json:"containerID"`
	NodeID             string                      `json:"nodeID"`
	NodeEphemeralID    string                      `json:"nodeEphemeralID"`
	ServingFleet       string                      `json:"servingFleet"`
	ServiceUID         string                      `json:"serviceUID"`
	RouteUID           string                      `json:"routeUID"`
	GatewayUID         string                      `json:"gatewayUID"`
	GatewayServiceUID  string                      `json:"gatewayServiceUID"`
	BindingDigest      string                      `json:"bindingDigest"`
	PreparerSecretUID  string                      `json:"preparerSecretUID"`
	WithdrawalArmed    bool                        `json:"withdrawalArmed,omitempty"`
	WithdrawalDeadline *metav1.MicroTime           `json:"withdrawalDeadline,omitempty"`
	WithdrawnAt        *metav1.MicroTime           `json:"withdrawnAt,omitempty"`
	AdmissionClosed    bool                        `json:"admissionClosed,omitempty"`
	HeadInventory      *WriterHeadInventoryStatus  `json:"headInventory,omitempty"`
	EvictionArmed      bool                        `json:"evictionArmed,omitempty"`
	VictimGone         bool                        `json:"victimGone,omitempty"`
	ReleaseReceipt     *WriterReleaseReceiptStatus `json:"releaseReceipt,omitempty"`
	ReceiptMissingAt   *metav1.MicroTime           `json:"receiptMissingAt,omitempty"`
	// ReceiptObserverOffset advances past unavailable receipt observers.
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:Maximum=511
	ReceiptObserverOffset int `json:"receiptObserverOffset,omitempty"`
	// InventoryObserverOffset advances past an unavailable durable head observer.
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:Maximum=511
	InventoryObserverOffset int `json:"inventoryObserverOffset,omitempty"`
	// +kubebuilder:validation:MaxItems=512
	Successors          []WriterSuccessorStatus `json:"successors,omitempty"`
	SuccessorScanOffset int                     `json:"successorScanOffset,omitempty"`
}

const MaxRetirementInventoryHeads = 512

// WriterHeadInventoryStatus records the heads found while the victim was fenced.
type WriterHeadInventoryStatus struct {
	ObservedAt          metav1.MicroTime `json:"observedAt"`
	ObserverPodUID      string           `json:"observerPodUID"`
	ObserverNodeID      string           `json:"observerNodeID"`
	ObserverEphemeralID string           `json:"observerEphemeralID"`
	// +kubebuilder:validation:MaxItems=512
	Heads []RetiringWriterHead `json:"heads"`
}

// RetiringWriterHead identifies one physical register and its observed owner term.
type RetiringWriterHead struct {
	Key       string `json:"key"`
	BucketID  string `json:"bucketID,omitempty"`
	Index     string `json:"index"`
	IndexUUID string `json:"indexUUID,omitempty"`
	Shard     int32  `json:"shard"`
	Term      int64  `json:"term"`
}

// WriterReleaseReceiptStatus maps the retired JVM's receipt to the immutable head inventory.
type WriterReleaseReceiptStatus struct {
	ObservedAt          metav1.MicroTime `json:"observedAt"`
	ObserverPodUID      string           `json:"observerPodUID"`
	ObserverNodeID      string           `json:"observerNodeID"`
	ObserverEphemeralID string           `json:"observerEphemeralID"`
	// +kubebuilder:validation:MaxItems=512
	Releases []WriterHeadRelease `json:"releases"`
}

type WriterHeadRelease struct {
	// InventoryIndex identifies an entry in headInventory.heads.
	InventoryIndex       int32  `json:"inventoryIndex"`
	Outcome              string `json:"outcome"`
	LocalCleanupComplete bool   `json:"localCleanupComplete"`
}

// WriterSuccessorStatus records a foreground write against a journaled head.
type WriterSuccessorStatus struct {
	InventoryIndex        int32            `json:"inventoryIndex"`
	PodUID                string           `json:"podUID"`
	NodeID                string           `json:"nodeID"`
	EphemeralID           string           `json:"ephemeralID"`
	Term                  int64            `json:"term"`
	ObservedAt            metav1.MicroTime `json:"observedAt"`
	PredecessorWALSettled bool             `json:"predecessorWALSettled,omitempty"`
}

// WeightedRolloutStatus records ordered, per-role route steps for one claim.
type WeightedRolloutStatus struct {
	InstanceGeneration int64 `json:"instanceGeneration"`
	AdmissionWeight    int   `json:"admissionWeight"`
	// +kubebuilder:validation:MaxProperties=2
	Roles map[string]WeightedRoleStatus `json:"roles"`
}

type WeightedRoleStatus struct {
	StableServiceUID string            `json:"stableServiceUID,omitempty"`
	RouteName        string            `json:"routeName"`
	RouteUID         string            `json:"routeUID"`
	RouteSpecDigest  string            `json:"routeSpecDigest"`
	BackendUIDs      map[string]string `json:"backendUIDs"`
	Revisions        map[string]string `json:"revisions"`
	InitialWeights   map[string]int    `json:"initialWeights"`
	// +kubebuilder:validation:MaxItems=32
	Targets       []WeightedTarget  `json:"targets,omitempty"`
	NextStep      int               `json:"nextStep"`
	AppliedAt     *metav1.MicroTime `json:"appliedAt,omitempty"`
	LastAppliedAt *metav1.MicroTime `json:"lastAppliedAt,omitempty"`
}

type WeightedTarget struct {
	Weights map[string]int `json:"weights"`
}

// RolloutRoleStatus pins one Service transition before any selector change.
type RolloutRoleStatus struct {
	ServiceUID        string            `json:"serviceUID"`
	SourceFleet       string            `json:"sourceFleet"`
	SourceRevision    string            `json:"sourceRevision,omitempty"`
	TargetFleet       string            `json:"targetFleet"`
	CandidateRevision string            `json:"candidateRevision"`
	AdoptExisting     bool              `json:"adoptExisting,omitempty"`
	Phase             string            `json:"phase"`
	AppliedAt         *metav1.MicroTime `json:"appliedAt,omitempty"`
	Converged         bool              `json:"converged,omitempty"`
}

// SettingValue records presence separately from the value, so absence is not
// confused with an explicitly configured empty string.
type SettingValue struct {
	Present bool   `json:"present"`
	Value   string `json:"value,omitempty"`
}

// OpenSearchServerlessOperation requests an explicit action.
// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Namespaced
type OpenSearchServerlessOperation struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
	Spec              OperationSpec   `json:"spec"`
	Status            OperationStatus `json:"status,omitempty"`
}

// OpenSearchServerlessOperationList contains operations.
// +kubebuilder:object:root=true
type OpenSearchServerlessOperationList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []OpenSearchServerlessOperation `json:"items"`
}
