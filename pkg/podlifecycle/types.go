package podlifecycle

import "time"

// Identity distinguishes an observed Pod from a replacement with the same name.
type Identity struct {
	Namespace string
	Name      string
	UID       string
	OwnerUID  string
}

// Group identifies a placement and availability boundary chosen by the caller.
type Group struct {
	Workload string
	Fleet    string
	Role     string
	Zone     string
}

// Pod is an observation taken by the caller. Eligibility must reflect the
// application's current process and serving policy, not just Pod readiness.
type Pod struct {
	Identity       Identity
	Group          Group
	Revision       string
	Eligible       bool
	Bound          bool
	Terminating    bool
	Terminal       bool
	RetirementHeld bool
	ObservedAt     time.Time
}

func (pod Pod) isProjectedServing() bool {
	return pod.Bound && pod.Eligible && !pod.Terminating && !pod.Terminal && !pod.RetirementHeld
}

// GroupPolicy defines a target and minimum serving count for one group.
type GroupPolicy struct {
	Group      Group
	Desired    int
	MinServing int
}

// Snapshot is supplied by the caller, which owns observation freshness, time,
// durable retry state and application-specific eligibility checks.
type Snapshot struct {
	Pods     []Pod
	Policies []GroupPolicy
	Now      time.Time
}

// ActionKind identifies a requested effect. Executors must re-read and
// validate ownership, identity and preconditions before a mutation.
type ActionKind string

const (
	CreatePod   ActionKind = "CreatePod"
	ReleaseGate ActionKind = "ReleaseGate"
	EvictPod    ActionKind = "EvictPod"
	WithdrawPod ActionKind = "WithdrawPod"
	Wait        ActionKind = "Wait"
)

// Action is a proposal. The caller persists operation intent and applies its
// own cross-group budgets before executing an action.
type Action struct {
	Kind   ActionKind
	Group  Group
	Pod    Identity
	Reason string
}
