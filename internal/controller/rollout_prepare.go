package controller

import (
	"context"
	"errors"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/huyz0/opensearch-serverless-operator/api/v1alpha1"
	"github.com/huyz0/opensearch-serverless-operator/internal/rollout"
	"github.com/huyz0/opensearch-serverless-operator/internal/workload"
)

var errRolloutSourceProcess = errors.New("source process cannot be verified for rollback")

// prepareObservedPromotion pins the current candidate revision and both live
// production Service identities. The caller holds the claim, serializes
// rollout writers and supplies fresh qualification from the platform and
// application adapters. The supplied reader must bypass the manager cache.
func (r *OperationReconciler) prepareObservedPromotion(ctx context.Context, reader client.Reader, instance *v1alpha1.OpenSearchServerless, operation *v1alpha1.OpenSearchServerlessOperation, candidates map[string]*rollout.CandidateEvidence, maxAge time.Duration) error {
	return r.qualifyObservedPromotion(ctx, reader, instance, operation, candidates, maxAge, false)
}

func (r *OperationReconciler) qualifyObservedPromotion(ctx context.Context, reader client.Reader, instance *v1alpha1.OpenSearchServerless, operation *v1alpha1.OpenSearchServerlessOperation, candidates map[string]*rollout.CandidateEvidence, maxAge time.Duration, claimOnly bool) error {
	if reader == nil || r.Client == nil || instance == nil || operation == nil || operation.Spec.Action != "Promote" || operation.Status.RolloutRoles != nil || maxAge <= 0 {
		return fmt.Errorf("live clients, a claimed promotion without role intents and an observation window are required")
	}
	state := currentPromotionCandidate(instance, operation.Spec.TargetFleet)
	if state == nil {
		return fmt.Errorf("the requested fleet must match a current, pinned preview")
	}
	// With both preview UIDs pinned, this call can only observe the Services.
	preview := map[string]types.UID{"search": types.UID(state.SearchServiceUID), "ingest": types.UID(state.IngestServiceUID)}
	if _, err := ensurePreviewServices(ctx, nil, r.Core, instance, state.Fleet, preview); err != nil {
		return err
	}
	protected, err := r.observeRolloutBudgets(ctx, instance, state.Fleet)
	if err != nil {
		return err
	}
	if !protected {
		return fmt.Errorf("waiting for target disruption budgets to protect the serving floors")
	}
	roles := make(map[string]v1alpha1.RolloutRoleStatus, 2)
	intents := make([]rollout.SwitchIntent, 0, 2)
	snapshots := make(map[string]rollout.SwitchSnapshot, 2)
	for _, role := range []string{"search", "ingest"} {
		intent := rollout.SwitchIntent{
			InstanceUID: string(instance.UID), OperationUID: string(operation.UID), Role: role,
			SourceFleet: operation.Spec.ExpectedServingFleetByRole[role], TargetFleet: state.Fleet,
			CandidateRevision: state.Revision, Phase: rollout.Prepared,
		}
		snapshot, err := r.observeRoleSwitch(ctx, instance, operation, intent, candidates[role], nil, maxAge)
		if err != nil {
			return err
		}
		intent.ServiceUID = snapshot.Service.ServiceUID
		sourceRevision, err := r.observeSourceFleetRevision(ctx, instance, role, intent.SourceFleet)
		if err != nil {
			return err
		}
		intents = append(intents, intent)
		snapshots[role] = snapshot
		roles[role] = v1alpha1.RolloutRoleStatus{
			ServiceUID: intent.ServiceUID, SourceFleet: intent.SourceFleet, SourceRevision: sourceRevision, TargetFleet: intent.TargetFleet,
			CandidateRevision: intent.CandidateRevision, Phase: string(intent.Phase),
		}
	}
	var current v1alpha1.OpenSearchServerless
	if err := reader.Get(ctx, client.ObjectKeyFromObject(instance), &current); err != nil {
		return err
	}
	if current.UID != instance.UID || current.ResourceVersion != instance.ResourceVersion {
		return fmt.Errorf("instance changed during promotion preparation")
	}
	now := time.Now()
	if r.Now != nil {
		now = r.Now()
	}
	for _, intent := range intents {
		snapshot := snapshots[intent.Role]
		snapshot.Now = now
		action, err := rollout.PlanSwitch(intent, snapshot)
		if err != nil {
			return err
		}
		if action.Kind != rollout.PatchService {
			return fmt.Errorf("%s promotion preparation held: %s", intent.Role, action.Reason)
		}
		if current.Status.ServingFleetIntentByRole[intent.Role] != intent.SourceFleet {
			return fmt.Errorf("%s recorded serving fleet changed before promotion", intent.Role)
		}
	}
	if claimOnly {
		var currentOperation v1alpha1.OpenSearchServerlessOperation
		if err := reader.Get(ctx, client.ObjectKeyFromObject(operation), &currentOperation); err != nil {
			return err
		}
		if currentOperation.UID != operation.UID || currentOperation.ResourceVersion != operation.ResourceVersion {
			return fmt.Errorf("operation changed before claiming promotion")
		}
		return ClaimOperation(ctx, r.Client, &current, &currentOperation)
	}
	return prepareRolloutProgress(ctx, r.Client, &current, operation, roles)
}

// observeSourceFleetRevision identifies the rollback revision from owned Pods
// selected by the source Service. Application qualification remains separate.
func (r *OperationReconciler) observeSourceFleetRevision(ctx context.Context, instance *v1alpha1.OpenSearchServerless, role, fleet string) (string, error) {
	revision, _, err := r.observeSourceFleet(ctx, instance, role, fleet)
	return revision, err
}

func (r *OperationReconciler) observeSourceFleet(ctx context.Context, instance *v1alpha1.OpenSearchServerless, role, fleet string) (string, []corev1.Pod, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	service, err := workload.RoleService(instance, role, fleet, 9200)
	if err != nil {
		return "", nil, err
	}
	pods, err := r.Core.Pods(instance.Namespace).List(ctx, metav1.ListOptions{
		LabelSelector: labels.SelectorFromSet(service.Spec.Selector).String(), Limit: 513,
	})
	if err != nil {
		return "", nil, fmt.Errorf("observe %s source Pods: %w", role, err)
	}
	if pods.Continue != "" || len(pods.Items) > 512 {
		return "", nil, fmt.Errorf("too many %s source Pods for one observation", role)
	}
	var revision string
	minima := servingZoneMinima(instance, role)
	ready := make(map[string]int, len(minima))
	for _, pod := range pods.Items {
		if pod.Annotations[emergencyCapacityAnnotation] == "true" {
			return "", nil, fmt.Errorf("%s source fleet still has temporary capacity", role)
		}
		if pod.DeletionTimestamp != nil || pod.Status.Phase == corev1.PodFailed || pod.Status.Phase == corev1.PodSucceeded {
			continue
		}
		if err := checkFleetRuntimePod(instance, fleet, role, &pod); err != nil {
			return "", nil, err
		}
		if err := checkFleetPluginPod(instance, fleet, role, &pod); err != nil {
			return "", nil, fmt.Errorf("%s source Pod does not match its plugin set: %w", role, err)
		}
		owner := metav1.GetControllerOf(&pod)
		observed := pod.Labels["serverless.opensearch.org/revision"]
		if owner == nil || owner.UID != instance.UID || owner.Name != instance.Name || owner.Kind != "OpenSearchServerless" ||
			owner.APIVersion != v1alpha1.GroupVersion.String() || pod.UID == "" || pod.ResourceVersion == "" ||
			observed == "" || (revision != "" && revision != observed) {
			return "", nil, fmt.Errorf("%s source Pods do not identify one owned revision", role)
		}
		revision = observed
		if zone := pod.Labels[workload.ZoneLabel]; minima[zone] > 0 && podReadyForServing(&pod) {
			if r.rolloutSourceProbe != nil {
				if err := r.rolloutSourceProbe(ctx, instance, &pod, role); err != nil {
					return "", nil, fmt.Errorf("%s: %w", role, errRolloutSourceProcess)
				}
				current, err := r.Core.Pods(instance.Namespace).Get(ctx, pod.Name, metav1.GetOptions{})
				if err != nil || current.UID != pod.UID || current.ResourceVersion != pod.ResourceVersion {
					return "", nil, fmt.Errorf("%s source Pod changed: %w", role, errRolloutSourceProcess)
				}
			}
			ready[zone]++
		}
	}
	belowZone, belowRegional := servingFloorShortfalls(instance, role, minima, ready)
	if belowZone || belowRegional {
		return "", nil, fmt.Errorf("Ready source Pods for %s do not meet the configured serving floors", role)
	}
	if pin := instance.Status.ServingRevisionByRole[role]; pin.Fleet == fleet && pin.Revision != revision {
		return "", nil, fmt.Errorf("%s source Pods disagree with the recorded serving revision", role)
	}
	return revision, pods.Items, nil
}

// qualifyPromotionSource captures both original fleets for a new selector write.
// Already applied writes can be acknowledged after the original fleet changes.
func (r *OperationReconciler) qualifyPromotionSource(ctx context.Context, instance *v1alpha1.OpenSearchServerless, operation *v1alpha1.OpenSearchServerlessOperation) (map[string][]corev1.Pod, string, string) {
	snapshots := make(map[string][]corev1.Pod, 2)
	for _, role := range []string{"search", "ingest"} {
		state := operation.Status.RolloutRoles[role]
		if state.SourceRevision == "" {
			return nil, role, "SourceRevisionUnqualified"
		}
		revision, pods, err := r.observeSourceFleet(ctx, instance, role, state.SourceFleet)
		if errors.Is(err, errRolloutSourceProcess) {
			return nil, role, "SourceProcessUnqualified"
		}
		if err != nil || revision != state.SourceRevision {
			return nil, role, "SourceRevisionUnqualified"
		}
		snapshots[role] = pods
	}
	return snapshots, "", ""
}

func currentPromotionCandidate(instance *v1alpha1.OpenSearchServerless, fleet string) *v1alpha1.CandidateFleetStatus {
	state := instance.Status.CandidateFleet
	config := instance.Spec.Rollout
	if instance.Status.RollbackTarget != nil || state == nil || config == nil || config.Strategy != "BlueGreen" || config.Candidate == nil || state.PreviewRetiring ||
		state.Fleet != fleet || state.Fleet != config.Candidate.Fleet || state.Image != config.Candidate.Image ||
		!candidateInputsMatch(state, config.Candidate) ||
		state.Revision == "" || state.Generation != instance.Generation ||
		state.SearchServiceUID == "" || state.IngestServiceUID == "" || state.SearchServiceUID == state.IngestServiceUID {
		return nil
	}
	if _, pinned := instance.Status.PluginSetsByFleet[fleet]; state.PluginOverride != nil && !pinned {
		return nil
	}
	if _, pinned := instance.Status.RuntimeByFleet[fleet]; state.RuntimeOverride != nil && !pinned {
		return nil
	}
	return state
}
