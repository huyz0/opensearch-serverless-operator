package controller

import (
	"context"
	"fmt"
	"maps"
	"reflect"
	"slices"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	typedcore "k8s.io/client-go/kubernetes/typed/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/huyz0/opensearch-serverless-operator/api/v1alpha1"
	"github.com/huyz0/opensearch-serverless-operator/internal/workload"
)

const gatewayBackendsCondition = "GatewayBackendsConfigured"

func desiredGatewayBackends(instance *v1alpha1.OpenSearchServerless) (map[string]v1alpha1.GatewayBackendStatus, error) {
	backends := make(map[string]v1alpha1.GatewayBackendStatus)
	add := func(role, fleet string) error {
		if fleet == "" {
			return fmt.Errorf("missing fleet for %s Gateway backend", role)
		}
		service, err := workload.GatewayBackendService(instance, role, fleet)
		if err != nil {
			return err
		}
		backends[role+"/"+fleet] = v1alpha1.GatewayBackendStatus{Role: role, Fleet: fleet, Name: service.Name}
		return nil
	}
	for _, role := range []string{"search", "ingest"} {
		if err := add(role, instance.Status.ServingFleetIntentByRole[role]); err != nil {
			return nil, err
		}
		if retained := instance.Status.RollbackTarget; retained != nil {
			if err := add(role, retained.Roles[role].Fleet); err != nil {
				return nil, err
			}
		}
		if candidate := instance.Status.CandidateFleet; candidate != nil &&
			!candidate.PreviewRetiring && candidate.Generation == instance.Generation &&
			instance.Spec.Rollout.Candidate != nil && instance.Spec.Rollout.Candidate.Fleet == candidate.Fleet &&
			instance.Spec.Rollout.Candidate.Image == candidate.Image {
			if err := add(role, candidate.Fleet); err != nil {
				return nil, err
			}
		}
	}
	return backends, nil
}

func readGatewayBackend(ctx context.Context, core typedcore.CoreV1Interface, instance *v1alpha1.OpenSearchServerless, intent v1alpha1.GatewayBackendStatus) (*corev1.Service, error) {
	wanted, err := workload.GatewayBackendService(instance, intent.Role, intent.Fleet)
	if err != nil {
		return nil, err
	}
	if wanted.Name != intent.Name {
		return nil, fmt.Errorf("Gateway backend name disagrees with its recorded role and fleet")
	}
	observed, err := core.Services(instance.Namespace).Get(ctx, wanted.Name, metav1.GetOptions{})
	if err != nil {
		return nil, err
	}
	if !matchesFleetService(instance, observed, wanted, types.UID(intent.ServiceUID)) {
		return nil, fmt.Errorf("Gateway backend %q conflicts with its recorded Service intent", wanted.Name)
	}
	return observed, nil
}

// reconcileGatewayBackends records an intent before creating a Service, then
// learns its UID from a later read. Pinned or historical backends are retained
// until an explicit route-aware retirement can prove they are unreferenced.
func (r *InstanceReconciler) reconcileGatewayBackends(ctx context.Context, instance *v1alpha1.OpenSearchServerless) (bool, error) {
	before := instance.DeepCopyObject().(*v1alpha1.OpenSearchServerless)
	record := func(status metav1.ConditionStatus, reason, message string) (bool, error) {
		meta.SetStatusCondition(&instance.Status.Conditions, metav1.Condition{
			Type: gatewayBackendsCondition, Status: status, Reason: reason,
			Message: message, ObservedGeneration: instance.Generation,
		})
		if reflect.DeepEqual(before.Status, instance.Status) {
			return false, nil
		}
		err := r.Status().Patch(ctx, instance, client.MergeFromWithOptions(before, client.MergeFromWithOptimisticLock{}))
		return err == nil, err
	}
	if instance.Spec.Rollout == nil || !instance.Spec.Rollout.GatewayBackends {
		if meta.FindStatusCondition(instance.Status.Conditions, gatewayBackendsCondition) == nil {
			return false, nil
		}
		return record(metav1.ConditionFalse, "BackendsDisabled", "Gateway backend reconciliation is disabled and recorded Services are retained")
	}
	desired, err := desiredGatewayBackends(instance)
	if err != nil {
		return record(metav1.ConditionFalse, "BackendIntentInvalid", err.Error())
	}
	// Preserve historical identities. Removing a fleet from the instance spec
	// is insufficient to prove a platform-owned route has dropped its Service.
	pins := maps.Clone(instance.Status.GatewayBackends)
	if pins == nil {
		pins = make(map[string]v1alpha1.GatewayBackendStatus)
	}
	for key, spec := range desired {
		if pinned, exists := pins[key]; !exists {
			pins[key] = spec
		} else if pinned.Role != spec.Role || pinned.Fleet != spec.Fleet || pinned.Name != spec.Name {
			return record(metav1.ConditionFalse, "BackendIntentConflict", fmt.Sprintf("Recorded backend identity %q changed", key))
		}
	}
	if len(pins) > 128 {
		return record(metav1.ConditionFalse, "BackendLimitReached", "Gateway backend history is full")
	}
	if len(pins) != len(instance.Status.GatewayBackends) {
		instance.Status.GatewayBackends = pins
		return record(metav1.ConditionUnknown, "BackendIntentsRecorded", "Gateway backend Service intents are recorded")
	}
	for _, key := range slices.Sorted(maps.Keys(desired)) {
		intent := pins[key]
		service, err := readGatewayBackend(ctx, r.Core, instance, intent)
		if apierrors.IsNotFound(err) && intent.ServiceUID == "" {
			reader := r.APIReader
			if reader == nil {
				reader = r.Client
			}
			var live v1alpha1.OpenSearchServerless
			if err := reader.Get(ctx, client.ObjectKeyFromObject(instance), &live); err != nil {
				return false, err
			}
			if live.UID != instance.UID || live.ResourceVersion != instance.ResourceVersion ||
				live.DeletionTimestamp != nil || live.Status.ActiveOperation != nil {
				return true, nil
			}
			wanted, err := workload.GatewayBackendService(&live, intent.Role, intent.Fleet)
			if err != nil {
				return false, err
			}
			if _, err := r.Core.Services(instance.Namespace).Create(ctx, wanted, metav1.CreateOptions{}); err != nil && !apierrors.IsAlreadyExists(err) {
				return record(metav1.ConditionFalse, "BackendCreateUnconfirmed", fmt.Sprintf("Create of %q was unconfirmed: %v", wanted.Name, err))
			}
			return true, nil
		}
		if err != nil {
			return record(metav1.ConditionFalse, "BackendUnavailable", err.Error())
		}
		if intent.ServiceUID == "" {
			intent.ServiceUID = string(service.UID)
			instance.Status.GatewayBackends[key] = intent
			return record(metav1.ConditionUnknown, "BackendUIDRecorded", "Observed Gateway backend Service UID is recorded")
		}
	}
	return record(metav1.ConditionTrue, "BackendsObserved", "Eligible fleets have pinned Gateway backend Services")
}
