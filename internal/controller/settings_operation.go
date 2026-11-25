package controller

import (
	"context"
	"fmt"
	"reflect"
	"time"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	"github.com/huyz0/opensearch-serverless-operator/api/v1alpha1"
	"github.com/huyz0/opensearch-serverless-operator/internal/java"
)

const settingsFinalizer = "serverless.opensearch.org/cluster-settings"

const (
	settingsWritePrepared = "WritePrepared"
	settingsWriting       = "Writing"
)

func settingsOutcomeRecorded(phase string) bool {
	return phase == "Persisted" || phase == "Conflict" || phase == "Cancelled"
}

func settingsMayHaveWritten(phase string) bool {
	// Older controllers dispatched from Prepared before Writing existed.
	return phase == "Prepared" || phase == settingsWriting
}

func validateDevelopmentPatch(patch *v1alpha1.ClusterSettingsPatch) error {
	if patch == nil || len(patch.Set)+len(patch.Remove) == 0 || len(patch.Set)+len(patch.Remove) > 1 {
		return fmt.Errorf("development settings operation requires one qualified key")
	}
	for key, value := range patch.Set {
		if key != "serverless.bucket" || (value != "auto" && value != "off") {
			return fmt.Errorf("setting or value is not qualified")
		}
	}
	for _, key := range patch.Remove {
		if key != "serverless.bucket" {
			return fmt.Errorf("setting is not qualified")
		}
	}
	return nil
}

func settingsBase(values map[string]string, patch *v1alpha1.ClusterSettingsPatch) map[string]v1alpha1.SettingValue {
	base := make(map[string]v1alpha1.SettingValue, len(patch.Set)+len(patch.Remove))
	for key := range patch.Set {
		value, present := values[key]
		base[key] = v1alpha1.SettingValue{Present: present, Value: value}
	}
	for _, key := range patch.Remove {
		value, present := values[key]
		base[key] = v1alpha1.SettingValue{Present: present, Value: value}
	}
	return base
}

func (r *OperationReconciler) patchSettingsStatus(ctx context.Context, operation *v1alpha1.OpenSearchServerlessOperation, before *v1alpha1.OpenSearchServerlessOperation) (ctrl.Result, error) {
	operation.Status.ObservedGeneration = operation.Generation
	if reflect.DeepEqual(before.Status, operation.Status) {
		return ctrl.Result{RequeueAfter: time.Minute}, nil
	}
	if err := r.Status().Patch(ctx, operation, client.MergeFromWithOptions(before, client.MergeFromWithOptimisticLock{})); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{Requeue: true}, nil
}

func (r *OperationReconciler) reconcileSettingsOperation(ctx context.Context, operation *v1alpha1.OpenSearchServerlessOperation) (ctrl.Result, error) {
	reader := r.operationReader()
	key := client.ObjectKeyFromObject(operation)
	var liveOperation v1alpha1.OpenSearchServerlessOperation
	if err := reader.Get(ctx, key, &liveOperation); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	if liveOperation.UID != operation.UID {
		return ctrl.Result{Requeue: true}, nil
	}
	operation = &liveOperation
	before := operation.DeepCopyObject().(*v1alpha1.OpenSearchServerlessOperation)
	condition := func(reason, message string) {
		meta.SetStatusCondition(&operation.Status.Conditions, metav1.Condition{Type: "Ready", Status: metav1.ConditionFalse, Reason: reason, Message: message, ObservedGeneration: operation.Generation})
	}
	if err := operation.Spec.Validate(); err != nil {
		condition("InvalidSpec", err.Error())
		return r.patchSettingsStatus(ctx, operation, before)
	}
	if err := validateDevelopmentPatch(operation.Spec.ClusterSettings); err != nil {
		condition("UnsupportedSetting", err.Error())
		return r.patchSettingsStatus(ctx, operation, before)
	}
	finalized := controllerutil.ContainsFinalizer(operation, settingsFinalizer)
	deleting := operation.DeletionTimestamp != nil
	terminal := settingsOutcomeRecorded(operation.Status.Phase)
	mayHaveWritten := settingsMayHaveWritten(operation.Status.Phase)
	instanceKey := types.NamespacedName{Namespace: operation.Namespace, Name: operation.Spec.InstanceRef.Name}
	var instance v1alpha1.OpenSearchServerless
	err := reader.Get(ctx, instanceKey, &instance)
	if client.IgnoreNotFound(err) != nil {
		return ctrl.Result{}, err
	}
	if err != nil || string(instance.UID) != operation.Spec.InstanceRef.UID || instance.DeletionTimestamp != nil {
		if mayHaveWritten {
			condition("WriteOutcomeUnknown", "The original live instance is required to verify a possible settings write")
			return r.patchSettingsStatus(ctx, operation, before)
		}
		if finalized {
			return r.updateOperationFinalizer(ctx, operation, settingsFinalizer, false)
		}
		if terminal || deleting {
			return ctrl.Result{}, nil
		}
		condition("InstanceMismatch", "The original live instance is unavailable")
		return r.patchSettingsStatus(ctx, operation, before)
	}
	want := operationClaimFor(operation)
	claim := instance.Status.ActiveOperation
	// Deletion can increment the generation of a CR with an immutable spec.
	if deleting && claim != nil && claim.Generation > 0 && claim.Generation <= want.Generation {
		want.Generation = claim.Generation
	}
	owned := claim != nil && *claim == want
	if mayHaveWritten && !owned {
		condition("WriteOutcomeUnknown", "The original instance claim is required to verify a possible settings write")
		return r.patchSettingsStatus(ctx, operation, before)
	}
	if terminal || (deleting && !owned) {
		if owned {
			old := instance.DeepCopyObject().(*v1alpha1.OpenSearchServerless)
			instance.Status.ActiveOperation = nil
			if err := r.Status().Patch(ctx, &instance, client.MergeFromWithOptions(old, client.MergeFromWithOptimisticLock{})); err != nil {
				return ctrl.Result{}, err
			}
			return ctrl.Result{Requeue: true}, nil
		}
		if finalized {
			if !terminal && claim == nil {
				// Fence a claim patch already sent by a previous leader before
				// removing the operation that the patch references.
				fence := "Closing settings operation " + string(operation.UID)
				previous := meta.FindStatusCondition(instance.Status.Conditions, "SettingsClaimFence")
				if previous == nil || previous.Message != fence {
					old := instance.DeepCopyObject().(*v1alpha1.OpenSearchServerless)
					meta.SetStatusCondition(&instance.Status.Conditions, metav1.Condition{
						Type: "SettingsClaimFence", Status: metav1.ConditionTrue, Reason: "OperationDeleting",
						Message: fence, ObservedGeneration: instance.Generation,
					})
					if err := r.Status().Patch(ctx, &instance, client.MergeFromWithOptions(old, client.MergeFromWithOptimisticLock{})); err != nil {
						return ctrl.Result{}, err
					}
					return ctrl.Result{Requeue: true}, nil
				}
			}
			return r.updateOperationFinalizer(ctx, operation, settingsFinalizer, false)
		}
		return ctrl.Result{}, nil
	}
	if deleting && (operation.Status.Phase == "" || operation.Status.Phase == "Pending" || operation.Status.Phase == "Claimed" || operation.Status.Phase == settingsWritePrepared) {
		operation.Status.Phase = "Cancelled"
		condition("CancelledBeforeDispatch", "Deletion was observed before a settings write was authorized")
		return r.patchSettingsStatus(ctx, operation, before)
	}
	if instance.Spec.Image != r.DevelopmentSettingsImage || instance.Spec.TLS != nil || instance.Spec.Security != nil {
		condition("UnqualifiedRuntime", "Development settings path requires the configured image and an isolated unsecured runtime")
		return r.patchSettingsStatus(ctx, operation, before)
	}
	if claim == nil {
		if instance.Status.FleetRetirement != nil {
			condition("FleetRetirementReserved", "The instance is reserved for fleet retirement")
			return r.patchSettingsStatus(ctx, operation, before)
		}
		if operation.Status.Phase != "" && operation.Status.Phase != "Pending" {
			condition("ClaimMissing", "The recorded settings operation has lost its instance claim")
			return r.patchSettingsStatus(ctx, operation, before)
		}
		if instance.Status.PrimaryStore == nil || instance.Status.PendingPodCreation != nil || instance.Status.PendingGateRelease != nil || instance.Status.ProvisioningAttempt != nil || instance.Status.SearchWarmupSlot != nil || instance.Status.ServingFleetIntentByRole["search"] == "" || instance.Status.ServingFleetIntentByRole["search"] != instance.Status.ServingFleetIntentByRole["ingest"] {
			condition("InstanceBusy", "Instance prerequisites or other work must settle before settings can be claimed")
			return r.patchSettingsStatus(ctx, operation, before)
		}
		serviceCondition := meta.FindStatusCondition(instance.Status.Conditions, "ServicesConfigured")
		if serviceCondition == nil || serviceCondition.Status != metav1.ConditionTrue {
			condition("ServicesUnavailable", "Stable Services must be observed before settings can be claimed")
			return r.patchSettingsStatus(ctx, operation, before)
		}
		if !finalized {
			return r.updateOperationFinalizer(ctx, operation, settingsFinalizer, true)
		}
		// Read the operation after taking the instance snapshot. Deletion
		// fences older instance snapshots before removing the finalizer.
		var currentOperation v1alpha1.OpenSearchServerlessOperation
		if err := reader.Get(ctx, key, &currentOperation); err != nil {
			return ctrl.Result{}, client.IgnoreNotFound(err)
		}
		if currentOperation.UID != operation.UID || currentOperation.ResourceVersion != operation.ResourceVersion {
			return ctrl.Result{Requeue: true}, nil
		}
		if err := ClaimOperation(ctx, r.Client, &instance, operation); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{Requeue: true}, nil
	}
	if !owned {
		condition("InstanceBusy", "A different operation owns the instance claim")
		return r.patchSettingsStatus(ctx, operation, before)
	}
	if !finalized && !deleting {
		return r.updateOperationFinalizer(ctx, operation, settingsFinalizer, true)
	}
	if operation.Status.Phase == "" || operation.Status.Phase == "Pending" {
		operation.Status.Phase = "Claimed"
		condition("Preparing", "Operation claimed; reading the previous settings")
		return r.patchSettingsStatus(ctx, operation, before)
	}
	return r.advanceClaimedSettings(ctx, operation, before, &instance, condition)
}

// advanceClaimedSettings reads the pinned admin endpoint, decides whether
// read-back settles the operation, and reserves the only permitted dispatch.
func (r *OperationReconciler) advanceClaimedSettings(ctx context.Context, operation, before *v1alpha1.OpenSearchServerlessOperation, instance *v1alpha1.OpenSearchServerless, condition func(string, string)) (ctrl.Result, error) {
	mayHaveWritten := settingsMayHaveWritten(operation.Status.Phase)
	if operation.Status.Phase != "Claimed" && operation.Status.Phase != settingsWritePrepared && !mayHaveWritten {
		return ctrl.Result{}, fmt.Errorf("unknown settings operation phase")
	}
	backend := r.SettingsBackend
	if backend == nil {
		backend = newDevelopmentSettingsBackend(r.Core)
	}
	observed, err := backend.Read(ctx, instance)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("read settings before operation: %w", err)
	}
	if observed.ServiceUID == "" || observed.Values == nil {
		return ctrl.Result{}, fmt.Errorf("settings observation lacks Service identity or values")
	}
	if operation.Status.Phase == "Claimed" {
		operation.Status.SettingsBase = settingsBase(observed.Values, operation.Spec.ClusterSettings)
		operation.Status.SettingsServiceUID = string(observed.ServiceUID)
		operation.Status.Phase = settingsWritePrepared
		condition("Prepared", "Prior values and admin Service identity are recorded")
		return r.patchSettingsStatus(ctx, operation, before)
	}
	if operation.Status.SettingsServiceUID == "" || operation.Status.SettingsBase == nil || operation.Status.SettingsServiceUID != string(observed.ServiceUID) {
		if mayHaveWritten {
			condition("WriteOutcomeUnknown", "A write may still commit. Read-back requires the recorded admin Service and settings base")
			return r.patchSettingsStatus(ctx, operation, before)
		}
		operation.Status.Phase = "Conflict"
		condition("Conflict", "Recorded settings base or admin Service identity changed")
		return r.patchSettingsStatus(ctx, operation, before)
	}
	if java.SettingsPatchApplied(observed.Values, *operation.Spec.ClusterSettings) {
		operation.Status.Phase = "Persisted"
		meta.SetStatusCondition(&operation.Status.Conditions, metav1.Condition{Type: "Persisted", Status: metav1.ConditionTrue, Reason: "ReadBack", Message: "The requested values were observed in persistent settings", ObservedGeneration: operation.Generation})
		meta.SetStatusCondition(&operation.Status.Conditions, metav1.Condition{Type: "VerifiedEffective", Status: metav1.ConditionUnknown, Reason: "EffectUnobserved", Message: "Fleet-wide effective behavior is unknown", ObservedGeneration: operation.Generation})
		condition("PersistenceObserved", "The settings write is finalized after read-back. Fleet-wide effectiveness is unknown")
		return r.patchSettingsStatus(ctx, operation, before)
	}
	if mayHaveWritten {
		condition("WriteOutcomeUnknown", "The requested value is not present. A dispatched write may still commit")
		return r.patchSettingsStatus(ctx, operation, before)
	}
	if !reflect.DeepEqual(operation.Status.SettingsBase, settingsBase(observed.Values, operation.Spec.ClusterSettings)) {
		operation.Status.Phase = "Conflict"
		condition("Conflict", "Current values differ from both the recorded base and requested result")
		return r.patchSettingsStatus(ctx, operation, before)
	}
	var currentOperation v1alpha1.OpenSearchServerlessOperation
	reader := r.operationReader()
	if err := reader.Get(ctx, client.ObjectKeyFromObject(operation), &currentOperation); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	var currentInstance v1alpha1.OpenSearchServerless
	if err := reader.Get(ctx, client.ObjectKeyFromObject(instance), &currentInstance); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	if currentOperation.UID != operation.UID || currentOperation.ResourceVersion != operation.ResourceVersion ||
		currentInstance.UID != instance.UID || currentInstance.ResourceVersion != instance.ResourceVersion {
		return ctrl.Result{Requeue: true}, nil
	}
	// Only the reconciler that records Writing can submit the PUT. A crash
	// or lost response after the status write leaves read-back as the sole
	// recovery path, since this Java endpoint has no operation token or CAS.
	operation.Status.Phase = settingsWriting
	condition("WriteRecorded", "One settings write has been authorized. Subsequent reconciles verify by reading the admin Service")
	if _, err := r.patchSettingsStatus(ctx, operation, before); err != nil {
		return ctrl.Result{}, err
	}
	if err := backend.Apply(ctx, instance, observed.ServiceUID, *operation.Spec.ClusterSettings); err != nil {
		return ctrl.Result{}, fmt.Errorf("settings write outcome is uncertain; read-back is required: %w", err)
	}
	return ctrl.Result{Requeue: true}, nil
}
