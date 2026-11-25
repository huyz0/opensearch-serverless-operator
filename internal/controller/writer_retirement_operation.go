package controller

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"time"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	"github.com/huyz0/opensearch-serverless-operator/api/v1alpha1"
	"github.com/huyz0/opensearch-serverless-operator/internal/java"
)

const writerRetirementFinalizer = "serverless.opensearch.org/writer-retirement"
const writerWithdrawalDispatchWindow = 30 * time.Second

// The backend calls recheck immediately before any Java mutation. Its reads
// and writes target the process in the journal and use the operation UID.
type writerRetirementBackend interface {
	observe(context.Context, *v1alpha1.OpenSearchServerless, *v1alpha1.OpenSearchServerlessOperation) (v1alpha1.WriterRetirementStatus, error)
	readState(context.Context, *v1alpha1.OpenSearchServerless, *v1alpha1.OpenSearchServerlessOperation) (java.WriterRetirementState, error)
	withdraw(context.Context, *v1alpha1.OpenSearchServerless, *v1alpha1.OpenSearchServerlessOperation, func(context.Context) error) (java.WriterRetirementState, error)
	prepare(context.Context, *v1alpha1.OpenSearchServerless, *v1alpha1.OpenSearchServerlessOperation, func(context.Context) error) (java.WriterRetirementState, error)
	inventory(context.Context, *v1alpha1.OpenSearchServerless, *v1alpha1.OpenSearchServerlessOperation, func(context.Context) error) (*v1alpha1.WriterHeadInventoryStatus, error)
}

type writerRetirementEvictionBackend interface {
	evictionEnabled() bool
	evict(context.Context, *v1alpha1.OpenSearchServerless, *v1alpha1.OpenSearchServerlessOperation, func(context.Context) error) (bool, error)
	writerVictimAbsent(context.Context, *v1alpha1.OpenSearchServerless, *v1alpha1.OpenSearchServerlessOperation) (bool, error)
}

func (r *OperationReconciler) reconcileWriterRetirement(ctx context.Context, key client.ObjectKey) (ctrl.Result, error) {
	reader := r.operationReader()
	var op v1alpha1.OpenSearchServerlessOperation
	if err := reader.Get(ctx, key, &op); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	if err := op.Spec.Validate(); err != nil {
		return r.writerRetirementStatus(ctx, &op, "Pending", "InvalidSpec", err.Error(), op.Status.WriterRetirement)
	}
	var instance v1alpha1.OpenSearchServerless
	instanceKey := client.ObjectKey{Namespace: key.Namespace, Name: op.Spec.InstanceRef.Name}
	err := reader.Get(ctx, instanceKey, &instance)
	if client.IgnoreNotFound(err) != nil {
		return ctrl.Result{}, err
	}
	sameInstance := err == nil && string(instance.UID) == op.Spec.InstanceRef.UID
	claim := operationClaimFor(&op)
	if op.DeletionTimestamp != nil && instance.Status.ActiveOperation != nil {
		generation := instance.Status.ActiveOperation.Generation
		if generation > 0 && generation <= claim.Generation {
			claim.Generation = generation
		}
	}
	held := sameInstance && instance.Status.ActiveOperation != nil && *instance.Status.ActiveOperation == claim
	journal := op.Status.WriterRetirement
	hasFinalizer := controllerutil.ContainsFinalizer(&op, writerRetirementFinalizer)
	wait := func(phase, reason, message string) (ctrl.Result, error) {
		return r.writerRetirementStatus(ctx, &op, phase, reason, message, journal)
	}
	if op.Status.Phase == "Succeeded" && op.Status.ObservedGeneration == op.Generation &&
		journal != nil && journal.VictimGone && journal.EvictionArmed && journal.AdmissionClosed &&
		validWriterSuccessors(journal, true) {
		if held {
			return r.clearObservedOperationClaim(ctx, &instance, string(op.UID))
		}
		if sameInstance && instance.Status.ActiveOperation != nil && instance.Status.ActiveOperation.UID == string(op.UID) {
			return ctrl.Result{RequeueAfter: 3 * time.Second}, nil
		}
		if hasFinalizer {
			return r.updateOperationFinalizer(ctx, &op, writerRetirementFinalizer, false)
		}
		return ctrl.Result{}, nil
	}
	canObserveEviction := journal != nil && journal.WithdrawalArmed && journal.EvictionArmed &&
		journal.AdmissionClosed && journal.HeadInventory != nil && held && hasFinalizer &&
		journal.InstanceGeneration == instance.Generation && instance.DeletionTimestamp == nil
	// Deletion stops eviction dispatch but allows exact Pod readback and
	// subsequent read-only verification.
	recoveringAfterEviction := canObserveEviction && journal.VictimGone
	eviction, _ := r.writerRetirement.(writerRetirementEvictionBackend)
	if op.DeletionTimestamp != nil && !recoveringAfterEviction {
		return r.cancelWriterRetirement(ctx, &instance, &op, sameInstance, held, canObserveEviction, eviction)
	}
	if !sameInstance || instance.DeletionTimestamp != nil {
		return wait(op.Status.Phase, "InstanceMismatch", "The referenced live instance is required")
	}
	if journal != nil && (!held || !hasFinalizer || journal.InstanceGeneration != instance.Generation) {
		if !journal.WithdrawalArmed {
			return r.writerRetirementStatus(ctx, &op, "Pending", "ReservationReset", "Unsent writer reservation must be qualified again", nil)
		}
		return wait(op.Status.Phase, "ReservationChanged", "The writer retirement reservation no longer matches the instance")
	}
	if r.writerRetirement == nil {
		if held && journal != nil && !journal.WithdrawalArmed {
			return r.writerRetirementStatus(ctx, &op, "Pending", "ReservationReset", "Unsent writer reservation must be qualified again", nil)
		}
		if held && journal == nil {
			return r.clearObservedOperationClaim(ctx, &instance, string(op.UID))
		}
		return wait(op.Status.Phase, "RetirementDisabled", "Writer retirement is disabled in this manager")
	}
	if journal == nil {
		return r.reserveWriterRetirement(ctx, reader, &instance, &op, held)
	}
	canDispatchEviction := eviction != nil && journal.WithdrawalArmed && journal.AdmissionClosed && journal.HeadInventory != nil && !journal.VictimGone &&
		(eviction.evictionEnabled() || journal.EvictionArmed)
	action := nextWriterRetirementAction(journal, canDispatchEviction)
	if action == authorizeWriterWithdrawal {
		return r.authorizeWriterWithdrawal(ctx, &instance, &op)
	}
	recheck := func(ctx context.Context) error {
		var current v1alpha1.OpenSearchServerlessOperation
		var live v1alpha1.OpenSearchServerless
		if err := reader.Get(ctx, key, &current); err != nil {
			return err
		}
		if err := reader.Get(ctx, instanceKey, &live); err != nil {
			return err
		}
		if current.UID != op.UID || current.ResourceVersion != op.ResourceVersion ||
			(current.DeletionTimestamp != nil && !recoveringAfterEviction) ||
			live.UID != instance.UID || live.ResourceVersion != instance.ResourceVersion || live.DeletionTimestamp != nil ||
			live.Status.ActiveOperation == nil || *live.Status.ActiveOperation != claim {
			return fmt.Errorf("writer retirement reservation changed")
		}
		return nil
	}
	switch action {
	case verifyWriterRelease:
		return r.reconcileWriterReleaseReceipt(ctx, &instance, &op, recheck)
	case inventoryWriterHeads:
		return r.recordWriterHeadInventory(ctx, &instance, &op, recheck)
	case armWriterEviction, dispatchWriterEviction:
		return r.advanceWriterEviction(ctx, &instance, &op, eviction, recheck)
	case holdClosedWriter:
		return wait("Running", "WriterAdmissionClosed", "Writer admission is closed and the durable head inventory is recorded")
	case withdrawWriter:
		return r.dispatchWriterWithdrawal(ctx, &instance, &op, recheck)
	case closeWriterAdmission:
		state, err := r.writerRetirement.prepare(ctx, &instance, &op, recheck)
		if err != nil || state != java.WriterAdmissionClosed {
			return wait("Running", "AdmissionClosurePending", "Waiting for guarded Java admission closure")
		}
		record := *journal
		record.AdmissionClosed = true
		return r.writerRetirementStatus(ctx, &op, "Running", "WriterAdmissionClosed", "Writer admission is closed and successor evidence is pending", &record)
	default:
		return ctrl.Result{}, fmt.Errorf("unexpected writer retirement action")
	}
}

type writerRetirementAction uint8

const (
	authorizeWriterWithdrawal writerRetirementAction = iota
	withdrawWriter
	closeWriterAdmission
	inventoryWriterHeads
	holdClosedWriter
	armWriterEviction
	dispatchWriterEviction
	verifyWriterRelease
)

func nextWriterRetirementAction(journal *v1alpha1.WriterRetirementStatus, canEvict bool) writerRetirementAction {
	switch {
	case !journal.WithdrawalArmed:
		return authorizeWriterWithdrawal
	case !journal.AdmissionClosed && journal.WithdrawnAt == nil:
		return withdrawWriter
	case !journal.AdmissionClosed:
		return closeWriterAdmission
	case journal.HeadInventory == nil:
		return inventoryWriterHeads
	case journal.VictimGone:
		return verifyWriterRelease
	case !canEvict:
		return holdClosedWriter
	case !journal.EvictionArmed:
		return armWriterEviction
	default:
		return dispatchWriterEviction
	}
}

func (r *OperationReconciler) cancelWriterRetirement(ctx context.Context, instance *v1alpha1.OpenSearchServerless, op *v1alpha1.OpenSearchServerlessOperation, sameInstance, held, canObserveEviction bool, backend writerRetirementEvictionBackend) (ctrl.Result, error) {
	journal := op.Status.WriterRetirement
	hasFinalizer := controllerutil.ContainsFinalizer(op, writerRetirementFinalizer)
	if backend != nil && canObserveEviction {
		gone, err := backend.writerVictimAbsent(ctx, instance, op)
		if err != nil {
			return ctrl.Result{}, err
		}
		if gone {
			return r.recordWriterPodGone(ctx, op)
		}
		return r.writerRetirementStatus(ctx, op, op.Status.Phase, "WriterEvictionReadbackPending", "The exact writer Pod still exists and deletion has stopped eviction dispatch", journal)
	}
	if journal != nil && journal.WithdrawalArmed {
		if held && r.writerRetirement != nil && journal.WithdrawnAt == nil && journal.WithdrawalDeadline != nil &&
			!r.writerRetirementNow().Before(journal.WithdrawalDeadline.Time) {
			state, err := r.writerRetirement.readState(ctx, instance, op)
			if err == nil && state == java.WriterAdmissionOpen {
				return r.writerRetirementStatus(ctx, op, "Cancelled", "WriterStillOpen", "The exact writer remained open past the withdrawal deadline", nil)
			}
		}
		return r.writerRetirementStatus(ctx, op, op.Status.Phase, "RetirementReserved", "Writer withdrawal may have been dispatched", journal)
	}
	if held {
		return r.clearObservedOperationClaim(ctx, instance, string(op.UID))
	}
	if hasFinalizer {
		if sameInstance && instance.Status.ActiveOperation == nil {
			// Invalidate any instance snapshot from a claim request already
			// sent by a former leader before removing the operation.
			fence := "Closing writer retirement " + string(op.UID)
			previous := meta.FindStatusCondition(instance.Status.Conditions, "WriterRetirementClaimFence")
			if previous == nil || previous.Message != fence {
				before := instance.DeepCopyObject().(*v1alpha1.OpenSearchServerless)
				meta.SetStatusCondition(&instance.Status.Conditions, metav1.Condition{
					Type: "WriterRetirementClaimFence", Status: metav1.ConditionTrue, Reason: "OperationDeleting",
					Message: fence, ObservedGeneration: instance.Generation,
				})
				err := r.Status().Patch(ctx, instance, client.MergeFromWithOptions(before, client.MergeFromWithOptimisticLock{}))
				return ctrl.Result{Requeue: true}, err
			}
		}
		return r.updateOperationFinalizer(ctx, op, writerRetirementFinalizer, false)
	}
	return ctrl.Result{}, nil
}

func (r *OperationReconciler) reserveWriterRetirement(ctx context.Context, reader client.Reader, instance *v1alpha1.OpenSearchServerless, op *v1alpha1.OpenSearchServerlessOperation, held bool) (ctrl.Result, error) {
	hasFinalizer := controllerutil.ContainsFinalizer(op, writerRetirementFinalizer)
	if (!held && instance.Status.ActiveOperation != nil) || instance.Status.FleetRetirement != nil ||
		instance.Status.EmergencyRetirement != nil || instance.Status.ScaleDownRetirement != nil ||
		instance.Status.PendingPodCreation != nil || instance.Status.PendingGateRelease != nil ||
		instance.Status.ProvisioningAttempt != nil || instance.Status.SearchWarmupSlot != nil {
		if held {
			return r.clearObservedOperationClaim(ctx, instance, string(op.UID))
		}
		return r.writerRetirementStatus(ctx, op, "Pending", "InstanceBusy", "Another operation or Pod lifecycle action is active", nil)
	}
	reservation, err := r.writerRetirement.observe(ctx, instance, op)
	if err != nil || reservation.InstanceGeneration != instance.Generation || reservation.WithdrawnAt != nil ||
		reservation.WithdrawalArmed || reservation.WithdrawalDeadline != nil || reservation.AdmissionClosed || reservation.HeadInventory != nil ||
		reservation.EvictionArmed || reservation.VictimGone || reservation.ReleaseReceipt != nil || reservation.ReceiptMissingAt != nil ||
		len(reservation.Successors) != 0 || reservation.SuccessorScanOffset != 0 ||
		reservation.ContainerID == "" || reservation.NodeID == "" ||
		reservation.NodeEphemeralID == "" || reservation.ServingFleet == "" || reservation.ServiceUID == "" ||
		reservation.RouteUID == "" || reservation.GatewayUID == "" || reservation.GatewayServiceUID == "" || reservation.BindingDigest == "" ||
		reservation.PreparerSecretUID == "" {
		if held {
			return r.clearObservedOperationClaim(ctx, instance, string(op.UID))
		}
		return r.writerRetirementStatus(ctx, op, "Pending", "WriterUnqualified", "The exact writer and sufficient surviving index capacity must qualify", nil)
	}
	if !hasFinalizer {
		return r.updateOperationFinalizer(ctx, op, writerRetirementFinalizer, true)
	}
	if !held {
		var live v1alpha1.OpenSearchServerlessOperation
		if err := reader.Get(ctx, client.ObjectKeyFromObject(op), &live); err != nil {
			return ctrl.Result{}, client.IgnoreNotFound(err)
		}
		if live.UID != op.UID || live.ResourceVersion != op.ResourceVersion {
			return ctrl.Result{Requeue: true}, nil
		}
		if err := ClaimOperation(ctx, r.Client, instance, op); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{Requeue: true}, nil
	}
	return r.writerRetirementStatus(ctx, op, "Running", "WriterReserved", "The exact writer and routing bindings are recorded", &reservation)
}

func (r *OperationReconciler) authorizeWriterWithdrawal(ctx context.Context, instance *v1alpha1.OpenSearchServerless, op *v1alpha1.OpenSearchServerlessOperation) (ctrl.Result, error) {
	journal := op.Status.WriterRetirement
	observation, err := r.writerRetirement.observe(ctx, instance, op)
	if errors.Is(err, errWriterEvictionUnsupportedHeads) {
		return r.writerRetirementStatus(ctx, op, "Running", "UnsupportedWriterHeads", "The open writer holds heads without a successor verification path", journal)
	}
	if errors.Is(err, errWriterPreflightInventory) {
		ctrl.LoggerFrom(ctx).V(1).Info("Writer head preflight remains pending", "operation", client.ObjectKeyFromObject(op), "reason", err.Error())
		return r.writerRetirementStatus(ctx, op, "Running", "WriterHeadPreflightPending", "Waiting for a complete preflight scan of the open writer", journal)
	}
	if err != nil || !reflect.DeepEqual(observation, *journal) {
		return r.writerRetirementStatus(ctx, op, "Pending", "ReservationReset", "Unsent writer reservation no longer qualifies", nil)
	}
	record := *journal
	record.WithdrawalArmed = true
	cutoff := metav1.NewMicroTime(r.writerRetirementNow().Add(writerWithdrawalDispatchWindow).UTC().Truncate(time.Microsecond))
	record.WithdrawalDeadline = &cutoff
	return r.writerRetirementStatus(ctx, op, "Running", "WithdrawalArmed", "Withdrawal is authorized for the recorded writer", &record)
}

func (r *OperationReconciler) recordWriterHeadInventory(ctx context.Context, instance *v1alpha1.OpenSearchServerless, op *v1alpha1.OpenSearchServerlessOperation, recheck func(context.Context) error) (ctrl.Result, error) {
	record := *op.Status.WriterRetirement
	inventory, err := r.writerRetirement.inventory(ctx, instance, op, recheck)
	if err != nil || inventory == nil || inventory.ObservedAt.IsZero() || inventory.Heads == nil ||
		len(inventory.Heads) > v1alpha1.MaxRetirementInventoryHeads || inventory.ObserverPodUID == "" ||
		inventory.ObserverNodeID == "" || inventory.ObserverEphemeralID == "" {
		reason, message := "InventoryUnavailable", "A complete inventory for the exact writer is pending"
		switch {
		case errors.Is(err, context.DeadlineExceeded):
			reason, message = "ScanTimedOut", "The durable head scan exceeded its attempt or observer lease window"
		case errors.Is(err, java.ErrWriterHeadInventoryLimit):
			reason, message = "InventoryTooLarge", "The durable head scan exceeded the operation journal limit"
		}
		rememberInventoryObserverFailure(&record, err)
		return r.writerRetirementStatus(ctx, op, "Running", "WriterAdmissionClosed", "Writer admission is closed; complete durable head inventory is pending", &record,
			metav1.Condition{Type: "HeadInventoryReady", Status: metav1.ConditionFalse, Reason: reason, Message: message})
	}
	record.HeadInventory = inventory
	return r.writerRetirementStatus(ctx, op, "Running", "WriterAdmissionClosed", "Writer admission is closed and the durable head inventory is recorded", &record,
		metav1.Condition{Type: "HeadInventoryReady", Status: metav1.ConditionTrue, Reason: "InventoryRecorded", Message: "The exact writer's durable head inventory is journaled"})
}

func (r *OperationReconciler) advanceWriterEviction(ctx context.Context, instance *v1alpha1.OpenSearchServerless, op *v1alpha1.OpenSearchServerlessOperation, backend writerRetirementEvictionBackend, recheck func(context.Context) error) (ctrl.Result, error) {
	journal := op.Status.WriterRetirement
	record := *journal
	wait := func(phase, reason, message string) (ctrl.Result, error) {
		return r.writerRetirementStatus(ctx, op, phase, reason, message, &record)
	}
	if !journal.EvictionArmed && !supportsWriterSuccessorHeads(journal.HeadInventory.Heads) {
		return wait("Running", "UnsupportedWriterHeads", "The inventoried heads require a successor verification path that is unavailable")
	}
	if !journal.EvictionArmed {
		record.EvictionArmed = true
		return r.writerRetirementStatus(ctx, op, "Running", "WriterEvictionArmed", "Eviction intent is recorded for the exact writer", &record)
	}
	gone, err := backend.evict(ctx, instance, op, recheck)
	rememberInventoryObserverFailure(&record, err)
	if err != nil {
		ctrl.LoggerFrom(ctx).V(1).Info("Writer eviction remains pending", "operation", client.ObjectKeyFromObject(op), "reason", err.Error())
	}
	if err == nil && gone {
		return r.recordWriterPodGone(ctx, op)
	}
	if errors.Is(err, errWriterEvictionHeadsChanged) {
		return wait("Running", "WriterHeadsChanged", "Durable writer heads differ from the recorded inventory and need explicit recovery")
	}
	if errors.Is(err, errWriterEvictionRestartPolicy) {
		return wait("Running", "WriterEvictionRestartPolicy", err.Error())
	}
	if errors.Is(err, errWriterEvictionUnsupportedHeads) {
		return wait("Running", "UnsupportedWriterHeads", err.Error())
	}
	return wait("Running", "WriterEvictionPending", "Waiting for current eviction evidence or confirmation that the exact writer Pod is gone")
}

func (r *OperationReconciler) dispatchWriterWithdrawal(ctx context.Context, instance *v1alpha1.OpenSearchServerless, op *v1alpha1.OpenSearchServerlessOperation, recheck func(context.Context) error) (ctrl.Result, error) {
	journal := op.Status.WriterRetirement
	state, err := r.writerRetirement.withdraw(ctx, instance, op, recheck)
	if err == nil && state == java.WriterAdmissionOpen && journal.WithdrawalDeadline != nil && !r.writerRetirementNow().Before(journal.WithdrawalDeadline.Time) {
		record := *journal
		record.WithdrawalArmed = false
		record.WithdrawalDeadline = nil
		return r.writerRetirementStatus(ctx, op, "Running", "WithdrawalWindowExpired", "The exact writer is still open after the withdrawal window", &record)
	}
	if err != nil || (state != java.WriterTrafficWithdrawn && state != java.WriterAdmissionClosing && state != java.WriterAdmissionClosed) {
		return r.writerRetirementStatus(ctx, op, "Running", "WithdrawalPending", "Exact-process Java withdrawal has not been confirmed", journal)
	}
	record := *journal
	withdrawnAt := metav1.NewMicroTime(r.writerRetirementNow().UTC().Truncate(time.Microsecond))
	record.WithdrawnAt = &withdrawnAt
	return r.writerRetirementStatus(ctx, op, "Running", "WriterWithdrawn", "Java published withdrawal for the exact writer", &record)
}

func (r *OperationReconciler) recordWriterPodGone(ctx context.Context, op *v1alpha1.OpenSearchServerlessOperation) (ctrl.Result, error) {
	record := *op.Status.WriterRetirement
	record.VictimGone = true
	record.InventoryObserverOffset = 0
	return r.writerRetirementStatus(ctx, op, "Running", "WriterPodGone", "The exact writer Pod is gone and successor verification is pending", &record)
}

func (r *OperationReconciler) writerRetirementNow() time.Time {
	if r.Now != nil {
		return r.Now()
	}
	return time.Now()
}

func (r *OperationReconciler) writerRetirementStatus(ctx context.Context, op *v1alpha1.OpenSearchServerlessOperation, phase, reason, message string, record *v1alpha1.WriterRetirementStatus, evidence ...metav1.Condition) (ctrl.Result, error) {
	before := op.DeepCopyObject().(*v1alpha1.OpenSearchServerlessOperation)
	op.Status.Phase = phase
	op.Status.ObservedGeneration = op.Generation
	op.Status.WriterRetirement = record
	ready := metav1.ConditionFalse
	if phase == "Succeeded" {
		ready = metav1.ConditionTrue
	}
	meta.SetStatusCondition(&op.Status.Conditions, metav1.Condition{Type: "Ready", Status: ready,
		ObservedGeneration: op.Generation, Reason: reason, Message: message})
	for _, condition := range evidence {
		condition.ObservedGeneration = op.Generation
		meta.SetStatusCondition(&op.Status.Conditions, condition)
	}
	if !reflect.DeepEqual(op.Status, before.Status) {
		if err := r.Status().Patch(ctx, op, client.MergeFromWithOptions(before, client.MergeFromWithOptimisticLock{})); err != nil {
			return ctrl.Result{}, client.IgnoreNotFound(err)
		}
	}
	return ctrl.Result{RequeueAfter: 3 * time.Second}, nil
}
