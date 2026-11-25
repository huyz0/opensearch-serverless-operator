//go:build envtest

package controller

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	"github.com/huyz0/opensearch-serverless-operator/api/v1alpha1"
	"github.com/huyz0/opensearch-serverless-operator/internal/java"
)

type writerJournalFixture struct {
	state                     java.WriterRetirementState
	withdrawals, preparations int
	failObservation           bool
	preflightError            error
	openAfterDeadline         bool
	failInventory             bool
	inventoryReads            int
	enableEviction            bool
	evictionAttempts          int
	evictionReadbacks         int
	victimGone                bool
	receiptReads              int
	receiptObserverFailure    bool
	successorTerm             int64
	walOnly, walSettled       bool
	successorsByHead          []v1alpha1.WriterSuccessorStatus
}

func (f *writerJournalFixture) observeSuccessors(ctx context.Context, _ *v1alpha1.OpenSearchServerless, op *v1alpha1.OpenSearchServerlessOperation, recheck func(context.Context) error) ([]v1alpha1.WriterSuccessorStatus, int, error) {
	if _, usable := inspectWriterRelease(op.Status.WriterRetirement); !usable {
		return nil, 0, errors.New("release receipt was not committed before successor verification")
	}
	if err := recheck(ctx); err != nil {
		return nil, 0, err
	}
	if f.successorsByHead != nil {
		return f.successorsByHead, 0, nil
	}
	if f.successorTerm == 0 {
		return nil, 0, nil
	}
	observedAt := metav1.NewMicroTime(time.Now().Truncate(time.Microsecond))
	if missing := op.Status.WriterRetirement.ReceiptMissingAt; missing != nil && observedAt.Before(missing) {
		observedAt = *missing
	}
	return []v1alpha1.WriterSuccessorStatus{{
		InventoryIndex: 0, PodUID: "surviving-pod", NodeID: "survivor", EphemeralID: "surviving-process", Term: f.successorTerm,
		ObservedAt: observedAt, PredecessorWALSettled: f.walSettled,
	}}, 0, nil
}

func testWriterRetirementReleaseEvidence(t *testing.T, c client.Client, namespace string, spec v1alpha1.OpenSearchServerlessSpec, emptyInventory bool) {
	t.Helper()
	ctx := context.Background()
	name := "partial-receipt"
	if emptyInventory {
		name = "empty-inventory"
	}
	instance := &v1alpha1.OpenSearchServerless{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace}, Spec: spec}
	if err := c.Create(ctx, instance); err != nil {
		t.Fatal(err)
	}
	op := &v1alpha1.OpenSearchServerlessOperation{
		ObjectMeta: metav1.ObjectMeta{Name: "retire-" + name, Namespace: namespace, Finalizers: []string{writerRetirementFinalizer}},
		Spec: v1alpha1.OperationSpec{
			Action: "RetireWriter", InstanceRef: v1alpha1.InstanceReference{Name: instance.Name, UID: string(instance.UID)},
			WriterRetirement: &v1alpha1.WriterRetirementTarget{PodName: "writer", PodUID: "retired-pod"},
		},
	}
	if err := c.Create(ctx, op); err != nil {
		t.Fatal(err)
	}
	claim := operationClaimFor(op)
	instance.Status.ActiveOperation = &claim
	if err := c.Status().Update(ctx, instance); err != nil {
		t.Fatal(err)
	}
	at := metav1.NewMicroTime(time.Now().UTC().Truncate(time.Microsecond))
	op.Status.Phase = "Running"
	op.Status.WriterRetirement = &v1alpha1.WriterRetirementStatus{
		InstanceGeneration: instance.Generation, NodeID: "retired", NodeEphemeralID: "retired-process",
		WithdrawalArmed: true, EvictionArmed: true, AdmissionClosed: true, VictimGone: true,
		HeadInventory: &v1alpha1.WriterHeadInventoryStatus{
			ObservedAt: at, ObserverPodUID: "survivor", ObserverNodeID: "surviving-node", ObserverEphemeralID: "surviving-process",
			Heads: []v1alpha1.RetiringWriterHead{
				{Key: "head-0", Index: "docs", IndexUUID: "uuid", Shard: 0, Term: 1},
				{Key: "head-1", Index: "docs", IndexUUID: "uuid", Shard: 1, Term: 1},
			},
		},
		ReleaseReceipt: &v1alpha1.WriterReleaseReceiptStatus{
			ObservedAt: at, ObserverPodUID: "survivor", ObserverNodeID: "surviving-node", ObserverEphemeralID: "surviving-process",
			Releases: []v1alpha1.WriterHeadRelease{{InventoryIndex: 0, Outcome: string(java.WriterHeadReleased)}},
		},
	}
	if emptyInventory {
		op.Status.WriterRetirement.HeadInventory.Heads = []v1alpha1.RetiringWriterHead{}
		op.Status.WriterRetirement.ReleaseReceipt = nil
	}
	if err := c.Status().Update(ctx, op); err != nil {
		t.Fatal(err)
	}
	backend := &writerJournalFixture{walOnly: emptyInventory}
	key := client.ObjectKeyFromObject(op)
	reconcile := func() {
		t.Helper()
		r := &OperationReconciler{Client: c, uncachedOperationReader: c, writerRetirement: backend}
		if _, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: key}); err != nil {
			t.Fatal(err)
		}
		if err := c.Get(ctx, key, op); err != nil {
			t.Fatal(err)
		}
		if err := c.Get(ctx, client.ObjectKeyFromObject(instance), instance); err != nil {
			t.Fatal(err)
		}
	}
	if emptyInventory {
		backend.receiptObserverFailure = true
		reconcile()
		if op.Status.WriterRetirement.ReceiptObserverOffset != 4 || op.Status.WriterRetirement.ReceiptMissingAt != nil {
			t.Fatal("unavailable observers must persist a resume offset without recording receipt absence")
		}
		backend.receiptObserverFailure = false
		reconcile()
		journal := op.Status.WriterRetirement
		if journal.ReceiptMissingAt == nil || journal.ReceiptObserverOffset != 0 || instance.Status.ActiveOperation == nil || op.Status.Phase == "Succeeded" {
			t.Fatal("missing receipt must be recorded before completing empty inventory")
		}
		reconcile()
		journal = op.Status.WriterRetirement
		if op.Status.Phase != "Succeeded" || journal.HeadInventory.Heads == nil || len(journal.HeadInventory.Heads) != 0 ||
			journal.ReleaseReceipt != nil || len(journal.Successors) != 0 || instance.Status.ActiveOperation == nil ||
			!meta.IsStatusConditionTrue(op.Status.Conditions, "WriterReleaseReady") ||
			!meta.IsStatusConditionTrue(op.Status.Conditions, "WriterSuccessorReady") {
			t.Fatal("confirmed empty inventory must finish without inventing a receipt or successor")
		}
		reconcile()
		if instance.Status.ActiveOperation != nil {
			t.Fatal("durable empty inventory completion did not release the claim")
		}
		return
	}
	first := v1alpha1.WriterSuccessorStatus{
		PodUID: "survivor", NodeID: "surviving-node", EphemeralID: "surviving-process", Term: 2, ObservedAt: at,
	}
	backend.successorsByHead = []v1alpha1.WriterSuccessorStatus{first}
	reconcile()
	if len(op.Status.WriterRetirement.Successors) != 1 || op.Status.Phase == "Succeeded" || instance.Status.ActiveOperation == nil {
		t.Fatal("receipt-covered head should accept its successor independently and keep the claim for the omitted head")
	}
	second := first
	second.InventoryIndex = 1
	backend.successorsByHead = []v1alpha1.WriterSuccessorStatus{second}
	reconcile()
	if len(op.Status.WriterRetirement.Successors) != 1 || op.Status.Phase == "Succeeded" || instance.Status.ActiveOperation == nil {
		t.Fatal("the omitted head must have an explicit WAL settlement proof")
	}
	backend.successorsByHead[0].PredecessorWALSettled = true
	reconcile()
	journal := op.Status.WriterRetirement
	if op.Status.Phase != "Succeeded" || !journal.Successors[1].PredecessorWALSettled ||
		len(journal.ReleaseReceipt.Releases) != 1 || journal.ReceiptMissingAt != nil ||
		!meta.IsStatusConditionTrue(op.Status.Conditions, "WriterReleaseReady") || instance.Status.ActiveOperation == nil {
		t.Fatal("mixed recovery must preserve the original receipt, both proofs and the claim until success is durable")
	}
	reconcile()
	if instance.Status.ActiveOperation != nil {
		t.Fatal("durable mixed evidence did not release the claim")
	}
}

func (f *writerJournalFixture) readReleaseReceipt(ctx context.Context, _ *v1alpha1.OpenSearchServerless, op *v1alpha1.OpenSearchServerlessOperation, recheck func(context.Context) error) (*v1alpha1.WriterReleaseReceiptStatus, int, error) {
	offset := op.Status.WriterRetirement.ReceiptObserverOffset
	if !op.Status.WriterRetirement.VictimGone {
		return nil, offset, errors.New("attempted receipt read before the original UID was confirmed absent")
	}
	if err := recheck(ctx); err != nil {
		return nil, offset, err
	}
	if f.receiptObserverFailure {
		return nil, offset + 4, errors.New("fixture observer batch unavailable")
	}
	f.receiptReads++
	if f.receiptReads == 1 || f.walOnly {
		return nil, 0, java.ErrWriterHandoffReceiptMissing
	}
	return &v1alpha1.WriterReleaseReceiptStatus{
		ObservedAt:     metav1.NewMicroTime(time.Now().Truncate(time.Microsecond)),
		ObserverPodUID: "surviving-pod", ObserverNodeID: "survivor", ObserverEphemeralID: "surviving-process",
		Releases: []v1alpha1.WriterHeadRelease{{InventoryIndex: 0, Outcome: string(java.WriterHeadReleasedWithWAL)}},
	}, 0, nil
}

func (f *writerJournalFixture) evictionEnabled() bool {
	return f.enableEviction
}

func (f *writerJournalFixture) writerVictimAbsent(_ context.Context, _ *v1alpha1.OpenSearchServerless, _ *v1alpha1.OpenSearchServerlessOperation) (bool, error) {
	f.evictionReadbacks++
	return f.victimGone, nil
}

func (f *writerJournalFixture) evict(ctx context.Context, _ *v1alpha1.OpenSearchServerless, op *v1alpha1.OpenSearchServerlessOperation, recheck func(context.Context) error) (bool, error) {
	if !op.Status.WriterRetirement.EvictionArmed || op.Status.WriterRetirement.HeadInventory == nil {
		return false, errors.New("eviction was called before the journal was persisted")
	}
	if err := recheck(ctx); err != nil {
		return false, err
	}
	if f.failInventory {
		return false, &writerInventoryObserverError{cause: context.DeadlineExceeded, next: 2}
	}
	f.evictionAttempts++
	if f.victimGone {
		return true, nil
	}
	return false, errors.New("lost eviction response")
}

func (f *writerJournalFixture) observe(_ context.Context, instance *v1alpha1.OpenSearchServerless, op *v1alpha1.OpenSearchServerlessOperation) (v1alpha1.WriterRetirementStatus, error) {
	if op.Status.WriterRetirement != nil && f.preflightError != nil {
		return v1alpha1.WriterRetirementStatus{}, f.preflightError
	}
	if f.failObservation {
		return v1alpha1.WriterRetirementStatus{}, errors.New("index survivor observation failed")
	}
	return v1alpha1.WriterRetirementStatus{
		InstanceGeneration: instance.Generation, NodeID: "node", NodeEphemeralID: "jvm", ContainerID: "container",
		ServiceUID: "index", ServingFleet: "initial", RouteUID: "route", GatewayUID: "gateway",
		GatewayServiceUID: "frontend", BindingDigest: "binding", PreparerSecretUID: "preparer",
	}, nil
}

func (f *writerJournalFixture) readState(context.Context, *v1alpha1.OpenSearchServerless, *v1alpha1.OpenSearchServerlessOperation) (java.WriterRetirementState, error) {
	if f.openAfterDeadline {
		return java.WriterAdmissionOpen, nil
	}
	return f.state, nil
}

func (f *writerJournalFixture) inventory(ctx context.Context, _ *v1alpha1.OpenSearchServerless, op *v1alpha1.OpenSearchServerlessOperation, recheck func(context.Context) error) (*v1alpha1.WriterHeadInventoryStatus, error) {
	if !op.Status.WriterRetirement.AdmissionClosed {
		return nil, fmt.Errorf("inventory must follow persisted admission closure")
	}
	f.inventoryReads++
	if f.failInventory {
		return nil, &writerInventoryObserverError{cause: errors.New("lost inventory page"), next: 1}
	}
	if err := recheck(ctx); err != nil {
		return nil, err
	}
	return &v1alpha1.WriterHeadInventoryStatus{
		ObservedAt: metav1.NewMicroTime(time.Now().UTC().Truncate(time.Microsecond)), ObserverPodUID: "surviving-pod",
		ObserverNodeID: "survivor", ObserverEphemeralID: "surviving-jvm",
		Heads: []v1alpha1.RetiringWriterHead{{Key: "key", Index: "docs", IndexUUID: "uuid", Term: 1}},
	}, nil
}

func (f *writerJournalFixture) withdraw(ctx context.Context, instance *v1alpha1.OpenSearchServerless, op *v1alpha1.OpenSearchServerlessOperation, recheck func(context.Context) error) (java.WriterRetirementState, error) {
	if op.Status.WriterRetirement == nil || !op.Status.WriterRetirement.WithdrawalArmed || op.Status.WriterRetirement.NodeEphemeralID != "jvm" ||
		op.Status.WriterRetirement.WithdrawalDeadline == nil || op.Status.WriterRetirement.WithdrawnAt != nil || instance.Status.ActiveOperation == nil {
		return "", fmt.Errorf("missing journal or claim")
	}
	if f.openAfterDeadline {
		return java.WriterAdmissionOpen, nil
	}
	if f.state == java.WriterTrafficWithdrawn {
		return f.state, nil
	}
	if err := recheck(ctx); err != nil {
		return "", err
	}
	f.withdrawals++
	f.state = java.WriterTrafficWithdrawn
	return "", errors.New("withdrawal response lost")
}

func (f *writerJournalFixture) prepare(ctx context.Context, _ *v1alpha1.OpenSearchServerless, op *v1alpha1.OpenSearchServerlessOperation, recheck func(context.Context) error) (java.WriterRetirementState, error) {
	if op.Status.WriterRetirement.WithdrawnAt == nil {
		return "", fmt.Errorf("withdrawal proof is missing")
	}
	if f.state == java.WriterAdmissionClosed {
		return f.state, nil
	}
	if err := recheck(ctx); err != nil {
		return "", err
	}
	f.preparations++
	f.state = java.WriterAdmissionClosed
	return "", errors.New("prepare response lost")
}

func testWriterRetirementJournal(t *testing.T, c client.Client, namespace string, spec v1alpha1.OpenSearchServerlessSpec, walOnly bool) {
	t.Helper()
	ctx := context.Background()
	instanceName := "journal-instance"
	if walOnly {
		instanceName = "wal-journal-instance"
	}
	instance := &v1alpha1.OpenSearchServerless{ObjectMeta: metav1.ObjectMeta{Name: instanceName, Namespace: namespace}, Spec: spec}
	if err := c.Create(ctx, instance); err != nil {
		t.Fatal(err)
	}
	instanceKey := client.ObjectKeyFromObject(instance)
	op := &v1alpha1.OpenSearchServerlessOperation{
		ObjectMeta: metav1.ObjectMeta{Name: "retire-one", Namespace: namespace},
		Spec: v1alpha1.OperationSpec{
			Action: "RetireWriter", InstanceRef: v1alpha1.InstanceReference{Name: instance.Name, UID: string(instance.UID)},
			WriterRetirement: &v1alpha1.WriterRetirementTarget{PodName: "writer", PodUID: "pod-uid"},
		},
	}
	if err := c.Create(ctx, op); err != nil {
		t.Fatal(err)
	}
	opKey := client.ObjectKeyFromObject(op)
	read := func() {
		t.Helper()
		if err := c.Get(ctx, opKey, op); err != nil {
			t.Fatal(err)
		}
		if err := c.Get(ctx, instanceKey, instance); err != nil {
			t.Fatal(err)
		}
	}
	fixture := &writerJournalFixture{walOnly: walOnly}
	now := time.Now()
	reconcile := func(backend writerRetirementBackend) {
		t.Helper()
		// Construct a new reconciler each time. Only the API server and
		// simulated remote JVM retain state across the steps.
		r := &OperationReconciler{Client: c, uncachedOperationReader: c, writerRetirement: backend, Now: func() time.Time { return now }}
		if _, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: opKey}); err != nil {
			t.Fatal(err)
		}
		read()
	}
	reconcile(fixture)
	if !controllerutil.ContainsFinalizer(op, writerRetirementFinalizer) || instance.Status.ActiveOperation != nil {
		t.Fatal("finalizer must precede the claim")
	}
	staleOperation := op.DeepCopyObject().(*v1alpha1.OpenSearchServerlessOperation)
	staleInstance := instance.DeepCopyObject().(*v1alpha1.OpenSearchServerless)
	if err := c.Delete(ctx, op); err != nil {
		t.Fatal(err)
	}
	reconcile(fixture)
	if err := ClaimOperation(ctx, c, staleInstance, staleOperation); !apierrors.IsConflict(err) {
		t.Fatalf("expected deletion fence to reject an in-flight claim, got %v", err)
	}
	r := &OperationReconciler{Client: c, uncachedOperationReader: c, writerRetirement: fixture}
	if _, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: opKey}); err != nil {
		t.Fatal(err)
	}
	if err := c.Get(ctx, opKey, op); !apierrors.IsNotFound(err) {
		t.Fatalf("unsent operation did not finalize: %v", err)
	}
	op = &v1alpha1.OpenSearchServerlessOperation{ObjectMeta: metav1.ObjectMeta{Name: opKey.Name, Namespace: namespace}, Spec: staleOperation.Spec}
	if err := c.Create(ctx, op); err != nil {
		t.Fatal(err)
	}
	reconcile(fixture)
	reconcile(fixture)
	if instance.Status.ActiveOperation == nil || op.Status.WriterRetirement != nil {
		t.Fatal("claim must precede the journal")
	}
	fixture.failObservation = true
	reconcile(fixture)
	if instance.Status.ActiveOperation != nil || fixture.withdrawals != 0 {
		t.Fatal("failed qualification retained an unarmed claim")
	}
	fixture.failObservation = false
	reconcile(fixture)
	reconcile(fixture)
	if op.Status.WriterRetirement == nil || op.Status.WriterRetirement.NodeEphemeralID != "jvm" || fixture.withdrawals != 0 {
		t.Fatal("exact JVM must be journaled before the first withdrawal")
	}
	for _, check := range []struct {
		err    error
		reason string
	}{
		{errWriterPreflightInventory, "WriterHeadPreflightPending"},
		{errWriterEvictionUnsupportedHeads, "UnsupportedWriterHeads"},
	} {
		fixture.preflightError = check.err
		reconcile(fixture)
		journal := op.Status.WriterRetirement
		ready := meta.FindStatusCondition(op.Status.Conditions, "Ready")
		if journal == nil || journal.WithdrawalArmed || journal.WithdrawalDeadline != nil ||
			fixture.withdrawals != 0 || instance.Status.ActiveOperation == nil || ready == nil || ready.Reason != check.reason {
			t.Fatalf("preflight %s did not preserve the unsent reservation: %#v", check.reason, op.Status)
		}
	}
	fixture.preflightError = nil
	reconcile(fixture)
	firstDeadline := op.Status.WriterRetirement.WithdrawalDeadline
	if !op.Status.WriterRetirement.WithdrawalArmed || firstDeadline == nil || !now.Before(firstDeadline.Time) || fixture.withdrawals != 0 {
		t.Fatal("authorization must be persisted in its own update before withdrawal")
	}
	now = firstDeadline.Time.Add(time.Second)
	fixture.openAfterDeadline = true
	reconcile(fixture)
	if op.Status.WriterRetirement.WithdrawalArmed || op.Status.WriterRetirement.WithdrawalDeadline != nil || fixture.withdrawals != 0 {
		t.Fatal("a fresh read of the open JVM did not clear the expired authorization")
	}
	fixture.openAfterDeadline = false
	reconcile(fixture)
	if newDeadline := op.Status.WriterRetirement.WithdrawalDeadline; newDeadline == nil || !newDeadline.After(firstDeadline.Time) || fixture.withdrawals != 0 {
		t.Fatal("the exact writer must requalify and record a fresh authorization before dispatch")
	}
	if err := c.Delete(ctx, op); err != nil {
		t.Fatal(err)
	}
	reconcile(fixture)
	if op.Status.WriterRetirement == nil || !op.Status.WriterRetirement.WithdrawalArmed || instance.Status.ActiveOperation == nil {
		t.Fatal("deletion did not wait for the active withdrawal window")
	}
	now = op.Status.WriterRetirement.WithdrawalDeadline.Time.Add(time.Second)
	fixture.openAfterDeadline = true
	reconcile(fixture)
	if op.Status.WriterRetirement != nil || op.Status.Phase != "Cancelled" || fixture.withdrawals != 0 {
		t.Fatal("post-deadline exact open response did not resolve deletion")
	}
	reconcile(fixture)
	reconcile(fixture)
	r.Now = func() time.Time { return now }
	if _, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: opKey}); err != nil {
		t.Fatal(err)
	}
	if err := c.Get(ctx, opKey, op); !apierrors.IsNotFound(err) {
		t.Fatalf("cancelled operation did not finalize: %v", err)
	}
	fixture.openAfterDeadline = false
	op = &v1alpha1.OpenSearchServerlessOperation{ObjectMeta: metav1.ObjectMeta{Name: opKey.Name, Namespace: namespace}, Spec: staleOperation.Spec}
	if err := c.Create(ctx, op); err != nil {
		t.Fatal(err)
	}
	for range 4 {
		reconcile(fixture)
	}
	reconcile(nil)
	if !meta.IsStatusConditionFalse(op.Status.Conditions, "Ready") || instance.Status.ActiveOperation == nil || fixture.withdrawals != 0 {
		t.Fatal("a restarted manager with no adapter released the reservation")
	}
	reconcile(fixture)
	if fixture.withdrawals != 1 || op.Status.WriterRetirement.WithdrawnAt != nil {
		t.Fatal("a lost response must leave withdrawal unconfirmed")
	}
	reconcile(fixture)
	withdrawn := op.Status.WriterRetirement.WithdrawnAt
	if withdrawn == nil || withdrawn.IsZero() || withdrawn.Time.After(now) || fixture.withdrawals != 1 || fixture.preparations != 0 {
		t.Fatal("reconciliation failed to persist recovered proof before preparation")
	}
	reconcile(fixture)
	if op.Status.WriterRetirement.AdmissionClosed || fixture.preparations != 1 {
		t.Fatal("a lost preparation response must leave closure unconfirmed")
	}
	reconcile(fixture)
	if !op.Status.WriterRetirement.AdmissionClosed || fixture.preparations != 1 || instance.Status.ActiveOperation == nil || op.Status.Phase != "Running" {
		t.Fatal("closure must remain reserved pending successor verification")
	}
	fixture.failInventory = true
	reconcile(fixture)
	if op.Status.WriterRetirement.HeadInventory != nil || fixture.inventoryReads != 1 || op.Status.WriterRetirement.InventoryObserverOffset != 1 ||
		!meta.IsStatusConditionFalse(op.Status.Conditions, "HeadInventoryReady") {
		t.Fatal("an incomplete scan must not record head evidence")
	}
	fixture.failInventory = false
	reconcile(fixture)
	captured := op.Status.WriterRetirement.HeadInventory
	if captured == nil || captured.ObserverPodUID != "surviving-pod" || len(captured.Heads) != 1 || captured.Heads[0].Term != 1 ||
		op.Status.WriterRetirement.InventoryObserverOffset != 1 ||
		!meta.IsStatusConditionTrue(op.Status.Conditions, "HeadInventoryReady") {
		t.Fatal("the complete inventory did not survive the API-server round trip")
	}
	reconcile(fixture)
	if fixture.inventoryReads != 2 || instance.Status.ActiveOperation == nil {
		t.Fatal("restart changed the inventory or released the retirement claim")
	}
	fixture.enableEviction = true
	op.Status.WriterRetirement.HeadInventory.Heads[0].BucketID = "shared"
	if err := c.Status().Update(ctx, op); err != nil {
		t.Fatal(err)
	}
	reconcile(fixture)
	if op.Status.WriterRetirement.EvictionArmed || meta.FindStatusCondition(op.Status.Conditions, "Ready").Reason != "UnsupportedWriterHeads" {
		t.Fatal("automatic eviction must hold a bucket without a complete successor evidence path")
	}
	op.Status.WriterRetirement.HeadInventory.Heads[0].BucketID = ""
	if err := c.Status().Update(ctx, op); err != nil {
		t.Fatal(err)
	}
	reconcile(fixture)
	if !op.Status.WriterRetirement.EvictionArmed || op.Status.WriterRetirement.VictimGone || fixture.evictionAttempts != 0 {
		t.Fatal("the exact eviction intent must survive a round trip before dispatch")
	}
	fixture.failInventory = true
	reconcile(fixture)
	if op.Status.WriterRetirement.InventoryObserverOffset != 2 || fixture.evictionAttempts != 0 || !op.Status.WriterRetirement.EvictionArmed {
		t.Fatal("an unavailable eviction observer must advance the offset without dispatching eviction")
	}
	fixture.failInventory = false
	reconcile(fixture)
	if !op.Status.WriterRetirement.EvictionArmed || op.Status.WriterRetirement.VictimGone || fixture.evictionAttempts != 1 {
		t.Fatal("an ambiguous eviction response must keep the intent and await confirmation")
	}
	if err := c.Delete(ctx, op); err != nil {
		t.Fatal(err)
	}
	reconcile(fixture)
	if op.DeletionTimestamp == nil || op.Status.WriterRetirement.VictimGone || fixture.evictionAttempts != 1 ||
		fixture.evictionReadbacks != 1 || instance.Status.ActiveOperation == nil ||
		meta.FindStatusCondition(op.Status.Conditions, "Ready").Reason != "WriterEvictionReadbackPending" {
		t.Fatal("deletion while an eviction is ambiguous must only read back the exact Pod")
	}
	fixture.victimGone = true
	reconcile(fixture)
	if !op.Status.WriterRetirement.VictimGone || op.Status.WriterRetirement.InventoryObserverOffset != 0 || fixture.evictionAttempts != 1 || fixture.evictionReadbacks != 2 ||
		instance.Status.ActiveOperation == nil || op.Status.Phase != "Running" {
		t.Fatal("the exact Pod's absence must be journaled while deleting without retrying eviction")
	}
	reconcile(fixture)
	if fixture.evictionAttempts != 1 || fixture.evictionReadbacks != 2 || fixture.receiptReads != 1 || op.Status.WriterRetirement.ReleaseReceipt != nil ||
		!meta.IsStatusConditionFalse(op.Status.Conditions, "WriterReleaseReady") {
		t.Fatal("a missing receipt must keep the claim without repeating eviction")
	}
	reconcile(fixture)
	reconcile(fixture)
	receipt := op.Status.WriterRetirement.ReleaseReceipt
	if walOnly {
		if receipt != nil || op.Status.WriterRetirement.ReceiptMissingAt == nil ||
			!meta.IsStatusConditionFalse(op.Status.Conditions, "WriterReleaseReady") || instance.Status.ActiveOperation == nil {
			t.Fatal("the recovery journal must retain the missing receipt and claim")
		}
	} else if fixture.receiptReads != 2 || receipt == nil || len(receipt.Releases) != 1 ||
		receipt.Releases[0].Outcome != string(java.WriterHeadReleasedWithWAL) ||
		!meta.IsStatusConditionTrue(op.Status.Conditions, "WriterReleaseReady") || instance.Status.ActiveOperation == nil {
		t.Fatal("release evidence must survive a restart and retain the claim pending successor proof")
	}
	if !meta.IsStatusConditionFalse(op.Status.Conditions, "WriterSuccessorReady") {
		t.Fatal("complete release must still wait for a foreground write")
	}
	fixture.successorTerm = 1
	reconcile(fixture)
	if len(op.Status.WriterRetirement.Successors) != 0 || instance.Status.ActiveOperation == nil {
		t.Fatal("the same term must not qualify")
	}
	if op.DeletionTimestamp == nil || instance.Status.ActiveOperation == nil || !controllerutil.ContainsFinalizer(op, writerRetirementFinalizer) {
		t.Fatal("deletion must keep the claim until successor verification finishes")
	}
	fixture.successorTerm = 2
	if walOnly {
		reconcile(fixture)
		if len(op.Status.WriterRetirement.Successors) != 0 || instance.Status.ActiveOperation == nil {
			t.Fatal("a newer write with unsettled predecessor WAL cannot replace the receipt")
		}
		fixture.walSettled = true
	}
	reconcile(fixture)
	if op.Status.Phase != "Succeeded" || !meta.IsStatusConditionTrue(op.Status.Conditions, "Ready") ||
		!meta.IsStatusConditionTrue(op.Status.Conditions, "WriterSuccessorReady") ||
		len(op.Status.WriterRetirement.Successors) != 1 || instance.Status.ActiveOperation == nil {
		t.Fatal("success must be persisted before releasing the claim")
	}
	if walOnly && (meta.FindStatusCondition(op.Status.Conditions, "WriterReleaseReady").Reason != "WALRecoveryVerified" ||
		!op.Status.WriterRetirement.Successors[0].PredecessorWALSettled || op.Status.WriterRetirement.ReleaseReceipt != nil) {
		t.Fatal("the recovered outcome must preserve settled WAL evidence without inventing a receipt")
	}
	reconcile(nil)
	if op.Status.Phase != "Succeeded" || instance.Status.ActiveOperation != nil {
		t.Fatal("the completed journal must recover claim release even with the backend disabled")
	}
	if _, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: opKey}); err != nil {
		t.Fatal(err)
	}
	if err := c.Get(ctx, opKey, op); !apierrors.IsNotFound(err) || fixture.preparations != 1 {
		t.Fatal("completed retirement did not clear the finalizer or replayed preparation")
	}
}
