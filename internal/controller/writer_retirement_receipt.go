package controller

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"

	"github.com/huyz0/opensearch-serverless-operator/api/v1alpha1"
	"github.com/huyz0/opensearch-serverless-operator/internal/java"
	"github.com/huyz0/opensearch-serverless-operator/internal/workload"
)

type writerRetirementReceiptBackend interface {
	readReleaseReceipt(context.Context, *v1alpha1.OpenSearchServerless, *v1alpha1.OpenSearchServerlessOperation, func(context.Context) error) (*v1alpha1.WriterReleaseReceiptStatus, int, error)
}

func (r *OperationReconciler) reconcileWriterReleaseReceipt(ctx context.Context, instance *v1alpha1.OpenSearchServerless, op *v1alpha1.OpenSearchServerlessOperation, recheck func(context.Context) error) (ctrl.Result, error) {
	journal := op.Status.WriterRetirement
	if writerCanRecoverMissingReceipt(journal) && len(journal.Successors) > 0 {
		return r.reconcileWriterSuccessors(ctx, instance, op, recheck)
	}
	backend, ok := r.writerRetirement.(writerRetirementReceiptBackend)
	if !ok {
		return r.writerRetirementStatus(ctx, op, "Running", "WriterPodGone", "The exact writer Pod is gone and release receipt readback is unavailable", journal)
	}
	record := *journal
	if record.ReleaseReceipt == nil {
		receipt, offset, err := backend.readReleaseReceipt(ctx, instance, op, recheck)
		record.ReceiptObserverOffset = offset
		if err != nil || receipt == nil {
			reason, message := "ReceiptUnavailable", "A qualified survivor could not read the exact writer receipt"
			if errors.Is(err, java.ErrWriterHandoffReceiptMissing) {
				reason, message = "ReceiptMissing", "The store has no receipt for the exact retired JVM"
				if writerCanRecoverMissingReceipt(journal) {
					return r.reconcileWriterSuccessors(ctx, instance, op, recheck)
				}
				if record.ReceiptMissingAt == nil {
					missingAt := metav1.NewMicroTime(r.writerRetirementNow().UTC().Truncate(time.Microsecond))
					record.ReceiptMissingAt = &missingAt
				}
			}
			return r.writerRetirementStatus(ctx, op, "Running", "WriterPodGone", "The exact writer Pod is gone and release evidence is pending", &record,
				metav1.Condition{Type: "WriterReleaseReady", Status: metav1.ConditionFalse, Reason: reason, Message: message})
		}
		record.ReleaseReceipt = receipt
		record.ReceiptMissingAt = nil
	}
	release, usable := inspectWriterRelease(&record)
	if !usable {
		return r.writerRetirementStatus(ctx, op, "Running", "WriterPodGone", "The exact writer Pod is gone and release evidence cannot be used", &record,
			metav1.Condition{Type: "WriterReleaseReady", Status: metav1.ConditionFalse, Reason: "ReleaseEvidenceInvalid", Message: "The release evidence does not match the independent head inventory"})
	}
	if journal.ReleaseReceipt != nil {
		return r.reconcileWriterSuccessors(ctx, instance, op, recheck)
	}
	if release.uncovered > 0 {
		return r.writerRetirementStatus(ctx, op, "Running", "WriterPodGone", "The exact writer Pod is gone and some heads need separate recovery evidence", &record,
			metav1.Condition{Type: "WriterReleaseReady", Status: metav1.ConditionFalse, Reason: "HeadsUnaccounted", Message: "The receipt does not account for release of every physical head in the journal"})
	}
	return r.writerRetirementStatus(ctx, op, "Running", "WriterPodGone", "Writer release is recorded and successor verification is pending", &record,
		metav1.Condition{Type: "WriterReleaseReady", Status: metav1.ConditionTrue, Reason: "ReceiptRecorded", Message: "The receipt accounts for release of every physical head in the journal"})
}

func writerCanRecoverMissingReceipt(record *v1alpha1.WriterRetirementStatus) bool {
	return record != nil && record.ReleaseReceipt == nil && record.ReceiptMissingAt != nil && !record.ReceiptMissingAt.IsZero() &&
		record.HeadInventory != nil && supportsWriterSuccessorHeads(record.HeadInventory.Heads)
}

type writerReleaseEvidence struct {
	observedAt metav1.MicroTime
	covered    []bool
	uncovered  int
}

func inspectWriterRelease(record *v1alpha1.WriterRetirementStatus) (writerReleaseEvidence, bool) {
	if record == nil || record.HeadInventory == nil || !supportsWriterSuccessorHeads(record.HeadInventory.Heads) {
		return writerReleaseEvidence{}, false
	}
	count := len(record.HeadInventory.Heads)
	evidence := writerReleaseEvidence{covered: make([]bool, count), uncovered: count}
	if record.ReleaseReceipt == nil {
		if !writerCanRecoverMissingReceipt(record) {
			return writerReleaseEvidence{}, false
		}
		evidence.observedAt = *record.ReceiptMissingAt
		return evidence, true
	}
	receipt := record.ReleaseReceipt
	if record.ReceiptMissingAt != nil || receipt.Releases == nil || receipt.ObservedAt.IsZero() {
		return writerReleaseEvidence{}, false
	}
	evidence.observedAt = receipt.ObservedAt
	previous := int32(-1)
	for _, release := range receipt.Releases {
		if release.InventoryIndex <= previous || int(release.InventoryIndex) >= count ||
			(release.Outcome != string(java.WriterHeadReleased) && release.Outcome != string(java.WriterHeadReleasedWithWAL)) {
			return writerReleaseEvidence{}, false
		}
		evidence.covered[release.InventoryIndex] = true
		evidence.uncovered--
		previous = release.InventoryIndex
	}
	return evidence, true
}

func (b developmentWriterRetirement) readReleaseReceipt(ctx context.Context, instance *v1alpha1.OpenSearchServerless, op *v1alpha1.OpenSearchServerlessOperation, recheck func(context.Context) error) (*v1alpha1.WriterReleaseReceiptStatus, int, error) {
	journal, target := op.Status.WriterRetirement, op.Spec.WriterRetirement
	if journal == nil || target == nil || !journal.VictimGone || !journal.EvictionArmed || !journal.AdmissionClosed ||
		journal.HeadInventory == nil || journal.HeadInventory.Heads == nil || instance.Status.PrimaryStore == nil {
		return nil, 0, fmt.Errorf("recorded eviction, complete inventory and pinned primary store are required")
	}
	offset := journal.ReceiptObserverOffset
	ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	bound, err := b.bind(ctx, instance, op)
	if err != nil || !matchesWriterRetirementBinding(bound, journal, instance) {
		return nil, offset, fmt.Errorf("recorded writer route or authorization changed")
	}
	r := b.runtime
	if err := r.checkRetiredWriterAbsent(ctx, instance, target); err != nil {
		return nil, offset, err
	}
	if err := r.checkPinnedPrimaryStore(ctx, instance); err != nil {
		return nil, offset, err
	}
	survivors, names, err := r.writerEvidenceSurvivors(ctx, instance, journal.ServingFleet, target.PodUID)
	if err != nil {
		return nil, offset, err
	}
	before, err := r.observePostEvictionIndexEndpoints(ctx, instance, journal, survivors)
	if err != nil {
		return nil, offset, err
	}
	ready := slices.Sorted(maps.Keys(before.ReadyPodVersions))
	if i := slices.Index(ready, types.UID(journal.HeadInventory.ObserverPodUID)); i > 0 {
		// Initially prefer the observer that inventoried the writer. Keep the
		// remaining order deterministic so the persisted offset can resume it.
		preferred := ready[i]
		copy(ready[1:i+1], ready[:i])
		ready[0] = preferred
	}
	observation, next, err := scanWriterReceiptObservers(ctx, ready, offset, func(ctx context.Context, uid types.UID) (writerReceiptObservation, error) {
		return r.readReceiptFromObserver(ctx, instance, journal, bound.access.Reader, before.ReadyPodVersions[uid], uid, names[uid])
	})
	if err != nil && !errors.Is(err, java.ErrWriterHandoffReceiptMissing) {
		return nil, next, err
	}
	if checkErr := b.recheckPostEvictionObservation(ctx, instance, op, before, survivors, observation.validUntil, recheck); checkErr != nil {
		return nil, next, checkErr
	}
	return observation.receipt, 0, err
}

type writerReceiptObservation struct {
	receipt    *v1alpha1.WriterReleaseReceiptStatus
	validUntil time.Time
}

func scanWriterReceiptObservers(ctx context.Context, ready []types.UID, offset int, read func(context.Context, types.UID) (writerReceiptObservation, error)) (writerReceiptObservation, int, error) {
	if len(ready) == 0 || offset < 0 {
		return writerReceiptObservation{}, offset, fmt.Errorf("a serving index endpoint and valid receipt offset are required")
	}
	offset %= len(ready)
	var lastErr error
	for checked := 0; checked < min(4, len(ready)) && ctx.Err() == nil; checked++ {
		uid := ready[offset]
		offset = (offset + 1) % len(ready)
		attempt, cancel := context.WithTimeout(ctx, 3*time.Second)
		observation, err := read(attempt, uid)
		if attempt.Err() != nil {
			err = attempt.Err()
		}
		cancel()
		if (err == nil && observation.receipt != nil) || errors.Is(err, java.ErrWriterHandoffReceiptMissing) {
			return observation, offset, err
		}
		lastErr = err
	}
	if err := ctx.Err(); err != nil {
		return writerReceiptObservation{}, offset, err
	}
	if lastErr != nil {
		return writerReceiptObservation{}, offset, fmt.Errorf("no qualified observer could read the writer receipt: %w", lastErr)
	}
	return writerReceiptObservation{}, offset, fmt.Errorf("no qualified observer returned a writer receipt")
}

func (r writerRuntime) readReceiptFromObserver(ctx context.Context, instance *v1alpha1.OpenSearchServerless, journal *v1alpha1.WriterRetirementStatus, credential podHealthCredential, version string, uid types.UID, name string) (writerReceiptObservation, error) {
	var observed writerReceiptObservation
	pod, err := r.core.Pods(instance.Namespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil || pod.UID != uid || pod.ResourceVersion != version ||
		pod.Annotations[keystoreUIDAnnotation] != instance.Status.BasicAuthBaseline.SecretUID || runningOpenSearch(pod) == nil {
		return observed, fmt.Errorf("receipt observer Pod or credential pin changed")
	}
	direct, endpoint, err := r.developmentPodHTTPTransport(ctx, instance, pod, 9200)
	if err != nil {
		return observed, err
	}
	defer direct.CloseIdleConnections()
	victim := java.WriterProcess{NodeID: journal.NodeID, EphemeralID: journal.NodeEphemeralID}
	probe := func(ctx context.Context) (java.WriterProcess, time.Time, error) {
		return probeWriterEvidenceObserver(ctx, direct, endpoint, pod.Name, victim, credential.username, credential.password)
	}
	observer, leaseUntil, err := probe(ctx)
	if err != nil {
		return observed, err
	}
	ctx, stop := context.WithDeadline(ctx, leaseUntil)
	defer stop()
	receipt, receiptErr := java.GetWriterHandoffReceipt(ctx, direct, endpoint, victim, credential.username, credential.password)
	if receiptErr != nil && !errors.Is(receiptErr, java.ErrWriterHandoffReceiptMissing) {
		return observed, receiptErr
	}
	observedAgain, currentLeaseUntil, err := probe(ctx)
	if err != nil || observedAgain != observer {
		return observed, fmt.Errorf("receipt observer JVM changed")
	}
	observed.validUntil = minTime(leaseUntil, currentLeaseUntil)
	if receiptErr != nil {
		return observed, receiptErr
	}
	observed.receipt = &v1alpha1.WriterReleaseReceiptStatus{
		ObservedAt:     metav1.NewMicroTime(time.Now().UTC().Truncate(time.Microsecond)),
		ObserverPodUID: string(uid), ObserverNodeID: observer.NodeID, ObserverEphemeralID: observer.EphemeralID,
		Releases: matchWriterReceipt(journal.HeadInventory.Heads, receipt),
	}
	return observed, nil
}

// recheckPostEvictionObservation applies the same final guard to receipts,
// confirmed missing receipts, and newly sampled successor writes.
func (b developmentWriterRetirement) recheckPostEvictionObservation(ctx context.Context, instance *v1alpha1.OpenSearchServerless, op *v1alpha1.OpenSearchServerlessOperation, before roleEndpointObservation, survivors map[types.UID]struct{}, validUntil time.Time, recheck func(context.Context) error) error {
	if !validUntil.IsZero() {
		var cancel context.CancelFunc
		ctx, cancel = context.WithDeadline(ctx, validUntil)
		defer cancel()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	r := b.runtime
	if err := r.checkPinnedPrimaryStore(ctx, instance); err != nil {
		return err
	}
	if err := r.checkRetiredWriterAbsent(ctx, instance, op.Spec.WriterRetirement); err != nil {
		return err
	}
	journal := op.Status.WriterRetirement
	bound, err := b.bind(ctx, instance, op)
	if err != nil || !matchesWriterRetirementBinding(bound, journal, instance) {
		return fmt.Errorf("recorded writer route or authorization changed during release verification")
	}
	after, err := r.observePostEvictionIndexEndpoints(ctx, instance, journal, survivors)
	if err != nil || before.ServiceUID != after.ServiceUID || before.ServiceVersion != after.ServiceVersion ||
		!maps.Equal(before.SliceVersions, after.SliceVersions) || !maps.Equal(before.ReadyPodVersions, after.ReadyPodVersions) {
		return fmt.Errorf("index routing changed during release verification")
	}
	if err := recheck(ctx); err != nil {
		return err
	}
	return ctx.Err()
}

func (r writerRuntime) checkRetiredWriterAbsent(ctx context.Context, instance *v1alpha1.OpenSearchServerless, target *v1alpha1.WriterRetirementTarget) error {
	pod, err := r.readExactRetiringWriterPod(ctx, instance, target)
	if err != nil {
		return err
	}
	if pod != nil {
		return fmt.Errorf("the exact writer Pod's absence is not confirmed")
	}
	return nil
}

func (r writerRuntime) observePostEvictionIndexEndpoints(ctx context.Context, instance *v1alpha1.OpenSearchServerless, journal *v1alpha1.WriterRetirementStatus, survivors map[types.UID]struct{}) (roleEndpointObservation, error) {
	service, err := workload.RoleService(instance, "ingest", journal.ServingFleet, 9200)
	if err != nil {
		return roleEndpointObservation{}, err
	}
	minima := servingZoneMinima(instance, "ingest")
	observed, err := observeLiveServiceEndpoints(ctx, r.core, r.discovery, instance, "ingest", journal.ServingFleet,
		service, types.UID(journal.ServiceUID), minima, survivors, nil)
	if err != nil {
		return roleEndpointObservation{}, err
	}
	if _, below := servingFloorShortfalls(instance, "ingest", minima, observed.ReadyByZone); below {
		return roleEndpointObservation{}, fmt.Errorf("surviving index endpoints are below the regional floor")
	}
	return observed, nil
}

func matchWriterReceipt(heads []v1alpha1.RetiringWriterHead, receipt java.WriterHandoffReceipt) []v1alpha1.WriterHeadRelease {
	type identity struct {
		name, uuid string
		shard      int32
		term       int64
	}
	byShard := make(map[identity]java.WriterShardHandoff, len(receipt.Shards))
	for _, shard := range receipt.Shards {
		byShard[identity{shard.Index, shard.IndexUUID, shard.Shard, shard.Term}] = shard
	}
	releases := make([]v1alpha1.WriterHeadRelease, 0, len(heads))
	for i, head := range heads {
		shard := byShard[identity{head.Index, head.IndexUUID, head.Shard, head.Term}]
		if head.BucketID != "" || (shard.Outcome != java.WriterHeadReleased && shard.Outcome != java.WriterHeadReleasedWithWAL) {
			continue
		}
		releases = append(releases, v1alpha1.WriterHeadRelease{
			InventoryIndex: int32(i), Outcome: string(shard.Outcome), LocalCleanupComplete: shard.LocalCleanupComplete,
		})
	}
	return releases
}
