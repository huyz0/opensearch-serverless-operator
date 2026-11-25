package controller

import (
	"context"
	"fmt"
	"maps"
	"net/http"
	"slices"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"

	"github.com/huyz0/opensearch-serverless-operator/api/v1alpha1"
	"github.com/huyz0/opensearch-serverless-operator/internal/java"
)

const writerSuccessorChecksPerPass = 32

func supportsWriterSuccessorHeads(heads []v1alpha1.RetiringWriterHead) bool {
	return heads != nil && !slices.ContainsFunc(heads, func(head v1alpha1.RetiringWriterHead) bool {
		return head.BucketID != ""
	})
}

type writerSuccessorBackend interface {
	observeSuccessors(context.Context, *v1alpha1.OpenSearchServerless, *v1alpha1.OpenSearchServerlessOperation, func(context.Context) error) ([]v1alpha1.WriterSuccessorStatus, int, error)
}

func (r *OperationReconciler) reconcileWriterSuccessors(ctx context.Context, instance *v1alpha1.OpenSearchServerless, op *v1alpha1.OpenSearchServerlessOperation, recheck func(context.Context) error) (ctrl.Result, error) {
	journal := op.Status.WriterRetirement
	release, usable := inspectWriterRelease(journal)
	record := *journal
	backend, ok := r.writerRetirement.(writerSuccessorBackend)
	if ok && usable {
		evidence, offset, err := backend.observeSuccessors(ctx, instance, op, recheck)
		if err == nil {
			record.Successors = append(slices.Clone(journal.Successors), evidence...)
			slices.SortFunc(record.Successors, func(a, b v1alpha1.WriterSuccessorStatus) int {
				return int(a.InventoryIndex) - int(b.InventoryIndex)
			})
			record.SuccessorScanOffset = offset
			if release.validSuccessors(&record, false) {
				if len(record.Successors) == len(record.HeadInventory.Heads) {
					record.SuccessorScanOffset = 0
					record.ReceiptObserverOffset = 0
					conditions := []metav1.Condition{
						{Type: "WriterSuccessorReady", Status: metav1.ConditionTrue, Reason: "SuccessorsVerified", Message: "Every inventoried head has a verified successor write at a newer term"},
					}
					message := "The exact writer is gone and all inventoried heads have successor evidence"
					if len(record.HeadInventory.Heads) == 0 {
						message = "The exact writer is gone and the durable inventory has no heads to transfer"
						conditions[0].Reason, conditions[0].Message = "NoHeadsToTransfer", message
						conditions = append(conditions, metav1.Condition{Type: "WriterReleaseReady", Status: metav1.ConditionTrue, Reason: "NoHeadsToTransfer", Message: message})
					} else if release.uncovered > 0 {
						conditions = append(conditions, metav1.Condition{Type: "WriterReleaseReady", Status: metav1.ConditionTrue, Reason: "WALRecoveryVerified",
							Message: "Every head without a release receipt entry has a newer writer with settled predecessor WAL"})
					}
					return r.writerRetirementStatus(ctx, op, "Succeeded", "WriterRetired", message, &record, conditions...)
				}
			} else {
				record = *journal
			}
		}
	}
	message := "Waiting for serving index JVMs to accept foreground writes for the remaining heads"
	if usable && release.uncovered > 0 {
		message = "Waiting for newer-term writes and for settled WAL on heads without a release receipt entry"
	}
	return r.writerRetirementStatus(ctx, op, "Running", "SuccessorWritesPending", message, &record,
		metav1.Condition{Type: "WriterSuccessorReady", Status: metav1.ConditionFalse, Reason: "WritesPending", Message: message})
}

func validWriterSuccessors(record *v1alpha1.WriterRetirementStatus, complete bool) bool {
	release, usable := inspectWriterRelease(record)
	return usable && release.validSuccessors(record, complete)
}

func (release writerReleaseEvidence) validSuccessors(record *v1alpha1.WriterRetirementStatus, complete bool) bool {
	if record.SuccessorScanOffset < 0 ||
		len(record.Successors) > len(record.HeadInventory.Heads) || (complete && len(record.Successors) != len(record.HeadInventory.Heads)) {
		return false
	}
	previous := int32(-1)
	for _, proof := range record.Successors {
		if proof.InventoryIndex <= previous || int(proof.InventoryIndex) >= len(record.HeadInventory.Heads) ||
			proof.PodUID == "" || proof.NodeID == "" || proof.EphemeralID == "" || proof.NodeID == record.NodeID ||
			proof.Term <= record.HeadInventory.Heads[proof.InventoryIndex].Term || proof.ObservedAt.IsZero() ||
			proof.ObservedAt.Before(&release.observedAt) || (!release.covered[proof.InventoryIndex] && !proof.PredecessorWALSettled) {
			return false
		}
		previous = proof.InventoryIndex
	}
	return true
}

type writerSuccessorCandidate struct {
	client   *http.Client
	endpoint string
	podName  string
	process  java.WriterProcess
	until    time.Time
}

func (b developmentWriterRetirement) observeSuccessors(ctx context.Context, instance *v1alpha1.OpenSearchServerless, op *v1alpha1.OpenSearchServerlessOperation, recheck func(context.Context) error) ([]v1alpha1.WriterSuccessorStatus, int, error) {
	journal, target := op.Status.WriterRetirement, op.Spec.WriterRetirement
	release, usable := inspectWriterRelease(journal)
	if !usable || target == nil || !journal.VictimGone || !release.validSuccessors(journal, false) {
		return nil, 0, fmt.Errorf("a confirmed eviction and qualified release readback are required")
	}
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	bound, err := b.bind(ctx, instance, op)
	if err != nil || !matchesWriterRetirementBinding(bound, journal, instance) {
		return nil, 0, fmt.Errorf("recorded writer route or authorization changed")
	}
	r := b.runtime
	if err := r.checkRetiredWriterAbsent(ctx, instance, target); err != nil {
		return nil, 0, err
	}
	if err := r.checkPinnedPrimaryStore(ctx, instance); err != nil {
		return nil, 0, err
	}
	survivors, names, err := r.writerEvidenceSurvivors(ctx, instance, journal.ServingFleet, target.PodUID)
	if err != nil {
		return nil, 0, err
	}
	before, err := r.observePostEvictionIndexEndpoints(ctx, instance, journal, survivors)
	if err != nil {
		return nil, 0, err
	}
	ready := slices.Sorted(maps.Keys(before.ReadyPodVersions))
	if len(ready) == 0 {
		return nil, 0, fmt.Errorf("a serving index endpoint is required for successor verification")
	}
	victim := java.WriterProcess{NodeID: journal.NodeID, EphemeralID: journal.NodeEphemeralID}
	credential := bound.access.Reader
	candidates := make(map[types.UID]*writerSuccessorCandidate)
	pods := r.core.Pods(instance.Namespace)
	probe := func(ctx context.Context, uid types.UID) *writerSuccessorCandidate {
		if candidate, read := candidates[uid]; read {
			return candidate
		}
		candidates[uid] = nil
		pod, err := pods.Get(ctx, names[uid], metav1.GetOptions{})
		if err != nil || pod.UID != uid || pod.ResourceVersion != before.ReadyPodVersions[uid] ||
			pod.Annotations[keystoreUIDAnnotation] != instance.Status.BasicAuthBaseline.SecretUID || runningOpenSearch(pod) == nil {
			return nil
		}
		direct, endpoint, err := r.developmentPodHTTPTransport(ctx, instance, pod, 9200)
		if err != nil {
			return nil
		}
		process, until, err := probeWriterEvidenceObserver(ctx, direct, endpoint, pod.Name, victim, credential.username, credential.password)
		if err != nil {
			direct.CloseIdleConnections()
			return nil
		}
		candidate := &writerSuccessorCandidate{client: direct, endpoint: endpoint, podName: pod.Name, process: process, until: until}
		candidates[uid] = candidate
		return candidate
	}
	defer func() {
		for _, candidate := range candidates {
			if candidate != nil {
				candidate.client.CloseIdleConnections()
			}
		}
	}()
	scanCtx, stopScan := context.WithTimeout(ctx, 15*time.Second)
	defer stopScan()
	evidence, offset := scanWriterSuccessors(scanCtx, journal, release, ready, func(ctx context.Context, uid types.UID, head v1alpha1.RetiringWriterHead, requireSettled bool) (java.WriterProcess, int64) {
		candidate := probe(ctx, uid)
		if candidate == nil {
			return java.WriterProcess{}, 0
		}
		ctx, stop := context.WithDeadline(ctx, candidate.until)
		defer stop()
		term, err := java.ObserveWriterSuccessor(ctx, candidate.client, candidate.endpoint, candidate.process,
			java.WriterSuccessorShard{Index: head.Index, IndexUUID: head.IndexUUID, Shard: head.Shard, PreviousTerm: head.Term, RequireSettledWAL: requireSettled},
			credential.username, credential.password)
		if err != nil {
			return java.WriterProcess{}, 0
		}
		return candidate.process, term
	})
	checked := make(map[types.UID]bool)
	var validUntil time.Time
	for _, proof := range evidence {
		uid := types.UID(proof.PodUID)
		if checked[uid] {
			continue
		}
		candidate := candidates[uid]
		process, leaseUntil, err := probeWriterEvidenceObserver(ctx, candidate.client, candidate.endpoint, candidate.podName, victim, credential.username, credential.password)
		if err != nil || process != candidate.process || !time.Now().Before(candidate.until) {
			return nil, 0, fmt.Errorf("successor JVM or its qualifying lease changed")
		}
		deadline := minTime(candidate.until, leaseUntil)
		if validUntil.IsZero() || deadline.Before(validUntil) {
			validUntil = deadline
		}
		checked[uid] = true
	}
	if err := b.recheckPostEvictionObservation(ctx, instance, op, before, survivors, validUntil, recheck); err != nil {
		return nil, 0, err
	}
	return evidence, offset, nil
}

// Scan a bounded window through the head/endpoint pairs. A pending shard or
// unavailable peer cannot starve a later pair across reconciliations.
func scanWriterSuccessors(ctx context.Context, journal *v1alpha1.WriterRetirementStatus, release writerReleaseEvidence, ready []types.UID, fetch func(context.Context, types.UID, v1alpha1.RetiringWriterHead, bool) (java.WriterProcess, int64)) ([]v1alpha1.WriterSuccessorStatus, int) {
	heads := journal.HeadInventory.Heads
	total := len(heads) * len(ready)
	if total == 0 {
		return nil, 0
	}
	verified := make([]bool, len(heads))
	for _, proof := range journal.Successors {
		verified[proof.InventoryIndex] = true
	}
	outstanding := len(heads) - len(journal.Successors)
	offset := journal.SuccessorScanOffset % total
	var results []v1alpha1.WriterSuccessorStatus
	for visited, checked := 0, 0; visited < total && outstanding > 0 && checked < writerSuccessorChecksPerPass && ctx.Err() == nil; visited++ {
		index := offset / len(ready)
		uid := ready[offset%len(ready)]
		offset = (offset + 1) % total
		if verified[index] {
			continue
		}
		checked++
		requireSettled := !release.covered[index]
		process, term := fetch(ctx, uid, heads[index], requireSettled)
		if term <= heads[index].Term {
			continue
		}
		timestamp := metav1.NewMicroTime(time.Now().UTC().Truncate(time.Microsecond))
		results = append(results, v1alpha1.WriterSuccessorStatus{
			InventoryIndex: int32(index), PodUID: string(uid), NodeID: process.NodeID, EphemeralID: process.EphemeralID,
			Term: term, ObservedAt: timestamp, PredecessorWALSettled: requireSettled,
		})
		verified[index] = true
		outstanding--
	}
	return results, offset
}
