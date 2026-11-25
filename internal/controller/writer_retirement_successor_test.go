package controller

import (
	"context"
	"errors"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	"github.com/huyz0/opensearch-serverless-operator/api/v1alpha1"
	"github.com/huyz0/opensearch-serverless-operator/internal/java"
)

func TestPostEvictionObservationRejectsExpiredLeaseBeforeRecheck(t *testing.T) {
	err := (developmentWriterRetirement{}).recheckPostEvictionObservation(context.Background(), nil, nil,
		roleEndpointObservation{}, nil, time.Unix(1, 0), func(context.Context) error {
			t.Fatal("expired evidence reached the operation check")
			return nil
		})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected an expired observation deadline, got %v", err)
	}
}

func TestWriterSuccessorScanResumesPastQuietHeads(t *testing.T) {
	heads := make([]v1alpha1.RetiringWriterHead, writerSuccessorChecksPerPass+1)
	for i := range heads {
		heads[i] = v1alpha1.RetiringWriterHead{Shard: int32(i), Term: 1}
	}
	journal := &v1alpha1.WriterRetirementStatus{
		HeadInventory: &v1alpha1.WriterHeadInventoryStatus{Heads: heads},
		ReleaseReceipt: &v1alpha1.WriterReleaseReceiptStatus{
			ObservedAt: metav1.NewMicroTime(time.Now().Add(-time.Minute)), Releases: []v1alpha1.WriterHeadRelease{},
		},
		NodeID: "retired",
	}
	release, usable := inspectWriterRelease(journal)
	if !usable {
		t.Fatal("the receipt should allow lookup of omitted heads")
	}
	ready := []types.UID{"serving"}
	queries := 0
	fetch := func(_ context.Context, uid types.UID, head v1alpha1.RetiringWriterHead, requireSettled bool) (java.WriterProcess, int64) {
		if !requireSettled {
			t.Fatal("an omitted head must request predecessor WAL settlement")
		}
		queries++
		if uid == "serving" && head.Shard == int32(len(heads)-1) {
			return java.WriterProcess{NodeID: "successor", EphemeralID: "new-process"}, 2
		}
		return java.WriterProcess{}, 0
	}
	first, offset := scanWriterSuccessors(context.Background(), journal, release, ready, fetch)
	if len(first) != 0 || queries != writerSuccessorChecksPerPass || offset != writerSuccessorChecksPerPass {
		t.Fatal("initial verification did not stop at its bounded work window")
	}
	journal.SuccessorScanOffset = offset
	second, _ := scanWriterSuccessors(context.Background(), journal, release, ready, fetch)
	journal.Successors = second
	if len(second) != 1 || second[0].InventoryIndex != int32(len(heads)-1) ||
		!validWriterSuccessors(journal, false) || validWriterSuccessors(journal, true) {
		t.Fatal("the resumed window did not record partial proof from the later shard")
	}
	journal.Successors = append(journal.Successors, second[0])
	if validWriterSuccessors(journal, false) {
		t.Fatal("duplicate proofs must not count toward completion")
	}
}

func TestWriterSuccessorScanUsesReceiptCoveragePerShard(t *testing.T) {
	journal := &v1alpha1.WriterRetirementStatus{
		HeadInventory: &v1alpha1.WriterHeadInventoryStatus{Heads: []v1alpha1.RetiringWriterHead{{Shard: 0, Term: 1}, {Shard: 1, Term: 1}}},
		ReleaseReceipt: &v1alpha1.WriterReleaseReceiptStatus{
			ObservedAt: metav1.NewMicroTime(time.Now().Add(-time.Minute)),
			Releases:   []v1alpha1.WriterHeadRelease{{InventoryIndex: 1, Outcome: string(java.WriterHeadReleased)}},
		},
	}
	release, usable := inspectWriterRelease(journal)
	if !usable || release.uncovered != 1 {
		t.Fatal("a single positive receipt row must account for exactly one head")
	}
	evidence, _ := scanWriterSuccessors(context.Background(), journal, release, []types.UID{"serving"},
		func(_ context.Context, _ types.UID, head v1alpha1.RetiringWriterHead, requireSettled bool) (java.WriterProcess, int64) {
			if requireSettled != (head.Shard == 0) {
				t.Fatalf("wrong WAL settlement requirement for shard %d", head.Shard)
			}
			return java.WriterProcess{NodeID: "new-node", EphemeralID: "new-process"}, 2
		})
	journal.Successors = evidence
	if len(evidence) != 2 || !evidence[0].PredecessorWALSettled || evidence[1].PredecessorWALSettled || !validWriterSuccessors(journal, true) {
		t.Fatal("the journal must combine receipt coverage with WAL settlement evidence")
	}
	journal.ReleaseReceipt.Releases = append(journal.ReleaseReceipt.Releases, journal.ReleaseReceipt.Releases[0])
	if validWriterSuccessors(journal, true) {
		t.Fatal("malformed release evidence must not allow claim release")
	}
}
