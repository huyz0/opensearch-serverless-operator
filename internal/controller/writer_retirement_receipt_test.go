package controller

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/types"

	"github.com/huyz0/opensearch-serverless-operator/api/v1alpha1"
	"github.com/huyz0/opensearch-serverless-operator/internal/java"
)

func TestReceiptObserverScanResumesAfterUnavailablePods(t *testing.T) {
	ready := []types.UID{"preferred", "b", "c", "d", "healthy"}
	var tried []types.UID
	read := func(ctx context.Context, uid types.UID) (writerReceiptObservation, error) {
		deadline, bounded := ctx.Deadline()
		if !bounded || time.Until(deadline) > 3*time.Second {
			t.Fatal("observer attempt must have its own timeout")
		}
		tried = append(tried, uid)
		if uid != "healthy" {
			return writerReceiptObservation{}, context.DeadlineExceeded
		}
		return writerReceiptObservation{receipt: &v1alpha1.WriterReleaseReceiptStatus{ObserverPodUID: string(uid)}}, nil
	}
	first, next, err := scanWriterReceiptObservers(context.Background(), ready, 0, read)
	if first.receipt != nil || next != 4 || len(tried) != 4 || !errors.Is(err, context.DeadlineExceeded) || errors.Is(err, java.ErrWriterHandoffReceiptMissing) {
		t.Fatalf("unavailable batch = %#v, next = %d, attempts = %v, error = %v", first, next, tried, err)
	}
	second, next, err := scanWriterReceiptObservers(context.Background(), ready, next, read)
	if err != nil || next != 0 || second.receipt == nil || second.receipt.ObserverPodUID != "healthy" || len(tried) != 5 {
		t.Fatalf("resumed scan did not reach healthy Pod: %#v, next = %d, attempts = %v, error = %v", second, next, tried, err)
	}
}

func TestReceiptObserverScanReturnsConfirmedAbsence(t *testing.T) {
	ready := []types.UID{"unavailable", "observer", "later"}
	checked := 0
	until := time.Now().Add(time.Second)
	observation, next, err := scanWriterReceiptObservers(context.Background(), ready, 0, func(_ context.Context, uid types.UID) (writerReceiptObservation, error) {
		checked++
		if uid == "unavailable" {
			return writerReceiptObservation{}, context.DeadlineExceeded
		}
		return writerReceiptObservation{validUntil: until}, java.ErrWriterHandoffReceiptMissing
	})
	if !errors.Is(err, java.ErrWriterHandoffReceiptMissing) || observation.receipt != nil || observation.validUntil != until || next != 2 || checked != 2 {
		t.Fatalf("qualified missing receipt = %#v, next = %d, attempts = %d, error = %v", observation, next, checked, err)
	}
}

func TestMatchWriterReceiptRequiresExactPhysicalShard(t *testing.T) {
	heads := []v1alpha1.RetiringWriterHead{
		{Key: "first", Index: "orders", IndexUUID: "a", Shard: 0, Term: 1},
		{Key: "second", Index: "orders", IndexUUID: "a", Shard: 1, Term: 2},
		{Key: "third", Index: "orders", IndexUUID: "a", Shard: 2, Term: 3},
		{Key: "fourth", Index: "orders", IndexUUID: "a", Shard: 3, Term: 4},
		{Key: "bucket", Index: "#bucket#shared", BucketID: "shared", Shard: 0, Term: 4},
	}
	receipt := java.WriterHandoffReceipt{Shards: []java.WriterShardHandoff{
		{Index: "orders", IndexUUID: "a", Shard: 0, Term: 1, Outcome: java.WriterHeadReleasedWithWAL, LocalCleanupComplete: true},
		{Index: "orders", IndexUUID: "a", Shard: 1, Term: 3, Outcome: java.WriterHeadReleased},
		{Index: "orders", IndexUUID: "recreated", Shard: 2, Term: 3, Outcome: java.WriterHeadReleased},
		{Index: "orders", IndexUUID: "a", Shard: 3, Term: 4, Outcome: java.WriterReleaseUnconfirmed},
		{Index: "member", IndexUUID: "m", Shard: 0, Term: 4, Outcome: java.WriterMemberReleased},
	}}
	want := []v1alpha1.WriterHeadRelease{{InventoryIndex: 0, Outcome: "RELEASED_WITH_WAL", LocalCleanupComplete: true}}
	if got := matchWriterReceipt(heads, receipt); !reflect.DeepEqual(got, want) {
		t.Fatalf("release evidence = %#v, want %#v", got, want)
	}
}
