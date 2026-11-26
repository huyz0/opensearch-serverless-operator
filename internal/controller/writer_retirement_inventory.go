package controller

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"net/http"
	"slices"
	"strings"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"

	"github.com/huyz0/opensearch-serverless-operator/api/v1alpha1"
	"github.com/huyz0/opensearch-serverless-operator/internal/java"
	"github.com/huyz0/opensearch-serverless-operator/internal/rollout"
	"github.com/huyz0/opensearch-serverless-operator/internal/workload"
	"github.com/huyz0/opensearch-serverless-operator/pkg/podlifecycle"
)

const writerHeadScanTimeout = 30 * time.Second

var errWriterPreflightInventory = errors.New("open writer head preflight is incomplete")

type writerInventoryObserverError struct {
	cause error
	next  int
}

func (e *writerInventoryObserverError) Error() string { return e.cause.Error() }
func (e *writerInventoryObserverError) Unwrap() error { return e.cause }

func rememberInventoryObserverFailure(record *v1alpha1.WriterRetirementStatus, err error) {
	var failure *writerInventoryObserverError
	if errors.As(err, &failure) {
		record.InventoryObserverOffset = failure.next
	}
}

func writerInventoryObserver(ready []types.UID, offset int) (types.UID, int, error) {
	if len(ready) == 0 || offset < 0 {
		return "", offset, fmt.Errorf("a serving index endpoint and valid inventory offset are required")
	}
	index := offset % len(ready)
	return ready[index], (index + 1) % len(ready), nil
}

// An open writer can acquire heads during or after the scan. Use this scan
// only to detect known unsupported ownership before arming withdrawal.
func checkOpenWriterHeads(ctx context.Context, direct *http.Client, endpoint string, process java.WriterProcess, username string, password []byte) error {
	ctx, cancel := context.WithTimeout(ctx, writerHeadScanTimeout)
	defer cancel()
	scan := *direct
	scan.Timeout = 0
	heads, err := java.GetBoundedWriterHeadInventory(ctx, &scan, endpoint, process, username, password, v1alpha1.MaxRetirementInventoryHeads)
	if err != nil {
		return fmt.Errorf("%w: %w", errWriterPreflightInventory, err)
	}
	entries, err := retirementInventoryEntries(heads, process)
	if err != nil {
		return fmt.Errorf("%w: %w", errWriterPreflightInventory, err)
	}
	if !supportsWriterSuccessorHeads(entries) {
		return errWriterEvictionUnsupportedHeads
	}
	return nil
}

func (b developmentWriterRetirement) inventory(ctx context.Context, instance *v1alpha1.OpenSearchServerless, op *v1alpha1.OpenSearchServerlessOperation, recheck func(context.Context) error) (*v1alpha1.WriterHeadInventoryStatus, error) {
	journal := op.Status.WriterRetirement
	if journal == nil || !journal.AdmissionClosed || op.Spec.WriterRetirement == nil || instance.Status.PrimaryStore == nil ||
		b.runtime.core == nil || instance.Status.BasicAuthBaseline == nil {
		return nil, fmt.Errorf("closed writer admission and pinned primary store are required")
	}
	ctx, cancel := context.WithTimeout(ctx, writerHeadScanTimeout)
	defer cancel()
	r := b.runtime
	if err := r.checkPinnedPrimaryStore(ctx, instance); err != nil {
		return nil, err
	}
	closed := func(ctx context.Context) error {
		state, err := b.readState(ctx, instance, op)
		if err != nil || state != java.WriterAdmissionClosed {
			return fmt.Errorf("the exact retiring Java process must still have closed admission")
		}
		return nil
	}
	if err := closed(ctx); err != nil {
		return nil, err
	}
	credential, err := rollout.ReadRouteCredential(ctx, r.core, instance, b.readerSecret)
	if err != nil || credential == nil {
		return nil, fmt.Errorf("the scoped Java inventory reader credential is required")
	}
	_, bootstrap, err := r.resolveBasicAuth(ctx, instance)
	if err != nil || bytes.Equal(credential.Password, bootstrap) {
		return nil, fmt.Errorf("the inventory reader must differ from the bootstrap credential")
	}
	target := op.Spec.WriterRetirement
	victim := podlifecycle.Identity{Namespace: instance.Namespace, OwnerUID: string(instance.UID), Name: target.PodName, UID: target.PodUID}
	process := java.WriterProcess{NodeID: journal.NodeID, EphemeralID: journal.NodeEphemeralID}
	survivors, survivorNames, err := r.writerEvidenceSurvivors(ctx, instance, journal.ServingFleet, victim.UID)
	if err != nil {
		return nil, err
	}
	endpoints := func(ctx context.Context) (roleEndpointObservation, error) {
		return r.observeWriterTrafficWithdrawal(ctx, instance, victim, types.UID(journal.ServiceUID), servingZoneMinima(instance, "ingest"), survivors)
	}
	before, err := endpoints(ctx)
	if err != nil {
		return nil, err
	}
	ready := slices.Sorted(maps.Keys(before.ReadyPodVersions))
	uid, nextObserver, err := writerInventoryObserver(ready, journal.InventoryObserverOffset)
	if err != nil {
		return nil, err
	}
	observerFailed := func(err error) (*v1alpha1.WriterHeadInventoryStatus, error) {
		return nil, &writerInventoryObserverError{cause: err, next: nextObserver}
	}
	pod, err := r.core.Pods(instance.Namespace).Get(ctx, survivorNames[uid], metav1.GetOptions{})
	if err != nil || pod.UID != uid || pod.ResourceVersion != before.ReadyPodVersions[uid] ||
		pod.Annotations[emergencyCapacityAnnotation] == "true" || pod.Annotations[keystoreUIDAnnotation] != instance.Status.BasicAuthBaseline.SecretUID ||
		workload.PodDisruptionTarget(pod) {
		return observerFailed(fmt.Errorf("serving index inventory observer changed or is not regular capacity"))
	}
	direct, endpoint, err := r.developmentPodHTTPTransport(ctx, instance, pod, 9200)
	if err != nil {
		return observerFailed(err)
	}
	defer direct.CloseIdleConnections()
	probe := func(ctx context.Context) (java.WriterProcess, time.Time, error) {
		return probeWriterEvidenceObserver(ctx, direct, endpoint, pod.Name, process, credential.Username, credential.Password)
	}
	observer, leaseUntil, err := probe(ctx)
	if err != nil {
		return observerFailed(err)
	}
	ctx, cancelScan := context.WithDeadline(ctx, leaseUntil)
	defer cancelScan()
	// A store page can take longer than a direct stats probe. Bound the
	// whole scan by the observer's lease and the enclosing attempt window.
	scan := *direct
	scan.Timeout = 0
	heads, err := java.GetBoundedWriterHeadInventory(ctx, &scan, endpoint, process, credential.Username, credential.Password, v1alpha1.MaxRetirementInventoryHeads)
	if err != nil {
		if errors.Is(err, java.ErrWriterHeadInventoryLimit) {
			return nil, err
		}
		return observerFailed(err)
	}
	entries, err := retirementInventoryEntries(heads, process)
	if err != nil {
		return nil, err
	}
	observedAgain, currentLeaseUntil, err := probe(ctx)
	if err != nil || observedAgain != observer {
		return observerFailed(fmt.Errorf("inventory observer JVM changed during the durable scan"))
	}
	ctx, endVerification := context.WithDeadline(ctx, currentLeaseUntil)
	defer endVerification()
	verify := func() error {
		if err := closed(ctx); err != nil {
			return err
		}
		if err := r.checkPinnedPrimaryStore(ctx, instance); err != nil {
			return err
		}
		if !credential.Unchanged(ctx, r.core, instance.Namespace) {
			return fmt.Errorf("inventory reader credential changed during the durable scan")
		}
		after, err := endpoints(ctx)
		if err != nil || before.ServiceVersion != after.ServiceVersion ||
			!maps.Equal(before.SliceVersions, after.SliceVersions) || !maps.Equal(before.ReadyPodVersions, after.ReadyPodVersions) {
			return fmt.Errorf("index endpoints changed during the durable scan")
		}
		return recheck(ctx)
	}
	verificationErr := verify()
	if err := ctx.Err(); err != nil {
		return observerFailed(err)
	}
	if verificationErr != nil {
		return nil, verificationErr
	}
	return &v1alpha1.WriterHeadInventoryStatus{
		ObservedAt:     metav1.NewMicroTime(time.Now().UTC().Truncate(time.Microsecond)),
		ObserverPodUID: string(uid), ObserverNodeID: observer.NodeID, ObserverEphemeralID: observer.EphemeralID,
		Heads: entries,
	}, nil
}

func (r writerRuntime) writerEvidenceSurvivors(ctx context.Context, instance *v1alpha1.OpenSearchServerless, fleet, victimUID string) (map[types.UID]struct{}, map[types.UID]string, error) {
	selector := labels.Set{workload.InstanceLabel: string(instance.UID), workload.RoleLabel: "ingest", workload.FleetLabel: fleet}.String()
	candidates, err := r.core.Pods(instance.Namespace).List(ctx, metav1.ListOptions{LabelSelector: selector, Limit: 513})
	if err != nil || candidates.Continue != "" || len(candidates.Items) > 512 {
		return nil, nil, fmt.Errorf("complete serving index Pod inventory is required")
	}
	survivors := make(map[types.UID]struct{})
	names := make(map[types.UID]string)
	for _, pod := range candidates.Items {
		if string(pod.UID) != victimUID && !pod.Spec.HostNetwork && pod.Annotations[emergencyCapacityAnnotation] != "true" && runningOpenSearch(&pod) != nil {
			survivors[pod.UID] = struct{}{}
			names[pod.UID] = pod.Name
		}
	}
	return survivors, names, nil
}

func probeWriterEvidenceObserver(ctx context.Context, direct *http.Client, endpoint, podName string, victim java.WriterProcess, username string, password []byte) (java.WriterProcess, time.Time, error) {
	started := time.Now()
	lease, err := java.GetLeaseWithBasicAuth(ctx, direct, endpoint, username, password)
	deadline := lease.EvidenceValidUntil(started, writerDispatchClockMargin)
	if err != nil || lease.NodeName != podName || len(lease.Roles) != 1 || !lease.HasRole("ingest") ||
		lease.NodeID == "" || lease.NodeID == victim.NodeID || lease.NodeEphemeralID == "" || !time.Now().Before(deadline) {
		return java.WriterProcess{}, time.Time{}, fmt.Errorf("evidence observer is not an identified surviving index JVM with a live lease")
	}
	return java.WriterProcess{NodeID: lease.NodeID, EphemeralID: lease.NodeEphemeralID}, deadline, nil
}

func retirementInventoryEntries(heads []java.WriterHead, victim java.WriterProcess) ([]v1alpha1.RetiringWriterHead, error) {
	if heads == nil || len(heads) > v1alpha1.MaxRetirementInventoryHeads {
		return nil, fmt.Errorf("complete bounded writer head inventory is required")
	}
	result := make([]v1alpha1.RetiringWriterHead, 0, len(heads))
	seen := make(map[string]bool, len(heads))
	for _, head := range heads {
		if victim.EphemeralID == "" || head.OwnerEphemeralID != victim.EphemeralID || head.Key == "" || seen[head.Key] ||
			head.Term < 1 || head.Shard < 0 || head.Index == "" || (head.BucketID == "" && head.IndexUUID == "") {
			return nil, fmt.Errorf("writer inventory contains an ambiguous or invalid durable head")
		}
		seen[head.Key] = true
		result = append(result, v1alpha1.RetiringWriterHead{
			Key: head.Key, BucketID: head.BucketID, Index: head.Index, IndexUUID: head.IndexUUID, Shard: head.Shard, Term: head.Term,
		})
	}
	slices.SortFunc(result, func(a, b v1alpha1.RetiringWriterHead) int { return strings.Compare(a.Key, b.Key) })
	encoded, err := json.Marshal(result)
	if err != nil {
		return nil, err
	}
	if len(encoded) > 384*1024 {
		return nil, fmt.Errorf("%w of 384 KiB", java.ErrWriterHeadInventoryLimit)
	}
	return result, nil
}

// A closed writer can lose ownership during a store outage. The current
// inventory may omit a head, but eviction never changes the original journal.
// After eviction, every original head still needs a newer write and, if its
// release receipt does not cover it, positive predecessor WAL settlement.
func writerInventoryAllowsEviction(original, current []v1alpha1.RetiringWriterHead) bool {
	if original == nil || current == nil || len(current) > len(original) {
		return false
	}
	// Both complete inventories are sorted by their unique durable keys.
	next := 0
	for _, head := range original {
		if next < len(current) && head == current[next] {
			next++
		}
	}
	return next == len(current)
}
