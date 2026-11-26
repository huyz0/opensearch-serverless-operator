package controller

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"reflect"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/huyz0/opensearch-serverless-operator/api/v1alpha1"
	"github.com/huyz0/opensearch-serverless-operator/internal/java"
	"github.com/huyz0/opensearch-serverless-operator/internal/workload"
	"github.com/huyz0/opensearch-serverless-operator/pkg/podlifecycle"
	podkube "github.com/huyz0/opensearch-serverless-operator/pkg/podlifecycle/kubernetes"
)

var (
	errWriterEvictionHeadsChanged     = errors.New("durable writer heads differ from the journal")
	errWriterEvictionRestartPolicy    = errors.New("writer eviction requires an index Pod with restartPolicy Never")
	errWriterEvictionUnsupportedHeads = errors.New("the inventoried heads require a successor verification path that is unavailable")
)

func (b developmentWriterRetirement) evictionEnabled() bool {
	return b.enableEviction
}

func (b developmentWriterRetirement) writerVictimAbsent(ctx context.Context, instance *v1alpha1.OpenSearchServerless, op *v1alpha1.OpenSearchServerlessOperation) (bool, error) {
	if b.runtime.core == nil || instance.Name != b.instanceName {
		return false, fmt.Errorf("writer Pod readback requires the configured instance and Kubernetes client")
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	pod, err := b.runtime.readExactRetiringWriterPod(ctx, instance, op.Spec.WriterRetirement)
	return err == nil && pod == nil, err
}

// evict always observes the exact Pod before retrying an ambiguous eviction.
// A successful API response still requires a later read to confirm absence.
func (b developmentWriterRetirement) evict(ctx context.Context, instance *v1alpha1.OpenSearchServerless, op *v1alpha1.OpenSearchServerlessOperation, recheck func(context.Context) error) (bool, error) {
	journal, target := op.Status.WriterRetirement, op.Spec.WriterRetirement
	r := b.runtime
	if journal == nil || !journal.EvictionArmed || journal.HeadInventory == nil || !journal.AdmissionClosed ||
		target == nil || r.core == nil || r.policy == nil {
		return false, fmt.Errorf("writer eviction requires a closed admission journal and Kubernetes clients")
	}
	ctx, cancel := context.WithTimeout(ctx, 45*time.Second)
	defer cancel()
	pod, err := r.readExactRetiringWriterPod(ctx, instance, target)
	if err != nil {
		return false, err
	}
	if pod == nil {
		return true, recheck(ctx)
	}
	if pod.DeletionTimestamp != nil || !b.enableEviction {
		return false, nil
	}
	if !supportsWriterSuccessorHeads(journal.HeadInventory.Heads) {
		return false, errWriterEvictionUnsupportedHeads
	}
	// Some API servers replace the requested resourceVersion in the unready
	// eviction path. The immutable Pod UID and restart policy block eviction
	// from crossing into another OpenSearch process's lifetime.
	if !workload.PodHasSingleOpenSearchLifetime(pod) {
		return false, errWriterEvictionRestartPolicy
	}

	current, err := b.inventory(ctx, instance, op, recheck)
	if err != nil {
		return false, err
	}
	if !writerInventoryAllowsEviction(journal.HeadInventory.Heads, current.Heads) {
		return false, errWriterEvictionHeadsChanged
	}
	bound, err := b.bindJournal(ctx, instance, op)
	if err != nil {
		return false, err
	}
	intent, access := bound.intent, bound.access
	victim, err := r.liveWriterVictim(ctx, instance, intent.Victim)
	if err != nil {
		return false, err
	}
	peers, err := r.observeWriterPeerWithdrawal(ctx, instance, intent.Victim, intent.Process, access.Reader.username, access.Reader.password)
	if err != nil {
		return false, err
	}
	ctx, stopPeers := context.WithDeadline(ctx, peers.validUntil)
	defer stopPeers()
	survivors, err := r.regularWriterSurvivors(ctx, instance, intent.Victim, peers)
	if err != nil {
		return false, err
	}
	observeEndpoints := func(ctx context.Context) (roleEndpointObservation, error) {
		return r.observeWriterTrafficWithdrawal(ctx, instance, intent.Victim, intent.ServiceUID, servingZoneMinima(instance, "ingest"), survivors)
	}
	before, err := observeEndpoints(ctx)
	if err != nil {
		return false, err
	}
	for uid, version := range before.ReadyPodVersions {
		if peers.pods[uid].Version != version {
			return false, fmt.Errorf("surviving endpoint differs from the qualified Java peer")
		}
	}
	state, err := b.readState(ctx, instance, op)
	if err != nil || state != java.WriterAdmissionClosed {
		return false, fmt.Errorf("exact Java writer admission is no longer confirmed closed")
	}
	after, err := observeEndpoints(ctx)
	if err != nil || before.ServiceVersion != after.ServiceVersion || !maps.Equal(before.SliceVersions, after.SliceVersions) ||
		!maps.Equal(before.ReadyPodVersions, after.ReadyPodVersions) {
		return false, fmt.Errorf("index routing changed during writer eviction")
	}
	edge, err := r.observeWriterEnvoyWithdrawal(ctx, instance, intent, access, victim.ResourceVersion, after)
	if err != nil {
		return false, err
	}
	ctx, stopEdge := context.WithDeadline(ctx, edge.validUntil)
	defer stopEdge()
	if err := b.checkWriterEvictionBudget(ctx, instance, victim, journal.ServingFleet); err != nil {
		return false, err
	}
	observed := writerEvictionObservation{peers: peers, endpoints: after, edge: edge}
	return false, r.evictObservedWriter(ctx, instance, intent.Victim, victim.ResourceVersion, observed, recheck)
}

type writerEvictionObservation struct {
	peers     writerPeerCohortEvidence
	endpoints roleEndpointObservation
	edge      writerEnvoyObservation
}

// evictObservedWriter checks the observations that qualified eviction after
// reading the PDB. The eviction itself still carries the victim preconditions.
func (r writerRuntime) evictObservedWriter(ctx context.Context, instance *v1alpha1.OpenSearchServerless, target podlifecycle.Identity, version string, observed writerEvictionObservation, recheck func(context.Context) error) error {
	if err := observed.edge.unchanged(ctx); err != nil {
		return err
	}
	peers, err := listRuntimeCohort(ctx, r.core.Pods(instance.Namespace), instance)
	if err != nil || !maps.Equal(observed.peers.pods, peers) {
		return fmt.Errorf("Java peer cohort changed during writer eviction")
	}
	endpoints := observed.endpoints
	service, err := r.core.Services(instance.Namespace).Get(ctx, endpoints.ServiceName, metav1.GetOptions{})
	if err != nil || service.DeletionTimestamp != nil || service.UID != endpoints.ServiceUID || service.ResourceVersion != endpoints.ServiceVersion {
		return fmt.Errorf("index Service changed during writer eviction")
	}
	if err := recheckEndpointSlices(ctx, r.discovery.EndpointSlices(instance.Namespace), endpoints.ServiceName, endpoints.SliceVersions); err != nil {
		return err
	}
	if err := recheck(ctx); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return podkube.EvictAtVersion(ctx, r.core, target, version)
}

// A same-name replacement does not inherit the retirement of the old UID.
// A terminating Pod still exists until the API confirms otherwise.
func (r writerRuntime) readExactRetiringWriterPod(ctx context.Context, instance *v1alpha1.OpenSearchServerless, target *v1alpha1.WriterRetirementTarget) (*corev1.Pod, error) {
	if target == nil || target.PodName == "" || target.PodUID == "" {
		return nil, fmt.Errorf("the exact writer namespace, name and UID are required")
	}
	pod, err := r.core.Pods(instance.Namespace).Get(ctx, target.PodName, metav1.GetOptions{})
	if apierrors.IsNotFound(err) || err == nil && string(pod.UID) != target.PodUID {
		return nil, nil
	}
	return pod, err
}

func (b developmentWriterRetirement) checkWriterEvictionBudget(ctx context.Context, instance *v1alpha1.OpenSearchServerless, victim *corev1.Pod, fleet string) error {
	zone := victim.Labels[workload.ZoneLabel]
	config, ok := instance.Spec.Pools["ingest"].Zones[zone]
	if !ok {
		return fmt.Errorf("victim is outside the configured index zones")
	}
	name := disruptionBudgetName(instance, fleet, "ingest", zone)
	expected, err := workload.DisruptionBudget(instance, name, fleet, "ingest", zone, config.MinServing)
	if err != nil {
		return err
	}
	actual, err := b.runtime.policy.PodDisruptionBudgets(instance.Namespace).Get(ctx, name, metav1.GetOptions{})
	// A withdrawn Pod is unready. IfHealthyBudget still permits its eviction
	// with zero disruptionsAllowed when the healthy cohort meets the floor.
	if err != nil || !disruptionBudgetShapeMatches(instance, actual, expected) ||
		!reflect.DeepEqual(actual.Spec.MinAvailable, expected.Spec.MinAvailable) || actual.Generation <= 0 ||
		actual.Status.ObservedGeneration != actual.Generation || actual.Status.CurrentHealthy < actual.Status.DesiredHealthy ||
		actual.Status.DesiredHealthy != config.MinServing || len(actual.Status.DisruptedPods) != 0 {
		return fmt.Errorf("current zonal index disruption budget does not qualify")
	}
	condition := meta.FindStatusCondition(actual.Status.Conditions, "DisruptionAllowed")
	if condition != nil && condition.Reason == "SyncFailed" {
		return fmt.Errorf("zonal index disruption budget could not be synchronized")
	}
	return nil
}
