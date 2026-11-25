package controller

import (
	"bytes"
	"context"
	"fmt"
	"maps"
	"time"

	"golang.org/x/sync/errgroup"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/huyz0/opensearch-serverless-operator/api/v1alpha1"
	"github.com/huyz0/opensearch-serverless-operator/internal/java"
	"github.com/huyz0/opensearch-serverless-operator/internal/runtimeconfig"
	"github.com/huyz0/opensearch-serverless-operator/pkg/podlifecycle"
)

type writerPeerObservation struct {
	process    java.WriterProcess
	validUntil time.Time
}

type writerPeerCohortEvidence struct {
	pods       map[types.UID]runtimeCohortPod
	validUntil time.Time
}

// observeWriterPeerWithdrawal samples every other live Java Pod over direct
// HTTPS, including peers outside the currently serving fleet.
// The caller must also verify the victim's exact reservation and current routes.
func (r writerRuntime) observeWriterPeerWithdrawal(ctx context.Context, instance *v1alpha1.OpenSearchServerless, victim podlifecycle.Identity, target java.WriterProcess, username string, password []byte) (writerPeerCohortEvidence, error) {
	if instance == nil || instance.Status.BasicAuthBaseline == nil || !hasManagedHTTPS(instance) || !r.supportsDevelopmentPodHTTP(instance) {
		return writerPeerCohortEvidence{}, fmt.Errorf("writer withdrawal requires pinned Java HTTPS and Basic Auth")
	}
	_, bootstrapPassword, err := r.resolveBasicAuth(ctx, instance)
	if err != nil {
		return writerPeerCohortEvidence{}, err
	}
	if username == "" || username == runtimeconfig.BasicAuthUser || len(password) == 0 || bytes.Equal(password, bootstrapPassword) {
		return writerPeerCohortEvidence{}, fmt.Errorf("writer withdrawal requires a distinct evidence reader credential")
	}
	return r.observeWriterPeerCohort(ctx, instance, victim, target, func(ctx context.Context, uid types.UID, peer runtimeCohortPod) (writerPeerObservation, error) {
		pod, err := r.core.Pods(instance.Namespace).Get(ctx, peer.Name, metav1.GetOptions{})
		if err != nil {
			return writerPeerObservation{}, err
		}
		if pod.UID != uid || pod.ResourceVersion != peer.Version ||
			pod.Annotations[keystoreUIDAnnotation] != instance.Status.BasicAuthBaseline.SecretUID {
			return writerPeerObservation{}, fmt.Errorf("peer Pod identity or credential pin changed")
		}
		httpClient, endpoint, err := r.developmentPodHTTPTransport(ctx, instance, pod, 9200)
		if err != nil {
			return writerPeerObservation{}, err
		}
		defer httpClient.CloseIdleConnections()
		started := time.Now()
		lease, err := java.GetLeaseWithBasicAuth(ctx, httpClient, endpoint, username, password)
		if err != nil {
			return writerPeerObservation{}, err
		}
		deadline := lease.EvidenceValidUntil(started, 5*time.Second)
		observer := java.WriterProcess{NodeID: lease.NodeID, EphemeralID: lease.NodeEphemeralID}
		if lease.NodeName != peer.Name || len(lease.Roles) != 1 || !lease.HasRole(peer.Role) || !time.Now().Before(deadline) {
			return writerPeerObservation{}, fmt.Errorf("peer stats did not identify a live Java process with the expected role")
		}
		viewStarted := time.Now()
		remaining, err := java.ObserveWriterWithdrawalView(ctx, httpClient, endpoint, observer, target, username, password)
		if err != nil {
			return writerPeerObservation{}, err
		}
		if remaining == 0 {
			return writerPeerObservation{}, fmt.Errorf("peer has not observed this writer's withdrawal")
		}
		viewDeadline := viewStarted.Add(remaining)
		if viewDeadline.Before(deadline) {
			deadline = viewDeadline
		}
		return writerPeerObservation{process: observer, validUntil: deadline}, nil
	})
}

func (r writerRuntime) observeWriterPeerCohort(ctx context.Context, instance *v1alpha1.OpenSearchServerless, victim podlifecycle.Identity, target java.WriterProcess, probe func(context.Context, types.UID, runtimeCohortPod) (writerPeerObservation, error)) (writerPeerCohortEvidence, error) {
	if r.reader == nil || r.core == nil || instance == nil || instance.UID == "" || instance.ResourceVersion == "" ||
		instance.DeletionTimestamp != nil || victim.Namespace != instance.Namespace || victim.OwnerUID != string(instance.UID) ||
		victim.UID == "" || victim.Name == "" || target.NodeID == "" || target.EphemeralID == "" || probe == nil {
		return writerPeerCohortEvidence{}, fmt.Errorf("an exact writer process, owned Pod and uncached instance clients are required")
	}
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	checkInstance := func() error {
		var live v1alpha1.OpenSearchServerless
		if err := r.reader.Get(ctx, client.ObjectKeyFromObject(instance), &live); err != nil {
			return err
		}
		if live.UID != instance.UID || live.ResourceVersion != instance.ResourceVersion || live.DeletionTimestamp != nil {
			return fmt.Errorf("instance changed during writer peer observation")
		}
		return nil
	}
	if err := checkInstance(); err != nil {
		return writerPeerCohortEvidence{}, err
	}
	pods := r.core.Pods(instance.Namespace)
	cohort, err := listRuntimeCohort(ctx, pods, instance)
	if err != nil {
		return writerPeerCohortEvidence{}, err
	}
	victimUID := types.UID(victim.UID)
	ingestFleet := instance.Status.ServingFleetIntentByRole["ingest"]
	if targetPod := cohort[victimUID]; targetPod.Name != victim.Name || targetPod.Role != "ingest" || targetPod.Fleet != ingestFleet {
		return writerPeerCohortEvidence{}, fmt.Errorf("retiring writer is absent from the observed serving fleet")
	}
	survivor := false
	for uid, peer := range cohort {
		if uid != victimUID && peer.Role == "ingest" && peer.Fleet == ingestFleet {
			survivor = true
			break
		}
	}
	if !survivor {
		return writerPeerCohortEvidence{}, fmt.Errorf("writer withdrawal requires a surviving serving ingest peer")
	}

	results := make([]writerPeerObservation, len(cohort)-1)
	group, work := errgroup.WithContext(ctx)
	group.SetLimit(8)
	index := 0
	for uid, peer := range cohort {
		if uid == victimUID {
			continue
		}
		slot := index
		index++
		group.Go(func() error {
			seen, err := probe(work, uid, peer)
			if err != nil {
				return fmt.Errorf("observe withdrawal on Pod %s: %w", peer.Name, err)
			}
			results[slot] = seen
			return nil
		})
	}
	if err := group.Wait(); err != nil {
		return writerPeerCohortEvidence{}, err
	}
	var validUntil time.Time
	seenNodes := map[string]bool{target.NodeID: true}
	for _, observation := range results {
		if observation.process.NodeID == "" || observation.process.EphemeralID == "" || seenNodes[observation.process.NodeID] {
			return writerPeerCohortEvidence{}, fmt.Errorf("peer observations do not identify distinct Java nodes")
		}
		seenNodes[observation.process.NodeID] = true
		if validUntil.IsZero() || observation.validUntil.Before(validUntil) {
			validUntil = observation.validUntil
		}
	}
	current, err := listRuntimeCohort(ctx, pods, instance)
	if err != nil {
		return writerPeerCohortEvidence{}, err
	}
	if !maps.Equal(cohort, current) {
		return writerPeerCohortEvidence{}, fmt.Errorf("Java Pod membership or identity changed during withdrawal observation")
	}
	if err := checkInstance(); err != nil {
		return writerPeerCohortEvidence{}, err
	}
	if !time.Now().Before(validUntil) {
		return writerPeerCohortEvidence{}, fmt.Errorf("Java peer lease observations expired")
	}
	return writerPeerCohortEvidence{pods: cohort, validUntil: validUntil}, nil
}
