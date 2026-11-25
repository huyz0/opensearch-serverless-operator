package controller

import (
	"bytes"
	"context"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/huyz0/opensearch-serverless-operator/api/v1alpha1"
	"github.com/huyz0/opensearch-serverless-operator/internal/java"
	"github.com/huyz0/opensearch-serverless-operator/internal/runtimeconfig"
	"github.com/huyz0/opensearch-serverless-operator/internal/workload"
)

// withdrawRetiringWriter targets the exact Pod, container, JVM and operation
// in the journal. The caller rechecks the journal and its route binding at the
// dispatch boundary.
func (r writerRuntime) withdrawRetiringWriter(ctx context.Context, instance *v1alpha1.OpenSearchServerless, intent writerPreparationIntent, journal v1alpha1.WriterRetirementStatus, preparer podHealthCredential, recheck func(context.Context) error) (java.WriterRetirementState, error) {
	if instance == nil || !journal.WithdrawalArmed || journal.WithdrawalDeadline == nil || journal.WithdrawalDeadline.IsZero() ||
		journal.WithdrawnAt != nil || journal.InstanceGeneration != instance.Generation ||
		intent.ServiceUID == "" || string(intent.ServiceUID) != journal.ServiceUID || intent.OperationUID == "" ||
		intent.Process.NodeID != journal.NodeID || intent.Process.EphemeralID != journal.NodeEphemeralID ||
		instance.Status.BasicAuthBaseline == nil || !hasManagedHTTPS(instance) || !r.supportsDevelopmentIngestProbe(instance) || recheck == nil {
		return "", fmt.Errorf("writer withdrawal requires authorized exact-process intent and qualified HTTPS")
	}
	_, bootstrap, err := r.resolveBasicAuth(ctx, instance)
	if err != nil {
		return "", err
	}
	if preparer.username == "" || preparer.username == runtimeconfig.BasicAuthUser || len(preparer.password) == 0 || bytes.Equal(preparer.password, bootstrap) {
		return "", fmt.Errorf("writer withdrawal requires a dedicated non-bootstrap preparer")
	}
	victim, err := r.liveWriterVictim(ctx, instance, intent.Victim)
	if err != nil {
		return "", err
	}
	container := runningOpenSearch(victim)
	if container == nil || container.ContainerID != journal.ContainerID || victim.Labels[workload.FleetLabel] != journal.ServingFleet {
		return "", fmt.Errorf("recorded writer container or serving fleet changed")
	}
	direct, endpoint, err := r.developmentPodHTTPTransport(ctx, instance, victim, 9200)
	if err != nil {
		return "", err
	}
	defer direct.CloseIdleConnections()
	operationID := string(intent.OperationUID)
	var protection servingRoleProtection
	return executeWriterWithdrawal(ctx, writerWithdrawalChecks{
		cutoff: journal.WithdrawalDeadline.Time,
		state: func(ctx context.Context) (java.WriterRetirementState, error) {
			return java.GetWriterRetirementState(ctx, direct, endpoint, intent.Process, operationID, preparer.username, preparer.password)
		},
		qualify: func(ctx context.Context) (time.Time, error) {
			var validUntil time.Time
			protection, validUntil, err = r.qualifyRetiringWriterCapacity(ctx, instance, victim, journal.ServiceUID)
			return validUntil, err
		},
		recheck: func(ctx context.Context) error {
			if err := recheckEndpointSlices(ctx, r.discovery.EndpointSlices(instance.Namespace), protection.Endpoints.ServiceName, protection.Endpoints.SliceVersions); err != nil {
				return err
			}
			service, err := r.core.Services(instance.Namespace).Get(ctx, protection.Endpoints.ServiceName, metav1.GetOptions{})
			if err != nil || service.UID != intent.ServiceUID || service.ResourceVersion != protection.ServiceVersion {
				return fmt.Errorf("stable index Service changed during withdrawal qualification")
			}
			for _, previous := range protection.PodSnapshot {
				pod, err := r.core.Pods(instance.Namespace).Get(ctx, previous.Name, metav1.GetOptions{})
				if err != nil || pod.UID != previous.UID || pod.ResourceVersion != previous.ResourceVersion {
					return fmt.Errorf("index Pod changed during withdrawal qualification")
				}
			}
			return recheck(ctx)
		},
		withdraw: func(ctx context.Context) (java.WriterRetirementState, error) {
			deadline, _ := ctx.Deadline()
			return java.WithdrawWriterTrafficBefore(ctx, direct, endpoint, intent.Process, operationID, preparer.username, preparer.password,
				deadline.Add(-writerDispatchClockMargin))
		},
	})
}

func (r writerRuntime) qualifyRetiringWriterCapacity(ctx context.Context, instance *v1alpha1.OpenSearchServerless, victim *corev1.Pod, serviceUID string) (servingRoleProtection, time.Time, error) {
	const maxAge = 10 * time.Second
	protection, err := r.observeServingIngestProtection(ctx, instance, maxAge)
	if err != nil {
		return protection, time.Time{}, err
	}
	zone := victim.Labels[workload.ZoneLabel]
	if protection.ServiceUID != serviceUID || protection.Endpoints.ReadyPodVersions[victim.UID] != victim.ResourceVersion ||
		victim.Annotations[emergencyCapacityAnnotation] == "true" || protection.RegularEndpointsByZone[zone] < 1 {
		return protection, time.Time{}, fmt.Errorf("recorded writer must still be a regular serving endpoint")
	}
	protection.RegularEndpointsByZone[zone]--
	minima := servingZoneMinima(instance, "ingest")
	if zoneShort, regionalShort := servingFloorShortfalls(instance, "ingest", minima, protection.RegularEndpointsByZone); zoneShort || regionalShort {
		return protection, time.Time{}, fmt.Errorf("index capacity would be below the serving floors after withdrawal")
	}
	validUntil := protection.ObservedAt.Add(maxAge)
	for _, pod := range protection.Pods {
		validUntil = minTime(validUntil, pod.LeaseValidUntil)
	}
	if !time.Now().Before(validUntil) {
		return protection, time.Time{}, fmt.Errorf("index capacity observation expired")
	}
	return protection, validUntil, nil
}

type writerWithdrawalChecks struct {
	state, withdraw func(context.Context) (java.WriterRetirementState, error)
	qualify         func(context.Context) (time.Time, error)
	recheck         func(context.Context) error
	cutoff          time.Time
}

// executeWriterWithdrawal reads the same JVM and operation before sending a
// POST. A lease publication still in flight is retried by readback on the
// next reconcile. An already published withdrawal never sends a second POST.
func executeWriterWithdrawal(ctx context.Context, check writerWithdrawalChecks) (java.WriterRetirementState, error) {
	// Only a read started after the dispatch window can certify that an
	// open writer has no pending withdrawal that Java may still accept.
	expired := !time.Now().Before(check.cutoff)
	state, err := check.state(ctx)
	if err != nil || state != java.WriterAdmissionOpen {
		return state, err
	}
	if expired {
		return state, nil
	}
	ctx, closeWindow := context.WithDeadline(ctx, check.cutoff)
	defer closeWindow()
	validUntil, err := check.qualify(ctx)
	if err != nil {
		return "", err
	}
	ctx, cancel := context.WithDeadline(ctx, validUntil)
	defer cancel()
	if err := check.recheck(ctx); err != nil {
		return "", err
	}
	if err := ctx.Err(); err != nil {
		return "", fmt.Errorf("writer withdrawal evidence expired: %w", err)
	}
	return check.withdraw(ctx)
}
