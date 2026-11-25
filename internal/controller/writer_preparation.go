package controller

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"fmt"
	"maps"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/huyz0/opensearch-serverless-operator/api/v1alpha1"
	"github.com/huyz0/opensearch-serverless-operator/internal/java"
	"github.com/huyz0/opensearch-serverless-operator/internal/runtimeconfig"
	"github.com/huyz0/opensearch-serverless-operator/internal/workload"
	"github.com/huyz0/opensearch-serverless-operator/pkg/podlifecycle"
)

const writerDispatchClockMargin = time.Second

type writerPreparationIntent struct {
	Victim                        podlifecycle.Identity
	Process                       java.WriterProcess
	OperationUID                  types.UID
	ServiceUID                    types.UID
	RouteName                     string
	RouteUID                      types.UID
	WithdrawnAt                   time.Time
	Gateway, GatewayService       client.ObjectKey
	GatewayUID, GatewayServiceUID types.UID
	GatewayListener               string
}

type writerPreparationAccess struct {
	Reader, Preparer          podHealthCredential
	RouteURL, RouteAuthSecret string
	PlatformKey               ed25519.PublicKey
}

// prepareRetiringWriter consumes one previously recorded withdrawal intent.
// recheck must read the exact persisted intent and exclusive claim from the
// API server, including before resuming after an ambiguous Java response.
func (r writerRuntime) prepareRetiringWriter(ctx context.Context, instance *v1alpha1.OpenSearchServerless, intent writerPreparationIntent, access writerPreparationAccess, recheck func(context.Context, writerPreparationIntent) error) (java.WriterRetirementState, error) {
	if instance == nil || r.core == nil || r.discovery == nil || r.reader == nil || recheck == nil || intent.OperationUID == "" ||
		intent.ServiceUID == "" || intent.RouteName == "" || intent.RouteUID == "" || intent.Process.NodeID == "" ||
		intent.Process.EphemeralID == "" || intent.WithdrawnAt.IsZero() || len(access.PlatformKey) != ed25519.PublicKeySize ||
		instance.Status.BasicAuthBaseline == nil || !hasManagedHTTPS(instance) || !r.supportsDevelopmentPodHTTP(instance) {
		return "", fmt.Errorf("writer preparation requires persisted withdrawal intent and qualified HTTPS access")
	}
	reservation := func(ctx context.Context) error {
		var live v1alpha1.OpenSearchServerless
		if err := r.reader.Get(ctx, client.ObjectKeyFromObject(instance), &live); err != nil {
			return err
		}
		if live.UID != instance.UID || live.ResourceVersion != instance.ResourceVersion || live.DeletionTimestamp != nil {
			return fmt.Errorf("instance changed during writer preparation")
		}
		return recheck(ctx, intent)
	}
	if err := reservation(ctx); err != nil {
		return "", err
	}
	_, bootstrap, err := r.resolveBasicAuth(ctx, instance)
	if err != nil {
		return "", err
	}
	if access.Preparer.username == "" || access.Preparer.username == runtimeconfig.BasicAuthUser || len(access.Preparer.password) == 0 ||
		bytes.Equal(access.Preparer.password, bootstrap) || access.Preparer.username == access.Reader.username ||
		bytes.Equal(access.Preparer.password, access.Reader.password) {
		return "", fmt.Errorf("writer preparation requires a distinct non-bootstrap preparer credential")
	}
	victim, err := r.liveWriterVictim(ctx, instance, intent.Victim)
	if err != nil {
		return "", err
	}
	direct, endpoint, err := r.developmentPodHTTPTransport(ctx, instance, victim, 9200)
	if err != nil {
		return "", err
	}
	defer direct.CloseIdleConnections()
	state := func(ctx context.Context) (java.WriterRetirementState, error) {
		return java.GetWriterRetirementState(ctx, direct, endpoint, intent.Process, string(intent.OperationUID), access.Preparer.username, access.Preparer.password)
	}
	var previousEndpoints roleEndpointObservation
	var peers writerPeerCohortEvidence
	survivors := map[types.UID]struct{}{}
	return executeWriterPreparation(ctx, writerPreparationChecks{
		state: state,
		peer: func(ctx context.Context) (time.Time, error) {
			var err error
			peers, err = r.observeWriterPeerWithdrawal(ctx, instance, intent.Victim, intent.Process, access.Reader.username, access.Reader.password)
			if err != nil {
				return time.Time{}, err
			}
			survivors, err = r.regularWriterSurvivors(ctx, instance, intent.Victim, peers)
			return peers.validUntil, err
		},
		edge: func(ctx context.Context) (time.Time, error) {
			observed, err := r.observeWriterTrafficWithdrawal(ctx, instance, intent.Victim, intent.ServiceUID, servingZoneMinima(instance, "ingest"), survivors)
			if err != nil {
				return time.Time{}, err
			}
			for uid, version := range observed.ReadyPodVersions {
				if peers.pods[uid].Version != version {
					return time.Time{}, fmt.Errorf("surviving endpoint differs from the qualified Java peer")
				}
			}
			previousEndpoints = observed
			edge, err := r.observeWriterEnvoyWithdrawal(ctx, instance, intent, access, victim.ResourceVersion, observed)
			return edge.validUntil, err
		},
		recheck: func(ctx context.Context) (time.Time, error) {
			current, err := r.liveWriterVictim(ctx, instance, intent.Victim)
			if err != nil {
				return time.Time{}, err
			}
			if current.ResourceVersion != victim.ResourceVersion {
				return time.Time{}, fmt.Errorf("writer Pod changed during preparation")
			}
			observed, err := r.observeWriterTrafficWithdrawal(ctx, instance, intent.Victim, intent.ServiceUID, servingZoneMinima(instance, "ingest"), survivors)
			if err != nil {
				return time.Time{}, err
			}
			if observed.ServiceVersion != previousEndpoints.ServiceVersion ||
				!maps.Equal(observed.SliceVersions, previousEndpoints.SliceVersions) ||
				!maps.Equal(observed.ReadyPodVersions, previousEndpoints.ReadyPodVersions) {
				return time.Time{}, fmt.Errorf("index routing changed during writer preparation")
			}
			currentPeers, err := listRuntimeCohort(ctx, r.core.Pods(instance.Namespace), instance)
			if err != nil || !maps.Equal(peers.pods, currentPeers) {
				return time.Time{}, fmt.Errorf("Java peer cohort changed during writer preparation")
			}
			if err := reservation(ctx); err != nil {
				return time.Time{}, err
			}
			edge, err := r.observeWriterEnvoyWithdrawal(ctx, instance, intent, access, victim.ResourceVersion, observed)
			return edge.validUntil, err
		},
		prepare: func(ctx context.Context) (java.WriterRetirementState, error) {
			deadline, _ := ctx.Deadline()
			// The Java host may lag the controller's clock. Dispatch under the
			// earlier common deadline and enforce it again in the Java lock.
			deadline = deadline.Add(-writerDispatchClockMargin)
			return java.PrepareWriterRetirementBefore(ctx, direct, endpoint, intent.Process, string(intent.OperationUID), access.Preparer.username, access.Preparer.password, deadline)
		},
	})
}

func (r writerRuntime) regularWriterSurvivors(ctx context.Context, instance *v1alpha1.OpenSearchServerless, victim podlifecycle.Identity, peers writerPeerCohortEvidence) (map[types.UID]struct{}, error) {
	survivors := map[types.UID]struct{}{}
	for uid, peer := range peers.pods {
		if string(uid) == victim.UID || peer.Role != "ingest" || peer.Fleet != instance.Status.ServingFleetIntentByRole["ingest"] {
			continue
		}
		pod, err := r.core.Pods(instance.Namespace).Get(ctx, peer.Name, metav1.GetOptions{})
		if err != nil || pod.UID != uid || pod.ResourceVersion != peer.Version {
			return nil, fmt.Errorf("surviving index Pod changed after Java peer observation")
		}
		if !pod.Spec.HostNetwork && pod.Annotations[emergencyCapacityAnnotation] != "true" {
			survivors[uid] = struct{}{}
		}
	}
	return survivors, nil
}

type writerPreparationChecks struct {
	state, prepare      func(context.Context) (java.WriterRetirementState, error)
	peer, edge, recheck func(context.Context) (time.Time, error)
}

func executeWriterPreparation(ctx context.Context, check writerPreparationChecks) (java.WriterRetirementState, error) {
	// The initial direct read recovers a prior ambiguous prepare without a repeat POST.
	state, err := check.state(ctx)
	if err != nil || state != java.WriterTrafficWithdrawn {
		return state, err
	}
	peersUntil, err := check.peer(ctx)
	if err != nil {
		return "", err
	}
	ctx, cancel := context.WithDeadline(ctx, peersUntil)
	defer cancel()
	edgeUntil, err := check.edge(ctx)
	if err != nil {
		return "", err
	}
	ctx, stop := context.WithDeadline(ctx, edgeUntil)
	defer stop()
	state, err = check.state(ctx)
	if err != nil || state != java.WriterTrafficWithdrawn {
		return state, err
	}
	edgeUntil, err = check.recheck(ctx)
	if err != nil {
		return "", err
	}
	ctx, final := context.WithDeadline(ctx, edgeUntil)
	defer final()
	if err := ctx.Err(); err != nil {
		return "", err
	}
	return check.prepare(ctx)
}

func (r writerRuntime) liveWriterVictim(ctx context.Context, instance *v1alpha1.OpenSearchServerless, victim podlifecycle.Identity) (*corev1.Pod, error) {
	if victim.Namespace != instance.Namespace || victim.OwnerUID != string(instance.UID) || victim.Name == "" || victim.UID == "" {
		return nil, fmt.Errorf("writer intent does not identify an owned Pod")
	}
	pod, err := r.core.Pods(instance.Namespace).Get(ctx, victim.Name, metav1.GetOptions{})
	if err != nil {
		return nil, err
	}
	owner := metav1.GetControllerOf(pod)
	if string(pod.UID) != victim.UID || pod.ResourceVersion == "" || pod.DeletionTimestamp != nil || runningOpenSearch(pod) == nil ||
		owner == nil || owner.UID != instance.UID || owner.Kind != "OpenSearchServerless" || owner.APIVersion != v1alpha1.GroupVersion.String() ||
		owner.Name != instance.Name || pod.Labels[workload.InstanceLabel] != string(instance.UID) || pod.Labels[workload.RoleLabel] != "ingest" ||
		pod.Labels[workload.FleetLabel] == "" || pod.Labels[workload.FleetLabel] != instance.Status.ServingFleetIntentByRole["ingest"] ||
		pod.Annotations[keystoreUIDAnnotation] != instance.Status.BasicAuthBaseline.SecretUID {
		return nil, fmt.Errorf("retiring writer Pod identity, process or admission pin changed")
	}
	return pod, nil
}
