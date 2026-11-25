package controller

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/huyz0/opensearch-serverless-operator/api/v1alpha1"
	"github.com/huyz0/opensearch-serverless-operator/internal/placement"
	"github.com/huyz0/opensearch-serverless-operator/internal/workload"
	"github.com/huyz0/opensearch-serverless-operator/pkg/podlifecycle"
	podkube "github.com/huyz0/opensearch-serverless-operator/pkg/podlifecycle/kubernetes"
)

func (r *InstanceReconciler) reconcileGateRelease(ctx context.Context, instance *v1alpha1.OpenSearchServerless, revision string) (ctrl.Result, error) {
	intent := instance.Status.PendingGateRelease
	if instance.Status.ProvisioningAttempt != nil {
		return ctrl.Result{RequeueAfter: time.Minute}, fmt.Errorf("gate release and provisioning attempt overlap")
	}
	pods := r.Core.Pods(instance.Namespace)
	pod, err := pods.Get(ctx, intent.Pod.Name, metav1.GetOptions{})
	if err != nil {
		if apierrors.IsNotFound(err) && intent.WithdrawalRequested {
			before := instance.DeepCopyObject().(*v1alpha1.OpenSearchServerless)
			if slot := instance.Status.SearchWarmupSlot; slot != nil && slot.Name == intent.Pod.Name && slot.PodUID == intent.PodUID {
				instance.Status.SearchWarmupSlot = nil
			}
			if bootstrap := instance.Status.ColdBootstrap; bootstrap != nil && bootstrap.Name == intent.Pod.Name {
				if bootstrap.PodUID != intent.PodUID {
					return ctrl.Result{RequeueAfter: time.Minute}, fmt.Errorf("cold bootstrap identity changed during gated withdrawal")
				}
				instance.Status.ColdBootstrap = nil
			}
			instance.Status.PendingGateRelease = nil
			if err := r.Status().Patch(ctx, instance, client.MergeFromWithOptions(before, client.MergeFromWithOptimisticLock{})); err != nil {
				return ctrl.Result{}, err
			}
			return ctrl.Result{Requeue: true}, nil
		}
		if apierrors.IsNotFound(err) && r.gateReleaseExpired(intent) {
			return r.requestRetiredGateWithdrawal(ctx, instance, intent)
		}
		return ctrl.Result{}, fmt.Errorf("read gate release target: %w", err)
	}
	if string(pod.UID) != intent.PodUID {
		return ctrl.Result{RequeueAfter: time.Minute}, fmt.Errorf("gate release target UID changed")
	}
	if err := validateCreationPodIdentity(instance, creationRecord(&intent.Pod), pod); err != nil {
		return ctrl.Result{RequeueAfter: time.Minute}, fmt.Errorf("validate gate release target: %w", err)
	}
	if pod.DeletionTimestamp != nil {
		if intent.WithdrawalRequested && hasPlacementGate(pod) {
			return ctrl.Result{RequeueAfter: time.Second}, nil
		}
		return ctrl.Result{RequeueAfter: time.Minute}, fmt.Errorf("gate release target is terminating")
	}
	if !hasPlacementGate(pod) {
		// An earlier update may have succeeded even if its response was lost.
		before := instance.DeepCopyObject().(*v1alpha1.OpenSearchServerless)
		if intent.WithdrawalRequested {
			for i := range instance.Status.RetiredPodCreations {
				retired := &instance.Status.RetiredPodCreations[i]
				if retired.Name == pod.Name {
					if err := validateCreationPodIdentity(instance, *retired, pod); err != nil {
						return ctrl.Result{RequeueAfter: time.Minute}, err
					}
					retired.PodUID = intent.PodUID
					retired.Released = true
				}
			}
		}
		instance.Status.PendingGateRelease = nil
		instance.Status.ProvisioningAttempt = &v1alpha1.ProvisioningAttempt{Role: intent.Pod.Role, Zone: intent.Pod.Zone, Name: pod.Name, PodUID: string(pod.UID), StartedAt: intent.StartedAt, Phase: "Watching", PeerDNS: intent.Pod.PeerDNS, PluginInstall: intent.Pod.PluginInstall}
		if err := r.Status().Patch(ctx, instance, client.MergeFromWithOptions(before, client.MergeFromWithOptimisticLock{})); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{Requeue: true}, nil
	}
	if intent.WithdrawalRequested {
		identity := podlifecycle.Identity{Namespace: pod.Namespace, Name: pod.Name, UID: string(pod.UID), OwnerUID: string(instance.UID)}
		if err := podkube.WithdrawGatedAtVersion(ctx, r.Core, identity, "serverless.opensearch.org/placement", pod.ResourceVersion); err != nil {
			return ctrl.Result{}, fmt.Errorf("withdraw stale gated Pod: %w", err)
		}
		// A version-conditional deletion proves the Pod still had its gate.
		// An ambiguous deletion or missing Pod does not refund the reservation.
		if intent.Pod.Fleet != instance.Status.ServingFleetIntentByRole[intent.Pod.Role] {
			matches := func(record v1alpha1.PodReleaseRecord) bool {
				return record.PodUID == intent.PodUID && record.Role == intent.Pod.Role && record.Zone == intent.Pod.Zone
			}
			if slices.ContainsFunc(instance.Status.RecentPodReleases, matches) {
				before := instance.DeepCopyObject().(*v1alpha1.OpenSearchServerless)
				instance.Status.RecentPodReleases = slices.DeleteFunc(instance.Status.RecentPodReleases, matches)
				if err := r.Status().Patch(ctx, instance, client.MergeFromWithOptions(before, client.MergeFromWithOptimisticLock{})); err != nil {
					return ctrl.Result{}, err
				}
			}
		}
		return ctrl.Result{Requeue: true}, nil
	}
	if r.gateReleaseExpired(intent) {
		return r.requestRetiredGateWithdrawal(ctx, instance, intent)
	}
	return r.qualifyAndReleaseGate(ctx, instance, intent, pod, revision)
}

// qualifyAndReleaseGate checks current inputs against the durable intent and
// removes the gate only from the exact Pod version it qualified.
func (r *InstanceReconciler) qualifyAndReleaseGate(ctx context.Context, instance *v1alpha1.OpenSearchServerless, intent *v1alpha1.GateReleaseIntent, pod *corev1.Pod, revision string) (ctrl.Result, error) {
	blocked, err := r.provisioningHolds(ctx, instance)
	if err != nil {
		return ctrl.Result{RequeueAfter: time.Minute}, err
	}
	if intent.Pod.Generation != instance.Generation || intent.Pod.Revision != revision || !servingRevisionMatchesIntent(instance, &intent.Pod) || !fleetAllowedForPod(instance, intent.Pod.Role, intent.Pod.Zone, intent.Pod.Fleet) ||
		blocked[provisioningGroup{intent.Pod.Role, intent.Pod.Zone}] || !r.emergencyIntentStillFits(ctx, instance, &intent.Pod, intent.PodUID) {
		return r.requestRetiredGateWithdrawal(ctx, instance, intent)
	}
	if !r.DevelopmentReleaseGates {
		return ctrl.Result{RequeueAfter: time.Minute}, nil
	}
	if intent.StartedAt.IsZero() {
		return ctrl.Result{RequeueAfter: time.Minute}, fmt.Errorf("gate release intent has no start time")
	}
	pluginInputs, err := r.pluginsForFleetRole(ctx, instance, intent.Pod.Fleet, intent.Pod.Role)
	if err != nil {
		return ctrl.Result{RequeueAfter: time.Minute}, err
	}
	if err := checkPluginIntent(instance, intent.Pod.Name, intent.Pod.PluginInstall, pluginInputs); err != nil {
		return ctrl.Result{RequeueAfter: time.Minute}, err
	}
	if intent.Pod.PluginInstall != nil {
		if intent.Pod.PluginInstall.RequestUID == "" || intent.Pod.PluginInstall.PolicyUID == "" {
			return ctrl.Result{RequeueAfter: time.Minute}, fmt.Errorf("plugin snapshot identities were not persisted")
		}
		if _, _, err := r.ensurePluginSnapshots(ctx, instance, intent.Pod.PluginInstall, pluginInputs); err != nil {
			return ctrl.Result{RequeueAfter: time.Minute}, err
		}
	}
	if pool, exists := instance.Spec.Pools[intent.Pod.Role]; exists && intent.Pod.Role == "search" && pool.WarmOnJoin {
		slot := instance.Status.SearchWarmupSlot
		if slot == nil || slot.PodUID != intent.PodUID || slot.Name != intent.Pod.Name {
			return ctrl.Result{RequeueAfter: time.Minute}, fmt.Errorf("search warm-up slot is missing or belongs to another Pod")
		}
	}
	profile, err := r.resolveCapacityProfile(ctx, instance, intent.Pod.Role, intent.Pod.Zone)
	if err != nil {
		return ctrl.Result{RequeueAfter: time.Minute}, err
	}
	if err := checkProfileIdentity(&intent.Pod, profile); err != nil {
		return ctrl.Result{RequeueAfter: time.Minute}, err
	}
	domain, err := r.peerDNSDomain(instance)
	if err != nil {
		return ctrl.Result{RequeueAfter: time.Minute}, err
	}
	peerTLS, err := r.resolvePeerTLS(ctx, instance, domain)
	if err != nil {
		return ctrl.Result{RequeueAfter: time.Minute}, err
	}
	peerTLS = peerTLSForFleet(instance, intent.Pod.Fleet, peerTLS)
	if peerTLS != nil && peerTLS.baseline.SnapshotUID == "" {
		return ctrl.Result{RequeueAfter: time.Minute}, fmt.Errorf("peer TLS snapshot identity was not persisted")
	}
	if err := r.checkPeerTLSIntent(ctx, instance, &intent.Pod, peerTLS, domain); err != nil {
		return ctrl.Result{RequeueAfter: time.Minute}, err
	}
	desired, err := renderGatedPod(instance, &intent.Pod, profile, domain, r.DevelopmentRuntimeSettingsImage)
	if err != nil {
		return ctrl.Result{}, err
	}
	if err := validateAdmittedGatedPod(desired, pod); err != nil {
		return ctrl.Result{RequeueAfter: time.Minute}, fmt.Errorf("gate release validation failed: %w", err)
	}
	if err := validateManagedTLSKeyAccess(instance, pod); err != nil {
		return ctrl.Result{RequeueAfter: time.Minute}, err
	}
	if profile != nil {
		if err := placement.CheckFit(pod, profile); err != nil {
			return ctrl.Result{RequeueAfter: time.Minute}, err
		}
	}
	selector := labels.SelectorFromSet(labels.Set{workload.InstanceLabel: string(instance.UID)}).String()
	observed, err := r.Core.Pods(instance.Namespace).List(ctx, metav1.ListOptions{LabelSelector: selector})
	if err != nil {
		return ctrl.Result{}, err
	}
	for _, other := range observed.Items {
		owner := metav1.GetControllerOf(&other)
		if owner == nil || owner.UID != instance.UID || other.UID == "" {
			return ctrl.Result{RequeueAfter: time.Minute}, fmt.Errorf("conflicting Pod in pending demand snapshot")
		}
		if other.UID != pod.UID && other.Spec.NodeName == "" && other.DeletionTimestamp == nil && other.Status.Phase != corev1.PodFailed && other.Status.Phase != corev1.PodSucceeded {
			return ctrl.Result{RequeueAfter: time.Minute}, nil
		}
	}
	if err := r.checkPinnedPrimaryStore(ctx, instance); err != nil {
		return ctrl.Result{RequeueAfter: time.Minute}, err
	}
	if r.confirmedOutageZones(ctx, instance)[intent.Pod.Zone] || !r.emergencyIntentStillFits(ctx, instance, &intent.Pod, intent.PodUID) {
		return r.requestRetiredGateWithdrawal(ctx, instance, intent)
	}
	httpTLS, err := r.resolveHTTPTLSForFleet(ctx, instance, domain, intent.Pod.Fleet)
	if err != nil {
		return ctrl.Result{RequeueAfter: time.Minute}, err
	}
	if err := httpTLS.validatePod(instance, intent.Pod.Name, r.now()); err != nil {
		return ctrl.Result{RequeueAfter: time.Minute}, err
	}
	authPins, _, err := r.resolveBasicAuth(ctx, instance)
	if err != nil {
		return ctrl.Result{RequeueAfter: time.Minute}, err
	}
	if authPins != nil && authPins.SecretUID != intent.Pod.KeystoreUID {
		return ctrl.Result{RequeueAfter: time.Minute}, fmt.Errorf("startup authentication Secret identity changed before gate release")
	}
	if err := r.checkCandidateAdmission(ctx, instance, &intent.Pod); err != nil {
		if errors.Is(err, errPreviewServingCapacity) {
			return r.requestRetiredGateWithdrawal(ctx, instance, intent)
		}
		return ctrl.Result{RequeueAfter: time.Minute}, err
	}
	if err := r.checkKeystoreUID(ctx, instance, intent.Pod.KeystoreUID); err != nil {
		return ctrl.Result{RequeueAfter: time.Minute}, err
	}
	if r.gateReleaseExpired(intent) || (profile != nil && !profile.Profile.ExpiresAt.Time.After(r.now())) {
		return r.requestRetiredGateWithdrawal(ctx, instance, intent)
	}
	identity := podlifecycle.Identity{Namespace: pod.Namespace, Name: pod.Name, UID: string(pod.UID), OwnerUID: string(instance.UID)}
	if err := podkube.ReleaseSchedulingGateAtVersion(ctx, r.Core, identity, "serverless.opensearch.org/placement", pod.ResourceVersion); err != nil {
		return ctrl.Result{}, err
	}
	// Keep intent until a subsequent API read observes the gate removed.
	return ctrl.Result{Requeue: true}, nil
}

func (r *InstanceReconciler) gateReleaseExpired(intent *v1alpha1.GateReleaseIntent) bool {
	return !intent.StartedAt.IsZero() && !r.now().Before(intent.StartedAt.Add(r.pendingTimeout()))
}

func (r *InstanceReconciler) requestRetiredGateWithdrawal(ctx context.Context, instance *v1alpha1.OpenSearchServerless, intent *v1alpha1.GateReleaseIntent) (ctrl.Result, error) {
	before := instance.DeepCopyObject().(*v1alpha1.OpenSearchServerless)
	if err := appendRetiredCreation(instance, &intent.Pod); err != nil {
		return ctrl.Result{RequeueAfter: time.Minute}, err
	}
	intent.WithdrawalRequested = true
	if err := r.Status().Patch(ctx, instance, client.MergeFromWithOptions(before, client.MergeFromWithOptimisticLock{})); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{Requeue: true}, nil
}
