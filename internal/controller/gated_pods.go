package controller

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"k8s.io/apimachinery/pkg/util/validation"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/huyz0/opensearch-serverless-operator/api/v1alpha1"
	podkube "github.com/huyz0/opensearch-serverless-operator/pkg/podlifecycle/kubernetes"
)

const creationIntentKey = "serverless.opensearch.org/creation-intent"
const profileUIDAnnotation = "serverless.opensearch.org/capacity-profile-uid"
const profileVersionAnnotation = "serverless.opensearch.org/capacity-profile-version"
const peerDomainAnnotation = "serverless.opensearch.org/peer-dns-domain"
const peerDNSPreflightAnnotation = "serverless.opensearch.org/peer-dns-preflight"
const warmupModeAnnotation = "serverless.opensearch.org/warmup-mode"

// Bump when operator-owned Pod fields change independently of the instance
// spec. In-flight work from an older template must not be adopted as new.
const developmentPodTemplateVersion = "v2-cross-namespace-affinity-and-token-mount"

func validDNSHost(host string) bool {
	if len(validation.IsDNS1123Subdomain(host)) != 0 {
		return false
	}
	for _, label := range strings.Split(host, ".") {
		if len(validation.IsDNS1123Label(label)) != 0 {
			return false
		}
	}
	return true
}

func (r *InstanceReconciler) peerDNSDomain(instance *v1alpha1.OpenSearchServerless) (string, error) {
	return r.podHTTP().peerDNSDomain(instance)
}

type gatedPodInputs struct {
	domain, keystoreUID string
	peerTLS             *peerTLSInput
	revisions           templateRevisions
	blockedGroups       map[provisioningGroup]bool
}

// recoverGatedPreflight handles completed or stale work before resolving new inputs.
func (r *InstanceReconciler) recoverGatedPreflight(ctx context.Context, instance *v1alpha1.OpenSearchServerless) (ctrl.Result, bool, error) {
	if intent := instance.Status.PendingGateRelease; intent != nil && (intent.WithdrawalRequested || (instance.Status.ActiveOperation == nil && r.gateReleaseExpired(intent))) {
		result, err := r.reconcileGateRelease(ctx, instance, "")
		return result, true, err
	}
	if instance.Status.ActiveOperation == nil {
		return r.recoverStalePreflight(ctx, instance)
	}
	return ctrl.Result{}, false, nil
}

// advanceGatedPod runs after the instance has recovered recorded preflights.
func (r *InstanceReconciler) advanceGatedPod(ctx context.Context, instance *v1alpha1.OpenSearchServerless) (ctrl.Result, error) {
	inputs, result, err := r.resolveGatedPodInputs(ctx, instance)
	if inputs == nil {
		return result, err
	}
	if intent := instance.Status.PendingGateRelease; intent != nil {
		if intent.Pod.KeystoreUID != inputs.keystoreUID {
			return ctrl.Result{RequeueAfter: time.Minute}, fmt.Errorf("keystore Secret identity changed before gate release")
		}
		revision := inputs.revisions.forFleetTemplate(intent.Pod.Fleet).compatibleOrCurrent(intent.Pod.Revision)
		return r.reconcileGateRelease(ctx, instance, revision)
	}
	if intent := instance.Status.PendingPodCreation; intent != nil {
		return r.executeGatedPodCreation(ctx, instance, intent, *inputs)
	}
	return r.reconcileGatedPodCapacity(ctx, instance, *inputs)
}

func (r *InstanceReconciler) resolveGatedPodInputs(ctx context.Context, instance *v1alpha1.OpenSearchServerless) (*gatedPodInputs, ctrl.Result, error) {
	ref, err := basicAuthReference(instance)
	if err != nil || (ref != nil && (instance.Spec.TLS == nil || instance.Spec.TLS.HTTP == nil || r.DevelopmentBasicAuthImage != instance.Spec.Image || r.DevelopmentBasicAuthImage == "")) {
		if err == nil {
			err = fmt.Errorf("configured Basic Auth requires the approved development HTTPS image")
		}
		return nil, ctrl.Result{RequeueAfter: time.Minute}, err
	}
	domain, err := r.peerDNSDomain(instance)
	if err != nil {
		return nil, ctrl.Result{RequeueAfter: time.Minute}, err
	}
	peerTLS, err := r.resolvePeerTLS(ctx, instance, domain)
	if errors.Is(err, errPeerTLSProvisionalSuperseded) {
		result, err := r.resetProvisionalPeerTLS(ctx, instance)
		return nil, result, err
	}
	if err != nil {
		return nil, ctrl.Result{RequeueAfter: time.Minute}, err
	}
	if peerTLS != nil && peerTLS.recoveredSnapshotUID != "" {
		before := instance.DeepCopyObject().(*v1alpha1.OpenSearchServerless)
		instance.Status.PeerTLSBaseline.SnapshotUID = string(peerTLS.recoveredSnapshotUID)
		if intent := instance.Status.PendingPodCreation; intent != nil {
			intent.PeerTLSSnapshotUID = string(peerTLS.recoveredSnapshotUID)
		}
		if err := r.Status().Patch(ctx, instance, client.MergeFromWithOptions(before, client.MergeFromWithOptimisticLock{})); err != nil {
			return nil, ctrl.Result{}, err
		}
		return nil, ctrl.Result{Requeue: true}, nil
	}
	if instance.Status.HTTPTLSBaseline == nil || instance.Status.HTTPTLSBaseline.LeafSHA256 == "" {
		httpTLS, err := r.resolveHTTPTLS(ctx, instance, domain)
		if errors.Is(err, errHTTPLeafStaging) {
			return nil, ctrl.Result{Requeue: true}, nil
		}
		if err != nil {
			return nil, ctrl.Result{RequeueAfter: time.Minute}, err
		}
		if httpTLS != nil {
			if instance.Status.HTTPTLSBaseline == nil {
				if err := httpTLS.validatePod(instance, podNameForSequence(instance, 1), r.now()); err != nil {
					return nil, ctrl.Result{RequeueAfter: time.Minute}, err
				}
			}
			before := instance.DeepCopyObject().(*v1alpha1.OpenSearchServerless)
			instance.Status.HTTPTLSBaseline = &httpTLS.baseline
			instance.Status.PendingHTTPTLS = nil
			if err := r.Status().Patch(ctx, instance, client.MergeFromWithOptions(before, client.MergeFromWithOptimisticLock{})); err != nil {
				return nil, ctrl.Result{}, err
			}
			return nil, ctrl.Result{Requeue: true}, nil
		}
	} else if !httpTLSBaseMatches(instance, domain) {
		return nil, ctrl.Result{RequeueAfter: time.Minute}, fmt.Errorf("HTTP TLS configuration differs from the pinned baseline")
	}
	authPins, _, err := r.resolveBasicAuth(ctx, instance)
	if err != nil {
		return nil, ctrl.Result{RequeueAfter: time.Minute}, err
	}
	if authPins != nil && instance.Status.BasicAuthBaseline == nil {
		before := instance.DeepCopyObject().(*v1alpha1.OpenSearchServerless)
		instance.Status.BasicAuthBaseline = authPins
		if err := r.Status().Patch(ctx, instance, client.MergeFromWithOptions(before, client.MergeFromWithOptimisticLock{})); err != nil {
			return nil, ctrl.Result{}, err
		}
		return nil, ctrl.Result{Requeue: true}, nil
	}
	if err := ensurePeerDNSService(ctx, r.Core, instance); err != nil {
		return nil, ctrl.Result{RequeueAfter: time.Minute}, err
	}
	if pool, ok := instance.Spec.Pools["search"]; ok && pool.WarmOnJoin && (r.DevelopmentWarmupImage == "" || instance.Spec.Image != r.DevelopmentWarmupImage) {
		return nil, ctrl.Result{RequeueAfter: time.Minute}, fmt.Errorf("warm-on-join requires the explicitly qualified development image digest")
	}
	if pool, ok := instance.Spec.Pools["search"]; ok && pool.WarmReadiness == "Strict" && pool.ColdStartPolicy != "AllowOneDegraded" {
		return nil, ctrl.Result{RequeueAfter: time.Minute}, fmt.Errorf("development strict search requires explicit AllowOneDegraded cold start policy")
	}
	keystoreUID, err := r.resolveKeystore(ctx, instance)
	if err != nil {
		return nil, ctrl.Result{RequeueAfter: time.Minute}, err
	}
	if authPins != nil && authPins.SecretUID != keystoreUID {
		return nil, ctrl.Result{RequeueAfter: time.Minute}, fmt.Errorf("startup authentication and keystore Secret identities differ")
	}
	revisions, err := podRevisions(instance, domain)
	if err != nil {
		return nil, ctrl.Result{}, err
	}
	blockedGroups, err := r.provisioningHolds(ctx, instance)
	if err != nil {
		return nil, ctrl.Result{RequeueAfter: time.Minute}, err
	}
	return &gatedPodInputs{domain: domain, keystoreUID: keystoreUID, peerTLS: peerTLS, revisions: revisions, blockedGroups: blockedGroups}, ctrl.Result{}, nil
}

func (r *InstanceReconciler) executeGatedPodCreation(ctx context.Context, instance *v1alpha1.OpenSearchServerless, intent *v1alpha1.PodCreationIntent, inputs gatedPodInputs) (ctrl.Result, error) {
	domain, keystoreUID := inputs.domain, inputs.keystoreUID
	revisions, blockedGroups := inputs.revisions, inputs.blockedGroups
	peerTLS := peerTLSForFleet(instance, intent.Fleet, inputs.peerTLS)
	if intent.WarmupMode == "Bootstrap" {
		bootstrap := instance.Status.ColdBootstrap
		if bootstrap == nil || bootstrap.Name != intent.Name || bootstrap.Fleet != intent.Fleet || bootstrap.Sequence != intent.Sequence {
			return ctrl.Result{RequeueAfter: time.Minute}, fmt.Errorf("bootstrap slot differs from persisted creation intent")
		}
	}
	if intent.Generation != instance.Generation || !revisions.forFleetTemplate(intent.Fleet).accepts(intent.Revision) || !servingRevisionMatchesIntent(instance, intent) || !fleetAllowedForPod(instance, intent.Role, intent.Zone, intent.Fleet) || intent.KeystoreUID != keystoreUID {
		return r.recoverStaleGatedCreation(ctx, instance)
	}
	if blockedGroups[provisioningGroup{intent.Role, intent.Zone}] {
		return r.recoverStaleGatedCreation(ctx, instance)
	}
	if !r.emergencyIntentStillFits(ctx, instance, intent, "") {
		return r.recoverStaleGatedCreation(ctx, instance)
	}
	if err := r.checkCandidateAdmission(ctx, instance, intent); err != nil {
		if errors.Is(err, errPreviewServingCapacity) {
			return r.recoverStaleGatedCreation(ctx, instance)
		}
		return ctrl.Result{RequeueAfter: time.Minute}, err
	}
	if peerTLS != nil && instance.Status.PeerTLSBaseline == nil {
		return ctrl.Result{RequeueAfter: time.Minute}, fmt.Errorf("peer TLS baseline was not persisted")
	}
	pluginInputs, err := r.pluginsForFleetRole(ctx, instance, intent.Fleet, intent.Role)
	if err != nil {
		return ctrl.Result{RequeueAfter: time.Minute}, err
	}
	if err := checkPluginIntent(instance, intent.Name, intent.PluginInstall, pluginInputs); err != nil {
		return ctrl.Result{RequeueAfter: time.Minute}, err
	}
	if intent.PluginInstall != nil {
		requestUID, policyUID, err := r.ensurePluginSnapshots(ctx, instance, intent.PluginInstall, pluginInputs)
		if err != nil {
			return ctrl.Result{RequeueAfter: time.Minute}, err
		}
		if intent.PluginInstall.RequestUID == "" {
			before := instance.DeepCopyObject().(*v1alpha1.OpenSearchServerless)
			intent.PluginInstall.RequestUID, intent.PluginInstall.PolicyUID = requestUID, policyUID
			if err := r.Status().Patch(ctx, instance, client.MergeFromWithOptions(before, client.MergeFromWithOptimisticLock{})); err != nil {
				return ctrl.Result{}, err
			}
			return ctrl.Result{Requeue: true}, nil
		}
	}
	profile, err := r.resolveCapacityProfile(ctx, instance, intent.Role, intent.Zone)
	if err != nil {
		return ctrl.Result{RequeueAfter: time.Minute}, err
	}
	if err := checkProfileIdentity(intent, profile); err != nil {
		return ctrl.Result{RequeueAfter: time.Minute}, err
	}
	if err := r.checkPeerTLSIntent(ctx, instance, intent, peerTLS, domain); err != nil {
		return ctrl.Result{RequeueAfter: time.Minute}, err
	}
	httpTLS, err := r.resolveHTTPTLSForFleet(ctx, instance, domain, intent.Fleet)
	if err != nil {
		return ctrl.Result{RequeueAfter: time.Minute}, err
	}
	if err := httpTLS.validatePod(instance, intent.Name, r.now()); err != nil {
		return ctrl.Result{RequeueAfter: time.Minute}, err
	}
	if peerTLS != nil && peerTLS.baseline.SnapshotUID == "" {
		staged, err := r.ensurePeerSnapshot(ctx, instance, intent, peerTLS, domain)
		if err != nil {
			return ctrl.Result{RequeueAfter: time.Minute}, err
		}
		before := instance.DeepCopyObject().(*v1alpha1.OpenSearchServerless)
		instance.Status.PeerTLSBaseline.SnapshotUID = string(staged.UID)
		instance.Status.PeerTLSBaseline.LeafPEMSHA256 = peerTLS.baseline.LeafPEMSHA256
		intent.PeerTLSSnapshotUID = string(staged.UID)
		if err := r.Status().Patch(ctx, instance, client.MergeFromWithOptions(before, client.MergeFromWithOptimisticLock{})); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{Requeue: true}, nil
	}
	pod, err := renderGatedPod(instance, intent, profile, domain, r.DevelopmentRuntimeSettingsImage)
	if err != nil {
		return ctrl.Result{RequeueAfter: time.Minute}, err
	}
	if err := r.checkKeystoreUID(ctx, instance, intent.KeystoreUID); err != nil {
		return ctrl.Result{RequeueAfter: time.Minute}, err
	}
	admitted, err := podkube.CreatePod(ctx, r.Core, pod, creationIntentKey, fmt.Sprintf("%s/%d", instance.UID, intent.Sequence))
	if err != nil {
		return ctrl.Result{}, err
	}
	if err := validateAdmittedGatedPod(pod, admitted); err != nil {
		return ctrl.Result{RequeueAfter: time.Minute}, fmt.Errorf("admitted Pod does not match creation intent: %w", err)
	}
	if err := validateManagedTLSKeyAccess(instance, admitted); err != nil {
		return ctrl.Result{RequeueAfter: time.Minute}, err
	}
	if intent.WarmupMode == "Bootstrap" && instance.Status.ColdBootstrap.PodUID != "" && instance.Status.ColdBootstrap.PodUID != string(admitted.UID) {
		return ctrl.Result{RequeueAfter: time.Minute}, fmt.Errorf("bootstrap Pod UID changed")
	}
	before := instance.DeepCopyObject().(*v1alpha1.OpenSearchServerless)
	if intent.WarmupMode == "Bootstrap" {
		instance.Status.ColdBootstrap.PodUID = string(admitted.UID)
	}
	instance.Status.PendingPodCreation = nil
	if err := r.Status().Patch(ctx, instance, client.MergeFromWithOptions(before, client.MergeFromWithOptimisticLock{})); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{Requeue: true}, nil
}
