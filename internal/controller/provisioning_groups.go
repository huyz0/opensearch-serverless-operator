package controller

import (
	"context"
	"fmt"
	"reflect"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/huyz0/opensearch-serverless-operator/api/v1alpha1"
)

// archiveBlockedAttempt isolates a failed or timed-out attempt by role and AZ.
// Recoverable bound Pods continue to be observed after isolation.
// Older attempts without group identity deliberately remain instance-wide.
func (r *InstanceReconciler) archiveBlockedAttempt(ctx context.Context, instance *v1alpha1.OpenSearchServerless) (ctrl.Result, bool, error) {
	attempt := instance.Status.ProvisioningAttempt
	if attempt == nil || attempt.Phase != "Blocked" || !isolatableReason(attempt.Reason) || attempt.Role == "" || attempt.Zone == "" {
		return ctrl.Result{}, false, nil
	}
	if attempt.Name == "" || attempt.PodUID == "" || attempt.StartedAt.IsZero() || len(instance.Status.BlockedProvisioningAttempts) >= 64 {
		return ctrl.Result{RequeueAfter: time.Minute}, false, fmt.Errorf("cannot isolate incomplete attempt or full blocked group history")
	}
	for _, held := range instance.Status.BlockedProvisioningAttempts {
		if held.PodUID == attempt.PodUID || (held.Role == attempt.Role && held.Zone == attempt.Zone) {
			return ctrl.Result{RequeueAfter: time.Minute}, false, fmt.Errorf("duplicate blocked provisioning identity or group")
		}
	}
	before := instance.DeepCopyObject().(*v1alpha1.OpenSearchServerless)
	instance.Status.BlockedProvisioningAttempts = append(instance.Status.BlockedProvisioningAttempts, *attempt)
	instance.Status.ProvisioningAttempt = nil
	meta.RemoveStatusCondition(&instance.Status.Conditions, "ProvisioningBlocked")
	meta.SetStatusCondition(&instance.Status.Conditions, metav1.Condition{Type: "ProvisioningGroupsBlocked", Status: metav1.ConditionTrue, Reason: "BlockedAttempts", Message: "One or more role and AZ groups have a blocked provisioning attempt", ObservedGeneration: instance.Generation})
	if err := r.Status().Patch(ctx, instance, client.MergeFromWithOptions(before, client.MergeFromWithOptimisticLock{})); err != nil {
		return ctrl.Result{}, false, err
	}
	return ctrl.Result{Requeue: true}, true, nil
}

type provisioningGroup struct{ role, zone string }

// confirmedOutageZones uses the opted-in namespaced report. Missing or
// expired evidence leaves ordinary provisioning under its existing policy.
func (r *InstanceReconciler) confirmedOutageZones(ctx context.Context, instance *v1alpha1.OpenSearchServerless) map[string]bool {
	availability := instance.Spec.Availability
	if availability == nil || availability.EmergencyCapacity == nil {
		return nil
	}
	evidence, _, err := r.readEmergencyCapacityEvidence(ctx, instance, availability.EmergencyCapacity.EvidenceRef.Name)
	if err != nil {
		return nil
	}
	zones := make(map[string]bool, len(evidence.UnavailableZones))
	for _, zone := range evidence.UnavailableZones {
		zones[zone] = true
	}
	return zones
}

func (r *InstanceReconciler) provisioningHolds(ctx context.Context, instance *v1alpha1.OpenSearchServerless) (map[provisioningGroup]bool, error) {
	groups, err := blockedProvisioningGroups(instance)
	if err != nil {
		return nil, err
	}
	for zone := range r.confirmedOutageZones(ctx, instance) {
		for _, role := range []string{"search", "ingest"} {
			groups[provisioningGroup{role, zone}] = true
		}
	}
	return groups, nil
}

func blockedProvisioningGroups(instance *v1alpha1.OpenSearchServerless) (map[provisioningGroup]bool, error) {
	groups := map[provisioningGroup]bool{}
	uids := map[string]bool{}
	if len(instance.Status.BlockedProvisioningAttempts) > 64 {
		return nil, fmt.Errorf("too many blocked provisioning groups")
	}
	for _, held := range instance.Status.BlockedProvisioningAttempts {
		key := provisioningGroup{held.Role, held.Zone}
		if held.Role == "" || held.Zone == "" || held.Name == "" || held.PodUID == "" || held.StartedAt.IsZero() || held.Phase != "Blocked" || !validGroupHoldReason(held.Reason) || groups[key] || uids[held.PodUID] {
			return nil, fmt.Errorf("blocked provisioning group state is invalid")
		}
		groups[key] = true
		uids[held.PodUID] = true
	}
	return groups, nil
}

func recoverableBoundReason(reason string) bool {
	return reason == "StartupTimeout" || reason == "PluginInstallTimeout" || reason == "PeerDNSPreflightTimeout"
}

func isolatableReason(reason string) bool {
	return recoverableBoundReason(reason) || reason == "PodGone" || reason == "PodTerminating" || reason == "PodTerminal"
}

func validGroupHoldReason(reason string) bool {
	return isolatableReason(reason) || reason == "IdentityChanged" || reason == "BindingChanged" || reason == "PeerDNSIdentityChanged" || reason == "PluginInstallIdentityChanged"
}

// observeIsolatedAttempts observes at most 64 held identities. A recovered Pod
// removes only its own hold; unknown observations keep it. Any change uses an
// optimistic status patch so a concurrent retry cannot clear a different hold.
func (r *InstanceReconciler) observeIsolatedAttempts(ctx context.Context, instance *v1alpha1.OpenSearchServerless) (ctrl.Result, bool, error) {
	if _, err := blockedProvisioningGroups(instance); err != nil {
		return ctrl.Result{RequeueAfter: time.Minute}, false, err
	}
	observing := false
	for i := range instance.Status.BlockedProvisioningAttempts {
		attempt := &instance.Status.BlockedProvisioningAttempts[i]
		if !recoverableBoundReason(attempt.Reason) {
			continue
		}
		observing = true
		pod, err := r.Core.Pods(instance.Namespace).Get(ctx, attempt.Name, metav1.GetOptions{})
		reason := ""
		switch {
		case apierrors.IsNotFound(err):
			reason = "PodGone"
		case err != nil:
			return ctrl.Result{}, false, fmt.Errorf("observe isolated provisioning Pod: %w", err)
		default:
			switch {
			case !provisioningPodMatches(instance, attempt, pod):
				reason = "IdentityChanged"
			case pod.DeletionTimestamp != nil:
				reason = "PodTerminating"
			case pod.Status.Phase == corev1.PodFailed || pod.Status.Phase == corev1.PodSucceeded:
				reason = "PodTerminal"
			case pod.Spec.NodeName == "":
				reason = "BindingChanged"
			case attempt.PeerDNS && (pod.Annotations[peerDNSPreflightAnnotation] != "v1" || pod.Annotations[peerDomainAnnotation] == ""):
				reason = "PeerDNSIdentityChanged"
			default:
				if attempt.PluginInstall != nil {
					identity, parseErr := pluginIntentFromPod(pod)
					if parseErr != nil || identity == nil || identity.RequestUID == "" || identity.PolicyUID == "" || !reflect.DeepEqual(identity, attempt.PluginInstall) {
						reason = "PluginInstallIdentityChanged"
					}
				}
				if reason == "" && (!attempt.PeerDNS || peerDNSPreflightComplete(pod)) && (attempt.PluginInstall == nil || pluginInstallerComplete(pod)) && podStartupReady(pod) {
					before := instance.DeepCopyObject().(*v1alpha1.OpenSearchServerless)
					instance.Status.BlockedProvisioningAttempts = append(instance.Status.BlockedProvisioningAttempts[:i], instance.Status.BlockedProvisioningAttempts[i+1:]...)
					if len(instance.Status.BlockedProvisioningAttempts) == 0 {
						meta.RemoveStatusCondition(&instance.Status.Conditions, "ProvisioningGroupsBlocked")
					}
					if err := r.Status().Patch(ctx, instance, client.MergeFromWithOptions(before, client.MergeFromWithOptimisticLock{})); err != nil {
						return ctrl.Result{}, false, err
					}
					return ctrl.Result{Requeue: true}, true, nil
				}
			}
		}
		if reason != "" {
			before := instance.DeepCopyObject().(*v1alpha1.OpenSearchServerless)
			attempt.Reason = reason
			if err := r.Status().Patch(ctx, instance, client.MergeFromWithOptions(before, client.MergeFromWithOptimisticLock{})); err != nil {
				return ctrl.Result{}, false, err
			}
			return ctrl.Result{Requeue: true}, true, nil
		}
	}
	if observing {
		return ctrl.Result{RequeueAfter: 5 * time.Second}, false, nil
	}
	return ctrl.Result{}, false, nil
}
