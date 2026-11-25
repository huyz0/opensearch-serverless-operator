package controller

import (
	"context"
	"fmt"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/huyz0/opensearch-serverless-operator/api/v1alpha1"
	"github.com/huyz0/opensearch-serverless-operator/pkg/podlifecycle"
	podkube "github.com/huyz0/opensearch-serverless-operator/pkg/podlifecycle/kubernetes"
)

// Retired names remain recorded because the API cannot bound how late an old
// create request may arrive. Only one UID is pinned or withdrawn per pass.
func (r *InstanceReconciler) reconcileRetiredPodCreations(ctx context.Context, instance *v1alpha1.OpenSearchServerless) (ctrl.Result, bool, error) {
	if len(instance.Status.RetiredPodCreations) > maxRetiredPodCreations {
		return ctrl.Result{RequeueAfter: time.Minute}, true, fmt.Errorf("retired creation history exceeds its limit")
	}
	byName := make(map[string]struct{}, len(instance.Status.RetiredPodCreations))
	for i, item := range instance.Status.RetiredPodCreations {
		if item.Name == "" || item.Sequence <= 0 || item.Sequence > instance.Status.PodCreationSequence || item.Name != podNameForSequence(instance, item.Sequence) || item.Role == "" || item.Zone == "" || item.Fleet == "" || item.Revision == "" {
			return ctrl.Result{RequeueAfter: time.Minute}, true, fmt.Errorf("retired creation identity is incomplete")
		}
		if _, exists := byName[item.Name]; exists {
			return ctrl.Result{RequeueAfter: time.Minute}, true, fmt.Errorf("duplicate retired creation identity")
		}
		if (instance.Status.PendingPodCreation != nil && instance.Status.PendingPodCreation.Name == item.Name) || (instance.Status.PendingGateRelease != nil && instance.Status.PendingGateRelease.Pod.Name == item.Name) ||
			(instance.Status.ProvisioningAttempt != nil && instance.Status.ProvisioningAttempt.Name == item.Name &&
				(!item.Released || item.PodUID != instance.Status.ProvisioningAttempt.PodUID)) {
			return ctrl.Result{RequeueAfter: time.Minute}, true, fmt.Errorf("retired Pod name conflicts with active work")
		}
		byName[item.Name] = struct{}{}
		pod, err := r.Core.Pods(instance.Namespace).Get(ctx, item.Name, metav1.GetOptions{})
		if apierrors.IsNotFound(err) {
			if attempt := instance.Status.ProvisioningAttempt; item.Released && attempt != nil &&
				attempt.Name == item.Name && attempt.PodUID == item.PodUID {
				// The provisioning observer must account for this UID's
				// disappearance before late-create cleanup forgets the UID.
				continue
			}
			if item.PodUID == "" {
				continue
			}
			before := instance.DeepCopyObject().(*v1alpha1.OpenSearchServerless)
			instance.Status.RetiredPodCreations[i].PodUID = ""
			instance.Status.RetiredPodCreations[i].Released = false
			if err := r.Status().Patch(ctx, instance, client.MergeFromWithOptions(before, client.MergeFromWithOptimisticLock{})); err != nil {
				return ctrl.Result{}, true, err
			}
			return ctrl.Result{Requeue: true}, true, nil
		}
		if err != nil {
			return ctrl.Result{}, true, err
		}
		if item.PodUID != "" && string(pod.UID) != item.PodUID {
			return ctrl.Result{RequeueAfter: time.Minute}, true, fmt.Errorf("retired Pod name was reused with a different UID")
		}
		if item.Released && item.PodUID == string(pod.UID) && item.PodUID != "" && !hasPlacementGate(pod) {
			if err := validateCreationPodIdentity(instance, item, pod); err != nil {
				return ctrl.Result{RequeueAfter: time.Minute}, true, err
			}
			continue
		}
		if pod.DeletionTimestamp != nil && hasPlacementGate(pod) && pod.Spec.NodeName == "" {
			return ctrl.Result{RequeueAfter: time.Second}, true, nil
		}
		if err := validateRetiredPod(instance, item, pod); err != nil {
			return ctrl.Result{RequeueAfter: time.Minute}, true, err
		}
		if item.PodUID == "" {
			before := instance.DeepCopyObject().(*v1alpha1.OpenSearchServerless)
			instance.Status.RetiredPodCreations[i].PodUID = string(pod.UID)
			if err := r.Status().Patch(ctx, instance, client.MergeFromWithOptions(before, client.MergeFromWithOptimisticLock{})); err != nil {
				return ctrl.Result{}, true, err
			}
			return ctrl.Result{Requeue: true}, true, nil
		}
		target := podlifecycle.Identity{Namespace: pod.Namespace, Name: pod.Name, UID: string(pod.UID), OwnerUID: string(instance.UID)}
		if err := podkube.WithdrawGatedAtVersion(ctx, r.Core, target, "serverless.opensearch.org/placement", pod.ResourceVersion); err != nil {
			return ctrl.Result{}, true, err
		}
		return ctrl.Result{Requeue: true}, true, nil
	}
	return ctrl.Result{}, false, nil
}
