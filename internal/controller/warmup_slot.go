package controller

import (
	"context"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/huyz0/opensearch-serverless-operator/api/v1alpha1"
	"github.com/huyz0/opensearch-serverless-operator/internal/java"
)

func runningOpenSearch(pod *corev1.Pod) *corev1.ContainerStatus {
	for i := range pod.Status.ContainerStatuses {
		status := &pod.Status.ContainerStatuses[i]
		if status.Name == "opensearch" && status.State.Running != nil && status.ContainerID != "" {
			return status
		}
	}
	return nil
}

func (r *InstanceReconciler) observePodWarmup(ctx context.Context, instance *v1alpha1.OpenSearchServerless, pod *corev1.Pod, port int) (java.Warmup, error) {
	ctx, cancel := context.WithTimeout(ctx, 6*time.Second)
	defer cancel()
	client, endpoint, err := r.podHTTP().developmentPodHTTP(ctx, instance, pod, port)
	if err != nil {
		return java.Warmup{}, err
	}
	defer client.CloseIdleConnections()
	warmup, err := java.GetWarmup(ctx, client, endpoint)
	if err != nil {
		return java.Warmup{}, err
	}
	lease, err := java.GetLease(ctx, client, endpoint)
	if err != nil || lease.NodeName != pod.Name || (lease.ProcessIncarnation != "" && lease.ProcessIncarnation != warmup.ProcessIncarnation) ||
		(hasManagedHTTPS(instance) && lease.ProcessIncarnation == "") {
		return java.Warmup{}, fmt.Errorf("local Java node identity is unavailable or mismatched")
	}
	after, err := java.GetWarmup(ctx, client, endpoint)
	if err != nil || after != warmup {
		return java.Warmup{}, fmt.Errorf("Java warm-up state changed during observation")
	}
	return warmup, nil
}

// reconcileWarmupSlot never interprets traffic readiness or timeout as worker
// completion. It observes a completed result twice for the same Pod, container
// and process before releasing the operator-initiated warm-up slot.
func (r *InstanceReconciler) reconcileWarmupSlot(ctx context.Context, instance *v1alpha1.OpenSearchServerless) (ctrl.Result, bool, error) {
	slot := instance.Status.SearchWarmupSlot
	if slot == nil {
		return ctrl.Result{}, false, nil
	}
	if slot.Name == "" || slot.PodUID == "" {
		return ctrl.Result{RequeueAfter: time.Minute}, false, fmt.Errorf("warm-up slot lacks Pod identity")
	}
	pods := r.Core.Pods(instance.Namespace)
	pod, err := pods.Get(ctx, slot.Name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) || (err == nil && string(pod.UID) != slot.PodUID) {
		before := instance.DeepCopyObject().(*v1alpha1.OpenSearchServerless)
		instance.Status.SearchWarmupSlot = nil
		if err := r.Status().Patch(ctx, instance, client.MergeFromWithOptions(before, client.MergeFromWithOptimisticLock{})); err != nil {
			return ctrl.Result{}, false, err
		}
		return ctrl.Result{Requeue: true}, true, nil
	}
	if err != nil {
		return ctrl.Result{}, false, err
	}
	owner := metav1.GetControllerOf(pod)
	if owner == nil || owner.UID != instance.UID {
		return ctrl.Result{RequeueAfter: time.Minute}, false, fmt.Errorf("warm-up Pod owner changed")
	}
	container := runningOpenSearch(pod)
	if pod.DeletionTimestamp != nil || pod.Spec.NodeName == "" || container == nil || pod.Status.PodIP == "" {
		return ctrl.Result{RequeueAfter: 5 * time.Second}, false, nil
	}
	observer := r.ObserveWarmup
	if observer == nil {
		observer = func(ctx context.Context, pod *corev1.Pod) (java.Warmup, error) {
			return r.observePodWarmup(ctx, instance, pod, 9200)
		}
	}
	warmup, err := observer(ctx, pod)
	if err != nil || warmup.Result == "not_requested" || warmup.ProcessIncarnation == "" {
		return ctrl.Result{RequeueAfter: 5 * time.Second}, false, nil
	}
	fresh, err := pods.Get(ctx, pod.Name, metav1.GetOptions{})
	if err != nil {
		return ctrl.Result{}, false, err
	}
	freshContainer := runningOpenSearch(fresh)
	if fresh.UID != pod.UID || fresh.ResourceVersion != pod.ResourceVersion || fresh.Status.PodIP != pod.Status.PodIP || fresh.DeletionTimestamp != nil || freshContainer == nil || freshContainer.ContainerID != container.ContainerID || freshContainer.RestartCount != container.RestartCount {
		return ctrl.Result{RequeueAfter: 5 * time.Second}, false, nil
	}
	same := slot.ContainerID == container.ContainerID && slot.RestartCount == container.RestartCount && slot.ProcessIncarnation == warmup.ProcessIncarnation && slot.Result == warmup.Result && slot.TimedOut == warmup.TimedOut
	if same && warmup.Finished() {
		before := instance.DeepCopyObject().(*v1alpha1.OpenSearchServerless)
		instance.Status.LastSearchWarmup = &v1alpha1.SearchWarmupObservation{Name: pod.Name, PodUID: string(pod.UID), ContainerID: container.ContainerID, ProcessIncarnation: warmup.ProcessIncarnation, Result: warmup.Result, TimedOut: warmup.TimedOut, ObservedAt: metav1.NewTime(r.now())}
		instance.Status.SearchWarmupSlot = nil
		if err := r.Status().Patch(ctx, instance, client.MergeFromWithOptions(before, client.MergeFromWithOptimisticLock{})); err != nil {
			return ctrl.Result{}, false, err
		}
		return ctrl.Result{Requeue: true}, true, nil
	}
	if !same {
		before := instance.DeepCopyObject().(*v1alpha1.OpenSearchServerless)
		slot.ContainerID = container.ContainerID
		slot.RestartCount = container.RestartCount
		slot.ProcessIncarnation = warmup.ProcessIncarnation
		slot.Result = warmup.Result
		slot.TimedOut = warmup.TimedOut
		if err := r.Status().Patch(ctx, instance, client.MergeFromWithOptions(before, client.MergeFromWithOptimisticLock{})); err != nil {
			return ctrl.Result{}, false, err
		}
		return ctrl.Result{Requeue: true}, true, nil
	}
	return ctrl.Result{RequeueAfter: 5 * time.Second}, false, nil
}
