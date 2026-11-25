package controller

import (
	"context"
	"net/http"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/huyz0/opensearch-serverless-operator/api/v1alpha1"
	"github.com/huyz0/opensearch-serverless-operator/internal/workload"
)

// sampleRoleServiceProbe records historical development diagnostics only; it
// never authorizes rollout, retirement or production route convergence.
func (r *InstanceReconciler) sampleRoleServiceProbe(ctx context.Context, instance *v1alpha1.OpenSearchServerless) (bool, error) {
	if r.Core == nil || r.DevelopmentServiceProbeImage == "" || instance.Spec.Image != r.DevelopmentServiceProbeImage || !qualifiedRoleProbeImage.MatchString(r.DevelopmentServiceProbeImage) || instance.Spec.Security != nil || (instance.Spec.TLS != nil && instance.Spec.TLS.HTTP != nil) {
		return false, nil
	}
	now := r.now()
	if last := instance.Status.LastServiceProbe; last != nil && last.ObservedGeneration == instance.Generation && !now.Before(last.ObservedAt.Time) && now.Sub(last.ObservedAt.Time) < time.Minute {
		return false, nil
	}
	roles := make([]string, 0, 2)
	for _, role := range []string{"ingest", "search"} {
		pool, ok := instance.Spec.Pools[role]
		if !ok || instance.Status.ServingFleetIntentByRole[role] == "" {
			continue
		}
		for _, zone := range pool.Zones {
			if int64(zone.Replicas)+int64(zone.WarmSpare) > 0 {
				roles = append(roles, role)
				break
			}
		}
	}
	if len(roles) == 0 {
		return false, nil
	}
	role := roles[0]
	if last := instance.Status.LastServiceProbe; last != nil {
		for _, candidate := range roles {
			if candidate > last.Role {
				role = candidate
				break
			}
		}
	}
	fleet := instance.Status.ServingFleetIntentByRole[role]
	status := &v1alpha1.ServiceProbeStatus{ObservedGeneration: instance.Generation, Role: role, Fleet: fleet, Result: "Unavailable", ObservedAt: metav1.NewTime(now)}
	probeCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	desired, err := workload.RoleService(instance, role, fleet, 9200)
	if err != nil {
		return false, err
	}
	service, serviceErr := r.Core.Services(instance.Namespace).Get(probeCtx, desired.Name, metav1.GetOptions{})
	selector := labels.SelectorFromSet(labels.Set{workload.InstanceLabel: string(instance.UID), workload.RoleLabel: role, workload.FleetLabel: fleet}).String()
	pods, podErr := r.Core.Pods(instance.Namespace).List(probeCtx, metav1.ListOptions{LabelSelector: selector, Limit: 513})
	if serviceErr == nil && podErr == nil && service.UID != "" && pods.Continue == "" && len(pods.Items) <= 512 {
		allowed := make(map[types.UID]struct{}, len(pods.Items))
		for _, pod := range pods.Items {
			if pod.UID != "" {
				allowed[pod.UID] = struct{}{}
			}
		}
		httpClient := r.ServiceProbeClient
		if httpClient == nil {
			httpClient = &http.Client{Timeout: 5 * time.Second, Transport: &http.Transport{Proxy: nil, DisableKeepAlives: true}}
		}
		observation, probeErr := probeDevelopmentRoleService(probeCtx, r.Core, instance, r.DevelopmentServiceProbeImage, role, fleet, service.UID, allowed, httpClient, 5*time.Second)
		if probeErr == nil {
			status.Result = "ServiceResponseObserved"
			status.ServiceUID = string(observation.ServiceUID)
			status.ServiceVersion = observation.ServiceVersion
			status.PodName = observation.PodName
			status.PodUID = string(observation.PodUID)
		}
	}
	before := instance.DeepCopyObject().(*v1alpha1.OpenSearchServerless)
	instance.Status.LastServiceProbe = status
	if err := r.Status().Patch(ctx, instance, client.MergeFromWithOptions(before, client.MergeFromWithOptimisticLock{})); err != nil {
		return false, err
	}
	return true, nil
}
