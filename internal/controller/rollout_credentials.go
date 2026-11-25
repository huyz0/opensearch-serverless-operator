package controller

import (
	"context"
	"time"

	"github.com/huyz0/opensearch-serverless-operator/api/v1alpha1"
	"github.com/huyz0/opensearch-serverless-operator/internal/java"
	"github.com/huyz0/opensearch-serverless-operator/internal/rollout"
)

func (r podHTTPAccess) verifyRolloutCredential(ctx context.Context, instance *v1alpha1.OpenSearchServerless, role string, observation candidateRoleObservation, credential *rollout.RouteCredential) time.Time {
	processes := make(map[string]string, len(observation.Pods))
	for _, pod := range observation.Pods {
		processes[pod.PodUID] = pod.ProcessIncarnation
	}
	var validUntil time.Time
	for i := range observation.PodSnapshot {
		pod := &observation.PodSnapshot[i]
		started := time.Now()
		client, endpoint, err := r.developmentPodHTTPWithAuth(ctx, instance, pod, 9200, &podHealthCredential{username: credential.Username, password: credential.Password})
		if err != nil {
			return time.Time{}
		}
		lease, err := java.GetLease(ctx, client, endpoint)
		client.CloseIdleConnections()
		deadline := lease.EvidenceValidUntil(started, 5*time.Second)
		if err != nil || lease.NodeName != pod.Name || !lease.HasRole(role) || lease.ProcessIncarnation == "" ||
			lease.ProcessIncarnation != processes[string(pod.UID)] || !time.Now().Before(deadline) {
			return time.Time{}
		}
		if validUntil.IsZero() || deadline.Before(validUntil) {
			validUntil = deadline
		}
	}
	return validUntil
}
