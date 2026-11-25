package controller

import (
	"context"
	"fmt"

	"github.com/huyz0/opensearch-serverless-operator/api/v1alpha1"
	"github.com/huyz0/opensearch-serverless-operator/internal/placement"
)

func (r *InstanceReconciler) resolveCapacityProfile(ctx context.Context, instance *v1alpha1.OpenSearchServerless, role, zone string) (*placement.Resolved, error) {
	pool, ok := instance.Spec.Pools[role]
	if !ok {
		return nil, fmt.Errorf("role pool is missing")
	}
	if pool.CapacityProfileRef == "" {
		return nil, nil
	}
	return placement.Resolve(ctx, r.Core, instance.Namespace, pool.CapacityProfileRef, role, zone, r.now())
}

func checkProfileIdentity(intent *v1alpha1.PodCreationIntent, profile *placement.Resolved) error {
	if profile == nil {
		if intent.CapacityProfileUID != "" || intent.CapacityProfileVersion != "" {
			return fmt.Errorf("recorded capacity profile is no longer present")
		}
		return nil
	}
	if intent.CapacityProfileUID != profile.UID || intent.CapacityProfileVersion != profile.ResourceVersion {
		return fmt.Errorf("capacity profile identity or version changed")
	}
	return nil
}
