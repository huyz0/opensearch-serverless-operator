package controller

import (
	"context"

	policyv1 "k8s.io/client-go/kubernetes/typed/policy/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/huyz0/opensearch-serverless-operator/api/v1alpha1"
)

// writerRuntime provides the read and execution dependencies for one writer handoff.
type writerRuntime struct {
	fleetQualification
	policy policyv1.PolicyV1Interface
	reader client.Reader
}

func (r *InstanceReconciler) writerRuntime() writerRuntime {
	return writerRuntime{fleetQualification: r.fleetQualification(), policy: r.Policy, reader: r.APIReader}
}

func (r writerRuntime) checkPinnedPrimaryStore(ctx context.Context, instance *v1alpha1.OpenSearchServerless) error {
	return checkPinnedPrimaryStore(ctx, r.core, instance)
}
