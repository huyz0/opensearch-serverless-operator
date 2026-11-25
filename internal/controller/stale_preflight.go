package controller

import (
	"context"
	"fmt"
	"time"

	ctrl "sigs.k8s.io/controller-runtime"

	"github.com/huyz0/opensearch-serverless-operator/api/v1alpha1"
)

// recoverStalePreflight only retires already recorded work. It does not
// resolve current credentials or authorize a new Pod or a gate release.
func (r *InstanceReconciler) recoverStalePreflight(ctx context.Context, instance *v1alpha1.OpenSearchServerless) (ctrl.Result, bool, error) {
	creation, release := instance.Status.PendingPodCreation, instance.Status.PendingGateRelease
	if creation == nil && release == nil {
		return ctrl.Result{}, false, nil
	}
	if creation != nil && release != nil {
		return ctrl.Result{RequeueAfter: time.Minute}, true, fmt.Errorf("creation and gate-release intents overlap")
	}
	var intent *v1alpha1.PodCreationIntent
	if creation != nil {
		intent = creation
	} else {
		intent = &release.Pod
	}
	stale := intent.Generation != instance.Generation || !fleetAllowedForPod(instance, intent.Role, intent.Zone, intent.Fleet)
	revision := ""
	if domain, err := r.peerDNSDomain(instance); err == nil {
		revisions, err := podRevisions(instance, domain)
		if err != nil {
			return ctrl.Result{}, true, err
		}
		revision = revisions.forFleetTemplate(intent.Fleet).compatibleOrCurrent(intent.Revision)
		stale = stale || intent.Revision != revision
	}
	if !stale {
		return ctrl.Result{}, false, nil
	}
	if creation != nil {
		if intent.WarmupMode == "Bootstrap" {
			bootstrap := instance.Status.ColdBootstrap
			if bootstrap == nil || bootstrap.Name != intent.Name || bootstrap.Fleet != intent.Fleet || bootstrap.Sequence != intent.Sequence {
				return ctrl.Result{RequeueAfter: time.Minute}, true, fmt.Errorf("bootstrap slot differs from stale creation intent")
			}
		}
		result, err := r.recoverStaleGatedCreation(ctx, instance)
		return result, true, err
	}
	result, err := r.reconcileGateRelease(ctx, instance, revision)
	return result, true, err
}
