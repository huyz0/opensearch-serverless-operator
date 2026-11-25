package controller

import (
	"context"
	"fmt"
	"net/http"
	"regexp"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation"
	typedcore "k8s.io/client-go/kubernetes/typed/core/v1"

	"github.com/huyz0/opensearch-serverless-operator/api/v1alpha1"
	"github.com/huyz0/opensearch-serverless-operator/internal/java"
	"github.com/huyz0/opensearch-serverless-operator/internal/workload"
)

var qualifiedRoleProbeImage = regexp.MustCompile(`^.+@sha256:[a-f0-9]{64}$`)

// roleServiceProbe is a historical, single-response observation. It is not
// evidence of an external route, current cache warmth or future availability.
type roleServiceProbe struct {
	ServiceUID     types.UID
	ServiceVersion string
	PodName        string
	PodUID         types.UID
	NodeID         string
	ObservedAt     time.Time
}

// probeDevelopmentRoleService sends one stats request through the stable role
// Service and matches the returned node name to an owned, Ready Pod. The caller
// supplies the allowed Pod UID set and an isolated HTTP client without ambient
// proxying. Application qualification remains the caller's responsibility. The development path is plaintext and is not
// authenticated: namespace and network access must be restricted. The request
// and API reads are not atomic, nor proof that an ingress or ALB has converged.
func probeDevelopmentRoleService(ctx context.Context, core typedcore.CoreV1Interface, instance *v1alpha1.OpenSearchServerless, qualifiedImage, role, fleet string, serviceUID types.UID, allowed map[types.UID]struct{}, client *http.Client, minimumLease time.Duration) (roleServiceProbe, error) {
	var out roleServiceProbe
	if core == nil || instance == nil || instance.UID == "" || instance.Spec.Image != qualifiedImage || !qualifiedRoleProbeImage.MatchString(qualifiedImage) || instance.Spec.Security != nil || (instance.Spec.TLS != nil && instance.Spec.TLS.HTTP != nil) || client == nil || serviceUID == "" || len(allowed) == 0 || minimumLease < 0 {
		return out, fmt.Errorf("qualified isolated HTTP runtime, Service and Pod identities are required")
	}
	desired, err := workload.RoleService(instance, role, fleet, 9200)
	if err != nil {
		return out, err
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	services := core.Services(instance.Namespace)
	before, err := services.Get(ctx, desired.Name, metav1.GetOptions{})
	if err != nil {
		return out, fmt.Errorf("read role Service: %w", err)
	}
	if !matchesFleetService(instance, before, desired, serviceUID) {
		return out, fmt.Errorf("role Service identity or selector changed")
	}
	// A redirect could escape the intended Service even if the caller's client
	// normally follows redirects. Copying leaves the caller's client untouched.
	probeClient := *client
	probeClient.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	start := time.Now()
	endpoint := fmt.Sprintf("http://%s.%s.svc:9200", desired.Name, instance.Namespace)
	lease, err := java.GetLease(ctx, &probeClient, endpoint)
	if err != nil {
		return out, fmt.Errorf("probe role Service: %w", err)
	}
	if len(validation.IsDNS1123Subdomain(lease.NodeName)) != 0 || !lease.Valid || lease.ExpiresInMillis <= minimumLease.Milliseconds() || !lease.HasRole(role) {
		return out, fmt.Errorf("responding process has no sufficiently valid lease")
	}
	pods := core.Pods(instance.Namespace)
	pod, err := pods.Get(ctx, lease.NodeName, metav1.GetOptions{})
	if err != nil {
		return out, fmt.Errorf("read responding Pod: %w", err)
	}
	_, ok := allowed[pod.UID]
	if !ok || !matchesRoleProbePod(instance, pod, role, fleet) {
		return out, fmt.Errorf("responding Pod does not match the allowed fleet member")
	}
	again, err := pods.Get(ctx, pod.Name, metav1.GetOptions{})
	if err != nil {
		return out, fmt.Errorf("re-read responding Pod: %w", err)
	}
	if again.UID != pod.UID || again.ResourceVersion != pod.ResourceVersion {
		return out, fmt.Errorf("responding Pod changed during probe")
	}
	after, err := services.Get(ctx, desired.Name, metav1.GetOptions{})
	if err != nil {
		return out, fmt.Errorf("re-read role Service: %w", err)
	}
	if !matchesFleetService(instance, after, desired, serviceUID) || after.ResourceVersion != before.ResourceVersion {
		return out, fmt.Errorf("role Service changed during probe")
	}
	elapsed := time.Since(start).Milliseconds()
	if lease.ExpiresInMillis <= elapsed || lease.ExpiresInMillis-elapsed <= minimumLease.Milliseconds() {
		return out, fmt.Errorf("responding process lease expired during probe")
	}
	return roleServiceProbe{ServiceUID: serviceUID, ServiceVersion: before.ResourceVersion, PodName: pod.Name, PodUID: pod.UID, NodeID: lease.NodeID, ObservedAt: start}, nil
}

func matchesRoleProbePod(instance *v1alpha1.OpenSearchServerless, pod *corev1.Pod, role, fleet string) bool {
	if !podReadyForServing(pod) || pod.UID == "" || pod.ResourceVersion == "" || pod.Status.PodIP == "" || pod.Namespace != instance.Namespace {
		return false
	}
	owner := metav1.GetControllerOf(pod)
	return owner != nil && owner.APIVersion == v1alpha1.GroupVersion.String() && owner.Kind == "OpenSearchServerless" && owner.Name == instance.Name && owner.UID == instance.UID &&
		pod.Labels[workload.InstanceLabel] == string(instance.UID) && pod.Labels[workload.RoleLabel] == role && pod.Labels[workload.FleetLabel] == fleet && pod.Labels[workload.ZoneLabel] != ""
}
