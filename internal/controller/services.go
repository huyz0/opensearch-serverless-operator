package controller

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"reflect"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation"
	typedcore "k8s.io/client-go/kubernetes/typed/core/v1"

	"github.com/huyz0/opensearch-serverless-operator/api/v1alpha1"
	"github.com/huyz0/opensearch-serverless-operator/internal/workload"
)

func roleServiceShapeMatches(instance *v1alpha1.OpenSearchServerless, observed, desired *corev1.Service) bool {
	if instance == nil || observed == nil || desired == nil || len(desired.Spec.Ports) != 1 || len(observed.Spec.Ports) != 1 {
		return false
	}
	owner := metav1.GetControllerOf(observed)
	if observed.DeletionTimestamp != nil || owner == nil || owner.UID != instance.UID || owner.Name != instance.Name || owner.Kind != "OpenSearchServerless" || owner.APIVersion != v1alpha1.GroupVersion.String() {
		return false
	}
	spec := observed.Spec
	if spec.Type != corev1.ServiceTypeClusterIP || spec.ClusterIP == "" || spec.ClusterIP == corev1.ClusterIPNone || spec.PublishNotReadyAddresses || len(spec.ExternalIPs) != 0 {
		return false
	}
	if spec.InternalTrafficPolicy != nil && *spec.InternalTrafficPolicy != corev1.ServiceInternalTrafficPolicyCluster {
		return false
	}
	port := spec.Ports[0]
	return port.Name == "http" && port.Port == 9200 && port.TargetPort == desired.Spec.Ports[0].TargetPort && port.Protocol == corev1.ProtocolTCP && port.NodePort == 0
}

// matchesFleetService adds exact selector and optional UID pinning to the
// role Service shape check. The caller reads the deterministic Service name.
func matchesFleetService(instance *v1alpha1.OpenSearchServerless, observed, desired *corev1.Service, pinned types.UID) bool {
	return roleServiceShapeMatches(instance, observed, desired) &&
		observed.UID != "" && (pinned == "" || observed.UID == pinned) && observed.ResourceVersion != "" &&
		maps.Equal(observed.Spec.Selector, desired.Spec.Selector)
}

// ensureRoleServices only creates absent stable Services and observes existing
// ones. It never adopts or changes an existing Service or its traffic selector.
// The caller must persist the serving intent before calling it.
func ensureRoleServices(ctx context.Context, core typedcore.CoreV1Interface, instance *v1alpha1.OpenSearchServerless) error {
	for _, role := range []string{"search", "ingest"} {
		fleet := instance.Status.ServingFleetIntentByRole[role]
		if fleet == "" {
			return fmt.Errorf("recorded fleet intent for %s is missing", role)
		}
		desired, err := workload.RoleService(instance, role, fleet, 9200)
		if err != nil {
			return fmt.Errorf("render %s Service: %w", role, err)
		}
		services := core.Services(instance.Namespace)
		observed, err := services.Get(ctx, desired.Name, metav1.GetOptions{})
		if apierrors.IsNotFound(err) {
			if _, err = services.Create(ctx, desired, metav1.CreateOptions{}); err != nil && !apierrors.IsAlreadyExists(err) {
				return fmt.Errorf("create %s Service: %w", role, err)
			}
			observed, err = services.Get(ctx, desired.Name, metav1.GetOptions{})
		}
		if err != nil {
			return fmt.Errorf("read %s Service: %w", role, err)
		}
		if !roleServiceShapeMatches(instance, observed, desired) || !reflect.DeepEqual(observed.Spec.Selector, desired.Spec.Selector) {
			return fmt.Errorf("%s Service conflicts with recorded ownership or routing intent", role)
		}
	}
	return nil
}

// switchRoleService performs one conditional selector change. The caller must
// first persist operation intent, exact Service UID and expected/target fleets,
// serialize competing mutations, and independently qualify the target fleet.
// A successful patch is not evidence of EndpointSlice or traffic convergence.
// An ambiguous response must be recovered by rereading the same Service UID.
func switchRoleService(ctx context.Context, core typedcore.CoreV1Interface, instance *v1alpha1.OpenSearchServerless, role string, uid types.UID, expectedFleet, targetFleet string) error {
	return switchRoleServiceConditional(ctx, core, instance, role, uid, "", expectedFleet, targetFleet, nil)
}

// switchRoleServiceConditional optionally requires the exact version used to
// qualify a planned switch. authorize runs after the final Service read.
func switchRoleServiceConditional(ctx context.Context, core typedcore.CoreV1Interface, instance *v1alpha1.OpenSearchServerless, role string, uid types.UID, requiredVersion, expectedFleet, targetFleet string, authorize func() error) error {
	if core == nil || instance == nil || uid == "" || expectedFleet == "" || targetFleet == "" || expectedFleet == targetFleet || len(validation.IsValidLabelValue(expectedFleet)) != 0 || len(validation.IsValidLabelValue(targetFleet)) != 0 {
		return fmt.Errorf("a client, exact Service UID and distinct valid fleet labels are required")
	}
	before, err := workload.RoleService(instance, role, expectedFleet, 9200)
	if err != nil {
		return err
	}
	after, err := workload.RoleService(instance, role, targetFleet, 9200)
	if err != nil {
		return err
	}
	services := core.Services(instance.Namespace)
	observed, err := services.Get(ctx, before.Name, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("read role Service: %w", err)
	}
	if requiredVersion != "" && observed.ResourceVersion != requiredVersion {
		return fmt.Errorf("role Service changed since switch planning")
	}
	if observed.UID != uid || observed.ResourceVersion == "" || !roleServiceShapeMatches(instance, observed, before) {
		return fmt.Errorf("role Service identity, ownership or routing shape changed")
	}
	if reflect.DeepEqual(observed.Spec.Selector, after.Spec.Selector) {
		return nil
	}
	if !reflect.DeepEqual(observed.Spec.Selector, before.Spec.Selector) {
		return fmt.Errorf("role Service selector differs from expected and target")
	}
	patch := []map[string]any{
		{"op": "test", "path": "/metadata/uid", "value": string(uid)},
		{"op": "test", "path": "/metadata/resourceVersion", "value": observed.ResourceVersion},
		{"op": "test", "path": "/spec/selector", "value": before.Spec.Selector},
		{"op": "replace", "path": "/spec/selector", "value": after.Spec.Selector},
	}
	data, err := json.Marshal(patch)
	if err != nil {
		return err
	}
	if authorize != nil {
		if err := authorize(); err != nil {
			return err
		}
	}
	if _, err := services.Patch(ctx, observed.Name, types.JSONPatchType, data, metav1.PatchOptions{}); err != nil {
		return fmt.Errorf("conditionally switch role Service: %w", err)
	}
	return nil
}

// ensurePeerDNSService creates or observes the per-instance headless Service;
// it never adopts a conflicting Service or rewrites its selectors.
func ensurePeerDNSService(ctx context.Context, core typedcore.CoreV1Interface, instance *v1alpha1.OpenSearchServerless) error {
	desired, err := workload.PeerDNSService(instance)
	if err != nil {
		return err
	}
	services := core.Services(instance.Namespace)
	observed, err := services.Get(ctx, desired.Name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		if _, err = services.Create(ctx, desired, metav1.CreateOptions{}); err != nil && !apierrors.IsAlreadyExists(err) {
			return fmt.Errorf("create peer DNS Service: %w", err)
		}
		observed, err = services.Get(ctx, desired.Name, metav1.GetOptions{})
	}
	if err != nil {
		return fmt.Errorf("read peer DNS Service: %w", err)
	}
	owner := metav1.GetControllerOf(observed)
	if observed.DeletionTimestamp != nil || owner == nil || owner.UID != instance.UID || owner.Name != instance.Name || owner.Kind != "OpenSearchServerless" || owner.APIVersion != v1alpha1.GroupVersion.String() || observed.Spec.Type != corev1.ServiceTypeClusterIP || observed.Spec.ClusterIP != corev1.ClusterIPNone || !observed.Spec.PublishNotReadyAddresses || !reflect.DeepEqual(observed.Spec.Selector, desired.Spec.Selector) || len(observed.Spec.Ports) != 1 || observed.Spec.Ports[0].Name != "transport" || observed.Spec.Ports[0].Port != 9300 || observed.Spec.Ports[0].TargetPort != desired.Spec.Ports[0].TargetPort || observed.Spec.Ports[0].Protocol != corev1.ProtocolTCP {
		return fmt.Errorf("peer DNS Service conflicts with recorded ownership or discovery intent")
	}
	return nil
}
