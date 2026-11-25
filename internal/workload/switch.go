package workload

import (
	"context"
	"fmt"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/typed/core/v1"
)

// SwitchServiceFleet conditionally changes a single owned Service. The caller
// must persist promotion intent and qualify the target fleet before calling.
// A conflict or ambiguous error requires rereading the Service and operation.
func SwitchServiceFleet(ctx context.Context, core v1.CoreV1Interface, namespace, name string, serviceUID, ownerUID types.UID, role, expectedFleet, targetFleet string) error {
	if namespace == "" || name == "" || serviceUID == "" || ownerUID == "" || expectedFleet == "" || targetFleet == "" || (role != "search" && role != "ingest") {
		return fmt.Errorf("complete Service identity, role and fleet assignments are required")
	}
	services := core.Services(namespace)
	service, err := services.Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("read Service: %w", err)
	}
	owner := metav1.GetControllerOf(service)
	if service.UID != serviceUID || owner == nil || owner.UID != ownerUID || service.ResourceVersion == "" {
		return fmt.Errorf("Service identity or controlling owner changed")
	}
	if service.Spec.Selector[InstanceLabel] != string(ownerUID) || service.Spec.Selector[RoleLabel] != role || service.Spec.Selector[FleetLabel] != expectedFleet || len(service.Spec.Selector) != 3 {
		return fmt.Errorf("Service selector does not match expected assignment")
	}
	service.Spec.Selector[FleetLabel] = targetFleet
	if _, err := services.Update(ctx, service, metav1.UpdateOptions{}); err != nil {
		return fmt.Errorf("switch Service fleet: %w", err)
	}
	return nil
}
