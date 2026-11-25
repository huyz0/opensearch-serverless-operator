// Package workload builds Kubernetes resources for serverless instances.
package workload

import (
	"crypto/sha256"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/apimachinery/pkg/util/validation"

	"github.com/huyz0/opensearch-serverless-operator/api/v1alpha1"
)

// PeerDNSService names the per-instance headless transport discovery Service.
// Readiness must not suppress records needed by nodes during their startup.
func PeerDNSService(instance *v1alpha1.OpenSearchServerless) (*corev1.Service, error) {
	if instance == nil || instance.UID == "" || instance.Namespace == "" || instance.Name == "" {
		return nil, fmt.Errorf("instance identity is required")
	}
	hash := sha256.Sum256([]byte(instance.UID))
	controller := true
	return &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: fmt.Sprintf("peer-%x", hash[:10]), Namespace: instance.Namespace,
			OwnerReferences: []metav1.OwnerReference{{APIVersion: v1alpha1.GroupVersion.String(), Kind: "OpenSearchServerless", Name: instance.Name, UID: instance.UID, Controller: &controller}}},
		Spec: corev1.ServiceSpec{Type: corev1.ServiceTypeClusterIP, ClusterIP: corev1.ClusterIPNone, PublishNotReadyAddresses: true,
			Selector: map[string]string{InstanceLabel: string(instance.UID)},
			Ports:    []corev1.ServicePort{{Name: "transport", Port: 9300, TargetPort: intstr.FromInt32(9300), Protocol: corev1.ProtocolTCP}}},
	}, nil
}

// PreviewService selects one candidate fleet and never selects a serving fleet
// merely by sharing its role. Its identity is stable for the instance UID,
// role and fleet, and it does not modify the production Service.
func PreviewService(instance *v1alpha1.OpenSearchServerless, role, fleet string) (*corev1.Service, error) {
	return namedFleetService(instance, "preview", role, fleet)
}

// GatewayBackendService keeps a fleet address independent of serving and
// preview changes. The HTTPRoute may reference it at zero or positive weight.
func GatewayBackendService(instance *v1alpha1.OpenSearchServerless, role, fleet string) (*corev1.Service, error) {
	return namedFleetService(instance, "fleet", role, fleet)
}

func namedFleetService(instance *v1alpha1.OpenSearchServerless, prefix, role, fleet string) (*corev1.Service, error) {
	if len(validation.IsValidLabelValue(fleet)) != 0 || fleet == "" {
		return nil, fmt.Errorf("a valid fleet label is required")
	}
	service, err := RoleService(instance, role, fleet, 9200)
	if err != nil {
		return nil, err
	}
	hash := sha256.Sum256([]byte(fmt.Sprintf("%s\x00%s\x00%s", instance.UID, role, fleet)))
	service.Name = fmt.Sprintf("%s-%s-%x", prefix, role, hash[:10])
	return service, nil
}

// PeerHostname is a DNS label derived from the already unique Pod name.
func PeerHostname(podName string) (string, error) {
	if podName == "" {
		return "", fmt.Errorf("Pod name is required")
	}
	hash := sha256.Sum256([]byte(podName))
	return fmt.Sprintf("node-%x", hash[:12]), nil
}

const (
	InstanceLabel = "serverless.opensearch.org/instance-uid"
	RoleLabel     = "serverless.opensearch.org/role"
	FleetLabel    = "serverless.opensearch.org/fleet"
	ZoneLabel     = "serverless.opensearch.org/zone"
)

// RoleService builds a stable Service for one role and serving fleet. A caller
// must verify fleet eligibility and use conditional updates when switching an
// existing Service. No Service is applied by this function.
func RoleService(instance *v1alpha1.OpenSearchServerless, role, fleet string, port int32) (*corev1.Service, error) {
	if instance == nil || instance.UID == "" || instance.Name == "" || instance.Namespace == "" || fleet == "" || port < 1 || port > 65535 {
		return nil, fmt.Errorf("instance identity, fleet and valid port are required")
	}
	suffix := ""
	switch role {
	case "search":
		suffix = "search"
	case "ingest":
		suffix = "index"
	default:
		return nil, fmt.Errorf("unsupported role %q", role)
	}
	controller := true
	return &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name: instance.Name + "-" + suffix, Namespace: instance.Namespace,
			OwnerReferences: []metav1.OwnerReference{{APIVersion: v1alpha1.GroupVersion.String(), Kind: "OpenSearchServerless", Name: instance.Name, UID: instance.UID, Controller: &controller}},
		},
		Spec: corev1.ServiceSpec{
			Selector: map[string]string{InstanceLabel: string(instance.UID), RoleLabel: role, FleetLabel: fleet},
			Ports:    []corev1.ServicePort{{Name: "http", Port: port, TargetPort: intstr.FromInt32(port), Protocol: corev1.ProtocolTCP}},
		},
	}, nil
}
