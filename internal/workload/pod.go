package workload

import (
	"fmt"
	"slices"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"

	"github.com/huyz0/opensearch-serverless-operator/api/v1alpha1"
)

// PodDisruptionTarget reports a disruption currently advertised by Kubernetes.
// A canceled disruption can clear the condition while the same Pod still runs.
func PodDisruptionTarget(pod *corev1.Pod) bool {
	return slices.ContainsFunc(pod.Status.Conditions, func(condition corev1.PodCondition) bool {
		return condition.Type == corev1.DisruptionTarget && condition.Status == corev1.ConditionTrue
	})
}

// PodHasSingleOpenSearchLifetime checks that kubelet cannot restart OpenSearch
// under the same Pod UID. Native sidecars can still have their own policy.
func PodHasSingleOpenSearchLifetime(pod *corev1.Pod) bool {
	return pod.Spec.RestartPolicy == corev1.RestartPolicyNever && slices.ContainsFunc(pod.Spec.Containers, func(container corev1.Container) bool {
		return container.Name == "opensearch" && container.RestartPolicy == nil
	})
}

// PodInput contains the already qualified launcher and resolved role inputs.
// Its command and arguments are passed as individual tokens without a shell.
type PodInput struct {
	Name                 string
	Role                 string
	Zone                 string
	Fleet                string
	Revision             string
	IntentKey            string
	Intent               string
	Command              []string
	Args                 []string
	DedicatedNode        bool
	ServingReadinessPort int32
	WarmupMode           string
}

// Pod builds an owned, scheduling-gated Pod for one role and zone. The caller
// must persist the creation intent and qualify the image, launcher, credentials,
// security, readiness and placement before using or releasing this Pod.
func Pod(instance *v1alpha1.OpenSearchServerless, input PodInput) (*corev1.Pod, error) {
	if instance == nil || instance.UID == "" || instance.Namespace == "" || instance.Name == "" || input.Name == "" || input.Zone == "" || input.Fleet == "" || input.Revision == "" || input.IntentKey == "" || input.Intent == "" || len(input.Command) == 0 {
		return nil, fmt.Errorf("instance identity, Pod intent, role placement and launcher are required")
	}
	if input.Role != "search" && input.Role != "ingest" {
		return nil, fmt.Errorf("unsupported role %q", input.Role)
	}
	pool, exists := instance.Spec.Pools[input.Role]
	if !exists {
		return nil, fmt.Errorf("role pool %q is missing", input.Role)
	}
	if _, exists := pool.Zones[input.Zone]; !exists {
		return nil, fmt.Errorf("zone %q is not configured for role %q", input.Zone, input.Role)
	}
	if pool.WarmReadiness == "Strict" && (input.ServingReadinessPort == 0 || pool.ColdStartPolicy != "AllowOneDegraded") {
		return nil, fmt.Errorf("strict search requires an explicit bootstrap policy and readiness probe")
	}
	controller := true
	automountToken := false
	containers := []corev1.Container{{Name: "opensearch", Image: instance.Spec.Image, Command: append([]string{}, input.Command...), Args: append([]string{}, input.Args...), Resources: *pool.Resources.DeepCopy()}}
	if input.ServingReadinessPort != 0 {
		if input.ServingReadinessPort < 1 || input.ServingReadinessPort > 65535 {
			return nil, fmt.Errorf("invalid readiness port")
		}
		if pool.WarmReadiness == "Strict" {
			if input.Role != "search" || (input.WarmupMode != "Strict" && input.WarmupMode != "Bootstrap") {
				return nil, fmt.Errorf("strict warmth requires an explicit process-local mode")
			}
			command := []string{"/opt/serverless/bin/health-probe", "-role", input.Role}
			if input.WarmupMode == "Strict" {
				command = append(command, "-require-warmup-completion")
			}
			containers[0].ReadinessProbe = &corev1.Probe{ProbeHandler: corev1.ProbeHandler{Exec: &corev1.ExecAction{Command: command}}, TimeoutSeconds: 6, PeriodSeconds: 5, SuccessThreshold: 1, FailureThreshold: 1}
		} else {
			if input.WarmupMode != "" {
				return nil, fmt.Errorf("unexpected warm-up mode")
			}
			containers[0].ReadinessProbe = &corev1.Probe{ProbeHandler: corev1.ProbeHandler{HTTPGet: &corev1.HTTPGetAction{
				Path: "/_serverless/ready", Port: intstr.FromInt32(input.ServingReadinessPort), Scheme: corev1.URISchemeHTTP,
			}}}
		}
	}
	for i := range pool.Sidecars {
		containers = append(containers, *pool.Sidecars[i].DeepCopy())
	}
	initContainers := make([]corev1.Container, 0, len(pool.NativeSidecars))
	for i := range pool.NativeSidecars {
		sidecar := *pool.NativeSidecars[i].DeepCopy()
		if sidecar.RestartPolicy != nil && *sidecar.RestartPolicy != corev1.ContainerRestartPolicyAlways {
			return nil, fmt.Errorf("native sidecar restart policy must be Always")
		}
		always := corev1.ContainerRestartPolicyAlways
		sidecar.RestartPolicy = &always
		initContainers = append(initContainers, sidecar)
	}
	volumes := make([]corev1.Volume, 0, len(pool.SidecarVolumes))
	for _, requested := range pool.SidecarVolumes {
		volume := corev1.Volume{Name: requested.Name}
		switch {
		case requested.SecretRef != nil:
			volume.Secret = &corev1.SecretVolumeSource{SecretName: requested.SecretRef.Name}
		case requested.ConfigMapRef != nil:
			volume.ConfigMap = &corev1.ConfigMapVolumeSource{LocalObjectReference: corev1.LocalObjectReference{Name: requested.ConfigMapRef.Name}}
		case requested.EmptyDir:
			volume.EmptyDir = &corev1.EmptyDirVolumeSource{}
			if requested.SizeLimit != "" {
				size, err := resource.ParseQuantity(requested.SizeLimit)
				if err != nil || size.Sign() <= 0 {
					return nil, fmt.Errorf("invalid sidecar volume size limit")
				}
				volume.EmptyDir.SizeLimit = &size
			}
		default:
			return nil, fmt.Errorf("sidecar volume %q has no source", requested.Name)
		}
		volumes = append(volumes, volume)
	}
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: input.Name, Namespace: instance.Namespace,
			Labels:          map[string]string{InstanceLabel: string(instance.UID), RoleLabel: input.Role, FleetLabel: input.Fleet, ZoneLabel: input.Zone, "serverless.opensearch.org/revision": input.Revision},
			Annotations:     map[string]string{input.IntentKey: input.Intent},
			OwnerReferences: []metav1.OwnerReference{{APIVersion: v1alpha1.GroupVersion.String(), Kind: "OpenSearchServerless", Name: instance.Name, UID: instance.UID, Controller: &controller}},
		},
		Spec: corev1.PodSpec{ServiceAccountName: instance.Spec.ServiceAccountName, AutomountServiceAccountToken: &automountToken, Containers: containers, InitContainers: initContainers, Volumes: volumes,
			NodeSelector:    map[string]string{corev1.LabelTopologyZone: input.Zone},
			SchedulingGates: []corev1.PodSchedulingGate{{Name: "serverless.opensearch.org/placement"}},
		},
	}
	if pool.ReplaceOnExit {
		pod.Spec.RestartPolicy = corev1.RestartPolicyNever
	}
	if input.DedicatedNode {
		pod.Spec.Affinity = &corev1.Affinity{PodAntiAffinity: &corev1.PodAntiAffinity{
			RequiredDuringSchedulingIgnoredDuringExecution: []corev1.PodAffinityTerm{DedicatedHostAntiAffinity()},
		}}
	}
	if pool.ProtectFromScaleDown {
		pod.Annotations["cluster-autoscaler.kubernetes.io/safe-to-evict"] = "false"
	}
	return pod, nil
}

// DedicatedHostAntiAffinity excludes Pods managed by this operator across
// namespaces. A platform taint and admission policy must protect the pool
// against workloads that lack this label or bypass the scheduler.
func DedicatedHostAntiAffinity() corev1.PodAffinityTerm {
	return corev1.PodAffinityTerm{
		LabelSelector:     &metav1.LabelSelector{MatchExpressions: []metav1.LabelSelectorRequirement{{Key: InstanceLabel, Operator: metav1.LabelSelectorOpExists}}},
		NamespaceSelector: &metav1.LabelSelector{},
		TopologyKey:       corev1.LabelHostname,
	}
}
