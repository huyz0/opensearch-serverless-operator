// Package placement resolves platform-qualified dedicated-node profiles.
package placement

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"maps"
	"sort"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/validation"
	coretyped "k8s.io/client-go/kubernetes/typed/core/v1"

	"github.com/huyz0/opensearch-serverless-operator/internal/workload"
	podkube "github.com/huyz0/opensearch-serverless-operator/pkg/podlifecycle/kubernetes"
)

// Profile is supplied by a trusted platform in an immutable ConfigMap.
// Allocatable is the conservative floor across allowed shapes; Reserved and
// Headroom cover required agents and explicit operational margins.
type Profile struct {
	Revision     string              `json:"revision"`
	Role         string              `json:"role"`
	Zones        []string            `json:"zones"`
	NodeSelector map[string]string   `json:"nodeSelector"`
	Taint        corev1.Taint        `json:"taint"`
	ObservedAt   metav1.Time         `json:"observedAt"`
	ExpiresAt    metav1.Time         `json:"expiresAt"`
	Allocatable  corev1.ResourceList `json:"allocatable"`
	Reserved     corev1.ResourceList `json:"reserved"`
	Headroom     corev1.ResourceList `json:"headroom"`
}

type Resolved struct {
	UID             string
	ResourceVersion string
	Profile         Profile
}

// Resolve reads the immutable profile only in the requested namespace. The
// caller must authorize the platform source and protect its name from reuse.
func Resolve(ctx context.Context, core coretyped.CoreV1Interface, namespace, name, role, zone string, now time.Time) (*Resolved, error) {
	if core == nil || namespace == "" || name == "" || (role != "search" && role != "ingest") || zone == "" {
		return nil, fmt.Errorf("profile reference, namespace, role and zone are required")
	}
	cm, err := core.ConfigMaps(namespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return nil, fmt.Errorf("read capacity profile: %w", err)
	}
	if cm.UID == "" || cm.ResourceVersion == "" || cm.DeletionTimestamp != nil || cm.Immutable == nil || !*cm.Immutable {
		return nil, fmt.Errorf("capacity profile must be immutable and have a stable identity")
	}
	raw := cm.Data["profile.json"]
	if len(raw) == 0 || len(raw) > 64<<10 {
		return nil, fmt.Errorf("capacity profile payload is missing or too large")
	}
	decoder := json.NewDecoder(strings.NewReader(raw))
	decoder.DisallowUnknownFields()
	var profile Profile
	if err := decoder.Decode(&profile); err != nil {
		return nil, fmt.Errorf("parse capacity profile: %w", err)
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return nil, fmt.Errorf("capacity profile has trailing JSON")
	}
	if profile.Revision == "" || profile.Role != role || profile.ObservedAt.IsZero() || profile.ExpiresAt.IsZero() || profile.ObservedAt.Time.After(now) || !profile.ExpiresAt.Time.After(now) || !profile.ExpiresAt.Time.After(profile.ObservedAt.Time) {
		return nil, fmt.Errorf("capacity profile role or evidence validity is invalid")
	}
	found := false
	for _, allowed := range profile.Zones {
		if allowed == zone {
			found = true
		}
	}
	if !found {
		return nil, fmt.Errorf("zone is not qualified by the capacity profile")
	}
	if len(profile.NodeSelector) == 0 {
		return nil, fmt.Errorf("capacity profile must select qualified nodes")
	}
	for key, value := range profile.NodeSelector {
		if len(validation.IsQualifiedName(key)) != 0 || len(validation.IsValidLabelValue(value)) != 0 || value == "" || key == corev1.LabelTopologyZone || key == corev1.LabelHostname {
			return nil, fmt.Errorf("invalid or conflicting profile selector")
		}
	}
	if len(validation.IsQualifiedName(profile.Taint.Key)) != 0 || len(validation.IsValidLabelValue(profile.Taint.Value)) != 0 || profile.Taint.Value == "" || profile.Taint.Effect != corev1.TaintEffectNoSchedule || profile.Taint.TimeAdded != nil {
		return nil, fmt.Errorf("capacity profile requires an exact permanent NoSchedule taint")
	}
	for _, name := range []corev1.ResourceName{corev1.ResourceCPU, corev1.ResourceMemory, corev1.ResourceEphemeralStorage, corev1.ResourcePods} {
		capacity, ok := profile.Allocatable[name]
		if !ok || capacity.Sign() <= 0 {
			return nil, fmt.Errorf("capacity profile lacks positive %s allocatable", name)
		}
		if _, ok := profile.Reserved[name]; !ok {
			return nil, fmt.Errorf("capacity profile lacks %s reservation", name)
		}
		if _, ok := profile.Headroom[name]; !ok {
			return nil, fmt.Errorf("capacity profile lacks %s headroom", name)
		}
	}
	for _, resources := range []corev1.ResourceList{profile.Allocatable, profile.Reserved, profile.Headroom} {
		for _, quantity := range resources {
			if quantity.Sign() < 0 {
				return nil, fmt.Errorf("capacity profile has negative resources")
			}
		}
		if pods, ok := resources[corev1.ResourcePods]; ok {
			if _, exact := pods.AsInt64(); !exact {
				return nil, fmt.Errorf("capacity profile Pod slots must be whole numbers")
			}
		}
	}
	return &Resolved{UID: string(cm.UID), ResourceVersion: cm.ResourceVersion, Profile: profile}, nil
}

// Apply adds hard profile placement to an already owned role/AZ Pod.
func Apply(pod *corev1.Pod, profile *Resolved, ownerUID string) error {
	if pod == nil || profile == nil || ownerUID == "" || pod.Labels[workload.InstanceLabel] != ownerUID {
		return fmt.Errorf("Pod, profile and owner identity are required")
	}
	if pod.Spec.Affinity != nil || len(pod.Spec.Tolerations) != 0 {
		return fmt.Errorf("existing affinity or tolerations require separate qualification")
	}
	for key, value := range profile.Profile.NodeSelector {
		if current, ok := pod.Spec.NodeSelector[key]; ok && current != value {
			return fmt.Errorf("profile selector conflicts with Pod")
		}
	}
	if pod.Spec.NodeSelector == nil {
		pod.Spec.NodeSelector = make(map[string]string, len(profile.Profile.NodeSelector))
	}
	maps.Copy(pod.Spec.NodeSelector, profile.Profile.NodeSelector)
	taint := profile.Profile.Taint
	pod.Spec.Tolerations = append(pod.Spec.Tolerations, corev1.Toleration{Key: taint.Key, Operator: corev1.TolerationOpEqual, Value: taint.Value, Effect: taint.Effect})
	pod.Spec.Affinity = &corev1.Affinity{
		NodeAffinity:    &corev1.NodeAffinity{RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{NodeSelectorTerms: []corev1.NodeSelectorTerm{{MatchExpressions: []corev1.NodeSelectorRequirement{{Key: corev1.LabelHostname, Operator: corev1.NodeSelectorOpExists}}}}}},
		PodAntiAffinity: &corev1.PodAntiAffinity{RequiredDuringSchedulingIgnoredDuringExecution: []corev1.PodAffinityTerm{workload.DedicatedHostAntiAffinity()}},
	}
	return nil
}

// CheckFit uses the admitted Pod including injected sidecars and overhead.
// It is a capacity check, not proof that every scheduler predicate succeeds.
func CheckFit(pod *corev1.Pod, resolved *Resolved) error {
	if pod == nil || resolved == nil {
		return fmt.Errorf("admitted Pod and profile are required")
	}
	profile := resolved.Profile
	for _, container := range append(append([]corev1.Container{}, pod.Spec.Containers...), pod.Spec.InitContainers...) {
		for _, name := range []corev1.ResourceName{corev1.ResourceCPU, corev1.ResourceMemory, corev1.ResourceEphemeralStorage} {
			request, ok := container.Resources.Requests[name]
			if !ok || request.Sign() <= 0 {
				return fmt.Errorf("container %s requires a positive %s request for dedicated fit", container.Name, name)
			}
		}
	}
	used := profile.Reserved.DeepCopy()
	for name, quantity := range profile.Headroom {
		current := used[name]
		current.Add(quantity)
		used[name] = current
	}
	requests, err := podkube.EffectiveRequests(pod)
	if err != nil {
		return fmt.Errorf("unsupported admitted Pod resources: %w", err)
	}
	for name := range used {
		if name != corev1.ResourcePods {
			if _, exists := requests[name]; !exists {
				requests[name] = resource.Quantity{}
			}
		}
	}
	short := make([]corev1.ResourceName, 0)
	for name, request := range requests {
		capacity, exists := profile.Allocatable[name]
		reserved := used[name]
		if !exists {
			if !request.IsZero() || !reserved.IsZero() {
				short = append(short, name)
			}
			continue
		}
		capacity = capacity.DeepCopy()
		capacity.Sub(reserved)
		if capacity.Cmp(request) < 0 {
			short = append(short, name)
		}
	}
	sort.Slice(short, func(i, j int) bool { return short[i] < short[j] })
	if len(short) > 0 {
		return fmt.Errorf("admitted Pod exceeds qualified profile capacity for %v", short)
	}
	available := profile.Allocatable[corev1.ResourcePods].DeepCopy()
	reserved := used[corev1.ResourcePods]
	available.Sub(reserved)
	one := resource.MustParse("1")
	if available.Cmp(one) < 0 {
		return fmt.Errorf("qualified profile has no remaining Pod slot")
	}
	return nil
}
