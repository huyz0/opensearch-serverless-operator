package placement_test

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/huyz0/opensearch-serverless-operator/internal/placement"
	"github.com/huyz0/opensearch-serverless-operator/internal/workload"
)

func TestProfilePlacementAndAdmittedFit(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	quantities := func(cpu, memory, ephemeral, pods string) corev1.ResourceList {
		return corev1.ResourceList{corev1.ResourceCPU: resource.MustParse(cpu), corev1.ResourceMemory: resource.MustParse(memory), corev1.ResourceEphemeralStorage: resource.MustParse(ephemeral), corev1.ResourcePods: resource.MustParse(pods)}
	}
	profile := placement.Profile{Revision: "v1", Role: "search", Zones: []string{"zone-a"}, NodeSelector: map[string]string{"platform.example/profile": "search"}, Taint: corev1.Taint{Key: "platform.example/dedicated", Value: "search", Effect: corev1.TaintEffectNoSchedule}, ObservedAt: metav1.NewTime(now.Add(-time.Minute)), ExpiresAt: metav1.NewTime(now.Add(time.Hour)), Allocatable: quantities("8", "16Gi", "40Gi", "20"), Reserved: quantities("1", "2Gi", "5Gi", "4"), Headroom: quantities("1", "2Gi", "5Gi", "1")}
	data, err := json.Marshal(profile)
	if err != nil {
		t.Fatal(err)
	}
	immutable := true
	cm := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Namespace: "n", Name: "search", UID: "profile-uid", ResourceVersion: "3"}, Immutable: &immutable, Data: map[string]string{"profile.json": string(data)}}
	client := fake.NewSimpleClientset(cm).CoreV1()
	resolved, err := placement.Resolve(context.Background(), client, "n", "search", "search", "zone-a", now)
	if err != nil {
		t.Fatal(err)
	}
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{workload.InstanceLabel: "instance"}}, Spec: corev1.PodSpec{NodeSelector: map[string]string{corev1.LabelTopologyZone: "zone-a"}, Containers: []corev1.Container{{Name: "app", Resources: corev1.ResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("4"), corev1.ResourceMemory: resource.MustParse("10Gi"), corev1.ResourceEphemeralStorage: resource.MustParse("1Gi")}}}}}}
	if err := placement.Apply(pod.DeepCopy(), resolved, "different-owner"); err == nil {
		t.Fatal("profile must reject a Pod with a different instance label")
	}
	if err := placement.Apply(pod, resolved, "instance"); err != nil {
		t.Fatal(err)
	}
	if pod.Spec.NodeSelector["platform.example/profile"] != "search" || pod.Spec.Affinity.PodAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution[0].TopologyKey != corev1.LabelHostname || len(pod.Spec.Tolerations) != 1 {
		t.Fatal("dedicated placement constraints missing")
	}
	if err := placement.CheckFit(pod, resolved); err != nil {
		t.Fatal(err)
	}
	withPodResources := pod.DeepCopy()
	withPodResources.Spec.Resources = &corev1.ResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("7")}}
	if err := placement.CheckFit(withPodResources, resolved); err == nil {
		t.Fatal("Pod-level request larger than the profile must not be ignored")
	}
	withClaim := pod.DeepCopy()
	withClaim.Spec.ResourceClaims = []corev1.PodResourceClaim{{Name: "device"}}
	if err := placement.CheckFit(withClaim, resolved); err == nil {
		t.Fatal("unqualified dynamic resource claim must fail fit")
	}
	pod.Spec.Containers = append(pod.Spec.Containers, corev1.Container{Name: "injected", Resources: corev1.ResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("100m"), corev1.ResourceMemory: resource.MustParse("3Gi"), corev1.ResourceEphemeralStorage: resource.MustParse("1Gi")}}})
	if err := placement.CheckFit(pod, resolved); err == nil {
		t.Fatal("admission-injected sidecar must count toward fit")
	}
	if _, err := placement.Resolve(context.Background(), client, "n", "search", "search", "zone-b", now); err == nil {
		t.Fatal("unqualified zone was accepted")
	}
	if _, err := placement.Resolve(context.Background(), client, "n", "search", "search", "zone-a", now.Add(2*time.Hour)); err == nil {
		t.Fatal("expired profile was accepted")
	}
}

func TestApplyPlacementDoesNotPartiallyUpdatePod(t *testing.T) {
	profile := &placement.Resolved{Profile: placement.Profile{
		NodeSelector: map[string]string{"platform.example/profile": "search", "platform.example/shape": "qualified"},
		Taint:        corev1.Taint{Key: "platform.example/dedicated", Value: "search", Effect: corev1.TaintEffectNoSchedule},
	}}
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{workload.InstanceLabel: "owner"}}}
	conflicting := pod.DeepCopy()
	conflicting.Spec.NodeSelector = map[string]string{"platform.example/shape": "other"}
	before := conflicting.DeepCopy()
	if err := placement.Apply(conflicting, profile, "owner"); err == nil || !reflect.DeepEqual(conflicting, before) {
		t.Fatalf("conflicting profile changed Pod placement: %v", err)
	}
	if err := placement.Apply(pod, profile, "owner"); err != nil || !reflect.DeepEqual(pod.Spec.NodeSelector, profile.Profile.NodeSelector) {
		t.Fatalf("profile could not initialize a missing selector: %v", err)
	}
}
