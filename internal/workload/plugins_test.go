package workload_test

import (
	"slices"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/huyz0/opensearch-serverless-operator/internal/workload"
	podkube "github.com/huyz0/opensearch-serverless-operator/pkg/podlifecycle/kubernetes"
)

func TestPluginInstallerIsolatedAndIncludedInScheduling(t *testing.T) {
	group := int64(10001)
	pod := &corev1.Pod{Spec: corev1.PodSpec{SecurityContext: &corev1.PodSecurityContext{FSGroup: &group}, Containers: []corev1.Container{
		{Name: "opensearch", Resources: corev1.ResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("1Gi")}}},
		{Name: "sidecar"},
	}}}
	input := workload.PluginInstallerInput{Image: "example/installer@sha256:" + strings.Repeat("a", 64), RequestConfigMap: "request", PolicyConfigMap: "policy", CredentialsSecret: "registry", JavaPluginsPath: "/work/plugins", VolumeSizeLimit: resource.MustParse("1Gi"), Resources: corev1.ResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("1"), corev1.ResourceMemory: resource.MustParse("2Gi"), corev1.ResourceEphemeralStorage: resource.MustParse("1Gi")}}}
	configured, err := workload.WithPluginInstaller(pod, input)
	if err != nil {
		t.Fatal(err)
	}
	if len(pod.Spec.Volumes) != 0 || len(configured.Spec.InitContainers) != 1 || len(configured.Spec.Containers[1].VolumeMounts) != 0 {
		t.Fatal("input was mutated or sidecar gained a mount")
	}
	installer := configured.Spec.InitContainers[0]
	if len(installer.VolumeMounts) != 4 || len(configured.Spec.Containers[0].VolumeMounts) != 1 || !configured.Spec.Containers[0].VolumeMounts[0].ReadOnly {
		t.Fatal("installer or Java mounts are incomplete")
	}
	for _, mount := range configured.Spec.Containers[0].VolumeMounts {
		if mount.Name == "serverless-plugin-credentials" {
			t.Fatal("registry credentials leaked into Java")
		}
	}
	requests, err := podkube.EffectiveRequests(configured)
	if err != nil {
		t.Fatal(err)
	}
	memory := requests[corev1.ResourceMemory]
	want := resource.MustParse("2Gi")
	if memory.Cmp(want) != 0 {
		t.Fatalf("installer memory is missing from effective requests: %s", memory.String())
	}
	input.PolicyConfigMap = input.RequestConfigMap
	if _, err := workload.WithPluginInstaller(pod, input); err == nil {
		t.Fatal("request and authorization policy must be separate inputs")
	}
	input.PolicyConfigMap = "policy"
	input.RequestSHA256 = strings.Repeat("a", 64)
	if _, err := workload.WithPluginInstaller(pod, input); err == nil {
		t.Fatal("one-sided hash pin was accepted")
	}
	input.PolicySHA256 = strings.Repeat("b", 64)
	input.CredentialsHasCA = true
	pinned, err := workload.WithPluginInstaller(pod, input)
	if err != nil || !slices.Contains(pinned.Spec.InitContainers[len(pinned.Spec.InitContainers)-1].Args, input.RequestSHA256) || !slices.Contains(pinned.Spec.InitContainers[len(pinned.Spec.InitContainers)-1].Args, input.PolicySHA256) {
		t.Fatalf("pinned input hashes missing: %v", err)
	}
	if !slices.Contains(pinned.Spec.InitContainers[len(pinned.Spec.InitContainers)-1].Args, "/inputs/credentials/ca.crt") {
		t.Fatal("registry CA path missing from installer")
	}
	for _, volume := range pinned.Spec.Volumes {
		if volume.Name == "serverless-plugin-credentials" && (volume.Secret == nil || len(volume.Secret.Items) != 2 || volume.Secret.Items[1].Key != "ca.crt") {
			t.Fatal("registry CA not mounted with installer credentials")
		}
	}
	pod.Spec.Containers[1].VolumeMounts = []corev1.VolumeMount{{Name: "serverless-plugin-credentials", MountPath: "/stolen"}}
	if _, err := workload.WithPluginInstaller(pod, input); err == nil {
		t.Fatal("existing sidecar must not be able to pre-mount installer credentials")
	}
}
