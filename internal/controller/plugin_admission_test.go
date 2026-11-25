package controller

import (
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/huyz0/opensearch-serverless-operator/api/v1alpha1"
)

func TestPluginAdmissionProtectsInputsAndCredentials(t *testing.T) {
	instance := &v1alpha1.OpenSearchServerless{ObjectMeta: metav1.ObjectMeta{Name: "catalog", Namespace: "n", UID: "owner"}, Spec: v1alpha1.OpenSearchServerlessSpec{Image: "image", ServiceAccountName: "runtime", Pools: map[string]v1alpha1.Pool{"search": {JVMHeap: "512Mi", Zones: map[string]v1alpha1.Zone{"a": {Replicas: 1}}, Resources: corev1.ResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("1"), corev1.ResourceMemory: resource.MustParse("1Gi")}}}}}, Status: v1alpha1.OpenSearchServerlessStatus{PrimaryStore: &v1alpha1.PrimaryStoreStatus{Bucket: "bucket", Region: "region"}}}
	plugin := &v1alpha1.PluginInstallIntent{InstallerImage: "installer@sha256:" + strings.Repeat("a", 64), PolicySourceName: "source", PolicySourceUID: "source-uid", PolicySourceVersion: "1", RequestName: "request", RequestUID: "request-uid", RequestSHA256: strings.Repeat("b", 64), PolicyName: "policy", PolicyUID: "policy-uid", PolicySHA256: strings.Repeat("c", 64), CredentialsName: "pull", CredentialsUID: "pull-uid"}
	intent := &v1alpha1.PodCreationIntent{Name: "pod", Role: "search", Zone: "a", Fleet: "initial", Revision: "revision", Sequence: 1, PluginInstall: plugin}
	desired, err := renderGatedPod(instance, intent, nil, "", "")
	if err != nil {
		t.Fatal(err)
	}
	observed := desired.DeepCopy()
	observed.UID = "pod-uid"
	if err := validateAdmittedGatedPod(desired, observed); err != nil {
		t.Fatalf("valid plugin Pod rejected: %v", err)
	}
	for _, scenario := range []struct {
		name   string
		mutate func(*corev1.Pod)
	}{
		{"sidecar credentials", func(p *corev1.Pod) {
			p.Spec.Containers = append(p.Spec.Containers, corev1.Container{Name: "sidecar", VolumeMounts: []corev1.VolumeMount{{Name: "serverless-plugin-credentials", MountPath: "/stolen"}}})
		}},
		{"installer arguments", func(p *corev1.Pod) { p.Spec.InitContainers[0].Args = []string{"-output", "/elsewhere"} }},
		{"installer envFrom", func(p *corev1.Pod) { p.Spec.InitContainers[0].EnvFrom = []corev1.EnvFromSource{{Prefix: "X"}} }},
		{"installer lifecycle", func(p *corev1.Pod) {
			p.Spec.InitContainers[0].Lifecycle = &corev1.Lifecycle{PostStart: &corev1.LifecycleHandler{Exec: &corev1.ExecAction{Command: []string{"/bin/sh"}}}}
		}},
		{"installer termination path", func(p *corev1.Pod) {
			p.Spec.InitContainers[0].TerminationMessagePath = "/inputs/credentials/credentials.json"
		}},
		{"installer shadow", func(p *corev1.Pod) {
			p.Spec.InitContainers[0].VolumeMounts = append(p.Spec.InitContainers[0].VolumeMounts, corev1.VolumeMount{Name: "other", MountPath: "/inputs"})
		}},
		{"java plugin mount", func(p *corev1.Pod) {
			p.Spec.Containers[0].VolumeMounts[len(p.Spec.Containers[0].VolumeMounts)-1].ReadOnly = false
		}},
		{"policy volume", func(p *corev1.Pod) {
			for i := range p.Spec.Volumes {
				if p.Spec.Volumes[i].Name == "serverless-plugin-policy" {
					p.Spec.Volumes[i].ConfigMap.Name = "other"
				}
			}
		}},
		{"intent annotation", func(p *corev1.Pod) { p.Annotations[pluginIntentAnnotation] = "{}" }},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			pod := observed.DeepCopy()
			scenario.mutate(pod)
			if err := validateAdmittedGatedPod(desired, pod); err == nil {
				t.Fatal("unsafe plugin admission accepted")
			}
		})
	}
}
