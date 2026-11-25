package controller

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
)

func TestHTTPSPrivateKeyAccessRejectsAliases(t *testing.T) {
	pod := &corev1.Pod{Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "opensearch"}, {Name: "sidecar"}}, Volumes: []corev1.Volume{
		{Name: httpCertificateVolume, VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{SecretName: "leaf"}}},
		{Name: "sidecar-config", VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{SecretName: "sidecar-settings"}}},
	}}}
	if err := validateHTTPKeyAccess(pod, "leaf"); err != nil {
		t.Fatalf("ordinary sidecar configuration held: %v", err)
	}
	for _, check := range []struct {
		name   string
		inject func(*corev1.Pod)
	}{
		{"volume alias", func(pod *corev1.Pod) { pod.Spec.Volumes[1].Secret.SecretName = "leaf" }},
		{"projected alias", func(pod *corev1.Pod) {
			pod.Spec.Volumes[1].VolumeSource = corev1.VolumeSource{Projected: &corev1.ProjectedVolumeSource{Sources: []corev1.VolumeProjection{
				{Secret: &corev1.SecretProjection{LocalObjectReference: corev1.LocalObjectReference{Name: "leaf"}}},
			}}}
		}},
		{"environment bulk import", func(pod *corev1.Pod) {
			pod.Spec.Containers[1].EnvFrom = []corev1.EnvFromSource{{SecretRef: &corev1.SecretEnvSource{LocalObjectReference: corev1.LocalObjectReference{Name: "leaf"}}}}
		}},
		{"environment key reference", func(pod *corev1.Pod) {
			pod.Spec.Containers[1].Env = []corev1.EnvVar{{Name: "KEY", ValueFrom: &corev1.EnvVarSource{SecretKeyRef: &corev1.SecretKeySelector{
				LocalObjectReference: corev1.LocalObjectReference{Name: "leaf"}, Key: corev1.TLSPrivateKeyKey,
			}}}}
		}},
	} {
		t.Run(check.name, func(t *testing.T) {
			changed := pod.DeepCopy()
			check.inject(changed)
			if err := validateHTTPKeyAccess(changed, "leaf"); err == nil {
				t.Fatal("alias of the managed private key was accepted")
			}
		})
	}
}
