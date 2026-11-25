package workload

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
)

func TestS3WebIdentityProjectionAndAdmission(t *testing.T) {
	disabled := false
	pod := &corev1.Pod{Spec: corev1.PodSpec{AutomountServiceAccountToken: &disabled, Containers: []corev1.Container{{Name: "opensearch"}, {Name: "sidecar"}}}}
	if err := AddS3WebIdentity(pod, "arn:aws:iam::123456789012:role/path/test"); err != nil {
		t.Fatal(err)
	}
	if err := ValidateS3WebIdentity(pod, pod.DeepCopy()); err != nil {
		t.Fatal(err)
	}
	if pod.Annotations[EKSSkipContainers] != "opensearch,sidecar" {
		t.Fatal("expected EKS webhook skip annotation for each intended container")
	}
	for _, test := range []struct {
		name   string
		mutate func(*corev1.Pod)
	}{
		{"token audience", func(p *corev1.Pod) { p.Spec.Volumes[0].Projected.Sources[0].ServiceAccountToken.Audience = "other" }},
		{"token expiry", func(p *corev1.Pod) {
			*p.Spec.Volumes[0].Projected.Sources[0].ServiceAccountToken.ExpirationSeconds = 600
		}},
		{"role", func(p *corev1.Pod) { p.Spec.Containers[0].Env[0].Value = "other" }},
		{"webhook skip", func(p *corev1.Pod) { delete(p.Annotations, EKSSkipContainers) }},
		{"envFrom", func(p *corev1.Pod) { p.Spec.Containers[0].EnvFrom = []corev1.EnvFromSource{{Prefix: "AWS_"}} }},
		{"sidecar", func(p *corev1.Pod) {
			p.Spec.Containers[1].VolumeMounts = []corev1.VolumeMount{{Name: WebIdentityVolume, MountPath: "/stolen"}}
		}},
		{"init", func(p *corev1.Pod) {
			p.Spec.InitContainers = []corev1.Container{{Name: "init", VolumeMounts: []corev1.VolumeMount{{Name: WebIdentityVolume, MountPath: "/stolen"}}}}
		}},
		{"shadow", func(p *corev1.Pod) {
			p.Spec.Containers[0].VolumeMounts = append(p.Spec.Containers[0].VolumeMounts, corev1.VolumeMount{Name: "other", MountPath: WebIdentityDir + "/token"})
		}},
		{"subpath", func(p *corev1.Pod) { p.Spec.Containers[0].VolumeMounts[0].SubPath = "token" }},
		{"writable", func(p *corev1.Pod) { p.Spec.Containers[0].VolumeMounts[0].ReadOnly = false }},
	} {
		t.Run(test.name, func(t *testing.T) {
			changed := pod.DeepCopy()
			test.mutate(changed)
			if err := ValidateS3WebIdentity(pod, changed); err == nil {
				t.Fatal("unsafe admitted identity accepted")
			}
		})
	}
}

func TestS3WebIdentityRejectsInvalidInputs(t *testing.T) {
	disabled := false
	for _, role := range []string{"", "arn:aws:iam::123:role/test", "arn:aws:iam::123456789012:user/test", "arn:aws:iam::123456789012:role/bad role"} {
		pod := &corev1.Pod{Spec: corev1.PodSpec{AutomountServiceAccountToken: &disabled, Containers: []corev1.Container{{Name: "opensearch"}}}}
		if err := AddS3WebIdentity(pod, role); err == nil {
			t.Fatalf("accepted role %q", role)
		}
	}
	pod := &corev1.Pod{Spec: corev1.PodSpec{AutomountServiceAccountToken: &disabled, Containers: []corev1.Container{{Name: "opensearch", VolumeMounts: []corev1.VolumeMount{{Name: "shadow", MountPath: "/var/run/serverless"}}}}}}
	if err := AddS3WebIdentity(pod, "arn:aws:iam::123456789012:role/test"); err == nil {
		t.Fatal("accepted shadowing mount")
	}
}
