package workload

import (
	"fmt"
	"reflect"
	"strings"

	corev1 "k8s.io/api/core/v1"

	"github.com/huyz0/opensearch-serverless-operator/api/v1alpha1"
	"github.com/huyz0/opensearch-serverless-operator/internal/runtimeconfig"
)

const (
	WebIdentityVolume = "serverless-s3-web-identity"
	WebIdentityDir    = "/var/run/serverless/s3-identity"
	WebIdentityToken  = runtimeconfig.S3WebIdentityTokenPath
	EKSSkipContainers = "eks.amazonaws.com/skip-containers"
)

// AddS3WebIdentity configures an explicit token projection. It does not
// qualify the image, IAM trust, admission webhooks or cloud access.
func AddS3WebIdentity(pod *corev1.Pod, roleARN string) error {
	if pod == nil || len(pod.Spec.Containers) == 0 || pod.Spec.Containers[0].Name != "opensearch" || !v1alpha1.ValidAWSRoleARN(roleARN) {
		return fmt.Errorf("valid Java container and IAM role ARN required")
	}
	if pod.Spec.AutomountServiceAccountToken == nil || *pod.Spec.AutomountServiceAccountToken {
		return fmt.Errorf("automatic service account token mounting must be disabled")
	}
	for _, v := range pod.Spec.Volumes {
		if v.Name == WebIdentityVolume {
			return fmt.Errorf("reserved web identity volume already exists")
		}
	}
	java := &pod.Spec.Containers[0]
	if len(java.EnvFrom) != 0 {
		return fmt.Errorf("Java envFrom is not allowed with web identity")
	}
	for _, env := range java.Env {
		if strings.HasPrefix(env.Name, "AWS_") {
			return fmt.Errorf("existing AWS environment is not allowed with web identity")
		}
	}
	for _, container := range append(append([]corev1.Container{}, pod.Spec.Containers...), pod.Spec.InitContainers...) {
		for _, mount := range container.VolumeMounts {
			if mount.Name == WebIdentityVolume || (container.Name == "opensearch" && overlapsWebIdentity(mount.MountPath)) {
				return fmt.Errorf("web identity mount conflicts with container %q", container.Name)
			}
		}
	}
	if pod.Annotations == nil {
		pod.Annotations = map[string]string{}
	}
	if _, exists := pod.Annotations[EKSSkipContainers]; exists {
		return fmt.Errorf("reserved EKS identity annotation already exists")
	}
	names := make([]string, 0, len(pod.Spec.Containers)+len(pod.Spec.InitContainers))
	for _, container := range pod.Spec.Containers {
		names = append(names, container.Name)
	}
	for _, container := range pod.Spec.InitContainers {
		names = append(names, container.Name)
	}
	pod.Annotations[EKSSkipContainers] = strings.Join(names, ",")
	mode, expiration := int32(0440), int64(3600)
	pod.Spec.Volumes = append(pod.Spec.Volumes, corev1.Volume{Name: WebIdentityVolume, VolumeSource: corev1.VolumeSource{Projected: &corev1.ProjectedVolumeSource{DefaultMode: &mode, Sources: []corev1.VolumeProjection{{ServiceAccountToken: &corev1.ServiceAccountTokenProjection{Audience: "sts.amazonaws.com", ExpirationSeconds: &expiration, Path: "token"}}}}}})
	java.VolumeMounts = append(java.VolumeMounts, corev1.VolumeMount{Name: WebIdentityVolume, MountPath: WebIdentityDir, ReadOnly: true})
	java.Env = append(java.Env, corev1.EnvVar{Name: "AWS_ROLE_ARN", Value: roleARN})
	return nil
}

func overlapsWebIdentity(path string) bool {
	path = strings.TrimRight(path, "/")
	return path == "" || path == WebIdentityDir || strings.HasPrefix(path, WebIdentityDir+"/") || strings.HasPrefix(WebIdentityDir, path+"/")
}

// ValidateS3WebIdentity checks the admitted volume, mount and consumer isolation
// against a trusted rendered Pod before the scheduling gate is removed.
func ValidateS3WebIdentity(desired, observed *corev1.Pod) error {
	if desired == nil || observed == nil || len(desired.Spec.Containers) == 0 {
		return fmt.Errorf("missing desired or admitted Pod")
	}
	if observed.Spec.AutomountServiceAccountToken == nil || *observed.Spec.AutomountServiceAccountToken {
		return fmt.Errorf("automatic token mount enabled")
	}
	if desired.Annotations[EKSSkipContainers] == "" || observed.Annotations[EKSSkipContainers] != desired.Annotations[EKSSkipContainers] {
		return fmt.Errorf("EKS identity skip annotation changed")
	}
	var expected, actual *corev1.Volume
	for i := range desired.Spec.Volumes {
		if desired.Spec.Volumes[i].Name == WebIdentityVolume {
			expected = &desired.Spec.Volumes[i]
		}
	}
	for i := range observed.Spec.Volumes {
		if observed.Spec.Volumes[i].Name == WebIdentityVolume {
			if actual != nil {
				return fmt.Errorf("duplicate web identity volume")
			}
			actual = &observed.Spec.Volumes[i]
		}
	}
	if expected == nil || actual == nil || !reflect.DeepEqual(expected.VolumeSource, actual.VolumeSource) {
		return fmt.Errorf("web identity projection changed")
	}
	javaFound, mountFound := false, false
	for _, container := range observed.Spec.Containers {
		java := container.Name == "opensearch"
		if java {
			if javaFound || len(container.EnvFrom) != 0 || !reflect.DeepEqual(container.Env, desired.Spec.Containers[0].Env) {
				return fmt.Errorf("Java identity environment changed")
			}
			javaFound = true
		}
		for _, mount := range container.VolumeMounts {
			if !java && mount.Name == WebIdentityVolume {
				return fmt.Errorf("sidecar mounts web identity token")
			}
			if java && mount.Name == WebIdentityVolume && mount.MountPath != WebIdentityDir {
				return fmt.Errorf("unexpected web identity mount")
			}
			if java && overlapsWebIdentity(mount.MountPath) {
				if mount.Name != WebIdentityVolume || mount.MountPath != WebIdentityDir || !mount.ReadOnly || mount.SubPath != "" || mount.SubPathExpr != "" || mount.MountPropagation != nil {
					return fmt.Errorf("web identity mount changed or shadowed")
				}
				if mountFound {
					return fmt.Errorf("duplicate web identity mount")
				}
				mountFound = true
			}
		}
		for _, device := range container.VolumeDevices {
			if device.Name == WebIdentityVolume {
				return fmt.Errorf("web identity volume used as device")
			}
		}
	}
	for _, container := range observed.Spec.InitContainers {
		for _, mount := range container.VolumeMounts {
			if mount.Name == WebIdentityVolume {
				return fmt.Errorf("init container mounts web identity token")
			}
		}
		for _, device := range container.VolumeDevices {
			if device.Name == WebIdentityVolume {
				return fmt.Errorf("init container uses web identity volume")
			}
		}
	}
	if !javaFound || !mountFound {
		return fmt.Errorf("Java web identity mount missing")
	}
	return nil
}
