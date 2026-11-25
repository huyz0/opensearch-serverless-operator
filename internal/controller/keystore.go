package controller

import (
	"context"
	"fmt"
	"path"
	"reflect"
	"strings"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/huyz0/opensearch-serverless-operator/api/v1alpha1"
	"github.com/huyz0/opensearch-serverless-operator/internal/runtimeconfig"
)

const keystoreUIDAnnotation = "serverless.opensearch.org/keystore-uid"
const keystoreVolumeName = "serverless-keystore"

// resolveKeystore validates the reference without exposing its data in status
// or errors. Immutable Secret names can still be deleted and recreated; callers
// check the UID again before each effect, and Kubernetes mounts by name.
func (r *InstanceReconciler) resolveKeystore(ctx context.Context, instance *v1alpha1.OpenSearchServerless) (string, error) {
	credentials := instance.Spec.Storage.Credentials
	if credentials == nil {
		return "", nil
	}
	if credentials.Type == "WorkloadIdentity" {
		if credentials.KeystoreSecretRef != nil || !v1alpha1.ValidAWSRoleARN(credentials.AWSRoleARN) || r.DevelopmentS3WebIdentityImage == "" || r.DevelopmentS3WebIdentityImage != instance.Spec.Image {
			return "", fmt.Errorf("S3 web identity requires a role ARN and an explicitly qualified matching development image")
		}
		return "", nil
	}
	if credentials.Type != "Keystore" || credentials.KeystoreSecretRef == nil || credentials.KeystoreSecretRef.Name == "" {
		return "", fmt.Errorf("invalid keystore credential reference")
	}
	if r.Core == nil {
		return "", fmt.Errorf("keystore Secret client is required")
	}
	secret, err := r.Core.Secrets(instance.Namespace).Get(ctx, credentials.KeystoreSecretRef.Name, metav1.GetOptions{})
	if err != nil {
		return "", fmt.Errorf("read keystore Secret: %w", err)
	}
	if secret.UID == "" || secret.DeletionTimestamp != nil || secret.Immutable == nil || !*secret.Immutable || len(secret.Data["opensearch.keystore"]) == 0 {
		return "", fmt.Errorf("keystore Secret must be immutable, non-terminating and contain opensearch.keystore")
	}
	return string(secret.UID), nil
}

func (r *InstanceReconciler) checkKeystoreUID(ctx context.Context, instance *v1alpha1.OpenSearchServerless, expected string) error {
	uid, err := r.resolveKeystore(ctx, instance)
	if err != nil {
		return err
	}
	if uid != expected {
		return fmt.Errorf("keystore Secret identity changed")
	}
	return nil
}

func validateKeystoreAccess(pod *corev1.Pod, secretName string) error {
	if pod.Spec.HostPID || (pod.Spec.ShareProcessNamespace != nil && *pod.Spec.ShareProcessNamespace) || len(pod.Spec.EphemeralContainers) != 0 {
		return fmt.Errorf("keystore credentials require an isolated Pod process namespace")
	}
	for _, group := range [][]corev1.Container{pod.Spec.Containers, pod.Spec.InitContainers} {
		for _, container := range group {
			if security := container.SecurityContext; security != nil && security.Privileged != nil && *security.Privileged {
				return fmt.Errorf("privileged containers cannot run alongside keystore credentials")
			}
		}
	}
	return validateProtectedSecretAccess(pod, secretName, keystoreVolumeName)
}

func keystoreVolume(pod *corev1.Pod) *corev1.SecretVolumeSource {
	for _, volume := range pod.Spec.Volumes {
		if volume.Name == keystoreVolumeName {
			return volume.Secret
		}
	}
	return nil
}

func validateKeystoreMount(desired, observed *corev1.Pod, runtime *corev1.Container) error {
	expected := keystoreVolume(desired)
	if expected == nil || !reflect.DeepEqual(expected, keystoreVolume(observed)) {
		return fmt.Errorf("keystore Secret changed")
	}
	if err := validateKeystoreAccess(observed, expected.SecretName); err != nil {
		return err
	}
	const configPath = "/var/lib/serverless/config"
	mountPath := configPath
	if len(expected.Items) > 1 {
		mountPath = runtimeconfig.BasicAuthSourceDir
	}
	mounted := false
	for _, mount := range runtime.VolumeMounts {
		switch {
		case mount.Name == keystoreVolumeName:
			if mounted || mount.MountPath != mountPath || !mount.ReadOnly || mount.SubPath != "" || mount.SubPathExpr != "" {
				return fmt.Errorf("keystore mount changed")
			}
			mounted = true
		case mount.Name == "serverless-data" && mount.MountPath == "/var/lib/serverless":
			// The operator supplies the parent volume for Java's config.
		case overlapsCredentialMount(mount.MountPath, mountPath) || overlapsCredentialMount(mount.MountPath, configPath):
			return fmt.Errorf("another volume shadows keystore credentials")
		}
	}
	if !mounted {
		return fmt.Errorf("keystore mount is missing")
	}
	return nil
}

func overlapsCredentialMount(mountPath, credentialDir string) bool {
	mountPath = path.Clean(mountPath)
	return mountPath == credentialDir || strings.HasPrefix(mountPath, credentialDir+"/") || strings.HasPrefix(credentialDir, strings.TrimRight(mountPath, "/")+"/")
}
