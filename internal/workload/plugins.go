package workload

import (
	"fmt"
	"path"
	"regexp"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/util/validation"
)

// PluginInstallerInput references independently authorized, immutable inputs.
// The caller must verify their contents, ownership and role-specific selection.
type PluginInstallerInput struct {
	Image             string
	RequestConfigMap  string
	PolicyConfigMap   string
	RequestSHA256     string
	PolicySHA256      string
	CredentialsSecret string
	CredentialsHasCA  bool
	JavaPluginsPath   string
	Resources         corev1.ResourceRequirements
	VolumeSizeLimit   resource.Quantity
}

var pinnedInstallerImage = regexp.MustCompile(`^.+@sha256:[a-f0-9]{64}$`)
var pinnedInputHash = regexp.MustCompile(`^[a-f0-9]{64}$`)

// WithPluginInstaller returns a copy with a restricted init container and a
// read-only Java plugin mount. The caller must ensure that the Java runtime
// runs as UID 10001, its image has no required filesystem plugins hidden by
// this mount, and the independent authorization and image qualification gates
// have passed. The Pod filesystem group must make emptyDir writable by 10001.
func WithPluginInstaller(pod *corev1.Pod, input PluginInstallerInput) (*corev1.Pod, error) {
	if pod == nil || !pinnedInstallerImage.MatchString(input.Image) || len(validation.IsDNS1123Subdomain(input.RequestConfigMap)) != 0 || len(validation.IsDNS1123Subdomain(input.PolicyConfigMap)) != 0 || input.RequestConfigMap == input.PolicyConfigMap || !path.IsAbs(input.JavaPluginsPath) || path.Clean(input.JavaPluginsPath) != input.JavaPluginsPath || input.JavaPluginsPath == "/" {
		return nil, fmt.Errorf("qualified installer image, separate configuration references and an absolute plugin path are required")
	}
	if input.CredentialsSecret != "" && len(validation.IsDNS1123Subdomain(input.CredentialsSecret)) != 0 {
		return nil, fmt.Errorf("invalid registry credentials Secret name")
	}
	if input.CredentialsHasCA && input.CredentialsSecret == "" {
		return nil, fmt.Errorf("registry CA requires a credentials Secret")
	}
	if (input.RequestSHA256 != "" || input.PolicySHA256 != "") && (!pinnedInputHash.MatchString(input.RequestSHA256) || !pinnedInputHash.MatchString(input.PolicySHA256)) {
		return nil, fmt.Errorf("request and policy hashes must both be valid SHA-256 values")
	}
	if pod.Spec.SecurityContext == nil || pod.Spec.SecurityContext.FSGroup == nil || *pod.Spec.SecurityContext.FSGroup != 10001 {
		return nil, fmt.Errorf("plugin volume requires filesystem group 10001")
	}
	if input.VolumeSizeLimit.Sign() <= 0 {
		return nil, fmt.Errorf("positive plugin volume size limit is required")
	}
	for _, name := range []corev1.ResourceName{corev1.ResourceCPU, corev1.ResourceMemory, corev1.ResourceEphemeralStorage} {
		request, exists := input.Resources.Requests[name]
		if !exists || request.Sign() <= 0 {
			return nil, fmt.Errorf("positive installer %s request is required", name)
		}
		if limit, exists := input.Resources.Limits[name]; exists && (limit.Sign() <= 0 || request.Cmp(limit) > 0) {
			return nil, fmt.Errorf("invalid installer %s limit", name)
		}
	}
	const data = "serverless-plugins"
	const request = "serverless-plugin-request"
	const policy = "serverless-plugin-policy"
	const credentials = "serverless-plugin-credentials"
	for _, volume := range pod.Spec.Volumes {
		if volume.Name == data || volume.Name == request || volume.Name == policy || volume.Name == credentials {
			return nil, fmt.Errorf("plugin volume name already exists")
		}
	}
	for _, container := range append(append([]corev1.Container{}, pod.Spec.Containers...), pod.Spec.InitContainers...) {
		if container.Name == "plugin-installer" {
			return nil, fmt.Errorf("plugin installer container already exists")
		}
		for _, mount := range container.VolumeMounts {
			if mount.Name == data || mount.Name == request || mount.Name == policy || mount.Name == credentials {
				return nil, fmt.Errorf("existing container references a reserved plugin volume")
			}
		}
	}
	out := pod.DeepCopy()
	app := -1
	for i := range out.Spec.Containers {
		if out.Spec.Containers[i].Name == "opensearch" {
			if app != -1 {
				return nil, fmt.Errorf("multiple OpenSearch containers")
			}
			app = i
		}
	}
	if app == -1 {
		return nil, fmt.Errorf("OpenSearch container is required")
	}
	for _, mount := range out.Spec.Containers[app].VolumeMounts {
		if mount.MountPath == input.JavaPluginsPath {
			return nil, fmt.Errorf("OpenSearch plugin mount already exists")
		}
	}
	sizeLimit := input.VolumeSizeLimit.DeepCopy()
	mode := int32(0440)
	out.Spec.Volumes = append(out.Spec.Volumes,
		corev1.Volume{Name: data, VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{SizeLimit: &sizeLimit}}},
		corev1.Volume{Name: request, VolumeSource: corev1.VolumeSource{ConfigMap: &corev1.ConfigMapVolumeSource{LocalObjectReference: corev1.LocalObjectReference{Name: input.RequestConfigMap}, DefaultMode: &mode, Items: []corev1.KeyToPath{{Key: "request.json", Path: "request.json"}}}}},
		corev1.Volume{Name: policy, VolumeSource: corev1.VolumeSource{ConfigMap: &corev1.ConfigMapVolumeSource{LocalObjectReference: corev1.LocalObjectReference{Name: input.PolicyConfigMap}, DefaultMode: &mode, Items: []corev1.KeyToPath{{Key: "policy.json", Path: "policy.json"}}}}},
	)
	args := []string{"-request", "/inputs/request/request.json", "-policy", "/inputs/policy/policy.json", "-output", "/plugin-volume/plugins"}
	if input.RequestSHA256 != "" {
		args = append(args, "-request-sha256", input.RequestSHA256, "-policy-sha256", input.PolicySHA256)
	}
	mounts := []corev1.VolumeMount{{Name: data, MountPath: "/plugin-volume"}, {Name: request, MountPath: "/inputs/request", ReadOnly: true}, {Name: policy, MountPath: "/inputs/policy", ReadOnly: true}}
	if input.CredentialsSecret != "" {
		items := []corev1.KeyToPath{{Key: "credentials.json", Path: "credentials.json"}}
		if input.CredentialsHasCA {
			items = append(items, corev1.KeyToPath{Key: "ca.crt", Path: "ca.crt"})
			args = append(args, "-registry-ca", "/inputs/credentials/ca.crt")
		}
		out.Spec.Volumes = append(out.Spec.Volumes, corev1.Volume{Name: credentials, VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{SecretName: input.CredentialsSecret, DefaultMode: &mode, Items: items}}})
		mounts = append(mounts, corev1.VolumeMount{Name: credentials, MountPath: "/inputs/credentials", ReadOnly: true})
		args = append(args, "-credentials", "/inputs/credentials/credentials.json")
	}
	no := false
	yes := true
	uid := int64(10001)
	out.Spec.InitContainers = append(out.Spec.InitContainers, corev1.Container{Name: "plugin-installer", Image: input.Image, ImagePullPolicy: corev1.PullIfNotPresent, TerminationMessagePath: corev1.TerminationMessagePathDefault, TerminationMessagePolicy: corev1.TerminationMessageReadFile, Args: args, Resources: *input.Resources.DeepCopy(), VolumeMounts: mounts, SecurityContext: &corev1.SecurityContext{RunAsUser: &uid, RunAsNonRoot: &yes, ReadOnlyRootFilesystem: &yes, AllowPrivilegeEscalation: &no, Capabilities: &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}}, SeccompProfile: &corev1.SeccompProfile{Type: corev1.SeccompProfileTypeRuntimeDefault}}})
	out.Spec.Containers[app].VolumeMounts = append(out.Spec.Containers[app].VolumeMounts, corev1.VolumeMount{Name: data, MountPath: input.JavaPluginsPath, SubPath: "plugins", ReadOnly: true})
	return out, nil
}
