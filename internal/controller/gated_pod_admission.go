package controller

import (
	"encoding/json"
	"fmt"
	"reflect"
	"slices"
	"strings"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/huyz0/opensearch-serverless-operator/api/v1alpha1"
	"github.com/huyz0/opensearch-serverless-operator/internal/workload"
)

// admittedContains checks fields explicitly represented in the requested JSON.
// Kubernetes may default additional fields. Ordered lists keep their order
// because it can affect environment and lifecycle logic.
func admittedContains(expected, actual any) bool {
	switch value := expected.(type) {
	case map[string]any:
		observed, ok := actual.(map[string]any)
		if !ok {
			return false
		}
		for key, want := range value {
			got, exists := observed[key]
			if !exists || !admittedContains(want, got) {
				return false
			}
		}
		return true
	case []any:
		observed, ok := actual.([]any)
		if !ok || len(value) != len(observed) {
			return false
		}
		for i := range value {
			if !admittedContains(value[i], observed[i]) {
				return false
			}
		}
		return true
	default:
		return reflect.DeepEqual(expected, actual)
	}
}

func validateSidecarConfiguration(expected, observed corev1.Container) error {
	wantJSON, err := json.Marshal(expected)
	if err != nil {
		return fmt.Errorf("cannot encode requested sidecar")
	}
	gotJSON, err := json.Marshal(observed)
	if err != nil {
		return fmt.Errorf("cannot encode admitted sidecar")
	}
	var want, got map[string]any
	if json.Unmarshal(wantJSON, &want) != nil || json.Unmarshal(gotJSON, &got) != nil {
		return fmt.Errorf("cannot inspect sidecar configuration")
	}
	for _, key := range []string{"env", "envFrom", "ports", "volumeMounts", "volumeDevices", "workingDir", "readinessProbe", "livenessProbe", "startupProbe", "lifecycle", "securityContext", "imagePullPolicy", "terminationMessagePath", "terminationMessagePolicy", "stdin", "stdinOnce", "tty"} {
		if value, present := want[key]; present && !admittedContains(value, got[key]) {
			return fmt.Errorf("requested sidecar %q %s changed", expected.Name, key)
		}
	}
	for name, limit := range expected.Resources.Limits {
		admitted, ok := observed.Resources.Limits[name]
		if !ok || admitted.Cmp(limit) != 0 {
			return fmt.Errorf("requested sidecar %q %s limit changed", expected.Name, name)
		}
	}
	return nil
}

// validateAdmittedGatedPod checks fields needed for safe recovery. Other
// admission additions are accounted for before any future gate release.
func validateAdmittedGatedPod(desired, observed *corev1.Pod) error {
	if desired == nil || observed == nil || observed.UID == "" || observed.DeletionTimestamp != nil || observed.Spec.NodeName != "" || observed.Name != desired.Name || observed.Namespace != desired.Namespace {
		return fmt.Errorf("Pod identity, binding or deletion state changed")
	}
	if err := validateAdmittedPodIdentity(desired, observed); err != nil {
		return err
	}
	if err := validateAdmittedPodPlacement(desired, observed); err != nil {
		return err
	}
	if err := validatePluginAdmission(desired, observed); err != nil {
		return err
	}
	if err := validateAdmittedSidecarVolumes(desired, observed); err != nil {
		return err
	}
	container, err := validateAdmittedRuntime(desired, observed)
	if err != nil {
		return err
	}
	if err := validateAdmittedSidecars(desired, observed); err != nil {
		return err
	}
	return validateAdmittedRuntimeVolumes(desired, observed, container)
}

func validateAdmittedSidecarVolumes(desired, observed *corev1.Pod) error {
	sidecarVolumes := map[string]bool{}
	for _, expected := range desired.Spec.Volumes {
		if strings.HasPrefix(expected.Name, "serverless-") {
			continue
		}
		sidecarVolumes[expected.Name] = true
		var got *corev1.Volume
		for i := range observed.Spec.Volumes {
			if observed.Spec.Volumes[i].Name == expected.Name {
				if got != nil {
					return fmt.Errorf("duplicate sidecar volume %q", expected.Name)
				}
				got = &observed.Spec.Volumes[i]
			}
		}
		if got == nil {
			return fmt.Errorf("sidecar volume %q missing", expected.Name)
		}
		wantJSON, err := json.Marshal(expected)
		if err != nil {
			return fmt.Errorf("cannot encode sidecar volume")
		}
		gotJSON, err := json.Marshal(got)
		if err != nil {
			return fmt.Errorf("cannot encode admitted volume")
		}
		var want, actual any
		if json.Unmarshal(wantJSON, &want) != nil || json.Unmarshal(gotJSON, &actual) != nil || !admittedContains(want, actual) {
			return fmt.Errorf("sidecar volume %q changed", expected.Name)
		}
	}
	requestedSidecars := map[string]bool{}
	for _, container := range desired.Spec.Containers[1:] {
		requestedSidecars[container.Name] = true
	}
	for _, container := range desired.Spec.InitContainers {
		if container.RestartPolicy != nil && *container.RestartPolicy == corev1.ContainerRestartPolicyAlways {
			requestedSidecars[container.Name] = true
		}
	}
	for _, container := range append(append([]corev1.Container{}, observed.Spec.Containers...), observed.Spec.InitContainers...) {
		if requestedSidecars[container.Name] {
			continue
		}
		for _, mount := range container.VolumeMounts {
			if sidecarVolumes[mount.Name] {
				return fmt.Errorf("non-sidecar container mounts sidecar volume")
			}
		}
	}
	for _, volume := range desired.Spec.Volumes {
		if volume.Name == workload.WebIdentityVolume {
			if err := workload.ValidateS3WebIdentity(desired, observed); err != nil {
				return err
			}
		}
	}
	return nil
}

func validateAdmittedPodIdentity(desired, observed *corev1.Pod) error {
	wantOwner, gotOwner := metav1.GetControllerOf(desired), metav1.GetControllerOf(observed)
	if wantOwner == nil || gotOwner == nil || wantOwner.UID != gotOwner.UID || wantOwner.Name != gotOwner.Name ||
		wantOwner.Kind != gotOwner.Kind || wantOwner.APIVersion != gotOwner.APIVersion {
		return fmt.Errorf("Pod controller reference changed")
	}
	for _, key := range []string{
		creationIntentKey, warmupModeAnnotation, keystoreUIDAnnotation,
		profileUIDAnnotation, profileVersionAnnotation, peerDomainAnnotation, peerDNSPreflightAnnotation,
		peerTLSLeafUIDAnnotation, peerTLSTrustUIDAnnotation, peerTLSSnapshotNameAnnotation, peerTLSSnapshotUIDAnnotation,
		emergencyCapacityAnnotation, emergencyEvidenceUIDAnnotation, emergencyEvidenceVersionAnnotation, bootstrapReplacementAnnotation,
	} {
		if observed.Annotations[key] != desired.Annotations[key] {
			return fmt.Errorf("operator Pod annotation %s changed", key)
		}
	}
	for key, value := range desired.Labels {
		if observed.Labels[key] != value {
			return fmt.Errorf("operator Pod label %s changed", key)
		}
	}
	if desired.Annotations["cluster-autoscaler.kubernetes.io/safe-to-evict"] == "false" && observed.Annotations["cluster-autoscaler.kubernetes.io/safe-to-evict"] != "false" {
		return fmt.Errorf("autoscaler protection annotation changed")
	}
	return nil
}

func validateAdmittedPodPlacement(desired, observed *corev1.Pod) error {
	if observed.Spec.HostNetwork != desired.Spec.HostNetwork {
		return fmt.Errorf("Pod network namespace changed")
	}
	for key, value := range desired.Spec.NodeSelector {
		if observed.Spec.NodeSelector[key] != value {
			return fmt.Errorf("required placement selector changed")
		}
	}
	if observed.Spec.ServiceAccountName != desired.Spec.ServiceAccountName {
		return fmt.Errorf("service account changed")
	}
	if observed.Spec.AutomountServiceAccountToken == nil || *observed.Spec.AutomountServiceAccountToken {
		return fmt.Errorf("automatic service account token mounting enabled")
	}
	if observed.Spec.Hostname != desired.Spec.Hostname || observed.Spec.Subdomain != desired.Spec.Subdomain {
		return fmt.Errorf("peer DNS hostname or subdomain changed")
	}
	if desired.Spec.Affinity != nil && !reflect.DeepEqual(desired.Spec.Affinity, observed.Spec.Affinity) {
		return fmt.Errorf("dedicated node affinity changed")
	}
	for _, required := range desired.Spec.Tolerations {
		if !slices.Contains(observed.Spec.Tolerations, required) {
			return fmt.Errorf("required profile toleration is missing")
		}
	}
	if !hasPlacementGate(observed) {
		return fmt.Errorf("operator scheduling gate is missing")
	}
	if observed.Spec.SecurityContext == nil || observed.Spec.SecurityContext.FSGroup == nil || *observed.Spec.SecurityContext.FSGroup != 10001 {
		return fmt.Errorf("runtime volume group changed")
	}
	return nil
}

func validateAdmittedRuntime(desired, observed *corev1.Pod) (*corev1.Container, error) {
	var actual *corev1.Container
	for i := range observed.Spec.Containers {
		container := &observed.Spec.Containers[i]
		if container.Name == "opensearch" {
			if actual != nil {
				return nil, fmt.Errorf("multiple OpenSearch containers")
			}
			actual = container
		} else {
			for _, mount := range container.VolumeMounts {
				if protectedRuntimeVolume(mount.Name) {
					return nil, fmt.Errorf("sidecar mounts a protected runtime volume")
				}
			}
		}
	}
	for _, container := range observed.Spec.InitContainers {
		for _, mount := range container.VolumeMounts {
			if protectedRuntimeVolume(mount.Name) {
				return nil, fmt.Errorf("init container mounts a protected runtime volume")
			}
		}
	}
	if desired.Annotations[peerDNSPreflightAnnotation] == "v1" {
		var expected, actual *corev1.Container
		for i := range desired.Spec.InitContainers {
			if desired.Spec.InitContainers[i].Name == "peer-dns-preflight" {
				expected = &desired.Spec.InitContainers[i]
			}
		}
		for i := range observed.Spec.InitContainers {
			if observed.Spec.InitContainers[i].Name == "peer-dns-preflight" {
				if actual != nil {
					return nil, fmt.Errorf("duplicate peer DNS preflight")
				}
				actual = &observed.Spec.InitContainers[i]
			}
		}
		if expected == nil || actual == nil || actual.Image != expected.Image || actual.RestartPolicy != nil || len(actual.VolumeMounts) != 0 || len(actual.VolumeDevices) != 0 || !slices.Equal(actual.Command, expected.Command) || !slices.Equal(actual.Args, expected.Args) || !reflect.DeepEqual(actual.Env, expected.Env) {
			return nil, fmt.Errorf("peer DNS preflight changed")
		}
		for name, request := range expected.Resources.Requests {
			admitted, ok := actual.Resources.Requests[name]
			if !ok || admitted.Cmp(request) < 0 {
				return nil, fmt.Errorf("peer DNS preflight resource request decreased")
			}
		}
	}
	want := desired.Spec.Containers[0]
	if actual == nil || actual.Image != want.Image || !slices.Equal(actual.Command, want.Command) || !slices.Equal(actual.Args, want.Args) || !reflect.DeepEqual(actual.Env, want.Env) || len(actual.EnvFrom) != 0 || actual.ReadinessProbe == nil || want.ReadinessProbe == nil || !reflect.DeepEqual(actual.ReadinessProbe, want.ReadinessProbe) {
		return nil, fmt.Errorf("Java launch or readiness configuration changed")
	}
	if actual.RestartPolicy != nil || podRestartPolicy(desired) != podRestartPolicy(observed) {
		return nil, fmt.Errorf("requested OpenSearch restart policy changed")
	}
	if err := validateAdmittedRuntimeResources(want, *actual); err != nil {
		return nil, err
	}
	return actual, nil
}

func podRestartPolicy(pod *corev1.Pod) corev1.RestartPolicy {
	if pod.Spec.RestartPolicy == "" {
		return corev1.RestartPolicyAlways
	}
	return pod.Spec.RestartPolicy
}

func validateAdmittedRuntimeResources(want, actual corev1.Container) error {
	for name, request := range want.Resources.Requests {
		admitted, ok := actual.Resources.Requests[name]
		if !ok || admitted.Cmp(request) < 0 {
			return fmt.Errorf("admitted %s request is below the intended request", name)
		}
	}
	for name, limit := range want.Resources.Limits {
		admitted, ok := actual.Resources.Limits[name]
		if !ok || admitted.Cmp(limit) != 0 {
			return fmt.Errorf("admitted %s limit differs from the intended limit", name)
		}
	}
	if limit, ok := actual.Resources.Limits[corev1.ResourceMemory]; ok {
		// Managed arguments start with the fixed initial and maximum heap.
		if len(want.Args) < 2 || !strings.HasPrefix(want.Args[1], "-Xmx") {
			return fmt.Errorf("cannot check the admitted memory limit against the JVM heap")
		}
		heap, err := v1alpha1.ParseJVMHeapBytes(strings.TrimPrefix(want.Args[1], "-Xmx"))
		if err != nil || limit.CmpInt64(heap) <= 0 {
			return fmt.Errorf("admitted memory limit must exceed the JVM heap")
		}
	}
	return nil
}

func validateAdmittedSidecars(desired, observed *corev1.Pod) error {
	// Admission may add containers, but it must preserve the launch identity
	// and resource requests of each sidecar explicitly requested by the pool.
	for _, expected := range desired.Spec.Containers[1:] {
		var found *corev1.Container
		for i := range observed.Spec.Containers {
			if observed.Spec.Containers[i].Name == expected.Name {
				if found != nil {
					return fmt.Errorf("duplicate requested sidecar %q", expected.Name)
				}
				found = &observed.Spec.Containers[i]
			}
		}
		if found == nil || found.Image != expected.Image || !slices.Equal(found.Command, expected.Command) || !slices.Equal(found.Args, expected.Args) {
			return fmt.Errorf("requested sidecar %q launch changed", expected.Name)
		}
		if err := validateSidecarConfiguration(expected, *found); err != nil {
			return err
		}
		for name, request := range expected.Resources.Requests {
			admitted, ok := found.Resources.Requests[name]
			if !ok || admitted.Cmp(request) < 0 {
				return fmt.Errorf("requested sidecar %q %s request decreased", expected.Name, name)
			}
		}
	}
	// Requested restartable sidecars must stay ahead of managed preflights
	// and installers too: their placement changes init resource overlap and
	// the startup contract. Admission may insert additional init containers.
	lastInitIndex := -1
	for _, expected := range desired.Spec.InitContainers {
		index := -1
		for i := range observed.Spec.InitContainers {
			if observed.Spec.InitContainers[i].Name == expected.Name {
				if index >= 0 {
					return fmt.Errorf("duplicate requested init container %q", expected.Name)
				}
				index = i
			}
		}
		if index <= lastInitIndex {
			return fmt.Errorf("requested init container %q is missing or reordered", expected.Name)
		}
		lastInitIndex = index
	}
	lastNativeIndex := -1
	for _, expected := range desired.Spec.InitContainers {
		if expected.RestartPolicy == nil || *expected.RestartPolicy != corev1.ContainerRestartPolicyAlways {
			continue
		}
		var found *corev1.Container
		index := -1
		for i := range observed.Spec.InitContainers {
			if observed.Spec.InitContainers[i].Name == expected.Name {
				if found != nil {
					return fmt.Errorf("duplicate native sidecar %q", expected.Name)
				}
				found, index = &observed.Spec.InitContainers[i], i
			}
		}
		if found == nil || index <= lastNativeIndex || found.RestartPolicy == nil || *found.RestartPolicy != corev1.ContainerRestartPolicyAlways || found.Image != expected.Image || !slices.Equal(found.Command, expected.Command) || !slices.Equal(found.Args, expected.Args) {
			return fmt.Errorf("native sidecar %q launch or order changed", expected.Name)
		}
		lastNativeIndex = index
		if err := validateSidecarConfiguration(expected, *found); err != nil {
			return err
		}
		for name, request := range expected.Resources.Requests {
			admitted, ok := found.Resources.Requests[name]
			if !ok || admitted.Cmp(request) < 0 {
				return fmt.Errorf("native sidecar %q %s request decreased", expected.Name, name)
			}
		}
	}
	return nil
}

func validateAdmittedRuntimeData(want corev1.Container, observed *corev1.Pod, actual *corev1.Container) error {
	const dataPath = "/var/lib/serverless"
	var dataVolume, dataMount bool
	for _, volume := range observed.Spec.Volumes {
		if volume.Name == "serverless-data" {
			dataVolume = volume.EmptyDir != nil && volume.EmptyDir.Medium == "" && volume.EmptyDir.SizeLimit == nil
		}
	}
	for _, mount := range actual.VolumeMounts {
		if mount.Name == "serverless-data" && mount.MountPath == dataPath && !mount.ReadOnly && mount.SubPath == "" && mount.SubPathExpr == "" {
			dataMount = true
		}
		if overlapsCredentialMount(mount.MountPath, dataPath) && !slices.ContainsFunc(want.VolumeMounts, func(expected corev1.VolumeMount) bool {
			return reflect.DeepEqual(expected, mount)
		}) {
			return fmt.Errorf("unexpected volume mount overlaps runtime data")
		}
	}
	if !dataVolume || !dataMount {
		return fmt.Errorf("runtime data volume or mount changed")
	}
	return nil
}

func validateAdmittedRuntimeVolumes(desired, observed *corev1.Pod, actual *corev1.Container) error {
	want := desired.Spec.Containers[0]
	if err := validateAdmittedRuntimeData(want, observed, actual); err != nil {
		return err
	}
	if desired.Annotations[keystoreUIDAnnotation] != "" {
		if err := validateKeystoreMount(desired, observed, actual); err != nil {
			return err
		}
	}
	for _, entry := range want.VolumeMounts {
		if !tlsRuntimeVolume(entry.Name) {
			continue
		}
		var expected, got *corev1.SecretVolumeSource
		for _, volume := range desired.Spec.Volumes {
			if volume.Name == entry.Name {
				expected = volume.Secret
			}
		}
		for _, volume := range observed.Spec.Volumes {
			if volume.Name == entry.Name {
				got = volume.Secret
			}
		}
		mounted := false
		for _, mount := range actual.VolumeMounts {
			if (mount.MountPath == entry.MountPath || strings.HasPrefix(mount.MountPath, entry.MountPath+"/") || strings.HasPrefix(entry.MountPath, strings.TrimRight(mount.MountPath, "/")+"/")) && (mount.Name != entry.Name || mount.MountPath != entry.MountPath) {
				return fmt.Errorf("another volume shadows TLS material")
			}
			if mount.Name == entry.Name && mount.MountPath == entry.MountPath && mount.ReadOnly && mount.SubPath == "" && mount.SubPathExpr == "" {
				mounted = true
			}
		}
		if expected == nil || got == nil || !reflect.DeepEqual(expected, got) || !mounted {
			return fmt.Errorf("TLS Secret or mount changed")
		}
		if entry.Name == httpCertificateVolume {
			if err := validateHTTPKeyAccess(observed, expected.SecretName); err != nil {
				return err
			}
		}
		if entry.Name == "serverless-peer-cert" {
			if err := validateProtectedSecretAccess(observed, expected.SecretName, "serverless-peer-cert", "serverless-peer-trust"); err != nil {
				return err
			}
		}
	}
	return nil
}
