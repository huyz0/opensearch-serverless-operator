package controller

import (
	"fmt"
	"reflect"
	"slices"
	"strings"

	corev1 "k8s.io/api/core/v1"
)

func pluginReservedVolume(name string) bool {
	return name == "serverless-plugins" || name == "serverless-plugin-request" || name == "serverless-plugin-policy" || name == "serverless-plugin-credentials"
}

func mountPathsOverlap(a, b string) bool {
	a, b = strings.TrimRight(a, "/"), strings.TrimRight(b, "/")
	return a == b || strings.HasPrefix(a, b+"/") || strings.HasPrefix(b, a+"/")
}

// validatePluginAdmission protects pinned plugin inputs, the installer and the
// Java mount from admission changes and sidecar access to credentials.
func validatePluginAdmission(desired, observed *corev1.Pod) error {
	enabled := desired.Annotations[pluginIntentAnnotation] != ""
	if observed.Annotations[pluginIntentAnnotation] != desired.Annotations[pluginIntentAnnotation] {
		return fmt.Errorf("plugin intent changed")
	}
	var expectedInstaller, actualInstaller, expectedApp, actualApp *corev1.Container
	for i := range desired.Spec.InitContainers {
		if desired.Spec.InitContainers[i].Name == "plugin-installer" {
			expectedInstaller = &desired.Spec.InitContainers[i]
		}
	}
	for i := range observed.Spec.InitContainers {
		c := &observed.Spec.InitContainers[i]
		if c.Name == "plugin-installer" {
			if actualInstaller != nil {
				return fmt.Errorf("duplicate plugin installer")
			}
			actualInstaller = c
			continue
		}
		for _, mount := range c.VolumeMounts {
			if pluginReservedVolume(mount.Name) {
				return fmt.Errorf("another init container mounts plugin inputs")
			}
		}
	}
	for i := range desired.Spec.Containers {
		if desired.Spec.Containers[i].Name == "opensearch" {
			expectedApp = &desired.Spec.Containers[i]
		}
	}
	for i := range observed.Spec.Containers {
		c := &observed.Spec.Containers[i]
		if c.Name == "opensearch" {
			actualApp = c
			continue
		}
		for _, mount := range c.VolumeMounts {
			if pluginReservedVolume(mount.Name) {
				return fmt.Errorf("sidecar mounts plugin inputs")
			}
		}
	}
	if !enabled {
		if actualInstaller != nil {
			return fmt.Errorf("unexpected plugin installer")
		}
		for _, volume := range observed.Spec.Volumes {
			if pluginReservedVolume(volume.Name) {
				return fmt.Errorf("unexpected plugin volume")
			}
		}
		return nil
	}
	if expectedInstaller == nil || actualInstaller == nil || expectedApp == nil || actualApp == nil || actualInstaller.Image != expectedInstaller.Image || actualInstaller.ImagePullPolicy != expectedInstaller.ImagePullPolicy || actualInstaller.TerminationMessagePath != expectedInstaller.TerminationMessagePath || actualInstaller.TerminationMessagePolicy != expectedInstaller.TerminationMessagePolicy || actualInstaller.WorkingDir != expectedInstaller.WorkingDir || actualInstaller.Stdin || actualInstaller.StdinOnce || actualInstaller.TTY || actualInstaller.Lifecycle != nil || actualInstaller.LivenessProbe != nil || actualInstaller.ReadinessProbe != nil || actualInstaller.StartupProbe != nil || actualInstaller.RestartPolicy != nil || len(actualInstaller.VolumeDevices) != 0 || !slices.Equal(actualInstaller.Command, expectedInstaller.Command) || !slices.Equal(actualInstaller.Args, expectedInstaller.Args) || !reflect.DeepEqual(actualInstaller.Env, expectedInstaller.Env) || !reflect.DeepEqual(actualInstaller.EnvFrom, expectedInstaller.EnvFrom) || !reflect.DeepEqual(actualInstaller.SecurityContext, expectedInstaller.SecurityContext) || !reflect.DeepEqual(actualApp.SecurityContext, expectedApp.SecurityContext) {
		return fmt.Errorf("plugin installer or Java identity changed")
	}
	for name, required := range expectedInstaller.Resources.Requests {
		actual, ok := actualInstaller.Resources.Requests[name]
		if !ok || actual.Cmp(required) < 0 {
			return fmt.Errorf("plugin installer resource request decreased")
		}
	}
	for _, expected := range expectedInstaller.VolumeMounts {
		found := false
		for _, actual := range actualInstaller.VolumeMounts {
			if reflect.DeepEqual(actual, expected) {
				found = true
			}
		}
		if !found {
			return fmt.Errorf("plugin installer mount changed")
		}
	}
	for _, actual := range actualInstaller.VolumeMounts {
		matched := false
		for _, expected := range expectedInstaller.VolumeMounts {
			if reflect.DeepEqual(actual, expected) {
				matched = true
			}
		}
		if !matched {
			if pluginReservedVolume(actual.Name) {
				return fmt.Errorf("plugin installer mount is shadowed")
			}
			for _, expected := range expectedInstaller.VolumeMounts {
				if mountPathsOverlap(actual.MountPath, expected.MountPath) {
					return fmt.Errorf("plugin installer mount is shadowed")
				}
			}
		}
	}
	const pluginPath = "/var/lib/serverless/plugins"
	pluginMount := corev1.VolumeMount{Name: "serverless-plugins", MountPath: pluginPath, SubPath: "plugins", ReadOnly: true}
	foundMount := false
	for _, mount := range actualApp.VolumeMounts {
		if reflect.DeepEqual(mount, pluginMount) {
			foundMount = true
			continue
		}
		if pluginReservedVolume(mount.Name) || mount.MountPath == pluginPath || strings.HasPrefix(mount.MountPath, pluginPath+"/") || (strings.HasPrefix(pluginPath, strings.TrimRight(mount.MountPath, "/")+"/") && !(mount.Name == "serverless-data" && mount.MountPath == "/var/lib/serverless")) {
			return fmt.Errorf("Java plugin mount is shadowed")
		}
	}
	if !foundMount {
		return fmt.Errorf("Java plugin mount changed")
	}
	for _, name := range []string{"serverless-plugins", "serverless-plugin-request", "serverless-plugin-policy", "serverless-plugin-credentials"} {
		var want, got *corev1.Volume
		for i := range desired.Spec.Volumes {
			if desired.Spec.Volumes[i].Name == name {
				want = &desired.Spec.Volumes[i]
			}
		}
		for i := range observed.Spec.Volumes {
			if observed.Spec.Volumes[i].Name == name {
				if got != nil {
					return fmt.Errorf("duplicate plugin volume")
				}
				got = &observed.Spec.Volumes[i]
			}
		}
		if want == nil || got == nil {
			if want != got {
				return fmt.Errorf("plugin volume changed")
			}
			continue
		}
		if name == "serverless-plugins" {
			if want.EmptyDir == nil || got.EmptyDir == nil || want.EmptyDir.SizeLimit == nil || got.EmptyDir.SizeLimit == nil || got.EmptyDir.SizeLimit.Cmp(*want.EmptyDir.SizeLimit) != 0 || want.EmptyDir.Medium != got.EmptyDir.Medium {
				return fmt.Errorf("plugin data volume changed")
			}
		} else if !reflect.DeepEqual(want.VolumeSource, got.VolumeSource) {
			return fmt.Errorf("plugin input volume changed")
		}
	}
	return nil
}
