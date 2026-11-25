package controller

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"reflect"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/huyz0/opensearch-serverless-operator/api/v1alpha1"
	"github.com/huyz0/opensearch-serverless-operator/internal/workload"
)

const pluginIntentAnnotation = "serverless.opensearch.org/plugin-intent"

func withPinnedPluginInstaller(pod *corev1.Pod, pin *v1alpha1.PluginInstallIntent) (*corev1.Pod, error) {
	return workload.WithPluginInstaller(pod, workload.PluginInstallerInput{
		Image: pin.InstallerImage, RequestConfigMap: pin.RequestName, PolicyConfigMap: pin.PolicyName,
		RequestSHA256: pin.RequestSHA256, PolicySHA256: pin.PolicySHA256,
		CredentialsSecret: pin.CredentialsName, CredentialsHasCA: pin.CredentialsHasCA,
		JavaPluginsPath: "/var/lib/serverless/plugins", VolumeSizeLimit: resource.MustParse("2Gi"),
		Resources: corev1.ResourceRequirements{Requests: corev1.ResourceList{
			corev1.ResourceCPU: resource.MustParse("250m"), corev1.ResourceMemory: resource.MustParse("512Mi"),
			corev1.ResourceEphemeralStorage: resource.MustParse("2Gi"),
		}},
	})
}

func validateRecordedPluginPod(pod *corev1.Pod, pin *v1alpha1.PluginInstallIntent) error {
	desired := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Annotations: map[string]string{pluginIntentAnnotation: pod.Annotations[pluginIntentAnnotation]}},
		Spec:       corev1.PodSpec{SecurityContext: pod.Spec.SecurityContext},
	}
	for _, container := range pod.Spec.Containers {
		if container.Name == "opensearch" {
			desired.Spec.Containers = append(desired.Spec.Containers, corev1.Container{Name: container.Name, SecurityContext: container.SecurityContext})
		}
	}
	if pin != nil {
		var err error
		desired, err = withPinnedPluginInstaller(desired, pin)
		if err != nil {
			return err
		}
		if podStartupReady(pod) && !pluginInstallerComplete(pod) {
			return fmt.Errorf("Ready Pod lacks a successful plugin installation")
		}
	}
	return validatePluginAdmission(desired, pod)
}

func pluginIntentFromPod(pod *corev1.Pod) (*v1alpha1.PluginInstallIntent, error) {
	if pod == nil {
		return nil, fmt.Errorf("Pod is required")
	}
	value := pod.Annotations[pluginIntentAnnotation]
	if value == "" {
		return nil, nil
	}
	if len(value) > 8192 {
		return nil, fmt.Errorf("plugin intent annotation is too large")
	}
	var intent v1alpha1.PluginInstallIntent
	decoder := json.NewDecoder(bytes.NewBufferString(value))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&intent); err != nil {
		return nil, fmt.Errorf("invalid plugin intent annotation")
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return nil, fmt.Errorf("plugin intent annotation has trailing data")
	}
	return &intent, nil
}

func pluginSnapshotName(ownerUID, scope, kind, digest string) string {
	hash := sha256.Sum256([]byte(ownerUID + "\n" + scope + "\n" + kind + "\n" + digest))
	return fmt.Sprintf("plugin-%s-%x", kind, hash[:10])
}

func pluginInstallIntent(instance *v1alpha1.OpenSearchServerless, inputs *pluginInputs) *v1alpha1.PluginInstallIntent {
	if inputs == nil {
		return nil
	}
	// The colon distinguishes shared content names from legacy Pod-name scopes.
	const scope = "shared:v1"
	return &v1alpha1.PluginInstallIntent{
		InstallerImage: inputs.InstallerImage, PolicySourceName: inputs.PolicyName,
		PolicySourceUID: inputs.PolicyUID, PolicySourceVersion: inputs.PolicyVersion,
		RequestName:   pluginSnapshotName(string(instance.UID), scope, "req", inputs.Resolution.RequestSHA256),
		PolicyName:    pluginSnapshotName(string(instance.UID), scope, "pol", inputs.Resolution.PolicySHA256),
		RequestSHA256: inputs.Resolution.RequestSHA256, PolicySHA256: inputs.Resolution.PolicySHA256,
		CredentialsName: inputs.Resolution.CredentialsSecret, CredentialsUID: inputs.CredentialsUID,
		CredentialsHasCA: inputs.CredentialsHasCA,
	}
}

func checkPluginIntent(instance *v1alpha1.OpenSearchServerless, podName string, intent *v1alpha1.PluginInstallIntent, inputs *pluginInputs) error {
	return checkRecordedPluginIntent(instance, podName, intent, pluginInstallIntent(instance, inputs))
}

func checkRecordedPluginIntent(instance *v1alpha1.OpenSearchServerless, podName string, intent, expected *v1alpha1.PluginInstallIntent) error {
	if expected == nil || intent == nil {
		if expected != nil || intent != nil {
			return fmt.Errorf("plugin intent differs from selected role inputs")
		}
		return nil
	}
	actual := *intent
	actual.RequestUID, actual.PolicyUID = "", ""
	// Earlier operators persisted a separate pair for each Pod. Reconcile
	// that exact pair when it is already recorded on the intent or Pod.
	want := *expected
	if actual.RequestName == pluginSnapshotName(string(instance.UID), podName, "req", expected.RequestSHA256) &&
		actual.PolicyName == pluginSnapshotName(string(instance.UID), podName, "pol", expected.PolicySHA256) {
		want.RequestName, want.PolicyName = actual.RequestName, actual.PolicyName
	}
	if actual != want {
		return fmt.Errorf("plugin input identity or content changed")
	}
	if (intent.RequestUID == "") != (intent.PolicyUID == "") {
		return fmt.Errorf("plugin snapshot UIDs must be recorded together")
	}
	return nil
}

// ensurePluginSnapshots never replaces a conflicting object. Once UIDs are
// committed, missing or recreated snapshots block rather than being recreated.
func (r *InstanceReconciler) ensurePluginSnapshots(ctx context.Context, instance *v1alpha1.OpenSearchServerless, intent *v1alpha1.PluginInstallIntent, inputs *pluginInputs) (string, string, error) {
	if intent == nil || inputs == nil {
		return "", "", fmt.Errorf("plugin intent and resolved inputs are required")
	}
	immutable, controller := true, true
	configMaps := r.Core.ConfigMaps(instance.Namespace)
	ensure := func(name, key, value, uid string) (*corev1.ConfigMap, error) {
		object, err := configMaps.Get(ctx, name, metav1.GetOptions{})
		if apierrors.IsNotFound(err) && uid == "" {
			object, err = configMaps.Create(ctx, &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: instance.Namespace, OwnerReferences: []metav1.OwnerReference{{APIVersion: v1alpha1.GroupVersion.String(), Kind: "OpenSearchServerless", Name: instance.Name, UID: instance.UID, Controller: &controller}}}, Immutable: &immutable, Data: map[string]string{key: value}}, metav1.CreateOptions{})
			if apierrors.IsAlreadyExists(err) {
				object, err = configMaps.Get(ctx, name, metav1.GetOptions{})
			}
		}
		if err != nil {
			return nil, fmt.Errorf("read or create plugin snapshot: %w", err)
		}
		owner := metav1.GetControllerOf(object)
		if object.UID == "" || object.DeletionTimestamp != nil || (uid != "" && string(object.UID) != uid) || owner == nil || owner.APIVersion != v1alpha1.GroupVersion.String() || owner.Kind != "OpenSearchServerless" || owner.Name != instance.Name || owner.UID != instance.UID || object.Immutable == nil || !*object.Immutable || len(object.BinaryData) != 0 || !reflect.DeepEqual(object.Data, map[string]string{key: value}) {
			return nil, fmt.Errorf("plugin snapshot conflicts with recorded identity or content")
		}
		return object, nil
	}
	request, err := ensure(intent.RequestName, "request.json", string(inputs.Resolution.RequestJSON), intent.RequestUID)
	if err != nil {
		return "", "", err
	}
	policy, err := ensure(intent.PolicyName, "policy.json", string(inputs.PolicyJSON), intent.PolicyUID)
	if err != nil {
		return "", "", err
	}
	return string(request.UID), string(policy.UID), nil
}
