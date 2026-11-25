//go:build envtest

package controller_test

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"

	apiextensionsclient "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"

	"github.com/huyz0/opensearch-serverless-operator/api/v1alpha1"
)

func TestAdmissionUpgradePreservesLegacyObjects(t *testing.T) {
	environment := &envtest.Environment{
		CRDDirectoryPaths:     []string{filepath.Join("..", "..", "config", "generated-dev")},
		ErrorIfCRDPathMissing: true,
	}
	config, err := environment.Start()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := environment.Stop(); err != nil {
			t.Error(err)
		}
	})
	scheme := runtime.NewScheme()
	if err := v1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	kube, err := client.New(config, client.Options{Scheme: scheme})
	if err != nil {
		t.Fatal(err)
	}
	extensions, err := apiextensionsclient.NewForConfig(config)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	defer cancel()
	crds := extensions.ApiextensionsV1().CustomResourceDefinitions()
	const crdName = "opensearchserverlessoperations.serverless.opensearch.org"
	tightened, err := crds.Get(ctx, crdName, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	previous := tightened.DeepCopy()
	schema := previous.Spec.Versions[0].Schema.OpenAPIV3Schema
	spec := schema.Properties["spec"]
	// Drop the settings payload rule to reproduce the older admission gap.
	for i, rule := range spec.XValidations {
		if strings.Contains(rule.Rule, "has(self.clusterSettings)") {
			spec.XValidations = append(spec.XValidations[:i], spec.XValidations[i+1:]...)
			break
		}
	}
	action := spec.Properties["action"]
	action.Enum = nil
	spec.Properties["action"] = action
	settings := spec.Properties["clusterSettings"]
	settings.XValidations = nil
	spec.Properties["clusterSettings"] = settings
	schema.Properties["spec"] = spec
	if _, err := crds.Update(ctx, previous, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	const finalizer = "test.serverless.opensearch.org/legacy-fixture"
	newInvalidOperation := func(name string) *v1alpha1.OpenSearchServerlessOperation {
		return &v1alpha1.OpenSearchServerlessOperation{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default", Finalizers: []string{finalizer}},
			Spec: v1alpha1.OperationSpec{
				Action:      "ApplyClusterSettings",
				InstanceRef: v1alpha1.InstanceReference{Name: "catalog", UID: "fixture"},
			},
		}
	}
	legacy := []struct {
		operation *v1alpha1.OpenSearchServerlessOperation
		rejection string
	}{
		{newInvalidOperation("legacy-settings"), "clusterSettings is required only"},
		{newInvalidOperation("legacy-empty-patch"), "patch must not be empty"},
		{newInvalidOperation("legacy-unknown-action"), "Unsupported value"},
	}
	legacy[1].operation.Spec.ClusterSettings = &v1alpha1.ClusterSettingsPatch{}
	legacy[2].operation.Spec.Action = "Unknown"
	for _, entry := range legacy {
		if err := wait.PollUntilContextTimeout(ctx, 100*time.Millisecond, 10*time.Second, true, func(ctx context.Context) (bool, error) {
			err := kube.Create(ctx, entry.operation)
			if apierrors.IsInvalid(err) {
				return false, nil
			}
			return err == nil, err
		}); err != nil {
			t.Fatalf("the old schema did not admit %s: %v", entry.operation.Name, err)
		}
	}
	current, err := crds.Get(ctx, crdName, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	current.Spec = tightened.Spec
	if _, err := crds.Update(ctx, current, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	for _, entry := range legacy {
		operation := entry.operation
		probe := newInvalidOperation("new-" + operation.Name)
		probe.Spec = operation.Spec
		if err := wait.PollUntilContextTimeout(ctx, 100*time.Millisecond, 10*time.Second, true, func(ctx context.Context) (bool, error) {
			err := kube.Create(ctx, probe, client.DryRunAll)
			if apierrors.IsInvalid(err) && strings.Contains(err.Error(), entry.rejection) {
				return true, nil
			}
			return false, err
		}); err != nil {
			t.Fatalf("the tightened schema did not reject %s: %v", probe.Name, err)
		}
		operation.Status.Phase = "Pending"
		if err := kube.Status().Update(ctx, operation); err != nil {
			t.Fatalf("%s must still accept status updates: %v", operation.Name, err)
		}
		if err := kube.Delete(ctx, operation); err != nil {
			t.Fatal(err)
		}
		if err := kube.Get(ctx, client.ObjectKeyFromObject(operation), operation); err != nil {
			t.Fatal(err)
		}
		operation.Finalizers = nil
		if err := kube.Update(ctx, operation); err != nil {
			t.Fatalf("%s must still allow finalizer cleanup: %v", operation.Name, err)
		}
	}
	t.Run("invalid instance inputs can be repaired", func(t *testing.T) {
		checkInstanceAdmissionUpgrade(t, ctx, kube, extensions)
	})

}

func checkInstanceAdmissionUpgrade(t *testing.T, ctx context.Context, kube client.Client, extensions *apiextensionsclient.Clientset) {
	t.Helper()
	crds := extensions.ApiextensionsV1().CustomResourceDefinitions()
	const crdName = "opensearchserverlesses.serverless.opensearch.org"
	tightened, err := crds.Get(ctx, crdName, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	previous := tightened.DeepCopy()
	schema := previous.Spec.Versions[0].Schema.OpenAPIV3Schema
	spec := schema.Properties["spec"]
	storage := spec.Properties["storage"]
	credentials := storage.Properties["credentials"]
	credentials.XValidations = nil
	storage.Properties["credentials"] = credentials
	spec.Properties["storage"] = storage
	plugin := spec.Properties["plugins"].Items.Schema
	artifact := plugin.Properties["artifact"]
	artifact.Pattern = ""
	plugin.Properties["artifact"] = artifact
	schema.Properties["spec"] = spec
	if _, err := crds.Update(ctx, previous, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	legacy := &v1alpha1.OpenSearchServerless{
		ObjectMeta: metav1.ObjectMeta{Name: "legacy-credentials", Namespace: "default"},
		Spec: v1alpha1.OpenSearchServerlessSpec{
			Image: "example/image@sha256:" + strings.Repeat("a", 64), ServiceAccountName: "runtime",
			Storage: v1alpha1.Storage{
				Type: "s3", Bucket: "bucket", Region: "region",
				Credentials: &v1alpha1.StorageCredentials{
					Type: "WorkloadIdentity", AWSRoleARN: "arn:aws:iam::123456789012:role/store",
					KeystoreSecretRef: &v1alpha1.LocalReference{Name: "invalid-for-web-identity"},
				},
			},
			Pools:   map[string]v1alpha1.Pool{"search": {Zones: map[string]v1alpha1.Zone{"a": {Replicas: 1}}}},
			Plugins: []v1alpha1.Plugin{{Name: "analysis-icu", Artifact: "example.com/plugins/analysis-icu:latest"}},
		},
	}
	if err := wait.PollUntilContextTimeout(ctx, 100*time.Millisecond, 10*time.Second, true, func(ctx context.Context) (bool, error) {
		err := kube.Create(ctx, legacy)
		if apierrors.IsInvalid(err) {
			return false, nil
		}
		return err == nil, err
	}); err != nil {
		t.Fatalf("the old instance schema did not admit the credential combination: %v", err)
	}
	current, err := crds.Get(ctx, crdName, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	current.Spec = tightened.Spec
	if _, err := crds.Update(ctx, current, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	if err := wait.PollUntilContextTimeout(ctx, 100*time.Millisecond, 10*time.Second, true, func(ctx context.Context) (bool, error) {
		probe := &v1alpha1.OpenSearchServerless{
			ObjectMeta: metav1.ObjectMeta{Name: "new-invalid-credentials", Namespace: legacy.Namespace},
			Spec:       *legacy.Spec.DeepCopy(),
		}
		err := kube.Create(ctx, probe, client.DryRunAll)
		if apierrors.IsInvalid(err) && strings.Contains(err.Error(), "select an AWS role ARN") &&
			strings.Contains(err.Error(), "plugins[0].artifact") {
			return true, nil
		}
		return false, err
	}); err != nil {
		t.Fatalf("the tightened schema did not reject new mixed credentials and the mutable plugin tag: %v", err)
	}
	legacy.Status.ObservedGeneration = legacy.Generation
	if err := kube.Status().Update(ctx, legacy); err != nil {
		t.Fatalf("legacy instance status must still be writable: %v", err)
	}
	legacy.Spec.Rollout = &v1alpha1.Rollout{Strategy: "Rolling"}
	if err := kube.Update(ctx, legacy); err != nil {
		t.Fatalf("unrelated spec changes must remain possible: %v", err)
	}
	invalid := legacy.DeepCopy()
	invalid.Spec.Storage.Credentials.KeystoreSecretRef.Name = "another-invalid-reference"
	if err := kube.Update(ctx, invalid); !apierrors.IsInvalid(err) || !strings.Contains(err.Error(), "select an AWS role ARN") {
		t.Fatalf("a changed credential combination must validate: %v", err)
	}
	legacy.Spec.Storage.Credentials.KeystoreSecretRef = nil
	if err := kube.Update(ctx, legacy); err != nil {
		t.Fatalf("legacy instance credentials must be repairable: %v", err)
	}
	legacy.Spec.Plugins = []v1alpha1.Plugin{{Name: "analysis-icu", Artifact: "example.com/plugins/analysis-icu@sha256:" + strings.Repeat("a", 64)}}
	if err := kube.Update(ctx, legacy); err != nil {
		t.Fatalf("legacy plugin selections must be repairable: %v", err)
	}
}
