//go:build envtest

package controller

import (
	"context"
	"path/filepath"
	"testing"

	corev1 "k8s.io/api/core/v1"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"

	"github.com/huyz0/opensearch-serverless-operator/api/v1alpha1"
)

func TestWeightRouteConditionalPatchAPIServer(t *testing.T) {
	preserve := true
	// This fixture isolates API-server patch semantics. Gateway API shape and
	// data-plane behavior are qualified separately against Envoy Gateway.
	environment := envtest.Environment{CRDDirectoryPaths: []string{filepath.Join("..", "..", "config", "generated-dev")}, ErrorIfCRDPathMissing: true, CRDs: []*apiextensionsv1.CustomResourceDefinition{{
		ObjectMeta: metav1.ObjectMeta{
			Name:        "httproutes.gateway.networking.k8s.io",
			Annotations: map[string]string{"api-approved.kubernetes.io": "unapproved, test-only schema"},
		},
		Spec: apiextensionsv1.CustomResourceDefinitionSpec{
			Group: httpRouteGVK.Group,
			Scope: apiextensionsv1.NamespaceScoped,
			Names: apiextensionsv1.CustomResourceDefinitionNames{Plural: "httproutes", Singular: "httproute", Kind: httpRouteGVK.Kind},
			Versions: []apiextensionsv1.CustomResourceDefinitionVersion{{
				Name: httpRouteGVK.Version, Served: true, Storage: true,
				Schema: &apiextensionsv1.CustomResourceValidation{OpenAPIV3Schema: &apiextensionsv1.JSONSchemaProps{
					Type: "object", Properties: map[string]apiextensionsv1.JSONSchemaProps{
						"spec": {Type: "object", XPreserveUnknownFields: &preserve},
					},
				}},
			}},
		},
	}}}
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
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := v1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	c, err := client.New(config, client.Options{Scheme: scheme})
	if err != nil {
		t.Fatal(err)
	}
	namespace := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "weight-route-test"}}
	if err := c.Create(context.Background(), namespace); err != nil {
		t.Fatal(err)
	}
	exerciseWeightRoutePatch(t, c, namespace.Name)
	core, err := kubernetes.NewForConfig(config)
	if err != nil {
		t.Fatal(err)
	}
	t.Run("signed proxy observations", func(t *testing.T) {
		exerciseSignedWeightEvidence(t, c, core.CoreV1(), namespace.Name)
	})
	t.Run("restart from weighted journal", func(t *testing.T) {
		exerciseWeightedJournal(t, c, core.CoreV1(), core.DiscoveryV1(), namespace.Name, false)
	})
	t.Run("rollback interrupted weighted promotion", func(t *testing.T) {
		mixed := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "weight-rollback-mixed"}}
		if err := c.Create(context.Background(), mixed); err != nil {
			t.Fatal(err)
		}
		exerciseWeightedJournal(t, c, core.CoreV1(), core.DiscoveryV1(), mixed.Name, true)
	})
}
