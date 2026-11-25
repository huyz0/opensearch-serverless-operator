package controller

import (
	"context"
	"fmt"
	"net/http"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	corev1 "k8s.io/client-go/kubernetes/typed/core/v1"

	"github.com/huyz0/opensearch-serverless-operator/api/v1alpha1"
	"github.com/huyz0/opensearch-serverless-operator/internal/java"
	"github.com/huyz0/opensearch-serverless-operator/internal/workload"
)

// SettingsRead ties a value observation to the exact admin Service identity.
type SettingsRead struct {
	Values     map[string]string
	ServiceUID types.UID
}

// SettingsBackend abstracts the qualified admin path for operation recovery.
type SettingsBackend interface {
	Read(context.Context, *v1alpha1.OpenSearchServerless) (SettingsRead, error)
	Apply(context.Context, *v1alpha1.OpenSearchServerless, types.UID, v1alpha1.ClusterSettingsPatch) error
}

var developmentSettings = map[string]bool{"serverless.bucket": true}

// developmentHTTPSettings is only suitable for the unauthenticated isolated
// development mode. The namespace must control Service and Pod mutation.
type developmentHTTPSettings struct {
	core corev1.CoreV1Interface
	http *http.Client
}

type settingsEndpoint struct {
	url     string
	uid     types.UID
	version string
}

func (b developmentHTTPSettings) endpoint(ctx context.Context, instance *v1alpha1.OpenSearchServerless) (settingsEndpoint, error) {
	if b.core == nil || instance == nil {
		return settingsEndpoint{}, fmt.Errorf("admin Service client or instance unavailable")
	}
	fleet := instance.Status.ServingFleetIntentByRole["ingest"]
	desired, err := workload.RoleService(instance, "ingest", fleet, 9200)
	if err != nil {
		return settingsEndpoint{}, err
	}
	service, err := b.core.Services(instance.Namespace).Get(ctx, desired.Name, metav1.GetOptions{})
	if err != nil {
		return settingsEndpoint{}, fmt.Errorf("read admin Service: %w", err)
	}
	if !matchesFleetService(instance, service, desired, "") {
		return settingsEndpoint{}, fmt.Errorf("admin Service identity or selector conflicts")
	}
	return settingsEndpoint{url: fmt.Sprintf("http://%s.%s.svc:9200", service.Name, instance.Namespace), uid: service.UID, version: service.ResourceVersion}, nil
}

func (b developmentHTTPSettings) Read(ctx context.Context, instance *v1alpha1.OpenSearchServerless) (SettingsRead, error) {
	endpoint, err := b.endpoint(ctx, instance)
	if err != nil {
		return SettingsRead{}, err
	}
	values, err := java.GetPersistentSettings(ctx, b.http, endpoint.url)
	if err != nil {
		return SettingsRead{}, err
	}
	after, err := b.endpoint(ctx, instance)
	if err != nil || endpoint != after {
		return SettingsRead{}, fmt.Errorf("admin Service changed during settings read")
	}
	return SettingsRead{Values: values, ServiceUID: endpoint.uid}, nil
}

func (b developmentHTTPSettings) Apply(ctx context.Context, instance *v1alpha1.OpenSearchServerless, expectedUID types.UID, patch v1alpha1.ClusterSettingsPatch) error {
	endpoint, err := b.endpoint(ctx, instance)
	if err != nil {
		return err
	}
	if endpoint.uid != expectedUID {
		return fmt.Errorf("admin Service was replaced")
	}
	if err := java.ApplyClusterSettings(ctx, b.http, endpoint.url, patch, developmentSettings); err != nil {
		return err
	}
	after, err := b.endpoint(ctx, instance)
	if err != nil || endpoint != after {
		return fmt.Errorf("admin Service changed during settings write; outcome is unknown")
	}
	return nil
}

func newDevelopmentSettingsBackend(core corev1.CoreV1Interface) SettingsBackend {
	return developmentHTTPSettings{core: core, http: &http.Client{Timeout: 5 * time.Second, Transport: &http.Transport{Proxy: nil}, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}}
}
