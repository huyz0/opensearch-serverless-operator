// Package storage resolves platform-provided primary-store configuration.
package storage

import (
	"context"
	"fmt"
	"net"
	"net/url"
	"strconv"
	"strings"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/client-go/kubernetes/typed/core/v1"

	"github.com/huyz0/opensearch-serverless-operator/api/v1alpha1"
)

// S3 is the non-secret configuration resolved for the Java primary store.
type S3 struct {
	Bucket          string
	Region          string
	Endpoint        string
	PathStyleAccess bool
}

// validateEndpoint accepts the host-only form understood by the pinned S3
// plugin as well as explicit HTTP(S) endpoints. Endpoint values are public
// configuration, so credentials, query parameters and fragments are rejected.
func validateEndpoint(endpoint string) error {
	if endpoint == "" {
		return nil
	}
	if len(endpoint) > 2048 || strings.TrimSpace(endpoint) != endpoint {
		return fmt.Errorf("invalid S3 endpoint")
	}
	value := endpoint
	if !strings.HasPrefix(value, "http://") && !strings.HasPrefix(value, "https://") {
		if strings.Contains(value, "://") {
			return fmt.Errorf("invalid S3 endpoint scheme")
		}
		value = "https://" + value
	}
	parsed, err := url.Parse(value)
	if err != nil || parsed == nil || (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.Host == "" || parsed.Hostname() == "" || parsed.User != nil || parsed.RawQuery != "" || parsed.ForceQuery || strings.Contains(value, "#") || parsed.Opaque != "" || strings.HasSuffix(parsed.Host, ":") {
		return fmt.Errorf("invalid S3 endpoint; use a public HTTP(S) endpoint without credentials, query or fragment")
	}
	host := parsed.Hostname()
	if net.ParseIP(host) == nil && len(validation.IsDNS1123Subdomain(strings.TrimSuffix(strings.ToLower(host), "."))) != 0 {
		return fmt.Errorf("invalid S3 endpoint host")
	}
	if port := parsed.Port(); port != "" {
		number, err := strconv.Atoi(port)
		if err != nil || number < 1 || number > 65535 {
			return fmt.Errorf("invalid S3 endpoint port")
		}
	}
	return nil
}

// ResolveS3 reads a same-namespace repository ConfigMap or inline settings.
// The caller must pin and compare the resolved identity before using it for an
// existing instance. Credentials are managed separately.
func ResolveS3(ctx context.Context, core v1.CoreV1Interface, namespace string, input v1alpha1.Storage) (S3, error) {
	var result S3
	if namespace == "" || input.Type != "s3" {
		return result, fmt.Errorf("namespace and s3 storage type are required")
	}
	if input.RepositoryRef == nil {
		result = S3{Bucket: input.Bucket, Region: input.Region, Endpoint: input.Endpoint}
		if input.PathStyleAccess != nil {
			result.PathStyleAccess = *input.PathStyleAccess
		}
	} else {
		if input.RepositoryRef.Name == "" || input.Bucket != "" || input.Region != "" || input.Endpoint != "" || input.PathStyleAccess != nil {
			return result, fmt.Errorf("repository reference cannot be combined with inline storage settings")
		}
		if core == nil {
			return result, fmt.Errorf("repository configuration client is required")
		}
		config, err := core.ConfigMaps(namespace).Get(ctx, input.RepositoryRef.Name, metav1.GetOptions{})
		if err != nil {
			return result, fmt.Errorf("read repository configuration: %w", err)
		}
		if config.DeletionTimestamp != nil {
			return result, fmt.Errorf("repository configuration is being deleted")
		}
		result.Bucket = config.Data["bucket"]
		result.Region = config.Data["region"]
		result.Endpoint = config.Data["endpoint"]
		if value := config.Data["pathStyleAccess"]; value != "" {
			parsed, err := strconv.ParseBool(value)
			if err != nil {
				return S3{}, fmt.Errorf("invalid pathStyleAccess boolean")
			}
			result.PathStyleAccess = parsed
		}
	}
	if result.Bucket == "" || result.Region == "" {
		return S3{}, fmt.Errorf("s3 bucket and region are required")
	}
	if err := validateEndpoint(result.Endpoint); err != nil {
		return S3{}, err
	}
	return result, nil
}

// PrimaryStoreStatus is the public identity used to pin the resolved store.
func (s S3) PrimaryStoreStatus() v1alpha1.PrimaryStoreStatus {
	return v1alpha1.PrimaryStoreStatus{
		Bucket: s.Bucket, Region: s.Region, Endpoint: s.Endpoint, PathStyleAccess: s.PathStyleAccess,
	}
}

// JavaSettings returns startup settings accepted by the pinned Java shell.
func (s S3) JavaSettings() map[string]string {
	settings := map[string]string{
		"serverless.store.type": "s3", "serverless.store.bucket": s.Bucket,
		"serverless.store.region": s.Region, "serverless.store.path_style_access": strconv.FormatBool(s.PathStyleAccess),
	}
	if s.Endpoint != "" {
		settings["serverless.store.endpoint"] = s.Endpoint
	}
	return settings
}
