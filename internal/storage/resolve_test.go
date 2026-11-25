package storage_test

import (
	"context"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/huyz0/opensearch-serverless-operator/api/v1alpha1"
	"github.com/huyz0/opensearch-serverless-operator/internal/storage"
)

func TestResolveS3Repository(t *testing.T) {
	config := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Namespace: "n", Name: "store"}, Data: map[string]string{
		"bucket": "bucket", "region": "region", "endpoint": "https://example.test", "pathStyleAccess": "true",
	}}
	client := fake.NewSimpleClientset(config)
	input := v1alpha1.Storage{Type: "s3", RepositoryRef: &v1alpha1.LocalReference{Name: "store"}}
	resolved, err := storage.ResolveS3(context.Background(), client.CoreV1(), "n", input)
	if err != nil || resolved.Bucket != "bucket" || !resolved.PathStyleAccess {
		t.Fatalf("unexpected resolution: %+v, %v", resolved, err)
	}
	if resolved.JavaSettings()["serverless.store.endpoint"] != config.Data["endpoint"] {
		t.Fatal("endpoint must be passed to Java")
	}
	if _, err := storage.ResolveS3(context.Background(), client.CoreV1(), "other", input); err == nil {
		t.Fatal("repository lookup must remain in the requested namespace")
	}
	input.Bucket = "another"
	if _, err := storage.ResolveS3(context.Background(), client.CoreV1(), "n", input); err == nil {
		t.Fatal("mixed source must be rejected")
	}
}

func TestInlinePathStyle(t *testing.T) {
	pathStyle := true
	input := v1alpha1.Storage{Type: "s3", Bucket: "bucket", Region: "region", PathStyleAccess: &pathStyle}
	resolved, err := storage.ResolveS3(context.Background(), fake.NewSimpleClientset().CoreV1(), "n", input)
	if err != nil || !resolved.PathStyleAccess || resolved.JavaSettings()["serverless.store.path_style_access"] != "true" {
		t.Fatalf("inline path-style setting was lost: %+v, %v", resolved, err)
	}
	input.RepositoryRef = &v1alpha1.LocalReference{Name: "store"}
	if _, err := storage.ResolveS3(context.Background(), fake.NewSimpleClientset().CoreV1(), "n", input); err == nil {
		t.Fatal("mixed inline and referenced settings must fail")
	}
}

func TestS3EndpointRejectsCredentialsAndUnsafeURLComponents(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct{ name, endpoint string }{
		{"url credentials", "https://user:supersecret@storage.example.test"},
		{"host-only credentials", "user:supersecret@storage.example.test:443"},
		{"query", "https://storage.example.test/?token=supersecret"},
		{"fragment", "https://storage.example.test/#supersecret"},
		{"empty fragment", "https://storage.example.test/#"},
		{"unsupported scheme", "ftp://storage.example.test"},
		{"out-of-range port", "https://storage.example.test:99999"},
		{"zero port", "https://storage.example.test:0"},
		{"empty port", "https://storage.example.test:"},
		{"empty query", "https://storage.example.test?"},
		{"control character", "https://storage.example.test/\nsupersecret"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			result, err := storage.ResolveS3(ctx, nil, "n", v1alpha1.Storage{Type: "s3", Bucket: "bucket", Region: "region", Endpoint: tc.endpoint})
			if err == nil || result.Endpoint != "" {
				t.Fatalf("unsafe endpoint accepted: %#v", result)
			}
			if strings.Contains(err.Error(), "supersecret") {
				t.Fatal("validation error exposed endpoint content")
			}
		})
	}

	for _, endpoint := range []string{"storage.example.test", "storage.example.test:9000", "https://storage.example.test", "http://127.0.0.1:9000", "https://[::1]:9000", "https://storage.example.test/prefix"} {
		result, err := storage.ResolveS3(ctx, nil, "n", v1alpha1.Storage{Type: "s3", Bucket: "bucket", Region: "region", Endpoint: endpoint})
		if err != nil || result.Endpoint != endpoint {
			t.Fatalf("valid endpoint rejected: %q %v", endpoint, err)
		}
	}
}

func TestS3RepositoryErrorsDoNotExposeBooleanValues(t *testing.T) {
	ctx := context.Background()
	input := v1alpha1.Storage{Type: "s3", RepositoryRef: &v1alpha1.LocalReference{Name: "store"}}
	if _, err := storage.ResolveS3(ctx, nil, "n", input); err == nil {
		t.Fatal("nil configuration client accepted")
	}
	config := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Namespace: "n", Name: "store"}, Data: map[string]string{"bucket": "bucket", "region": "region", "pathStyleAccess": "supersecret"}}
	core := fake.NewSimpleClientset(config)
	if _, err := storage.ResolveS3(ctx, core.CoreV1(), "n", input); err == nil || strings.Contains(err.Error(), "supersecret") {
		t.Fatalf("unsafe boolean error: %v", err)
	}
	config.Data["pathStyleAccess"] = "true"
	config.Data["endpoint"] = "https://user:supersecret@storage.example.test"
	if _, err := core.CoreV1().ConfigMaps("n").Update(ctx, config, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := storage.ResolveS3(ctx, core.CoreV1(), "n", input); err == nil || strings.Contains(err.Error(), "supersecret") {
		t.Fatalf("unsafe endpoint error: %v", err)
	}
}

func TestResolveRejectsDeletingRepository(t *testing.T) {
	now := metav1.Now()
	config := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Namespace: "n", Name: "store", DeletionTimestamp: &now}, Data: map[string]string{"bucket": "bucket", "region": "region"}}
	core := fake.NewSimpleClientset(config)
	input := v1alpha1.Storage{Type: "s3", RepositoryRef: &v1alpha1.LocalReference{Name: "store"}}
	if _, err := storage.ResolveS3(context.Background(), core.CoreV1(), "n", input); err == nil {
		t.Fatal("deleting repository accepted")
	}
}
