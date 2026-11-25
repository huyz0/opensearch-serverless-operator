package v1alpha1_test

import (
	"math"
	"strings"
	"testing"

	"github.com/huyz0/opensearch-serverless-operator/api/v1alpha1"
)

func TestReserveReplicaValidation(t *testing.T) {
	spec := v1alpha1.OpenSearchServerlessSpec{Image: "example/image@sha256:" + strings.Repeat("a", 64), ServiceAccountName: "runtime", Storage: v1alpha1.Storage{Type: "s3", Bucket: "bucket", Region: "region"}, Pools: map[string]v1alpha1.Pool{"search": {Zones: map[string]v1alpha1.Zone{"a": {Replicas: 2, WarmSpare: 1, MinServing: 3}}}}}
	if err := spec.Validate(); err != nil {
		t.Fatalf("floor within combined capacity rejected: %v", err)
	}
	pool := spec.Pools["search"]
	pool.Zones["a"] = v1alpha1.Zone{Replicas: math.MaxInt32, WarmSpare: 1}
	if err := spec.Validate(); err == nil {
		t.Fatal("combined replica overflow accepted")
	}
	pool.Zones["a"] = v1alpha1.Zone{Replicas: 1, WarmSpare: -1}
	if err := spec.Validate(); err == nil {
		t.Fatal("negative reserve accepted")
	}
}
