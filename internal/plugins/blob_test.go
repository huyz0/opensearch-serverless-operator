package plugins_test

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"testing"

	"github.com/huyz0/opensearch-serverless-operator/internal/plugins"
)

func TestVerifyBlob(t *testing.T) {
	content := []byte("plugin data")
	digest := fmt.Sprintf("sha256:%x", sha256.Sum256(content))
	var output bytes.Buffer
	if err := plugins.VerifyBlob(&output, bytes.NewReader(content), digest, int64(len(content)), 100); err != nil || !bytes.Equal(output.Bytes(), content) {
		t.Fatalf("valid blob failed: %v", err)
	}
	output.Reset()
	if err := plugins.VerifyBlob(&output, bytes.NewReader([]byte("tampered")), digest, int64(len(content)), 100); err == nil {
		t.Fatal("changed blob must fail")
	}
	if err := plugins.VerifyBlob(&output, bytes.NewReader(content), digest, int64(len(content)), 2); err == nil {
		t.Fatal("blob above limit must fail")
	}
}
