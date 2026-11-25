package main

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	kubernetesfake "k8s.io/client-go/kubernetes/fake"
	clienttesting "k8s.io/client-go/testing"

	"github.com/huyz0/opensearch-serverless-operator/internal/rollout"
)

func TestMountedWriterEvidenceUsesTheSameBoundsAsStdin(t *testing.T) {
	directory := t.TempDir()
	report := filepath.Join(directory, "report.json")
	missingKey := filepath.Join(directory, "missing.pem")
	if err := os.WriteFile(report, []byte("{}"), 0600); err != nil {
		t.Fatal(err)
	}

	err := run("ns", "op", missingKey, report, false, false, true, strings.NewReader("invalid stdin"))
	var pathErr *os.PathError
	if !errors.As(err, &pathErr) || pathErr.Path != missingKey || !os.IsNotExist(err) {
		t.Fatalf("publisher did not parse the mounted report instead of stdin: %v", err)
	}

	if err := os.WriteFile(report, []byte(strings.Repeat(" ", rollout.PlatformEvidenceMaxBytes+1)), 0600); err != nil {
		t.Fatal(err)
	}
	err = run("ns", "op", missingKey, report, false, false, true, strings.NewReader("{}"))
	if err == nil || !strings.Contains(err.Error(), "evidence is missing or too large") {
		t.Fatalf("publisher did not reject an oversized file: %v", err)
	}
}

func TestWriterReportWaitsForStagedConfigMap(t *testing.T) {
	kube := kubernetesfake.NewSimpleClientset()
	name := "writer-observation-report"
	maps := kube.CoreV1().ConfigMaps("platform")
	attempts := 0
	kube.PrependReactor("get", "configmaps", func(action clienttesting.Action) (bool, runtime.Object, error) {
		if action.(clienttesting.GetAction).GetName() != name || action.GetNamespace() != "platform" {
			t.Fatal("read outside the named platform report")
		}
		attempts++
		switch attempts {
		case 1:
			return false, nil, nil
		case 2:
			return true, &corev1.ConfigMap{}, nil
		default:
			return true, &corev1.ConfigMap{Data: map[string]string{"report.json": "{}"}}, nil
		}
	})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	input, err := waitForWriterReport(ctx, maps, name, time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	data, err := io.ReadAll(input)
	if err != nil || string(data) != "{}" || attempts != 3 {
		t.Fatalf("got %q after %d reads: %v", data, attempts, err)
	}
}
