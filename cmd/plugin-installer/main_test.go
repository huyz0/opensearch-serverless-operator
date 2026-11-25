package main

import (
	"crypto/sha256"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestReadJSONPinsExactBytesAndRejectsOversizedInput(t *testing.T) {
	path := filepath.Join(t.TempDir(), "input.json")
	data := []byte(`{"maxBytes":1024,"maxFiles":2}`)
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
	hash := fmt.Sprintf("%x", sha256.Sum256(data))
	var parsed policy
	if err := readJSON(path, &parsed, hash); err != nil || parsed.MaxBytes != 1024 {
		t.Fatalf("valid pinned input rejected: %v", err)
	}
	if err := os.WriteFile(path, append(data, ' '), 0600); err != nil {
		t.Fatal(err)
	}
	if err := readJSON(path, &parsed, hash); err == nil {
		t.Fatal("changed JSON bytes accepted")
	}
	oversized := []byte(`{}` + strings.Repeat(" ", 1<<20))
	if err := os.WriteFile(path, oversized, 0600); err != nil {
		t.Fatal(err)
	}
	if err := readJSON(path, &parsed, ""); err == nil {
		t.Fatal("oversized valid JSON prefix accepted")
	}
	if err := os.WriteFile(path, []byte(`{"unknown":true}`), 0600); err != nil {
		t.Fatal(err)
	}
	if err := readJSON(path, &parsed, ""); err == nil {
		t.Fatal("unknown policy field accepted")
	}
}

func TestRegistryCARejectsInvalidBundle(t *testing.T) {
	path := filepath.Join(t.TempDir(), "invalid.pem")
	if err := os.WriteFile(path, []byte("not a certificate"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := registryHTTPClient(path); err == nil {
		t.Fatal("invalid registry CA accepted")
	}
}
