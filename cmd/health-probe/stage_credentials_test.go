package main

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/huyz0/opensearch-serverless-operator/internal/java"
)

func TestStartupCredentialsRejectReplacedBundle(t *testing.T) {
	source, destination := t.TempDir(), t.TempDir()
	keystore, password := []byte("original keystore fixture"), []byte("original-password")
	expected := java.StartupBundleSHA256(keystore, password)
	write := func(name string, body []byte) {
		t.Helper()
		if err := os.WriteFile(filepath.Join(source, name), body, 0600); err != nil {
			t.Fatal(err)
		}
	}
	write("opensearch.keystore", []byte("replacement keystore fixture"))
	write("probe-password", []byte("replacement-password"))
	if err := stageCredentials(source, destination, expected); err == nil {
		t.Fatal("replacement pair was staged for Java")
	}
	write("opensearch.keystore", keystore)
	if err := stageCredentials(source, destination, expected); err == nil {
		t.Fatal("replacement password with original keystore was staged for Java")
	}
	if _, err := os.Stat(filepath.Join(destination, "opensearch.keystore")); !os.IsNotExist(err) {
		t.Fatalf("credential staging wrote a keystore before verification: %v", err)
	}
	write("probe-password", password)
	if err := stageCredentials(source, destination, expected); err != nil {
		t.Fatal(err)
	}
	write("opensearch.keystore", []byte("replacement after staging"))
	if staged, err := os.ReadFile(filepath.Join(destination, "opensearch.keystore")); err != nil || string(staged) != string(keystore) {
		t.Fatal("staged keystore did not retain the verified bytes")
	}
	if staged, err := os.ReadFile(filepath.Join(destination, "probe-password")); err != nil || string(staged) != string(password) {
		t.Fatal("staged password did not retain the verified bytes")
	}
}
