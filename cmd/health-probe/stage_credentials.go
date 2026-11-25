package main

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/huyz0/opensearch-serverless-operator/internal/java"
)

func stageCredentials(source, destination, expected string) error {
	if source == "" || destination == "" || source == destination || len(expected) != 64 {
		return fmt.Errorf("startup credential staging requires a source, private destination, and bundle checksum")
	}
	const keystoreFile, passwordFile = "opensearch.keystore", "probe-password"
	keystore, err := os.ReadFile(filepath.Join(source, keystoreFile))
	if err != nil {
		return fmt.Errorf("read startup keystore: %w", err)
	}
	password, err := os.ReadFile(filepath.Join(source, passwordFile))
	if err != nil {
		return fmt.Errorf("read startup password: %w", err)
	}
	if len(keystore) == 0 || len(password) == 0 || len(password) > java.MaxHealthPasswordBytes || java.StartupBundleSHA256(keystore, password) != expected {
		return fmt.Errorf("mounted startup credentials differ from the approved bundle")
	}
	if err := os.WriteFile(filepath.Join(destination, keystoreFile), keystore, 0600); err != nil {
		return fmt.Errorf("stage startup keystore: %w", err)
	}
	if err := os.WriteFile(filepath.Join(destination, passwordFile), password, 0600); err != nil {
		return fmt.Errorf("stage startup password: %w", err)
	}
	return nil
}
