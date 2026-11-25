package main

import (
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"

	"github.com/huyz0/opensearch-serverless-operator/internal/java"
)

func configureBasicAuth(client *http.Client, host, username, passwordPath string) error {
	if username == "" && passwordPath == "" {
		return nil
	}
	if username == "" || strings.Contains(username, ":") || passwordPath == "" {
		return fmt.Errorf("Basic Auth requires a username without a colon and a password file")
	}
	file, err := os.Open(passwordPath)
	if err != nil {
		return fmt.Errorf("open probe Basic Auth password file: %w", err)
	}
	defer file.Close()
	password, err := io.ReadAll(io.LimitReader(file, java.MaxHealthPasswordBytes+1))
	if err != nil {
		return fmt.Errorf("read probe Basic Auth password file: %w", err)
	}
	if len(password) == 0 || len(password) > java.MaxHealthPasswordBytes {
		return fmt.Errorf("probe Basic Auth password file must contain 1 to %d bytes", java.MaxHealthPasswordBytes)
	}
	return java.ConfigureHealthBasicAuth(client, host, username, password)
}
