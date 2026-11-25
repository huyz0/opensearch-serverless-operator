// plugin-installer assembles an approved OCI plugin set in a private volume.
package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"

	"oras.land/oras-go/v2/registry"
	"oras.land/oras-go/v2/registry/remote"
	"oras.land/oras-go/v2/registry/remote/auth"

	"github.com/huyz0/opensearch-serverless-operator/internal/plugins"
)

type policy = plugins.Policy

type credential struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

func readJSON(path string, target any, expectedSHA256 string) error {
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()
	data, err := io.ReadAll(io.LimitReader(file, (1<<20)+1))
	if err != nil || len(data) > 1<<20 {
		return fmt.Errorf("read JSON failed or exceeded size limit")
	}
	if expectedSHA256 != "" {
		actual := fmt.Sprintf("%x", sha256.Sum256(data))
		if actual != expectedSHA256 {
			return fmt.Errorf("JSON input does not match the pinned SHA-256")
		}
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return fmt.Errorf("unexpected trailing JSON")
	}
	return nil
}

func registryHTTPClient(caPath string) (*http.Client, error) {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	if caPath != "" {
		file, err := os.Open(caPath)
		if err != nil {
			return nil, fmt.Errorf("open registry CA bundle: %w", err)
		}
		data, readErr := io.ReadAll(io.LimitReader(file, (1<<20)+1))
		closeErr := file.Close()
		if readErr != nil || closeErr != nil || len(data) == 0 || len(data) > 1<<20 {
			return nil, fmt.Errorf("registry CA bundle is unavailable or too large")
		}
		roots, err := x509.SystemCertPool()
		if err != nil || roots == nil {
			return nil, fmt.Errorf("system trust roots are unavailable")
		}
		if !roots.AppendCertsFromPEM(data) {
			return nil, fmt.Errorf("registry CA bundle is invalid")
		}
		transport.TLSClientConfig = &tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS12}
	}
	return &http.Client{Timeout: 60 * time.Second, Transport: transport}, nil
}

func run() error {
	var requestPath, policyPath, credentialsPath, output, requestHash, policyHash, registryCA string
	flag.StringVar(&requestPath, "request", "", "Path to JSON array of requested plugins")
	flag.StringVar(&policyPath, "policy", "", "Path to independently approved installation policy")
	flag.StringVar(&requestHash, "request-sha256", "", "Expected SHA-256 of the request file")
	flag.StringVar(&policyHash, "policy-sha256", "", "Expected SHA-256 of the policy file")
	flag.StringVar(&credentialsPath, "credentials", "", "Optional path to registry credential JSON")
	flag.StringVar(&registryCA, "registry-ca", "", "Optional PEM bundle added to system registry trust")
	flag.StringVar(&output, "output", "", "New absolute output directory in a private volume")
	flag.Parse()
	var requested []plugins.Installation
	var approvedPolicy policy
	if (requestHash == "") != (policyHash == "") {
		return fmt.Errorf("request and policy hashes must be supplied together")
	}
	if err := readJSON(requestPath, &requested, requestHash); err != nil {
		return fmt.Errorf("read request: %w", err)
	}
	if err := readJSON(policyPath, &approvedPolicy, policyHash); err != nil {
		return fmt.Errorf("read policy: %w", err)
	}
	credentials := map[string]credential{}
	if credentialsPath != "" {
		if err := readJSON(credentialsPath, &credentials, ""); err != nil {
			return fmt.Errorf("read credentials: %w", err)
		}
	}
	httpClient, err := registryHTTPClient(registryCA)
	if err != nil {
		return err
	}
	approved := make(map[string]bool, len(approvedPolicy.ApprovedArtifacts))
	for _, ref := range approvedPolicy.ApprovedArtifacts {
		approved[ref] = true
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	return plugins.InstallSet(ctx, output, requested, approved, approvedPolicy.OpenSearchVersion, approvedPolicy.JavaVersion, approvedPolicy.MaxBytes, approvedPolicy.MaxFiles, func(ref registry.Reference) (*remote.Repository, error) {
		repository, err := remote.NewRepository(ref.Registry + "/" + ref.Repository)
		if err != nil {
			return nil, err
		}
		cred := credentials[ref.Registry]
		repository.Client = &auth.Client{Client: httpClient, Credential: auth.StaticCredential(ref.Registry, auth.Credential{Username: cred.Username, Password: cred.Password}), Cache: auth.NewCache()}
		return repository, nil
	})
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
