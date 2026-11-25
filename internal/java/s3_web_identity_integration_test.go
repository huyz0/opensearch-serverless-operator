//go:build java_integration

package java_test

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// This exercises the actual shell entrypoint and S3 client against a local STS
// protocol fixture. It does not establish cloud IAM trust or kubelet rotation.
func TestShellUsesProjectedS3WebIdentity(t *testing.T) {
	lib := os.Getenv("OPERATOR_JAVA_RUNTIME_LIB")
	if lib == "" {
		t.Skip("set OPERATOR_JAVA_RUNTIME_LIB to the patched runtime bundle")
	}
	lib, err := filepath.Abs(lib)
	if err != nil {
		t.Fatal(err)
	}
	const token = "synthetic-operator-token"
	const rotatedToken = "synthetic-rotated-operator-token"
	const role = "arn:aws:iam::123456789012:role/test-operator"
	requests := make(chan string, 64)
	sts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := r.ParseForm(); err != nil {
			http.Error(w, "bad form", 400)
			return
		}
		select {
		case requests <- fmt.Sprintf("%s|%s|%s", r.Form.Get("Action"), r.Form.Get("RoleArn"), r.Form.Get("WebIdentityToken")):
		default:
		}
		access, session := "FAKEACCESSKEY", "fake-session"
		if r.Form.Get("WebIdentityToken") == rotatedToken {
			access, session = "ROTATEDACCESSKEY", "rotated-session"
		}
		w.Header().Set("Content-Type", "text/xml")
		fmt.Fprintf(w, `<AssumeRoleWithWebIdentityResponse xmlns="https://sts.amazonaws.com/doc/2011-06-15/"><AssumeRoleWithWebIdentityResult><Credentials><AccessKeyId>%s</AccessKeyId><SecretAccessKey>fake-secret</SecretAccessKey><SessionToken>%s</SessionToken><Expiration>%s</Expiration></Credentials><SubjectFromWebIdentityToken>fixture</SubjectFromWebIdentityToken></AssumeRoleWithWebIdentityResult><ResponseMetadata><RequestId>fixture</RequestId></ResponseMetadata></AssumeRoleWithWebIdentityResponse>`, access, session, time.Now().Add(61*time.Second).UTC().Format(time.RFC3339))
	}))
	defer sts.Close()
	s3Requests := make(chan string, 64)
	s3 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		credential := "unexpected"
		if strings.Contains(r.Header.Get("Authorization"), "FAKEACCESSKEY") && r.Header.Get("X-Amz-Security-Token") == "fake-session" {
			credential = "original"
		} else if strings.Contains(r.Header.Get("Authorization"), "ROTATEDACCESSKEY") && r.Header.Get("X-Amz-Security-Token") == "rotated-session" {
			credential = "rotated"
		}
		select {
		case s3Requests <- credential:
		default:
		}
		http.Error(w, "fixture", http.StatusNotFound)
	}))
	defer s3.Close()
	dir := t.TempDir()
	home := filepath.Join(dir, "home")
	config := filepath.Join(home, "config")
	if err := os.MkdirAll(config, 0700); err != nil {
		t.Fatal(err)
	}
	tokenPath := filepath.Join(dir, "token")
	if err := os.WriteFile(tokenPath, []byte(token), 0600); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	args := []string{"--add-opens=java.base/java.lang=ALL-UNNAMED", "--add-opens=java.base/java.nio=ALL-UNNAMED", "--add-opens=java.base/java.util=ALL-UNNAMED", "-Dio.netty.noUnsafe=true", "-Dio.netty.noKeySetOptimization=true", "-Dio.netty.recycler.maxCapacityPerThread=0", "-Dopensearch.set.netty.runtime.available.processors=false", "-Dopensearch.path.conf=" + config, "-Dpath.home=" + home, "-Dnode.name=identity-fixture", "-Dcluster.name=identity-fixture", "-Dnetwork.host=127.0.0.1", "-Dhttp.port=0", "-Dtransport.port=0", "-Dserverless.roles=ingest", "-Dserverless.store.type=s3", "-Dserverless.store.bucket=fixture", "-Dserverless.store.region=us-east-1", "-Dserverless.store.endpoint=" + s3.URL, "-Dserverless.store.path_style_access=true", "-Ds3.client.default.identity_token_file=" + tokenPath, "-Daws.stsEndpointOverride=" + sts.URL, "-cp", filepath.Join(lib, "*"), "org.opensearch.serverless.shell.ServerlessBootstrap"}
	cmd := exec.CommandContext(ctx, "java", args...)
	for _, env := range os.Environ() {
		if !strings.HasPrefix(strings.ToUpper(env), "AWS_") {
			cmd.Env = append(cmd.Env, env)
		}
	}
	cmd.Env = append(cmd.Env, "AWS_ROLE_ARN="+role, "AWS_EC2_METADATA_DISABLED=true")
	log, err := os.Create(filepath.Join(dir, "java.log"))
	if err != nil {
		t.Fatal(err)
	}
	defer log.Close()
	cmd.Stdout, cmd.Stderr = log, log
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	defer func() { cancel(); <-done }()
	select {
	case got := <-requests:
		if want := "AssumeRoleWithWebIdentity|" + role + "|" + token; got != want {
			t.Fatalf("unexpected STS request: %q", got)
		}
	case err := <-done:
		done <- err
		_, _ = log.Seek(0, io.SeekStart)
		data, _ := io.ReadAll(log)
		t.Fatalf("Java exited before STS request: %v\n%s", err, data)
	case <-ctx.Done():
		_, _ = log.Seek(0, io.SeekStart)
		data, _ := io.ReadAll(log)
		t.Fatalf("timed out waiting for STS request\n%s", data)
	}
	select {
	case credential := <-s3Requests:
		if credential != "original" {
			t.Fatal("S3 request did not use the credentials returned by STS")
		}
	case <-ctx.Done():
		t.Fatal("timed out waiting for a signed S3 request")
	}
	if err := os.WriteFile(tokenPath, []byte(rotatedToken), 0600); err != nil {
		t.Fatal(err)
	}
	refreshDeadline := time.NewTimer(20 * time.Second)
	defer refreshDeadline.Stop()
	refreshed := false
	for !refreshed {
		select {
		case got := <-requests:
			if got == "AssumeRoleWithWebIdentity|"+role+"|"+rotatedToken {
				refreshed = true
			}
		case err := <-done:
			done <- err
			t.Fatalf("Java exited before credential refresh: %v", err)
		case <-refreshDeadline.C:
			t.Fatal("Java did not refresh credentials with the rotated token")
		}
	}
	for {
		select {
		case credential := <-s3Requests:
			if credential == "rotated" {
				return
			}
		case err := <-done:
			done <- err
			t.Fatalf("Java exited before using refreshed credentials: %v", err)
		case <-refreshDeadline.C:
			t.Fatal("Java did not sign an S3 request with refreshed credentials")
		}
	}
}
