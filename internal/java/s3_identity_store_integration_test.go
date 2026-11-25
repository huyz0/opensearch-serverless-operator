//go:build java_integration

package java_test

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	javaadapter "github.com/huyz0/opensearch-serverless-operator/internal/java"
)

// This starts a disposable local S3-compatible store and the actual shell.
// STS and token values are synthetic; no cloud credentials are used.
func TestWebIdentityS3WriteAndReplacement(t *testing.T) {
	lib, weed := os.Getenv("OPERATOR_JAVA_RUNTIME_LIB"), os.Getenv("OPERATOR_SEAWEED_EXE")
	if lib == "" || weed == "" {
		t.Skip("set OPERATOR_JAVA_RUNTIME_LIB and OPERATOR_SEAWEED_EXE")
	}
	lib, err := filepath.Abs(lib)
	if err != nil {
		t.Fatal(err)
	}
	weed, err = filepath.Abs(weed)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Second)
	defer cancel()
	dir := t.TempDir()
	port := func() int {
		t.Helper()
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		p := l.Addr().(*net.TCPAddr).Port
		_ = l.Close()
		return p
	}
	master, masterGRPC, volume, volumeGRPC, filer, filerGRPC, s3port, s3GRPC := port(), port(), port(), port(), port(), port(), port(), port()
	data := filepath.Join(dir, "store")
	if err := os.Mkdir(data, 0700); err != nil {
		t.Fatal(err)
	}
	weedLog, err := os.Create(filepath.Join(dir, "weed.log"))
	if err != nil {
		t.Fatal(err)
	}
	defer weedLog.Close()
	gateway := exec.CommandContext(ctx, weed, "server", "-s3", "-ip=127.0.0.1", "-master.telemetry=false", "-volume.max=2", "-dir="+data, fmt.Sprintf("-master.port=%d", master), fmt.Sprintf("-master.port.grpc=%d", masterGRPC), fmt.Sprintf("-volume.port=%d", volume), fmt.Sprintf("-volume.port.grpc=%d", volumeGRPC), fmt.Sprintf("-filer.port=%d", filer), fmt.Sprintf("-filer.port.grpc=%d", filerGRPC), fmt.Sprintf("-s3.port=%d", s3port), fmt.Sprintf("-s3.port.grpc=%d", s3GRPC))
	gateway.Stdout, gateway.Stderr = weedLog, weedLog
	if err := gateway.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = gateway.Process.Kill(); _ = gateway.Wait() }()
	client := &http.Client{Timeout: 3 * time.Second}
	s3URL := fmt.Sprintf("http://127.0.0.1:%d", s3port)
	lastResult := "no request"
	request := func(method, endpoint, body string) (int, string, error) {
		req, err := http.NewRequestWithContext(ctx, method, endpoint, strings.NewReader(body))
		if err != nil {
			return 0, "", err
		}
		req.Header.Set("Content-Type", "application/json")
		resp, err := client.Do(req)
		if err != nil {
			lastResult = err.Error()
			return 0, "", err
		}
		defer resp.Body.Close()
		b, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
		lastResult = fmt.Sprintf("status=%d body=%s", resp.StatusCode, b)
		return resp.StatusCode, string(b), err
	}
	until := func(description string, operation func() bool) {
		t.Helper()
		deadline := time.NewTimer(45 * time.Second)
		defer deadline.Stop()
		ticker := time.NewTicker(250 * time.Millisecond)
		defer ticker.Stop()
		for {
			if operation() {
				return
			}
			select {
			case <-ticker.C:
			case <-deadline.C:
				t.Fatalf("timed out: %s; last response: %s", description, lastResult)
			case <-ctx.Done():
				t.Fatalf("context expired: %s", description)
			}
		}
	}
	until("create disposable S3 bucket", func() bool {
		code, _, _ := request(http.MethodPut, s3URL+"/identity-store", "")
		return code >= 200 && code < 300
	})
	const role = "arn:aws:iam::123456789012:role/store"
	var firstToken, secondToken, rejectedToken atomic.Bool
	sts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = r.ParseForm()
		if r.Form.Get("Action") != "AssumeRoleWithWebIdentity" || r.Form.Get("RoleArn") != role {
			http.Error(w, "bad identity", 400)
			return
		}
		switch r.Form.Get("WebIdentityToken") {
		case "first-token":
			firstToken.Store(true)
		case "second-token":
			secondToken.Store(true)
		default:
			rejectedToken.Store(true)
			http.Error(w, "bad token", 403)
			return
		}
		w.Header().Set("Content-Type", "text/xml")
		fmt.Fprintf(w, `<AssumeRoleWithWebIdentityResponse xmlns="https://sts.amazonaws.com/doc/2011-06-15/"><AssumeRoleWithWebIdentityResult><Credentials><AccessKeyId>FAKEACCESSKEY</AccessKeyId><SecretAccessKey>fake-secret</SecretAccessKey><SessionToken>fake-session</SessionToken><Expiration>%s</Expiration></Credentials><SubjectFromWebIdentityToken>fixture</SubjectFromWebIdentityToken></AssumeRoleWithWebIdentityResult><ResponseMetadata><RequestId>fixture</RequestId></ResponseMetadata></AssumeRoleWithWebIdentityResponse>`, time.Now().Add(time.Hour).UTC().Format(time.RFC3339))
	}))
	defer sts.Close()
	target, _ := url.Parse(s3URL)
	proxy := httputil.NewSingleHostReverseProxy(target)
	var signed atomic.Int64
	var denyStore atomic.Bool
	var deniedRequests atomic.Int64
	proxyServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.Contains(r.Header.Get("Authorization"), "FAKEACCESSKEY") || r.Header.Get("X-Amz-Security-Token") != "fake-session" {
			http.Error(w, "missing fixture credentials", 403)
			return
		}
		if denyStore.Load() {
			deniedRequests.Add(1)
			http.Error(w, "fixture access denied", http.StatusForbidden)
			return
		}
		signed.Add(1)
		proxy.ServeHTTP(w, r)
	}))
	defer proxyServer.Close()
	tokenPath := filepath.Join(dir, "token")
	if err := os.WriteFile(tokenPath, []byte("first-token"), 0600); err != nil {
		t.Fatal(err)
	}
	start := func(name string) (*exec.Cmd, int) {
		t.Helper()
		home := filepath.Join(dir, name)
		config := filepath.Join(home, "config")
		if err := os.MkdirAll(config, 0700); err != nil {
			t.Fatal(err)
		}
		httpPort, transportPort := port(), port()
		args := []string{"--add-opens=java.base/java.lang=ALL-UNNAMED", "--add-opens=java.base/java.nio=ALL-UNNAMED", "--add-opens=java.base/java.util=ALL-UNNAMED", "-Dio.netty.noUnsafe=true", "-Dio.netty.noKeySetOptimization=true", "-Dio.netty.recycler.maxCapacityPerThread=0", "-Dopensearch.set.netty.runtime.available.processors=false", "-Dopensearch.path.conf=" + config, "-Dpath.home=" + home, "-Dnode.name=" + name, "-Dcluster.name=identity-store", "-Dnetwork.host=127.0.0.1", fmt.Sprintf("-Dhttp.port=%d", httpPort), fmt.Sprintf("-Dtransport.port=%d", transportPort), "-Dserverless.roles=ingest", "-Dserverless.lease.ttl_millis=12000", "-Dserverless.store.type=s3", "-Dserverless.store.bucket=identity-store", "-Dserverless.store.region=us-east-1", "-Dserverless.store.endpoint=" + proxyServer.URL, "-Dserverless.store.path_style_access=true", "-Ds3.client.default.identity_token_file=" + tokenPath, "-Daws.stsEndpointOverride=" + sts.URL, "-cp", filepath.Join(lib, "*"), "org.opensearch.serverless.shell.ServerlessBootstrap"}
		cmd := exec.CommandContext(ctx, "java", args...)
		for _, e := range os.Environ() {
			if !strings.HasPrefix(strings.ToUpper(e), "AWS_") {
				cmd.Env = append(cmd.Env, e)
			}
		}
		cmd.Env = append(cmd.Env, "AWS_ROLE_ARN="+role, "AWS_EC2_METADATA_DISABLED=true")
		log, err := os.Create(filepath.Join(dir, name+".log"))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = log.Close() })
		cmd.Stdout, cmd.Stderr = log, log
		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}
		return cmd, httpPort
	}
	first, httpPort := start("first")
	defer func() {
		if first != nil {
			_ = first.Process.Kill()
			_ = first.Wait()
		}
	}()
	base := fmt.Sprintf("http://127.0.0.1:%d", httpPort)
	until("first shell readiness", func() bool {
		code, body, _ := request(http.MethodGet, base+"/_serverless/ready", "")
		return code == 200 && strings.Contains(body, `"ready":true`)
	})
	until("create index", func() bool {
		code, body, _ := request(http.MethodPut, base+"/identity-index", "{}")
		return (code >= 200 && code < 300) || (code == 400 && strings.Contains(body, "index_already_exists"))
	})
	until("write document", func() bool {
		code, _, _ := request(http.MethodPut, base+"/identity-index/_doc/1?refresh=true", `{"message":"identity-recovery"}`)
		return code >= 200 && code < 300
	})
	if !firstToken.Load() || signed.Load() == 0 {
		t.Fatal("write did not use web identity credentials")
	}
	checkServing := func() error { return javaadapter.CheckServing(ctx, client, base, "first", "ingest", 5*time.Second) }
	until("first writer has an eligible lease", func() bool { return checkServing() == nil })
	denyStore.Store(true)
	until("store denies signed requests", func() bool { return deniedRequests.Load() > 0 })
	// Allow the last successful 12-second lease to expire before testing the
	// same lease-aware check used by the Pod readiness probe.
	select {
	case <-time.After(15 * time.Second):
	case <-ctx.Done():
		t.Fatal("context expired during lease expiry")
	}
	lease, err := javaadapter.GetLease(ctx, client, base)
	if err != nil || lease.Valid || lease.ExpiresInMillis >= 0 {
		t.Fatalf("expected an observed expired lease from the running JVM, got %+v, %v", lease, err)
	}
	for i := 0; i < 3; i++ {
		if err := checkServing(); err == nil {
			t.Fatal("store-denied node remained eligible after its lease expired")
		}
	}
	denyStore.Store(false)
	until("writer regains an eligible lease after store access returns", func() bool { return checkServing() == nil })
	_ = first.Process.Kill()
	_ = first.Wait()
	first = nil
	if err := os.WriteFile(tokenPath, []byte("second-token"), 0600); err != nil {
		t.Fatal(err)
	}
	second, secondPort := start("second")
	defer func() { _ = second.Process.Kill(); _ = second.Wait() }()
	base = fmt.Sprintf("http://127.0.0.1:%d", secondPort)
	until("replacement recovers document", func() bool {
		code, body, _ := request(http.MethodGet, base+"/identity-index/_doc/1", "")
		return code == 200 && strings.Contains(body, "identity-recovery")
	})
	if !secondToken.Load() {
		t.Fatal("replacement did not authenticate using its updated token")
	}
	if err := os.WriteFile(tokenPath, []byte("invalid-token"), 0600); err != nil {
		t.Fatal(err)
	}
	deniedNode, deniedPort := start("denied")
	defer func() { _ = deniedNode.Process.Kill(); _ = deniedNode.Wait() }()
	until("STS rejects the invalid identity token", func() bool { return rejectedToken.Load() })
	deniedBase := fmt.Sprintf("http://127.0.0.1:%d", deniedPort)
	for i := 0; i < 5; i++ {
		if err := javaadapter.CheckServing(ctx, client, deniedBase, "denied", "ingest", 5*time.Second); err == nil {
			t.Fatal("node with denied STS credentials became eligible")
		}
		select {
		case <-time.After(time.Second):
		case <-ctx.Done():
			t.Fatal("context expired while checking denied credentials")
		}
	}
}
