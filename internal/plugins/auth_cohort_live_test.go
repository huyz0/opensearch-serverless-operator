//go:build java_integration

package plugins_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptrace"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/huyz0/opensearch-serverless-operator/internal/security"
)

type authCohortMember struct {
	name, role, endpoint string
	incarnation          string
	validatedConnection  net.Conn
}

func startAuthSearchShell(t *testing.T, dir, ingestHome string, ingestArgs []string) authCohortMember {
	t.Helper()
	const name = "auth-search"
	home := filepath.Join(dir, name)
	for _, name := range []string{"config", "plugins"} {
		if err := os.CopyFS(filepath.Join(home, name), os.DirFS(filepath.Join(ingestHome, name))); err != nil {
			t.Fatal(err)
		}
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := listener.Addr().(*net.TCPAddr).Port
	_ = listener.Close()
	// The installation and trust roots are identical. Give the search JVM
	// independent local state and identity while it uses the same backing store.
	args := slices.Clone(ingestArgs)
	for i, arg := range args {
		switch {
		case strings.HasPrefix(arg, "-Dopensearch.path.conf="):
			args[i] = "-Dopensearch.path.conf=" + filepath.Join(home, "config")
		case strings.HasPrefix(arg, "-Dpath.home="):
			args[i] = "-Dpath.home=" + home
		case strings.HasPrefix(arg, "-Dnode.name="):
			args[i] = "-Dnode.name=" + name
		case strings.HasPrefix(arg, "-Dhttp.port="):
			args[i] = fmt.Sprintf("-Dhttp.port=%d", port)
		case strings.HasPrefix(arg, "-Dserverless.roles="):
			args[i] = "-Dserverless.roles=search"
		}
	}
	logPath := filepath.Join(dir, "java-search.log")
	log, err := os.Create(logPath)
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command("java", args...)
	cmd.Stdout, cmd.Stderr = log, log
	err = cmd.Start()
	_ = log.Close()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		if t.Failed() {
			if body, err := os.ReadFile(logPath); err == nil {
				t.Logf("search JVM log tail: %s", body[max(0, len(body)-4500):])
			}
		}
	})
	return authCohortMember{name: name, role: "search", endpoint: fmt.Sprintf("https://127.0.0.1:%d", port)}
}

func authCohortRequest(ctx context.Context, client *http.Client, endpoint, path, user, password string) (int, net.Conn, []byte, error) {
	var connection net.Conn
	trace := &httptrace.ClientTrace{GotConn: func(info httptrace.GotConnInfo) { connection = info.Conn }}
	request, err := http.NewRequestWithContext(httptrace.WithClientTrace(ctx, trace), http.MethodGet, endpoint+path, nil)
	if err != nil {
		return 0, nil, nil, err
	}
	request.SetBasicAuth(user, password)
	response, err := client.Do(request)
	if err != nil {
		return 0, nil, nil, err
	}
	defer response.Body.Close()
	body, err := io.ReadAll(io.LimitReader(response.Body, 1<<20))
	return response.StatusCode, connection, body, err
}

func (member authCohortMember) identity(ctx context.Context, admin security.UserAdmin) (string, error) {
	code, _, body, err := authCohortRequest(ctx, admin.Client, member.endpoint, "/_serverless/stats", admin.Username, admin.Password)
	if err != nil || code != http.StatusOK {
		return "", fmt.Errorf("fixture stats HTTP %d: %v", code, err)
	}
	var stats struct {
		Name        string   `json:"name"`
		Roles       []string `json:"roles"`
		Incarnation string   `json:"process_incarnation"`
	}
	if err := json.Unmarshal(body, &stats); err != nil || stats.Name != member.name || !slices.Contains(stats.Roles, member.role) || stats.Incarnation == "" {
		return "", fmt.Errorf("stats did not identify the expected JVM and role")
	}
	return stats.Incarnation, nil
}

func testShellAuthCohort(t *testing.T, dir, ingestHome string, args []string, admin security.UserAdmin) {
	t.Helper()
	const user, password, rotated = "cohort-client", "cohort-initial-password", "cohort-rotated-password"
	search := startAuthSearchShell(t, dir, ingestHome, args)
	members := []authCohortMember{{name: "auth-fixture", role: "ingest", endpoint: admin.BaseURL}, search}
	ctx, cancel := context.WithTimeout(context.Background(), 70*time.Second)
	defer cancel()
	await := func(scope context.Context, probe func() error) {
		t.Helper()
		ticker := time.NewTicker(250 * time.Millisecond)
		defer ticker.Stop()
		for {
			err := probe()
			if err == nil {
				return
			}
			select {
			case <-scope.Done():
				t.Fatal(err)
			case <-ticker.C:
			}
		}
	}
	for i := range members {
		member := &members[i]
		await(ctx, func() error {
			incarnation, err := member.identity(ctx, admin)
			member.incarnation = incarnation
			return err
		})
	}
	if members[0].incarnation == members[1].incarnation {
		t.Fatal("ingest and search must identify distinct processes")
	}
	if probe := os.Getenv("OPERATOR_AUTH_HEALTH_PROBE"); probe != "" {
		t.Run("authenticated_search_health_probe", func(t *testing.T) {
			_, port, err := net.SplitHostPort(strings.TrimPrefix(search.endpoint, "https://"))
			if err != nil {
				t.Fatal(err)
			}
			testShellAuthHealthProbe(t, probe, dir, search.name, search.role, port)
		})
	}
	verify := func(member authCohortMember, operation, previous, desired string) {
		t.Helper()
		observer := admin
		observer.BaseURL = member.endpoint
		process := security.AccountProcess{Name: member.name, Role: member.role, Incarnation: member.incarnation}
		if operation == "creation" {
			learned, verified, err := observer.ObserveAccount(ctx, security.AccountProcess{Name: member.name, Role: member.role}, user, previous, desired)
			if err != nil || !verified || learned != process {
				t.Fatalf("%s creation did not identify the expected process: %v %v", member.role, verified, err)
			}
			return
		}
		if verified, err := observer.VerifyAccount(ctx, process, user, previous, desired); err != nil || !verified {
			t.Fatalf("%s %s did not pass the account observer: %v %v", member.role, operation, verified, err)
		}
	}
	if err := admin.Put(ctx, user, password); err != nil {
		t.Fatalf("create account on ingest: %v", err)
	}
	for i := range members {
		member := &members[i]
		await(ctx, func() error {
			code, connection, _, err := authCohortRequest(ctx, admin.Client, member.endpoint, "/", user, password)
			if err != nil || code != http.StatusOK || connection == nil {
				return fmt.Errorf("%s requires a successful authentication before rotation: HTTP %d (%v)", member.role, code, err)
			}
			member.validatedConnection = connection
			return nil
		})
		verify(*member, "creation", "", password)
	}
	if err := admin.Put(ctx, user, rotated); err != nil {
		t.Fatalf("rotate account on ingest: %v", err)
	}
	freshTransport := admin.Client.Transport.(*http.Transport).Clone()
	freshTransport.DisableKeepAlives = true
	defer freshTransport.CloseIdleConnections()
	freshClient := &http.Client{Transport: freshTransport, Timeout: 3 * time.Second}
	checkChange := func(name, previous, next string) {
		t.Helper()
		started := time.Now()
		// The fixture pins a five-minute positive authentication cache TTL.
		// Expiry cannot account for invalidation within this window.
		window, stop := context.WithTimeout(ctx, 8*time.Second)
		defer stop()
		for i := range members {
			member := &members[i]
			await(window, func() error {
				code, connection, _, err := authCohortRequest(window, admin.Client, member.endpoint, "/", user, previous)
				if err != nil || code != http.StatusUnauthorized {
					return fmt.Errorf("%s %s must reject the cached credential: HTTP %d (%v)", member.role, name, code, err)
				}
				if connection != member.validatedConnection {
					t.Fatalf("%s %s replaced the previously authenticated HTTPS connection", member.role, name)
				}
				if next != "" {
					code, connection, _, err = authCohortRequest(window, admin.Client, member.endpoint, "/", user, next)
					if err != nil || code != http.StatusOK {
						return fmt.Errorf("%s %s must accept the new credential: HTTP %d (%v)", member.role, name, code, err)
					}
					if connection != member.validatedConnection {
						t.Fatalf("%s %s accepted the new credential on a replacement HTTPS connection", member.role, name)
					}
				}
				observed, err := member.identity(window, admin)
				if err != nil || observed != member.incarnation {
					t.Fatalf("%s %s changed the enforcing process: %v", member.role, name, err)
				}
				return nil
			})
			t.Logf("%s %s observed after %s on its existing HTTPS connection", member.role, name, time.Since(started).Round(time.Millisecond))
		}
		for _, member := range members {
			code, _, _, err := authCohortRequest(ctx, freshClient, member.endpoint, "/", user, previous)
			if err != nil || code != http.StatusUnauthorized {
				t.Fatalf("%s %s must also reject the prior credential on a fresh connection: HTTP %d (%v)", member.role, name, code, err)
			}
			if next != "" {
				code, _, _, err := authCohortRequest(ctx, freshClient, member.endpoint, "/", user, next)
				if err != nil || code != http.StatusOK {
					t.Fatalf("%s %s must accept the new credential on a fresh connection: HTTP %d (%v)", member.role, name, code, err)
				}
			}
			verify(member, name, previous, next)
		}
	}
	checkChange("rotation", password, rotated)
	if found, err := admin.Delete(ctx, user); err != nil || !found {
		t.Fatalf("delete account on ingest: %v %v", found, err)
	}
	if exists, err := admin.Exists(ctx, user); err != nil || exists {
		t.Fatalf("account record remained after deletion: %v %v", exists, err)
	}
	checkChange("deletion", rotated, "")
}
