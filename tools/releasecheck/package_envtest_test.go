//go:build envtest && !windows

package releasecheck

import (
	"bytes"
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"k8s.io/client-go/tools/clientcmd"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
)

func TestPackagedManagerTransitionOnAPIServer(t *testing.T) {
	root, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	directory := t.TempDir()
	run := func(program string, input []byte, args ...string) (string, error) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		cmd := exec.CommandContext(ctx, program, args...)
		cmd.Dir = root
		cmd.Stdin = bytes.NewReader(input)
		output, runErr := cmd.CombinedOutput()
		return strings.TrimSpace(string(output)), runErr
	}
	mustRun := func(program string, input []byte, args ...string) string {
		t.Helper()
		out, runErr := run(program, input, args...)
		if runErr != nil {
			t.Fatalf("%s %v: %v\n%s", program, args, runErr, out)
		}
		return out
	}
	signingKey := filepath.Join(directory, "release.key")
	publicKey := filepath.Join(directory, "release.pub")
	mustRun("openssl", nil, "genpkey", "-algorithm", "ED25519", "-out", signingKey)
	mustRun("openssl", nil, "pkey", "-in", signingKey, "-pubout", "-out", publicKey)
	namespace := "release-check"
	javaImage := "registry.example/java@sha256:" + strings.Repeat("c", 64)
	previousImage := "registry.example/operator@sha256:" + strings.Repeat("a", 64)
	candidateImage := "registry.example/operator@sha256:" + strings.Repeat("b", 64)
	render := func(version, image string) string {
		t.Helper()
		output := filepath.Join(directory, version)
		mustRun("python3", nil, "tools/package_development_release.py",
			"--version", version, "--manager-image", image, "--java-image", javaImage,
			"--namespace", namespace, "--cluster-domain", "cluster.local", "--output", output, "--signing-key", signingKey)
		return output
	}
	previous := render("v0.1.0-dev.0", previousImage)
	candidate := render("v0.1.0-dev.1", candidateImage)
	for _, pair := range [][2]string{{previous, candidate}, {candidate, previous}} {
		mustRun("python3", nil, "tools/verify_development_release.py", pair[1],
			"--public-key", publicKey, "--manager-image-only-from", pair[0])
	}

	useExistingCluster := false
	environment := &envtest.Environment{UseExistingCluster: &useExistingCluster}
	t.Cleanup(func() {
		if err := environment.Stop(); err != nil {
			t.Error(err)
		}
	})
	config, err := environment.Start()
	if err != nil {
		t.Fatal(err)
	}
	kubeconfig := filepath.Join(directory, "kubeconfig")
	clientConfig := clientcmdapi.NewConfig()
	clientConfig.Clusters["fixture"] = &clientcmdapi.Cluster{Server: config.Host, CertificateAuthorityData: config.CAData}
	clientConfig.AuthInfos["fixture"] = &clientcmdapi.AuthInfo{ClientCertificateData: config.CertData, ClientKeyData: config.KeyData}
	clientConfig.Contexts["fixture"] = &clientcmdapi.Context{Cluster: "fixture", AuthInfo: "fixture", Namespace: namespace}
	clientConfig.CurrentContext = "fixture"
	if err := clientcmd.WriteToFile(*clientConfig, kubeconfig); err != nil {
		t.Fatal(err)
	}
	kube := func(input []byte, args ...string) string {
		t.Helper()
		return mustRun("kubectl", input, append([]string{"--kubeconfig", kubeconfig, "--request-timeout=15s"}, args...)...)
	}
	crds := []string{
		"crd/opensearchserverlesses.serverless.opensearch.org",
		"crd/opensearchserverlessoperations.serverless.opensearch.org",
	}
	kube(nil, "apply", "-f", filepath.Join(previous, "crds.yaml"))
	kube(nil, append([]string{"wait", "--for=condition=Established", "--timeout=45s"}, crds...)...)
	releaseCRDs := func() string {
		return kube(nil, append([]string{"get", "-o", "jsonpath={range .items[*]}{.metadata.uid}{.metadata.generation}{.status.storedVersions}{end}"}, crds...)...)
	}
	initialCRDs := releaseCRDs()
	if strings.Count(initialCRDs, "v1alpha1") != 2 {
		t.Fatalf("expected both CRDs to store v1alpha1: %q", initialCRDs)
	}

	kube(nil, "apply", "-f", filepath.Join(previous, "manager.yaml"))
	account := "system:serviceaccount:" + namespace + ":opensearch-serverless-operator"
	if got := kube(nil, "--as", account, "auth", "can-i", "list", "endpointslices"); got != "yes" {
		t.Fatalf("manager EndpointSlice access = %q, want yes", got)
	}
	got, accessErr := run("kubectl", nil, "--kubeconfig", kubeconfig, "--as", account, "auth", "can-i", "list", "nodes")
	if accessErr == nil || !strings.HasSuffix(got, "no") {
		t.Fatalf("manager Node listing unexpectedly allowed: %q, %v", got, accessErr)
	}
	example, err := os.ReadFile(filepath.Join(root, "config", "operator-dev-secure", "instance.example.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	example = bytes.ReplaceAll(example, []byte("namespace: operator-development"), []byte("namespace: "+namespace))
	example = bytes.ReplaceAll(example,
		[]byte("example.invalid/opensearch-java@sha256:"+strings.Repeat("0", 64)), []byte(javaImage))
	kube(example, "apply", "-f", "-")
	kube(nil, "patch", "opensearchserverless/catalog", "--subresource=status", "--type=merge",
		"-p", `{"status":{"observedGeneration":1}}`)
	state := func() string {
		return kube(nil, "get", "opensearchserverless/catalog",
			"-o", "jsonpath={.metadata.uid} {.metadata.generation} {.status.observedGeneration} {.spec.image}")
	}
	initialState := state()
	if !strings.HasSuffix(initialState, " 1 1 "+javaImage) {
		t.Fatalf("instance status or image did not round-trip through API storage: %q", initialState)
	}
	deployment := "deployment/opensearch-serverless-operator"
	deployed := func() string {
		return kube(nil, "get", deployment,
			"-o", "jsonpath={.metadata.uid} {.spec.template.spec.containers[0].image} {.spec.template.spec.containers[0].args}")
	}
	initialDeployment := deployed()
	if !strings.Contains(initialDeployment, "--development-observe-endpoints") || !strings.Contains(initialDeployment, previousImage) {
		t.Fatalf("signed-install profile did not reach the Deployment: %q", initialDeployment)
	}
	for _, transition := range []struct{ path, image string }{{candidate, candidateImage}, {previous, previousImage}} {
		kube(nil, "apply", "-f", filepath.Join(transition.path, "manager.yaml"))
		if got := deployed(); got != strings.Replace(initialDeployment, previousImage, transition.image, 1) {
			t.Fatalf("manager transition changed more than its image: %q", got)
		}
		if got := state(); got != initialState {
			t.Fatalf("instance changed across manager transition: %q, want %q", got, initialState)
		}
		if got := releaseCRDs(); got != initialCRDs {
			t.Fatalf("CRDs changed across manager transition: %q, want %q", got, initialCRDs)
		}
	}
}
