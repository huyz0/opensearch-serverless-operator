package main

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/huyz0/opensearch-serverless-operator/api/v1alpha1"
	"github.com/huyz0/opensearch-serverless-operator/internal/rollout"
)

func main() {
	var inventory rollout.KubernetesEnvoyInventory
	var writer rollout.KubernetesEnvoyWriterInventory
	var writerPodCA, writerClusterDomain, writerPodTunnels string
	withdrawal := flag.Bool("writer-withdrawal", false, "Observe the index route after a claimed Java writer withdrawal")
	var routeURL, publicKey, tunnelsFile, gatewayUID, caFile string
	flag.StringVar(&inventory.Namespace, "namespace", "", "Namespace of the claimed rollout")
	flag.StringVar(&inventory.Operation, "operation", "", "Weighted rollout operation name")
	flag.StringVar(&inventory.Role, "role", "", "Route role (search or ingest)")
	flag.StringVar(&inventory.GatewayService.Namespace, "gateway-namespace", "", "Namespace of the serving Envoy Gateway Service")
	flag.StringVar(&inventory.GatewayService.Name, "gateway-service", "", "Serving Envoy Gateway Service name")
	flag.StringVar(&gatewayUID, "gateway-service-uid", "", "Approved serving Gateway Service UID")
	flag.StringVar(&routeURL, "route-url", "", "Production HTTPS base URL for the role")
	flag.StringVar(&inventory.RouteAuthSecret, "route-auth-secret", "", "Namespaced rollout-probe Secret for authenticated HTTPS samples")
	flag.StringVar(&publicKey, "platform-key", "", "Padded base64 Ed25519 public key for fleet approval")
	flag.StringVar(&tunnelsFile, "tunnels-file", "", "Local JSON mapping Gateway Pod UIDs to verified loopback tunnels")
	flag.StringVar(&caFile, "route-ca-file", "", "Optional PEM CA bundle added to system trust")
	flag.StringVar(&writer.InstanceName, "writer-instance", "", "Instance pinned in the writer retirement deployment")
	flag.StringVar(&writer.RouteName, "writer-http-route", "", "Index HTTPRoute pinned in the writer retirement deployment")
	flag.StringVar(&writer.Gateway.Namespace, "writer-gateway-namespace", "", "Namespace of the HTTPS Gateway")
	flag.StringVar(&writer.Gateway.Name, "writer-gateway", "", "Name of the HTTPS Gateway")
	flag.StringVar(&writer.GatewayListener, "writer-gateway-listener", "", "HTTPS Gateway listener for index requests")
	flag.StringVar(&writer.ReaderSecret, "writer-reader-secret", "", "Namespaced Java reader credential")
	flag.StringVar(&writerPodCA, "writer-pod-ca-file", "", "PEM CA bundle for the Java Pod certificates")
	flag.StringVar(&writerClusterDomain, "writer-cluster-domain", "cluster.local", "Cluster domain for verifying Java Pod DNS identities")
	flag.StringVar(&writerPodTunnels, "writer-pod-tunnels-file", "", "Optional local JSON mapping Java Pod UIDs to verified loopback tunnels")
	samples := flag.Int("samples", 560, "HTTPS process observations per serving replica")
	flag.Parse()
	inventory.GatewayServiceUID = types.UID(gatewayUID)
	var writerOptions *writerObserverOptions
	if *withdrawal {
		writerOptions = &writerObserverOptions{inventory: writer, podCA: writerPodCA, clusterDomain: writerClusterDomain, podTunnels: writerPodTunnels}
	}
	if err := run(inventory, routeURL, publicKey, tunnelsFile, caFile, *samples, writerOptions, os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

type writerObserverOptions struct {
	inventory                        rollout.KubernetesEnvoyWriterInventory
	podCA, clusterDomain, podTunnels string
}

func run(inventory rollout.KubernetesEnvoyInventory, routeURL, publicKey, tunnelsFile, caFile string, samples int, writer *writerObserverOptions, output io.Writer) error {
	if writer == nil {
		key, err := base64.StdEncoding.Strict().DecodeString(publicKey)
		if err != nil || len(key) != ed25519.PublicKeySize {
			return fmt.Errorf("a trusted Ed25519 platform public key is required")
		}
		inventory.PlatformKey = key
	}
	inventory.RouteURL = routeURL
	if err := readObserverJSON(tunnelsFile, &inventory.Tunnels); err != nil {
		return err
	}
	if len(inventory.Tunnels) == 0 {
		return fmt.Errorf("tunnels file must contain one UID-to-tunnels JSON object")
	}
	roots, err := observerRoots(caFile)
	if err != nil {
		return err
	}
	config, err := ctrl.GetConfig()
	if err != nil {
		return err
	}
	scheme := runtime.NewScheme()
	if err := v1alpha1.AddToScheme(scheme); err != nil {
		return err
	}
	inventory.Reader, err = client.New(config, client.Options{Scheme: scheme})
	if err != nil {
		return err
	}
	inventory.Kube, err = kubernetes.NewForConfig(config)
	if err != nil {
		return err
	}
	var evidence any
	if writer != nil {
		evidence, err = observeWriter(inventory, writer, roots)
	} else {
		observer := rollout.EnvoyObserver{RouteURL: routeURL, Roots: roots, Samples: samples, ReadInventory: inventory.Read}
		evidence, err = observer.Observe(context.Background())
	}
	if err != nil {
		return err
	}
	raw, err := json.Marshal(evidence)
	if err != nil {
		return err
	}
	if len(raw)+1 > rollout.PlatformEvidenceMaxBytes {
		return fmt.Errorf("collected evidence exceeds the publisher's size limit")
	}
	_, err = output.Write(append(raw, '\n'))
	return err
}

func observeWriter(base rollout.KubernetesEnvoyInventory, writer *writerObserverOptions, roots *x509.CertPool) (rollout.WriterWithdrawalEvidence, error) {
	var podTunnels map[types.UID]string
	if writer.podTunnels != "" {
		if err := readObserverJSON(writer.podTunnels, &podTunnels); err != nil {
			return rollout.WriterWithdrawalEvidence{}, err
		}
	}
	podRoots, err := observerRoots(writer.podCA)
	if err != nil {
		return rollout.WriterWithdrawalEvidence{}, err
	}
	inventory := writer.inventory
	inventory.Reader, inventory.Kube = base.Reader, base.Kube
	inventory.Namespace, inventory.Operation = base.Namespace, base.Operation
	inventory.RouteURL, inventory.RouteAuthSecret = base.RouteURL, base.RouteAuthSecret
	inventory.GatewayService, inventory.Tunnels = base.GatewayService, base.Tunnels
	inventory.ProbeLease, err = rollout.NewWriterLeaseProbe(podRoots, writer.clusterDomain, podTunnels)
	if err != nil {
		return rollout.WriterWithdrawalEvidence{}, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), rollout.PlatformEvidenceMaxAge)
	defer cancel()
	return rollout.ObserveEnvoyWriterWithdrawal(ctx, base.RouteURL, roots, inventory.Read)
}

func readObserverJSON(path string, out any) error {
	raw, err := readBoundedFile(path)
	if err != nil {
		return err
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(out); err != nil || decoder.Decode(new(any)) != io.EOF {
		return fmt.Errorf("observer input %q must contain one valid JSON value", path)
	}
	return nil
}

func observerRoots(path string) (*x509.CertPool, error) {
	if path == "" {
		return nil, nil
	}
	roots, err := x509.SystemCertPool()
	if err != nil {
		return nil, err
	}
	pem, err := readBoundedFile(path)
	if err != nil {
		return nil, err
	}
	if !roots.AppendCertsFromPEM(pem) {
		return nil, fmt.Errorf("CA bundle has no usable certificates")
	}
	return roots, nil
}
func readBoundedFile(path string) ([]byte, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	raw, err := io.ReadAll(io.LimitReader(file, rollout.PlatformEvidenceMaxBytes+1))
	if err != nil {
		return nil, err
	}
	if len(raw) > rollout.PlatformEvidenceMaxBytes {
		return nil, fmt.Errorf("observer input file exceeds the size limit")
	}
	return raw, nil
}
