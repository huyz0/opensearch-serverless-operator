package main

import (
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"regexp"
	"strings"
	"time"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	"github.com/huyz0/opensearch-serverless-operator/api/v1alpha1"
	"github.com/huyz0/opensearch-serverless-operator/internal/controller"
)

func main() {
	var metricsAddress, probeAddress, watchNamespace, developmentWarmupImage, developmentSettingsImage string
	var developmentPeerDNSImage, developmentPeerTLSImage, developmentClusterDomain, developmentServiceProbeImage string
	var developmentPeerCertificateIssuers, developmentHTTPCertificateIssuers string
	var developmentHTTPTLSImage string
	var developmentBasicAuthImage string
	var developmentBasicAuthOperations bool
	var developmentPluginInstallerImage, developmentPluginRuntimeImage, developmentPluginPolicy string
	var developmentS3WebIdentityImage string
	var developmentRuntimeSettingsImage string
	var rolloutInstance, rolloutSearchURL, rolloutIngestURL, rolloutKey, rolloutRouteCA string
	var rolloutSearchAuthSecret, rolloutIngestAuthSecret string
	var rolloutSearchHTTPRoute, rolloutIngestHTTPRoute string
	var writerRetirementConfig string
	var leaderElection bool
	var developmentGatedPods bool
	var developmentReleaseGates bool
	var developmentRetryProvisioning bool
	var developmentObserveEndpoints bool
	var developmentEmergencyFailback bool
	var developmentSearchScaleDown bool
	var developmentPendingTimeout, developmentReleaseWindow time.Duration
	var developmentReleaseLimit int
	flag.StringVar(&metricsAddress, "metrics-bind-address", "0", "Metrics listen address, or 0 to disable")
	flag.StringVar(&probeAddress, "health-probe-bind-address", ":8081", "Health probe listen address")
	flag.StringVar(&watchNamespace, "watch-namespace", "", "Namespace containing managed instances and leader election lease")
	flag.BoolVar(&leaderElection, "leader-elect", true, "Use leader election")
	flag.BoolVar(&developmentGatedPods, "development-create-gated-pods", false, "Enable bounded development Pod creation")
	flag.BoolVar(&developmentReleaseGates, "development-release-gated-pods", false, "Allow one pending development Pod to schedule; requires gated Pod creation")
	flag.BoolVar(&developmentRetryProvisioning, "development-retry-provisioning", false, "Enable explicit development recovery from a provisioning hold")
	flag.BoolVar(&developmentObserveEndpoints, "development-observe-endpoints", false, "Record historical Kubernetes EndpointSlice observations; requires namespaced EndpointSlice list access")
	flag.BoolVar(&developmentEmergencyFailback, "development-emergency-failback", false, "Evict one qualified temporary Pod at a time after verified recovery")
	flag.BoolVar(&developmentSearchScaleDown, "development-search-scale-down", false, "Retire surplus search Pods after verifying the remaining steady capacity")
	flag.StringVar(&developmentPluginInstallerImage, "development-plugin-installer-image", "", "Digest-pinned qualified OCI plugin installer image")
	flag.StringVar(&developmentPluginRuntimeImage, "development-plugin-runtime-image", "", "Exact runtime image digest qualified with the installer")
	flag.StringVar(&developmentPluginPolicy, "development-plugin-policy", "", "Immutable namespaced plugin approval ConfigMap")
	flag.DurationVar(&developmentReleaseWindow, "development-release-window", 10*time.Minute, "Per-role/AZ release rate window (1s to 1h)")
	flag.IntVar(&developmentReleaseLimit, "development-release-limit", 32, "Maximum Pod gate releases per role and AZ in the rate window (1 to 64)")
	flag.DurationVar(&developmentPendingTimeout, "development-pending-timeout", 10*time.Minute, "Maximum unbound time before holding and withdrawing a development Pod")
	flag.StringVar(&developmentServiceProbeImage, "development-service-probe-image", "", "Exact digest-pinned runtime image for unauthenticated development Service samples")
	flag.StringVar(&developmentWarmupImage, "development-warmup-image", "", "Exact digest-pinned image qualified for the development warm-up observation contract")
	flag.StringVar(&developmentRuntimeSettingsImage, "development-runtime-settings-image", "", "Exact digest-pinned image qualified for the startup settings catalog")
	flag.StringVar(&developmentS3WebIdentityImage, "development-s3-web-identity-image", "", "Exact digest-pinned image qualified for the development S3 web identity contract")
	flag.StringVar(&developmentSettingsImage, "development-settings-image", "", "Exact digest-pinned image qualified for development cluster settings writes")
	flag.StringVar(&developmentPeerDNSImage, "development-peer-dns-image", "", "Exact digest-pinned image qualified for peer DNS advertisement")
	flag.StringVar(&developmentPeerTLSImage, "development-peer-tls-image", "", "Exact digest-pinned image qualified for development peer TLS")
	flag.StringVar(&developmentPeerCertificateIssuers, "development-peer-cert-issuers", "", "Comma-separated namespaced cert-manager Issuers approved for peer TLS")
	flag.StringVar(&developmentHTTPCertificateIssuers, "development-http-cert-issuers", "", "Comma-separated namespaced cert-manager Issuers approved for HTTP TLS")
	flag.StringVar(&developmentHTTPTLSImage, "development-http-tls-image", "", "Exact digest-pinned image qualified for managed development HTTPS")
	flag.StringVar(&developmentBasicAuthImage, "development-basic-auth-image", "", "Exact digest-pinned HTTPS image qualified for development Basic Auth")
	flag.BoolVar(&developmentBasicAuthOperations, "development-basic-auth-operations", false, "Enable single-account changes on the approved Basic Auth runtime")
	flag.StringVar(&developmentClusterDomain, "development-cluster-domain", "", "Cluster DNS domain for development peer identities")
	flag.StringVar(&rolloutInstance, "development-rollout-instance", "", "Instance eligible for opt-in blue/green rollout")
	flag.StringVar(&rolloutSearchURL, "development-rollout-search-url", "", "Production search HTTPS base URL through the stable proxy")
	flag.StringVar(&rolloutIngestURL, "development-rollout-ingest-url", "", "Production ingest HTTPS base URL through the stable proxy")
	flag.StringVar(&rolloutKey, "development-rollout-platform-key", "", "Base64 Ed25519 public key for namespaced platform rollout evidence")
	flag.StringVar(&rolloutRouteCA, "development-rollout-route-ca-file", "", "Optional PEM CA bundle added to system trust for the rollout HTTPS routes")
	flag.StringVar(&rolloutSearchAuthSecret, "development-rollout-search-auth-secret", "", "Namespaced client credential Secret for the search rollout route")
	flag.StringVar(&rolloutIngestAuthSecret, "development-rollout-ingest-auth-secret", "", "Namespaced client credential Secret for the ingest rollout route")
	flag.StringVar(&rolloutSearchHTTPRoute, "development-rollout-search-httproute", "", "Approved HTTPRoute for weighted search traffic")
	flag.StringVar(&rolloutIngestHTTPRoute, "development-rollout-ingest-httproute", "", "Approved HTTPRoute for weighted indexing traffic")
	flag.StringVar(&writerRetirementConfig, "development-writer-retirement-config", "", "JSON binding for one development instance's writer withdrawal and preparation drill")
	logging := zap.Options{}
	logging.BindFlags(flag.CommandLine)
	flag.Parse()
	if developmentBasicAuthOperations && (!leaderElection || developmentBasicAuthImage == "") {
		ctrl.Log.Error(nil, "development Basic Auth operations require leader election and the approved Basic Auth image")
		os.Exit(1)
	}
	if developmentBasicAuthImage != "" && (!developmentGatedPods || developmentBasicAuthImage != developmentHTTPTLSImage) {
		ctrl.Log.Error(nil, "development Basic Auth requires gated Pods and the same approved HTTPS runtime image")
		os.Exit(1)
	}
	if developmentServiceProbeImage != "" && (!developmentGatedPods || !regexp.MustCompile(`^.+@sha256:[a-f0-9]{64}$`).MatchString(developmentServiceProbeImage)) {
		ctrl.Log.Error(nil, "development service probing requires gated Pod creation and a digest-pinned image")
		os.Exit(1)
	}
	if developmentReleaseGates && !developmentGatedPods {
		ctrl.Log.Error(nil, "development-release-gated-pods requires development-create-gated-pods")
		os.Exit(1)
	}
	if developmentEmergencyFailback && (!leaderElection || !developmentReleaseGates || !developmentObserveEndpoints) {
		ctrl.Log.Error(nil, "development-emergency-failback requires leader election, gate release, and endpoint observation")
		os.Exit(1)
	}
	if developmentSearchScaleDown && (!leaderElection || !developmentReleaseGates || !developmentObserveEndpoints || developmentWarmupImage == "") {
		ctrl.Log.Error(nil, "development-search-scale-down requires leader election, gate release, endpoint observation, and a qualified warm-up image")
		os.Exit(1)
	}
	if developmentSettingsImage != "" && !leaderElection {
		ctrl.Log.Error(nil, "development settings writes require leader election")
		os.Exit(1)
	}
	if developmentS3WebIdentityImage != "" && (!developmentGatedPods || !regexp.MustCompile(`^.+@sha256:[a-f0-9]{64}$`).MatchString(developmentS3WebIdentityImage)) {
		ctrl.Log.Error(nil, "development S3 web identity requires gated Pod creation and a digest-pinned image")
		os.Exit(1)
	}
	if developmentRuntimeSettingsImage != "" && (!developmentGatedPods || !regexp.MustCompile(`^.+@sha256:[a-f0-9]{64}$`).MatchString(developmentRuntimeSettingsImage)) {
		ctrl.Log.Error(nil, "runtime startup settings require gated Pods and a digest-pinned image")
		os.Exit(1)
	}
	if (developmentPluginInstallerImage != "" || developmentPluginRuntimeImage != "" || developmentPluginPolicy != "") && (!developmentGatedPods || developmentPluginInstallerImage == "" || developmentPluginRuntimeImage == "" || developmentPluginPolicy == "") {
		ctrl.Log.Error(nil, "development plugin installation requires gated Pods, installer image, runtime image and policy")
		os.Exit(1)
	}
	if (developmentPeerDNSImage == "") != (developmentClusterDomain == "") || (developmentPeerDNSImage != "" && !developmentGatedPods) {
		ctrl.Log.Error(nil, "development peer DNS requires gated Pod creation, a qualified image, and a cluster domain")
		os.Exit(1)
	}
	if developmentPeerTLSImage != "" && (developmentPeerDNSImage == "" || developmentPeerTLSImage != developmentPeerDNSImage) {
		ctrl.Log.Error(nil, "development peer TLS requires the same qualified peer DNS image")
		os.Exit(1)
	}
	if developmentHTTPTLSImage != "" && (developmentHTTPTLSImage != developmentPeerDNSImage || !regexp.MustCompile(`^.+@sha256:[a-f0-9]{64}$`).MatchString(developmentHTTPTLSImage)) {
		ctrl.Log.Error(nil, "development HTTP TLS requires the same digest-pinned peer DNS image")
		os.Exit(1)
	}
	if developmentReleaseWindow < time.Second || developmentReleaseWindow > time.Hour || developmentReleaseLimit < 1 || developmentReleaseLimit > 64 {
		ctrl.Log.Error(nil, "development release rate requires a window from 1s to 1h and a limit from 1 to 64")
		os.Exit(1)
	}
	if developmentPendingTimeout <= 0 {
		ctrl.Log.Error(nil, "development-pending-timeout must be positive")
		os.Exit(1)
	}
	ctrl.SetLogger(zap.New(zap.UseFlagOptions(&logging)))
	if watchNamespace == "" {
		ctrl.Log.Error(nil, "watch-namespace is required")
		os.Exit(1)
	}

	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		ctrl.Log.Error(err, "register Kubernetes API")
		os.Exit(1)
	}
	if err := v1alpha1.AddToScheme(scheme); err != nil {
		ctrl.Log.Error(err, "register operator API")
		os.Exit(1)
	}
	manager, err := ctrl.NewManager(ctrl.GetConfigOrDie(), ctrl.Options{
		Scheme: scheme, Metrics: metricsserver.Options{BindAddress: metricsAddress},
		HealthProbeBindAddress: probeAddress, LeaderElection: leaderElection,
		LeaderElectionID: "opensearch-serverless-operator.serverless.opensearch.org", LeaderElectionNamespace: watchNamespace,
		Cache: cache.Options{DefaultNamespaces: map[string]cache.Config{watchNamespace: {}}},
	})
	if err != nil {
		ctrl.Log.Error(err, "create manager")
		os.Exit(1)
	}
	core, err := kubernetes.NewForConfig(manager.GetConfig())
	if err != nil {
		ctrl.Log.Error(err, "create Kubernetes client")
		os.Exit(1)
	}
	instanceReconciler := &controller.InstanceReconciler{Client: manager.GetClient(), Core: core.CoreV1(), Policy: core.PolicyV1(), Discovery: core.DiscoveryV1(), DevelopmentObserveEndpoints: developmentObserveEndpoints, DevelopmentServiceProbeImage: developmentServiceProbeImage, DevelopmentGatedPods: developmentGatedPods, DevelopmentReleaseGates: developmentReleaseGates, DevelopmentPendingTimeout: developmentPendingTimeout, DevelopmentReleaseWindow: developmentReleaseWindow, DevelopmentReleaseLimit: developmentReleaseLimit, DevelopmentWarmupImage: developmentWarmupImage, DevelopmentS3WebIdentityImage: developmentS3WebIdentityImage, DevelopmentPeerDNSImage: developmentPeerDNSImage, DevelopmentPeerTLSImage: developmentPeerTLSImage, DevelopmentClusterDomain: developmentClusterDomain, DevelopmentPluginInstallerImage: developmentPluginInstallerImage, DevelopmentPluginRuntimeImage: developmentPluginRuntimeImage, DevelopmentPluginPolicy: developmentPluginPolicy}
	instanceReconciler.APIReader = manager.GetAPIReader()
	instanceReconciler.DevelopmentHTTPTLSImage = developmentHTTPTLSImage
	instanceReconciler.DevelopmentBasicAuthImage = developmentBasicAuthImage
	instanceReconciler.DevelopmentRuntimeSettingsImage = developmentRuntimeSettingsImage
	instanceReconciler.DevelopmentEmergencyFailback = developmentEmergencyFailback
	instanceReconciler.DevelopmentSearchScaleDown = developmentSearchScaleDown
	if developmentPeerCertificateIssuers != "" || developmentHTTPCertificateIssuers != "" {
		for _, scope := range []struct {
			names string
			image string
			dest  *map[string]bool
		}{
			{developmentPeerCertificateIssuers, developmentPeerTLSImage, &instanceReconciler.PeerCertificateIssuers},
			{developmentHTTPCertificateIssuers, developmentHTTPTLSImage, &instanceReconciler.HTTPCertificateIssuers},
		} {
			if scope.names == "" {
				continue
			}
			if scope.image == "" {
				ctrl.Log.Error(nil, "cert-manager issuance requires an approved TLS runtime image")
				os.Exit(1)
			}
			issuers := map[string]bool{}
			for _, field := range strings.Split(scope.names, ",") {
				name := strings.TrimSpace(field)
				if len(validation.IsDNS1123Subdomain(name)) != 0 {
					ctrl.Log.Error(nil, "invalid cert-manager Issuer name", "name", name)
					os.Exit(1)
				}
				issuers[name] = true
			}
			*scope.dest = issuers
		}
		certificates, err := dynamic.NewForConfig(manager.GetConfig())
		if err != nil {
			ctrl.Log.Error(err, "create cert-manager client")
			os.Exit(1)
		}
		instanceReconciler.Certificates = certificates
	}
	if err := instanceReconciler.SetupWithManager(manager); err != nil {
		ctrl.Log.Error(err, "register instance controller")
		os.Exit(1)
	}
	operationReconciler := &controller.OperationReconciler{Client: manager.GetClient(), Core: core.CoreV1(), Discovery: core.DiscoveryV1(), Policy: core.PolicyV1(), DevelopmentSettingsImage: developmentSettingsImage, DevelopmentRetryProvisioning: developmentRetryProvisioning}
	if developmentBasicAuthOperations {
		operationReconciler.ConfigureDevelopmentSecurityAccounts(instanceReconciler, manager.GetAPIReader())
	}
	if rolloutInstance != "" || rolloutSearchURL != "" || rolloutIngestURL != "" || rolloutKey != "" || rolloutRouteCA != "" ||
		rolloutSearchHTTPRoute != "" || rolloutIngestHTTPRoute != "" || rolloutSearchAuthSecret != "" || rolloutIngestAuthSecret != "" {
		if !leaderElection {
			ctrl.Log.Error(nil, "development rollout requires leader election")
			os.Exit(1)
		}
		key, err := base64.StdEncoding.Strict().DecodeString(rolloutKey)
		if err != nil {
			ctrl.Log.Error(err, "decode rollout platform public key")
			os.Exit(1)
		}
		roots, err := rolloutRouteTrust(rolloutRouteCA)
		if err != nil {
			ctrl.Log.Error(err, "load rollout route trust")
			os.Exit(1)
		}
		if err := operationReconciler.ConfigureDevelopmentRollout(instanceReconciler, rolloutInstance, rolloutSearchURL, rolloutIngestURL, key, roots, rolloutSearchAuthSecret, rolloutIngestAuthSecret); err != nil {
			ctrl.Log.Error(err, "configure development rollout")
			os.Exit(1)
		}
		instanceReconciler.ConfigureDevelopmentFleetRetirement(rolloutInstance, key)
		if rolloutSearchHTTPRoute != "" || rolloutIngestHTTPRoute != "" {
			if err := operationReconciler.ConfigureDevelopmentWeightedRollout(instanceReconciler, rolloutSearchHTTPRoute, rolloutIngestHTTPRoute, key); err != nil {
				ctrl.Log.Error(err, "configure development weighted rollout")
				os.Exit(1)
			}
		}
	}
	if writerRetirementConfig != "" {
		if !leaderElection {
			ctrl.Log.Error(nil, "development writer retirement requires leader election")
			os.Exit(1)
		}
		cfg, err := readWriterRetirementConfig(writerRetirementConfig)
		if err != nil {
			ctrl.Log.Error(err, "load development writer retirement binding")
			os.Exit(1)
		}
		if err := operationReconciler.ConfigureDevelopmentWriterRetirement(instanceReconciler, cfg); err != nil {
			ctrl.Log.Error(err, "configure development writer retirement")
			os.Exit(1)
		}
	}
	if err := operationReconciler.SetupWithManager(manager); err != nil {
		ctrl.Log.Error(err, "register operation controller")
		os.Exit(1)
	}
	if err := manager.AddHealthzCheck("healthz", healthz.Ping); err != nil {
		ctrl.Log.Error(err, "register health check")
		os.Exit(1)
	}
	if err := manager.AddReadyzCheck("readyz", healthz.Ping); err != nil {
		ctrl.Log.Error(err, "register readiness check")
		os.Exit(1)
	}
	if err := manager.Start(ctrl.SetupSignalHandler()); err != nil {
		ctrl.Log.Error(err, "run manager")
		os.Exit(1)
	}
}

func readWriterRetirementConfig(path string) (controller.DevelopmentWriterRetirementConfig, error) {
	var cfg controller.DevelopmentWriterRetirementConfig
	file, err := os.Open(path)
	if err != nil {
		return cfg, err
	}
	defer file.Close()
	decoder := json.NewDecoder(file)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&cfg); err != nil {
		return cfg, err
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return cfg, fmt.Errorf("writer retirement config must contain one JSON object")
	}
	return cfg, nil
}

func rolloutRouteTrust(path string) (*x509.CertPool, error) {
	if path == "" {
		return nil, nil
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	roots, err := x509.SystemCertPool()
	if err != nil {
		return nil, err
	}
	if !roots.AppendCertsFromPEM(data) {
		return nil, fmt.Errorf("rollout route CA file contains no PEM certificates")
	}
	return roots, nil
}
