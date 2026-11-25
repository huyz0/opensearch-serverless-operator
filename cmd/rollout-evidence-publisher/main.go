package main

import (
	"context"
	"crypto/ed25519"
	"crypto/x509"
	"encoding/pem"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
	typedcore "k8s.io/client-go/kubernetes/typed/core/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/huyz0/opensearch-serverless-operator/api/v1alpha1"
	"github.com/huyz0/opensearch-serverless-operator/internal/rollout"
)

func main() {
	namespace := flag.String("namespace", "", "Namespace of the approved rollout")
	operationName := flag.String("operation", "", "Name of the rollout operation")
	keyFile := flag.String("signing-key-file", "", "Path to an Ed25519 private key in PKCS#8 PEM format")
	evidenceFile := flag.String("evidence-file", "", "Path to an observer report; omit to read stdin")
	reportConfigMap := flag.String("writer-report-configmap", "", "Wait for report.json in this platform namespace/name ConfigMap")
	weightedRoute := flag.Bool("weighted-route", false, "Publish proxy observations for a weighted HTTPRoute")
	retirement := flag.Bool("fleet-retirement", false, "Publish a fleet drain reservation request or completion receipt")
	writerWithdrawal := flag.Bool("writer-withdrawal", false, "Publish signed Envoy evidence for a claimed writer withdrawal")
	flag.Parse()
	input := io.Reader(os.Stdin)
	var err error
	if *reportConfigMap != "" {
		if !*writerWithdrawal || *evidenceFile != "" || *weightedRoute || *retirement {
			err = fmt.Errorf("--writer-report-configmap requires --writer-withdrawal and excludes --evidence-file")
		} else {
			input, err = writerReportConfigMap(*reportConfigMap)
		}
	}
	if err == nil {
		err = run(*namespace, *operationName, *keyFile, *evidenceFile, *weightedRoute, *retirement, *writerWithdrawal, input)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func writerReportConfigMap(reference string) (io.Reader, error) {
	namespace, name, ok := strings.Cut(reference, "/")
	if !ok || len(validation.IsDNS1123Label(namespace)) != 0 || len(validation.IsDNS1123Subdomain(name)) != 0 {
		return nil, fmt.Errorf("writer report ConfigMap must be a valid namespace/name")
	}
	config, err := ctrl.GetConfig()
	if err != nil {
		return nil, err
	}
	core, err := typedcore.NewForConfig(config)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	return waitForWriterReport(ctx, core.ConfigMaps(namespace), name, time.Second)
}

func waitForWriterReport(ctx context.Context, configMaps typedcore.ConfigMapInterface, name string, interval time.Duration) (io.Reader, error) {
	var raw string
	err := wait.PollUntilContextCancel(ctx, interval, true, func(ctx context.Context) (bool, error) {
		report, err := configMaps.Get(ctx, name, metav1.GetOptions{})
		if apierrors.IsNotFound(err) {
			return false, nil
		}
		if err != nil {
			return false, err
		}
		if report.DeletionTimestamp != nil {
			return false, fmt.Errorf("writer report ConfigMap is terminating")
		}
		raw = report.Data["report.json"]
		return raw != "", nil
	})
	if err != nil {
		return nil, fmt.Errorf("read writer report ConfigMap %q: %w", name, err)
	}
	return strings.NewReader(raw), nil
}

func run(namespace, operationName, keyFile, evidenceFile string, weightedRoute, retirement, writerWithdrawal bool, input io.Reader) error {
	if namespace == "" || operationName == "" || keyFile == "" {
		return fmt.Errorf("namespace, operation and signing key file are required")
	}
	if (weightedRoute && retirement) || (writerWithdrawal && (weightedRoute || retirement)) {
		return fmt.Errorf("choose one evidence type")
	}
	if evidenceFile != "" {
		file, err := os.Open(evidenceFile)
		if err != nil {
			return fmt.Errorf("open observer report: %w", err)
		}
		defer file.Close()
		input = file
	}
	raw, err := io.ReadAll(io.LimitReader(input, rollout.PlatformEvidenceMaxBytes+1))
	if err != nil {
		return err
	}
	var platform rollout.PlatformEvidence
	var weights rollout.WeightEvidence
	var retired rollout.FleetRetirementEvidence
	var withdrawn rollout.WriterWithdrawalEvidence
	if writerWithdrawal {
		withdrawn, err = rollout.ParseWriterWithdrawalEvidence(string(raw))
	} else if retirement {
		retired, err = rollout.ParseFleetRetirementEvidence(string(raw))
	} else if weightedRoute {
		weights, err = rollout.ParseWeightEvidence(string(raw))
	} else {
		platform, err = rollout.ParsePlatformEvidence(string(raw))
	}
	if err != nil {
		return err
	}
	key, err := loadSigningKey(keyFile)
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
	reader, err := client.New(config, client.Options{Scheme: scheme})
	if err != nil {
		return err
	}
	kube, err := kubernetes.NewForConfig(config)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if writerWithdrawal {
		return rollout.PublishWriterWithdrawalEvidence(ctx, reader, kube.CoreV1(), namespace, operationName, withdrawn, key)
	}
	if retirement {
		var operation v1alpha1.OpenSearchServerlessOperation
		if err := reader.Get(ctx, client.ObjectKey{Namespace: namespace, Name: operationName}, &operation); err != nil {
			return err
		}
		if operation.UID != retired.CommitUID || operation.Status.Phase != "Succeeded" || operation.DeletionTimestamp != nil {
			return fmt.Errorf("fleet retirement must identify the completed rollout operation")
		}
		var instance v1alpha1.OpenSearchServerless
		if err := reader.Get(ctx, client.ObjectKey{Namespace: namespace, Name: operation.Spec.InstanceRef.Name}, &instance); err != nil {
			return err
		}
		if operation.Spec.InstanceRef.UID != string(instance.UID) {
			return fmt.Errorf("rollout operation belongs to a different instance UID")
		}
		return rollout.PublishFleetRetirementEvidence(ctx, kube.CoreV1(), &instance, retired, key)
	}
	if weightedRoute {
		return rollout.PublishWeightEvidence(ctx, reader, kube.CoreV1(), namespace, operationName, weights, key)
	}
	return rollout.PublishPlatformEvidence(ctx, reader, kube.CoreV1(), namespace, operationName, platform, key)
}

func loadSigningKey(path string) (ed25519.PrivateKey, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	raw, err := io.ReadAll(io.LimitReader(file, 4097))
	if err != nil {
		return nil, err
	}
	block, rest := pem.Decode(raw)
	if len(raw) > 4096 || block == nil || block.Type != "PRIVATE KEY" || len(rest) != 0 {
		return nil, fmt.Errorf("signing key must be a single PKCS#8 PEM private key")
	}
	parsed, err := x509.ParsePKCS8PrivateKey(block.Bytes)
	if err != nil {
		return nil, fmt.Errorf("cannot parse PKCS#8 signing key")
	}
	key, ok := parsed.(ed25519.PrivateKey)
	if !ok {
		return nil, fmt.Errorf("signing key must use Ed25519")
	}
	return key, nil
}
