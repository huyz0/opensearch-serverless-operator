//go:build envtest && !windows

package main

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/yaml"
	"k8s.io/client-go/tools/clientcmd"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"

	"github.com/huyz0/opensearch-serverless-operator/api/v1alpha1"
	"github.com/huyz0/opensearch-serverless-operator/internal/java"
	"github.com/huyz0/opensearch-serverless-operator/internal/rollout"
)

func TestStagedWriterReportPublishesThroughNamespacedRBAC(t *testing.T) {
	repository := filepath.Join("..", "..")
	useExisting := false
	environment := &envtest.Environment{
		UseExistingCluster: &useExisting,
		CRDDirectoryPaths:  []string{filepath.Join(repository, "config", "generated-dev")},
	}
	t.Cleanup(func() {
		if err := environment.Stop(); err != nil {
			t.Error(err)
		}
	})
	config, err := environment.Start()
	if err != nil {
		t.Fatal(err)
	}
	scheme := runtime.NewScheme()
	for _, add := range []func(*runtime.Scheme) error{corev1.AddToScheme, v1alpha1.AddToScheme} {
		if err := add(scheme); err != nil {
			t.Fatal(err)
		}
	}
	admin, err := client.New(config, client.Options{Scheme: scheme})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	create := func(object client.Object) {
		t.Helper()
		if err := admin.Create(ctx, object); err != nil {
			t.Fatal(err)
		}
	}
	updateStatus := func(object client.Object) {
		t.Helper()
		if err := admin.Status().Update(ctx, object); err != nil {
			t.Fatal(err)
		}
	}
	const namespace, platform, operation = "operator-development", "operator-platform", "retire-index-writer"
	for _, name := range []string{namespace, platform} {
		create(&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: name}})
	}
	instance := &v1alpha1.OpenSearchServerless{
		ObjectMeta: metav1.ObjectMeta{Name: "catalog", Namespace: namespace},
		Spec: v1alpha1.OpenSearchServerlessSpec{
			Image: "registry.example/java@sha256:" + strings.Repeat("a", 64), ServiceAccountName: "runtime",
			Storage: v1alpha1.Storage{Type: "s3", Bucket: "fixture", Region: "us-east-1"},
			Pools:   map[string]v1alpha1.Pool{"ingest": {Zones: map[string]v1alpha1.Zone{"zone-a": {Replicas: 2, MinServing: 1}}}},
		},
	}
	create(instance)
	op := &v1alpha1.OpenSearchServerlessOperation{
		ObjectMeta: metav1.ObjectMeta{Name: operation, Namespace: namespace,
			Finalizers: []string{"serverless.opensearch.org/writer-retirement"}},
		Spec: v1alpha1.OperationSpec{Action: "RetireWriter",
			InstanceRef:      v1alpha1.InstanceReference{Name: instance.Name, UID: string(instance.UID)},
			WriterRetirement: &v1alpha1.WriterRetirementTarget{PodName: "writer", PodUID: "victim"}},
	}
	create(op)
	instance.Status.ActiveOperation = &v1alpha1.OperationClaim{
		Name: op.Name, Action: "RetireWriter", UID: string(op.UID), Generation: op.Generation,
	}
	instance.Status.ServingFleetIntentByRole = map[string]string{"ingest": "initial"}
	updateStatus(instance)
	cm := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: rollout.WriterWithdrawalEvidenceName(op.UID), Namespace: namespace}}
	create(cm)
	role, err := os.ReadFile(filepath.Join(repository, "config", "dev", "writer-withdrawal-publisher-rbac.example.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	roles := yaml.NewYAMLOrJSONDecoder(strings.NewReader(strings.ReplaceAll(
		string(role), "writer-withdrawal-replace-with-operation-hash", cm.Name)), 4096)
	for {
		object := &unstructured.Unstructured{}
		if err := roles.Decode(object); errors.Is(err, io.EOF) {
			break
		} else if err != nil {
			t.Fatal(err)
		}
		create(object)
	}
	jobData, err := os.ReadFile(filepath.Join(repository, "config", "dev", "writer-withdrawal-publisher-job.example.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	job := &unstructured.Unstructured{}
	if err := yaml.NewYAMLOrJSONDecoder(strings.NewReader(string(jobData)), 4096).Decode(job); err != nil {
		t.Fatal(err)
	}
	if err := admin.Create(ctx, job, client.DryRunAll); err != nil {
		t.Fatalf("publisher Job admission failed: %v", err)
	}
	const identity = "system:serviceaccount:operator-platform:writer-withdrawal-publisher"
	kubeconfig := filepath.Join(t.TempDir(), "publisher.kubeconfig")
	kube := clientcmdapi.NewConfig()
	kube.Clusters["test"] = &clientcmdapi.Cluster{Server: config.Host, CertificateAuthorityData: config.CAData}
	kube.AuthInfos["publisher"] = &clientcmdapi.AuthInfo{
		ClientCertificateData: config.CertData, ClientKeyData: config.KeyData, Impersonate: identity,
		ImpersonateGroups: []string{"system:authenticated", "system:serviceaccounts", "system:serviceaccounts:operator-platform"},
	}
	kube.Contexts["test"] = &clientcmdapi.Context{Cluster: "test", AuthInfo: "publisher"}
	kube.CurrentContext = "test"
	if err := clientcmd.WriteToFile(*kube, kubeconfig); err != nil {
		t.Fatal(err)
	}
	t.Setenv("KUBECONFIG", kubeconfig)

	dir := t.TempDir()
	public, key, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	der, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	keyPath := filepath.Join(dir, "key.pem")
	if err := os.WriteFile(keyPath, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der}), 0600); err != nil {
		t.Fatal(err)
	}

	at := time.Now().Add(-4 * time.Second).Truncate(time.Microsecond)
	withdrawnAt := metav1.NewMicroTime(at)
	op.Status.WriterRetirement = &v1alpha1.WriterRetirementStatus{
		InstanceGeneration: instance.Generation, WithdrawalArmed: true, WithdrawnAt: &withdrawnAt,
		NodeID: "node", NodeEphemeralID: "jvm", ContainerID: "runtime",
		ServiceUID: "service", RouteUID: "route", GatewayUID: "gateway", GatewayServiceUID: "frontend",
		ServingFleet: "initial", BindingDigest: strings.Repeat("a", 64), PreparerSecretUID: "preparer",
	}
	updateStatus(op)
	scope := rollout.WriterWithdrawalScope{
		RouteAuthIdentity: rollout.RouteAuthIdentity{AuthSecretName: "edge", AuthSecretUID: "auth", AuthSecretVersion: "13", AuthProbeURL: "https://index.example"},
		Namespace:         namespace, InstanceUID: instance.UID, InstanceGeneration: instance.Generation, OperationUID: op.UID,
		VictimPodUID: "victim", VictimPodVersion: "5", Writer: java.WriterProcess{NodeID: "node", EphemeralID: "jvm"},
		ServiceName: "catalog-index", ServiceUID: "service", ServiceVersion: "7", RouteName: "catalog-index", RouteUID: "route", RouteVersion: "8",
		GatewayNamespace: namespace, GatewayName: "edge", GatewayUID: "gateway", GatewayVersion: "9", GatewayListener: "https", GatewayServiceUID: "frontend",
	}
	survivors := map[types.UID]string{"survivor": "10"}
	report := rollout.WriterWithdrawalEvidence{
		WriterWithdrawalScope: scope, ObservedAt: at.Add(2 * time.Second),
		EndpointVersions: map[types.UID]string{"slice": "11"}, GatewayVersions: map[string]string{"service/frontend": "12"},
		ServingProxyUIDs: []string{"proxy"},
		Proxies:          []rollout.WriterProxyReport{{ProxyUID: "proxy", ObservedAt: at.Add(2 * time.Second), LiveBackends: survivors}},
	}
	reportPath := filepath.Join(dir, "report.json")
	publish := func(evidence rollout.WriterWithdrawalEvidence) error {
		t.Helper()
		data, err := json.Marshal(evidence)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(reportPath, data, 0600); err != nil {
			t.Fatal(err)
		}
		return run(namespace, operation, keyPath, reportPath, false, false, true, strings.NewReader("invalid stdin"))
	}
	const reportName = "writer-observation-report"
	reportConfigMap := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: reportName, Namespace: platform}}
	create(reportConfigMap)

	staged := make(chan error, 1)
	go func() {
		input, err := writerReportConfigMap(platform + "/" + reportName)
		if err == nil {
			err = run(namespace, operation, keyPath, "", false, false, true, input)
		}
		staged <- err
	}()

	reportData, err := json.Marshal(report)
	if err != nil {
		t.Fatal(err)
	}
	reportConfigMap.Data = map[string]string{"report.json": string(reportData)}
	if err := admin.Update(ctx, reportConfigMap); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-staged:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if _, err := writerReportConfigMap(platform + "/unrelated"); !apierrors.IsForbidden(err) {
		t.Fatalf("publisher must be denied reads of unrelated reports: %v", err)
	}
	var stored corev1.ConfigMap
	if err := admin.Get(ctx, client.ObjectKeyFromObject(cm), &stored); err != nil {
		t.Fatal(err)
	}
	expiry, err := rollout.ObserveSignedWriterWithdrawal(stored.Data, public, scope, report.EndpointVersions, survivors,
		rollout.GatewayReplicaObservation{Versions: report.GatewayVersions, UIDs: map[string]bool{"proxy": true}}, at, time.Now())
	if err != nil || stored.UID != cm.UID || !expiry.Equal(report.ObservedAt.Add(rollout.PlatformEvidenceMaxAge)) {
		t.Fatalf("wrong evidence or replacement ConfigMap: expiry=%s err=%v", expiry, err)
	}
	version := stored.ResourceVersion

	earlier := report
	earlier.ObservedAt = at.Add(time.Second)
	if err := publish(earlier); err != nil {
		t.Fatal(err)
	}
	if err := admin.Get(ctx, client.ObjectKeyFromObject(cm), &stored); err != nil {
		t.Fatal(err)
	}
	if stored.ResourceVersion != version {
		t.Fatal("earlier observation replaced the signed evidence")
	}
}
