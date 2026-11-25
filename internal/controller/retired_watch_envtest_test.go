//go:build envtest

package controller

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	"github.com/huyz0/opensearch-serverless-operator/api/v1alpha1"
)

type indexedWatchClient struct {
	client.Client
	events chan string
}

func (c *indexedWatchClient) List(ctx context.Context, list client.ObjectList, options ...client.ListOption) error {
	result := &client.ListOptions{}
	for _, option := range options {
		option.ApplyToList(result)
	}
	if err := c.Client.List(ctx, list, options...); err != nil {
		return err
	}
	if instances, ok := list.(*v1alpha1.OpenSearchServerlessList); ok && result.FieldSelector != nil && len(instances.Items) == 1 && instances.Items[0].Name == "retired-watch" {
		select {
		case c.events <- result.FieldSelector.String():
		default:
		}
	}
	return nil
}

func TestManagerWatchesIndexedConfigMapsAndUnownedRetiredPods(t *testing.T) {
	environment := &envtest.Environment{CRDDirectoryPaths: []string{filepath.Join("..", "..", "config", "generated-dev")}, ErrorIfCRDPathMissing: true}
	config, err := environment.Start()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := environment.Stop(); err != nil {
			t.Error(err)
		}
	})
	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := v1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	direct, err := client.New(config, client.Options{Scheme: scheme})
	if err != nil {
		t.Fatal(err)
	}
	core, err := kubernetes.NewForConfig(config)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	instance := &v1alpha1.OpenSearchServerless{ObjectMeta: metav1.ObjectMeta{Name: "retired-watch", Namespace: "default"}, Spec: v1alpha1.OpenSearchServerlessSpec{Image: "example.invalid/image@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", ServiceAccountName: "runtime", Storage: v1alpha1.Storage{Type: "s3", Bucket: "bucket", Region: "region"}, Pools: map[string]v1alpha1.Pool{"search": {CapacityProfileRef: "search-profile", Zones: map[string]v1alpha1.Zone{"a": {Replicas: 1}}}}}}
	if err := direct.Create(ctx, instance); err != nil {
		t.Fatal(err)
	}
	instance.Status.RetiredPodCreations = []v1alpha1.RetiredPodCreation{{Name: "late-name"}}
	if err := direct.Status().Update(ctx, instance); err != nil {
		t.Fatal(err)
	}
	manager, err := ctrl.NewManager(config, ctrl.Options{Scheme: scheme, Metrics: metricsserver.Options{BindAddress: "0"}, HealthProbeBindAddress: "0"})
	if err != nil {
		t.Fatal(err)
	}
	watchClient := &indexedWatchClient{Client: manager.GetClient(), events: make(chan string, 8)}
	r := &InstanceReconciler{Client: watchClient, Core: core.CoreV1(), DevelopmentGatedPods: true}
	if err := r.SetupWithManager(manager); err != nil {
		t.Fatal(err)
	}
	managerCtx, stop := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- manager.Start(managerCtx) }()
	defer func() {
		stop()
		if err := <-done; err != nil {
			t.Error(err)
		}
	}()
	if !manager.GetCache().WaitForCacheSync(ctx) {
		t.Fatal("manager cache did not sync")
	}
	// Wait for the retired-name index before the unowned Pod event is sent.
	for {
		var indexed v1alpha1.OpenSearchServerlessList
		if err := manager.GetClient().List(ctx, &indexed, client.InNamespace("default"), client.MatchingFields{retiredPodNameIndex: "late-name"}); err == nil && len(indexed.Items) == 1 {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal("retired-name index did not sync")
		case <-time.After(100 * time.Millisecond):
		}
	}
	if _, err := core.CoreV1().Pods("default").Create(ctx, &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "late-name"}, Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "foreign", Image: "example.invalid/foreign"}}}}, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := core.CoreV1().ConfigMaps("default").Create(ctx, &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "search-profile"}}, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	waiting := map[string]bool{retiredPodNameIndex + "=late-name": true, configMapReferenceIndex + "=search-profile": true}
	for len(waiting) > 0 {
		select {
		case matched := <-watchClient.events:
			delete(waiting, matched)
		case <-ctx.Done():
			t.Fatalf("watch did not match indexed references: %v", waiting)
		}
	}
}
