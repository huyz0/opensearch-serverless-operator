//go:build envtest

package controller

import (
	"context"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"

	"github.com/huyz0/opensearch-serverless-operator/api/v1alpha1"
	"github.com/huyz0/opensearch-serverless-operator/internal/security"
	"github.com/huyz0/opensearch-serverless-operator/internal/workload"
)

func TestAccountCohortOnAPIServer(t *testing.T) {
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
	if err := v1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	kube, err := client.New(config, client.Options{Scheme: scheme})
	if err != nil {
		t.Fatal(err)
	}
	core, err := kubernetes.NewForConfig(config)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	image := "example/image@sha256:" + strings.Repeat("a", 64)
	instance := &v1alpha1.OpenSearchServerless{
		ObjectMeta: metav1.ObjectMeta{Name: "catalog", Namespace: "default"},
		Spec: v1alpha1.OpenSearchServerlessSpec{
			Image: image, ServiceAccountName: "runtime", Storage: v1alpha1.Storage{Type: "s3", Bucket: "bucket", Region: "region"},
			Pools: map[string]v1alpha1.Pool{
				"search": {Zones: map[string]v1alpha1.Zone{"a": {Replicas: 1}}},
				"ingest": {Zones: map[string]v1alpha1.Zone{"a": {Replicas: 1}}},
			},
		},
	}
	if err := kube.Create(ctx, instance); err != nil {
		t.Fatal(err)
	}
	instance.Status.ServingFleetIntentByRole = map[string]string{"search": "blue", "ingest": "blue"}
	if err := kube.Status().Update(ctx, instance); err != nil {
		t.Fatal(err)
	}
	pods := core.CoreV1().Pods(instance.Namespace)
	makePod := func(name, role, fleet, address string, owned bool) (*corev1.Pod, error) {
		desired := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: instance.Namespace, Labels: map[string]string{workload.RoleLabel: role, workload.FleetLabel: fleet}},
			Spec:       corev1.PodSpec{NodeName: "node-a", Containers: []corev1.Container{{Name: "opensearch", Image: image}}},
		}
		if owned {
			desired.Labels[workload.InstanceLabel] = string(instance.UID)
			desired.OwnerReferences = []metav1.OwnerReference{*metav1.NewControllerRef(instance, v1alpha1.GroupVersion.WithKind("OpenSearchServerless"))}
		}
		if fleet == "retained" {
			desired.Finalizers = []string{"serverless.opensearch.org/test-retained"}
		}
		pod, err := pods.Create(ctx, desired, metav1.CreateOptions{})
		if err != nil {
			return nil, err
		}
		pod.Status.Phase, pod.Status.PodIP = corev1.PodRunning, address
		pod.Status.ContainerStatuses = []corev1.ContainerStatus{{Name: "opensearch", Image: image, ImageID: image, ContainerID: "containerd://" + name,
			State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{StartedAt: metav1.Now()}}}}
		return pods.UpdateStatus(ctx, pod, metav1.UpdateOptions{})
	}
	want := map[string]string{}
	for _, item := range []struct{ name, role, fleet, address string }{
		{"search-blue", "search", "blue", "10.10.0.1"},
		{"ingest-blue", "ingest", "blue", "10.10.0.2"},
		{"search-preview", "search", "green", "10.10.0.3"},
		{"ingest-retained", "ingest", "retained", "10.10.0.4"},
	} {
		pod, err := makePod(item.name, item.role, item.fleet, item.address, true)
		if err != nil {
			t.Fatal(err)
		}
		want[string(pod.UID)] = pod.Name + "-process"
		if item.fleet == "retained" {
			pod.Status.Phase, pod.Status.Reason = corev1.PodFailed, "NodeLost"
			if _, err := pods.UpdateStatus(ctx, pod, metav1.UpdateOptions{}); err != nil {
				t.Fatal(err)
			}
			if err := pods.Delete(ctx, pod.Name, metav1.DeleteOptions{}); err != nil {
				t.Fatal(err)
			}
		}
	}
	if _, err := makePod("foreign", "search", "blue", "10.10.0.5", false); err != nil {
		t.Fatal(err)
	}
	observer := accountCohortObserver{Core: core.CoreV1(), Reader: kube}
	qualify := func(_ context.Context, pod runtimeCohortPod) (security.AccountProcess, error) {
		return security.AccountProcess{Name: pod.Name, Role: pod.Role, Incarnation: pod.Name + "-process"}, nil
	}
	observed, err := observer.observe(ctx, instance, nil, qualify)
	if err != nil || len(observed) != len(want) {
		t.Fatalf("cohort = %v, %v; want all four owned JVMs", observed, err)
	}
	for _, item := range observed {
		if want[item.PodUID] != item.ProcessID {
			t.Fatalf("unexpected evidence: %#v", item)
		}
		delete(want, item.PodUID)
	}
	var once sync.Once
	var mutationErr error
	observed, err = observer.observe(ctx, instance, nil, func(ctx context.Context, pod runtimeCohortPod) (security.AccountProcess, error) {
		once.Do(func() {
			_, mutationErr = makePod("ingest-new-preview", "ingest", "green", "10.10.0.6", true)
		})
		if mutationErr != nil {
			return security.AccountProcess{}, mutationErr
		}
		return qualify(ctx, pod)
	})
	if observed != nil || err == nil || !strings.Contains(err.Error(), "membership or identity changed") {
		t.Fatalf("changed inventory returned evidence: %v, %v", observed, err)
	}
	recordReads := 0
	observed, err = observer.observe(ctx, instance, func(context.Context) error {
		recordReads++
		if recordReads == 1 {
			return nil
		}
		_, err := makePod("ingest-joined-during-record-read", "ingest", "green", "10.10.0.7", true)
		return err
	}, qualify)
	if recordReads != 2 || observed != nil || err == nil || !strings.Contains(err.Error(), "membership or identity changed") {
		t.Fatalf("Pod joining during the final account record read returned evidence: %v, %v", observed, err)
	}
	once, mutationErr = sync.Once{}, nil
	observed, err = observer.observe(ctx, instance, nil, func(ctx context.Context, pod runtimeCohortPod) (security.AccountProcess, error) {
		once.Do(func() {
			restarted, err := pods.Get(ctx, "search-blue", metav1.GetOptions{})
			if err != nil {
				mutationErr = err
				return
			}
			restarted.Status.ContainerStatuses[0].RestartCount++
			restarted.Status.ContainerStatuses[0].ContainerID = "containerd://search-restarted"
			_, mutationErr = pods.UpdateStatus(ctx, restarted, metav1.UpdateOptions{})
		})
		if mutationErr != nil {
			return security.AccountProcess{}, mutationErr
		}
		return qualify(ctx, pod)
	})
	if observed != nil || err == nil || !strings.Contains(err.Error(), "membership or identity changed") {
		t.Fatalf("restarted container returned evidence: %v, %v", observed, err)
	}
	preview, err := pods.Get(ctx, "search-preview", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	delete(preview.Labels, workload.InstanceLabel)
	if _, err := pods.Update(ctx, preview, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	if evidence, err := observer.observe(ctx, instance, nil, qualify); evidence != nil || err == nil || !strings.Contains(err.Error(), "conflicting instance identity") {
		t.Fatalf("missing owner label was silently ignored: %v, %v", evidence, err)
	}
}
