//go:build envtest

package controller

import (
	"context"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"

	"github.com/huyz0/opensearch-serverless-operator/api/v1alpha1"
	"github.com/huyz0/opensearch-serverless-operator/internal/workload"
)

func TestWebIdentityCreationAndGateReleaseOnAPIServer(t *testing.T) {
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
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	image := "registry.example/runtime@sha256:" + strings.Repeat("b", 64)
	role := "arn:aws:iam::123456789012:role/store"
	repository, err := core.CoreV1().ConfigMaps("default").Create(ctx, &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "identity-store", Namespace: "default"}, Data: map[string]string{"bucket": "bucket", "region": "region"}}, metav1.CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	instance := &v1alpha1.OpenSearchServerless{ObjectMeta: metav1.ObjectMeta{Name: "identity-test", Namespace: "default"}, Spec: v1alpha1.OpenSearchServerlessSpec{Image: image, ServiceAccountName: "runtime", Storage: v1alpha1.Storage{Type: "s3", RepositoryRef: &v1alpha1.LocalReference{Name: repository.Name}, Credentials: &v1alpha1.StorageCredentials{Type: "WorkloadIdentity", AWSRoleARN: role}}, Pools: map[string]v1alpha1.Pool{"search": {JVMHeap: "512Mi", Zones: map[string]v1alpha1.Zone{"a": {Replicas: 1}}, Resources: corev1.ResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("1"), corev1.ResourceMemory: resource.MustParse("1Gi")}}}}}}
	if err := kube.Create(ctx, instance); err != nil {
		t.Fatal(err)
	}
	instance.Status.PrimaryStore = &v1alpha1.PrimaryStoreStatus{Bucket: "bucket", Region: "region"}
	instance.Status.ServingFleetIntentByRole = map[string]string{"search": "initial"}
	if err := kube.Status().Update(ctx, instance); err != nil {
		t.Fatal(err)
	}
	key := types.NamespacedName{Namespace: instance.Namespace, Name: instance.Name}
	r := &InstanceReconciler{Client: kube, Core: core.CoreV1(), DevelopmentPendingTimeout: time.Minute, DevelopmentReleaseGates: true, DevelopmentReleaseLimit: 2, DevelopmentReleaseWindow: 10 * time.Minute}
	if _, err := r.reconcileGatedPod(ctx, instance); err == nil {
		t.Fatal("unqualified image was accepted")
	}
	r.DevelopmentS3WebIdentityImage = image
	step := func() {
		t.Helper()
		if err := kube.Get(ctx, key, instance); err != nil {
			t.Fatal(err)
		}
		if _, err := r.reconcileGatedPod(ctx, instance); err != nil {
			t.Fatal(err)
		}
	}
	step()
	if err := kube.Get(ctx, key, instance); err != nil {
		t.Fatal(err)
	}
	if instance.Spec.Storage.Credentials.AWSRoleARN != role || instance.Status.PendingPodCreation == nil {
		t.Fatal("API lost role ARN or creation intent")
	}
	instance.Status.PendingPodCreation.Revision = "retired-creation"
	if err := kube.Status().Update(ctx, instance); err != nil {
		t.Fatal(err)
	}
	lateIntent := *instance.Status.PendingPodCreation
	latePod, err := renderGatedPod(instance, &lateIntent, nil, "", "")
	if err != nil {
		t.Fatal(err)
	}
	// Current image qualification can disappear after the stale decision;
	// retiring old gated work must not require a new credential qualification.
	r.DevelopmentS3WebIdentityImage = ""
	if _, err := r.reconcileGatedPod(ctx, instance); err != nil {
		t.Fatal(err)
	}
	r.DevelopmentS3WebIdentityImage = image
	if err := kube.Get(ctx, key, instance); err != nil {
		t.Fatal(err)
	}
	if instance.Status.PendingPodCreation != nil || len(instance.Status.RetiredPodCreations) != 1 {
		t.Fatal("missing stale creation was not retired")
	}
	step()
	if err := kube.Get(ctx, key, instance); err != nil {
		t.Fatal(err)
	}
	if instance.Status.PendingPodCreation == nil || instance.Status.PendingPodCreation.Name == lateIntent.Name {
		t.Fatal("new intent was not allocated after retirement")
	}
	delete(latePod.Labels, workload.InstanceLabel)
	latePod, err = core.CoreV1().Pods("default").Create(ctx, latePod, metav1.CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := r.reconcileRetiredPodCreations(ctx, instance); err == nil {
		t.Fatal("unlabelled Pod occupying a retired name was not detected")
	}
	latePod.Labels[workload.InstanceLabel] = string(instance.UID)
	latePod, err = core.CoreV1().Pods("default").Update(ctx, latePod, metav1.UpdateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if _, changed, err := r.reconcileRetiredPodCreations(ctx, instance); err != nil || !changed {
		t.Fatalf("late Pod was not pinned: %v", err)
	}
	if err := kube.Get(ctx, key, instance); err != nil {
		t.Fatal(err)
	}
	if instance.Status.RetiredPodCreations[0].PodUID != string(latePod.UID) {
		t.Fatal("late Pod UID was not persisted")
	}
	restartedLate := *r
	r = &restartedLate
	for i := 0; i < 10; i++ {
		if _, _, err := r.reconcileRetiredPodCreations(ctx, instance); err != nil {
			t.Fatal(err)
		}
		if err := kube.Get(ctx, key, instance); err != nil {
			t.Fatal(err)
		}
		if instance.Status.RetiredPodCreations[0].PodUID == "" {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if instance.Status.RetiredPodCreations[0].PodUID != "" {
		t.Fatal("late Pod withdrawal did not finish")
	}
	if _, err := core.CoreV1().Pods("default").Get(ctx, latePod.Name, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Fatalf("late Pod still exists: %v", err)
	}
	// Simulate an old controller that recorded creation intent but whose Pod
	// create completed only after the template changed.
	instance.Status.PendingPodCreation.Revision = "legacy-creation"
	if err := kube.Status().Update(ctx, instance); err != nil {
		t.Fatal(err)
	}
	oldIntent := instance.Status.PendingPodCreation
	oldName := oldIntent.Name
	oldPod, err := renderGatedPod(instance, oldIntent, nil, "", "")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := core.CoreV1().Pods("default").Create(ctx, oldPod, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	step()
	if err := kube.Get(ctx, key, instance); err != nil {
		t.Fatal(err)
	}
	if instance.Status.PendingPodCreation != nil || instance.Status.PendingGateRelease == nil || !instance.Status.PendingGateRelease.WithdrawalRequested {
		t.Fatal("stale created Pod was not pinned for withdrawal")
	}
	restartedCreation := *r
	r = &restartedCreation
	for i := 0; i < 10; i++ {
		step()
		if err := kube.Get(ctx, key, instance); err != nil {
			t.Fatal(err)
		}
		if instance.Status.PendingGateRelease == nil {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if instance.Status.PendingGateRelease != nil {
		t.Fatal("stale creation withdrawal did not finish")
	}
	if _, err := core.CoreV1().Pods("default").Get(ctx, oldName, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Fatalf("stale creation Pod still exists: %v", err)
	}
	step()
	if err := kube.Get(ctx, key, instance); err != nil {
		t.Fatal(err)
	}
	if instance.Status.PendingPodCreation == nil || instance.Status.PendingPodCreation.Name == oldName {
		t.Fatal("fresh creation intent was not allocated")
	}
	name := instance.Status.PendingPodCreation.Name
	step()
	pod, err := core.CoreV1().Pods("default").Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if !hasPlacementGate(pod) || !slices.Contains(pod.Spec.Containers[0].Args, "-Ds3.client.default.identity_token_file="+workload.WebIdentityToken) {
		t.Fatal("held Pod is missing the web identity launch contract")
	}
	step()
	if err := kube.Get(ctx, key, instance); err != nil {
		t.Fatal(err)
	}
	if instance.Status.PendingGateRelease == nil {
		t.Fatal("gate release intent was not persisted")
	}
	// Model a Pod and its durable intent from the same previous template.
	pod.Labels[podRevisionLabel] = "legacy-template"
	if _, err := core.CoreV1().Pods("default").Update(ctx, pod, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	instance.Status.PendingGateRelease.Pod.Revision = "legacy-template"
	if err := kube.Status().Update(ctx, instance); err != nil {
		t.Fatal(err)
	}
	r.DevelopmentS3WebIdentityImage = ""
	if _, err := r.reconcileGatedPod(ctx, instance); err != nil {
		t.Fatal(err)
	}
	r.DevelopmentS3WebIdentityImage = image
	pod, err = core.CoreV1().Pods("default").Get(ctx, name, metav1.GetOptions{})
	if err != nil || !hasPlacementGate(pod) {
		t.Fatalf("stale template changed the scheduling gate: %v", err)
	}
	if err := kube.Get(ctx, key, instance); err != nil {
		t.Fatal(err)
	}
	if !instance.Status.PendingGateRelease.WithdrawalRequested {
		t.Fatal("stale withdrawal decision was not persisted")
	}
	// Resume using a new reconciler after the decision was persisted.
	restarted := *r
	r = &restarted
	for i := 0; i < 10; i++ {
		step()
		if err := kube.Get(ctx, key, instance); err != nil {
			t.Fatal(err)
		}
		if instance.Status.PendingGateRelease == nil {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if instance.Status.PendingGateRelease != nil {
		t.Fatal("withdrawal did not finish")
	}
	if _, err := core.CoreV1().Pods("default").Get(ctx, name, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Fatalf("stale Pod still exists: %v", err)
	}
	for i := 0; i < 5; i++ {
		if instance.Status.PendingGateRelease != nil {
			break
		}
		step()
		if err := kube.Get(ctx, key, instance); err != nil {
			t.Fatal(err)
		}
	}
	if instance.Status.PendingGateRelease == nil {
		t.Fatal("fresh release intent was not prepared")
	}
	repository.Data["bucket"] = "changed-bucket"
	repository, err = core.CoreV1().ConfigMaps("default").Update(ctx, repository, metav1.UpdateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := r.reconcileGatedPod(ctx, instance); err == nil {
		t.Fatal("changed repository did not hold gate release")
	}
	stillHeld, err := core.CoreV1().Pods("default").Get(ctx, instance.Status.PendingGateRelease.Pod.Name, metav1.GetOptions{})
	if err != nil || !hasPlacementGate(stillHeld) {
		t.Fatalf("changed repository released the Pod: %v", err)
	}
	repository.Data["bucket"] = "bucket"
	if _, err := core.CoreV1().ConfigMaps("default").Update(ctx, repository, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 5; i++ {
		step()
	}
	if err := kube.Get(ctx, key, instance); err != nil {
		t.Fatal(err)
	}
	if instance.Status.ProvisioningAttempt == nil || instance.Status.ProvisioningAttempt.Name == name {
		t.Fatal("new Pod was not released after withdrawal")
	}
	if len(instance.Status.RecentPodReleases) != 2 {
		t.Fatal("withdrawal must preserve the original release reservation")
	}
	pod, err = core.CoreV1().Pods("default").Get(ctx, instance.Status.ProvisioningAttempt.Name, metav1.GetOptions{})
	if err != nil || hasPlacementGate(pod) {
		t.Fatalf("qualified Pod gate did not release: %v", err)
	}
}
