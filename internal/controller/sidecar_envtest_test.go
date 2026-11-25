//go:build envtest

package controller

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"

	"github.com/huyz0/opensearch-serverless-operator/api/v1alpha1"
)

func TestSidecarAdmissionWithAPIServerDefaults(t *testing.T) {
	env := &envtest.Environment{CRDDirectoryPaths: []string{filepath.Join("..", "..", "config", "generated-dev")}, ErrorIfCRDPathMissing: true}
	config, err := env.Start()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := env.Stop(); err != nil {
			t.Error(err)
		}
	})
	client, err := kubernetes.NewForConfig(config)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	instance := &v1alpha1.OpenSearchServerless{ObjectMeta: metav1.ObjectMeta{Name: "catalog", Namespace: "default", UID: "owner"}, Spec: v1alpha1.OpenSearchServerlessSpec{Image: "example.invalid/java:v1", Pools: map[string]v1alpha1.Pool{"search": {Zones: map[string]v1alpha1.Zone{"a": {Replicas: 1}}, Sidecars: []corev1.Container{{Name: "metrics", Image: "example.invalid/metrics:v1", Env: []corev1.EnvVar{{Name: "MODE", Value: "serve"}}, ReadinessProbe: &corev1.Probe{ProbeHandler: corev1.ProbeHandler{Exec: &corev1.ExecAction{Command: []string{"true"}}}}}}}}}, Status: v1alpha1.OpenSearchServerlessStatus{PrimaryStore: &v1alpha1.PrimaryStoreStatus{Bucket: "bucket", Region: "region"}}}
	pool := instance.Spec.Pools["search"]
	pool.JVMHeap = "512Mi"
	pool.Resources.Requests = corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("1"), corev1.ResourceMemory: resource.MustParse("1Gi")}
	pool.SidecarVolumes = []v1alpha1.SidecarVolume{{Name: "metrics-config", ConfigMapRef: &v1alpha1.LocalReference{Name: "metrics-config"}}, {Name: "scratch", EmptyDir: true, SizeLimit: "2Gi"}}
	pool.NativeSidecars = []corev1.Container{{Name: "native-metrics", Image: "example.invalid/native:v1", VolumeMounts: []corev1.VolumeMount{{Name: "scratch", MountPath: "/scratch"}}}}
	pool.Sidecars[0].VolumeMounts = []corev1.VolumeMount{{Name: "metrics-config", MountPath: "/config", ReadOnly: true}, {Name: "scratch", MountPath: "/scratch"}}
	instance.Spec.Pools["search"] = pool
	instance.Spec.Image = "example.invalid/java@sha256:" + strings.Repeat("a", 64)
	instance.Spec.ServiceAccountName = "runtime"
	instance.Spec.Storage = v1alpha1.Storage{Type: "s3", Bucket: "bucket", Region: "region"}
	scheme := runtime.NewScheme()
	if err := v1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	api, err := ctrlclient.New(config, ctrlclient.Options{Scheme: scheme})
	if err != nil {
		t.Fatal(err)
	}
	instance.UID = ""
	if err := api.Create(ctx, instance); err != nil {
		t.Fatal(err)
	}
	var fetched v1alpha1.OpenSearchServerless
	if err := api.Get(ctx, ctrlclient.ObjectKeyFromObject(instance), &fetched); err != nil {
		t.Fatal(err)
	}
	if len(fetched.Spec.Pools["search"].SidecarVolumes) != 2 || fetched.Spec.Pools["search"].SidecarVolumes[0].ConfigMapRef.Name != "metrics-config" || fetched.Spec.Pools["search"].SidecarVolumes[1].SizeLimit != "2Gi" {
		t.Fatal("sidecar volume did not survive the CRD API")
	}
	if len(fetched.Spec.Pools["search"].NativeSidecars) != 1 {
		t.Fatal("native sidecar did not survive the CRD API")
	}
	for _, test := range []struct {
		name    string
		volumes []v1alpha1.SidecarVolume
	}{
		{"missing-source", []v1alpha1.SidecarVolume{{Name: "config"}}},
		{"multiple-sources", []v1alpha1.SidecarVolume{{Name: "config", EmptyDir: true, SecretRef: &v1alpha1.LocalReference{Name: "credentials"}}}},
		{"reserved", []v1alpha1.SidecarVolume{{Name: "serverless-data", EmptyDir: true}}},
		{"invalid-name", []v1alpha1.SidecarVolume{{Name: "invalid.name", EmptyDir: true}}},
		{"duplicate", []v1alpha1.SidecarVolume{{Name: "config", EmptyDir: true}, {Name: "config", EmptyDir: true}}},
		{"limit-on-secret", []v1alpha1.SidecarVolume{{Name: "config", SecretRef: &v1alpha1.LocalReference{Name: "secret"}, SizeLimit: "1Gi"}}},
	} {
		t.Run(test.name, func(t *testing.T) {
			invalid := fetched.DeepCopyObject().(*v1alpha1.OpenSearchServerless)
			invalid.Name = test.name
			invalid.UID = ""
			invalid.ResourceVersion = ""
			invalidPool := invalid.Spec.Pools["search"]
			invalidPool.SidecarVolumes = test.volumes
			invalid.Spec.Pools["search"] = invalidPool
			if err := api.Create(ctx, invalid); !apierrors.IsInvalid(err) {
				t.Fatalf("expected invalid volume error, got %v", err)
			}
			if err := invalid.Spec.Validate(); err == nil {
				t.Fatal("controller validation accepted invalid volume")
			}
		})
	}
	instance.Status.PrimaryStore = &v1alpha1.PrimaryStoreStatus{Bucket: "bucket", Region: "region"}
	intent := &v1alpha1.PodCreationIntent{Name: "sidecar-test", Role: "search", Zone: "a", Fleet: "initial", Revision: "revision", Sequence: 1}
	desired, err := renderGatedPod(instance, intent, nil, "", "")
	if err != nil {
		t.Fatal(err)
	}
	observed, err := client.CoreV1().Pods("default").Create(ctx, desired, metav1.CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if observed.Spec.Containers[1].ReadinessProbe.PeriodSeconds == 0 {
		t.Fatal("API did not default the probe")
	}
	if observed.Spec.Volumes[1].EmptyDir == nil || observed.Spec.Volumes[1].EmptyDir.SizeLimit == nil || observed.Spec.Volumes[1].EmptyDir.SizeLimit.String() != "2Gi" {
		t.Fatal("emptyDir size limit lost during Pod admission")
	}
	if err := validateAdmittedGatedPod(desired, observed); err != nil {
		t.Fatalf("API defaulting rejected: %v", err)
	}
	if len(observed.Spec.InitContainers) != 1 || observed.Spec.InitContainers[0].RestartPolicy == nil || *observed.Spec.InitContainers[0].RestartPolicy != corev1.ContainerRestartPolicyAlways {
		t.Fatal("native sidecar restart policy missing")
	}
	removedNative := observed.DeepCopy()
	removedNative.Spec.InitContainers = nil
	if err := validateAdmittedGatedPod(desired, removedNative); err == nil {
		t.Fatal("missing native sidecar accepted")
	}
	changedNative := observed.DeepCopy()
	changedNative.Spec.InitContainers[0].Image = "other"
	if err := validateAdmittedGatedPod(desired, changedNative); err == nil {
		t.Fatal("changed native sidecar accepted")
	}
	changedVolume := observed.DeepCopy()
	changedVolume.Spec.Volumes[0].ConfigMap.Name = "different"
	if err := validateAdmittedGatedPod(desired, changedVolume); err == nil {
		t.Fatal("changed sidecar volume accepted")
	}
	stolenVolume := observed.DeepCopy()
	stolenVolume.Spec.Containers[0].VolumeMounts = append(stolenVolume.Spec.Containers[0].VolumeMounts, corev1.VolumeMount{Name: "metrics-config", MountPath: "/stolen"})
	if err := validateAdmittedGatedPod(desired, stolenVolume); err == nil {
		t.Fatal("sidecar volume exposed to Java")
	}
	observed.Spec.Containers[1].Env[0].Value = "different"
	if err := validateAdmittedGatedPod(desired, observed); err == nil {
		t.Fatal("changed sidecar environment accepted")
	}
}
