package controller

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	ctrl "sigs.k8s.io/controller-runtime"
	clientfake "sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/huyz0/opensearch-serverless-operator/api/v1alpha1"
	"github.com/huyz0/opensearch-serverless-operator/internal/workload"
)

func TestPodRevisionSeparatesPriorTemplate(t *testing.T) {
	instance := &v1alpha1.OpenSearchServerless{Spec: v1alpha1.OpenSearchServerlessSpec{Image: "image", Pools: map[string]v1alpha1.Pool{"search": {CapacityProfileRef: "profile"}}}}
	encoded, err := json.Marshal(instance.Spec)
	if err != nil {
		t.Fatal(err)
	}
	legacy := fmt.Sprintf("%x", sha256.Sum256(encoded))[:16]
	current, err := podRevision(instance, "")
	if err != nil {
		t.Fatal(err)
	}
	if current == legacy {
		t.Fatal("old Pod template shares the current revision")
	}
	again, err := podRevision(instance, "")
	if err != nil || current != again {
		t.Fatal("current Pod revision is not deterministic")
	}
	withDNS, err := podRevision(instance, "cluster.local")
	if err != nil || withDNS == current {
		t.Fatal("peer DNS must still affect the revision")
	}
}

func TestLegacyPodTemplateCannotReleaseGate(t *testing.T) {
	instance := &v1alpha1.OpenSearchServerless{ObjectMeta: metav1.ObjectMeta{Name: "catalog", Namespace: "n", UID: "instance", Generation: 1, ResourceVersion: "1"}, Status: v1alpha1.OpenSearchServerlessStatus{PodCreationSequence: 1, ServingFleetIntentByRole: map[string]string{"search": "initial"}}}
	current, err := podRevision(instance, "")
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := json.Marshal(instance.Spec)
	if err != nil {
		t.Fatal(err)
	}
	legacy := fmt.Sprintf("%x", sha256.Sum256(encoded))[:16]
	intent := v1alpha1.PodCreationIntent{Name: podNameForSequence(instance, 1), Role: "search", Zone: "a", Fleet: "initial", Revision: legacy, Generation: 1, Sequence: 1}
	pod := podWithRecordedCreation(instance, intent)
	pod.Spec.SchedulingGates = []corev1.PodSchedulingGate{{Name: "serverless.opensearch.org/placement"}}
	instance.Status.PendingGateRelease = &v1alpha1.GateReleaseIntent{PodUID: string(pod.UID), Pod: intent, StartedAt: metav1.Now()}
	scheme := runtime.NewScheme()
	if err := v1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	kube := clientfake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(instance).WithObjects(instance.DeepCopyObject().(*v1alpha1.OpenSearchServerless)).Build()
	r := &InstanceReconciler{Client: kube, Core: fake.NewSimpleClientset(pod).CoreV1(), DevelopmentReleaseGates: true, DevelopmentPendingTimeout: time.Minute}
	if _, err := r.reconcileGateRelease(context.Background(), instance, current); err != nil {
		t.Fatal(err)
	}
	if !instance.Status.PendingGateRelease.WithdrawalRequested {
		t.Fatal("legacy template withdrawal intent was not recorded")
	}
	stillHeld, err := r.Core.Pods("n").Get(context.Background(), pod.Name, metav1.GetOptions{})
	if err != nil || !hasPlacementGate(stillHeld) {
		t.Fatal("legacy Pod gate was changed")
	}
}

func TestWithdrawalRecoversAlreadyReleasedGate(t *testing.T) {
	instance := &v1alpha1.OpenSearchServerless{ObjectMeta: metav1.ObjectMeta{Name: "catalog", Namespace: "n", UID: "instance", ResourceVersion: "1"}}
	intent := v1alpha1.PodCreationIntent{Name: podNameForSequence(instance, 1), Role: "search", Zone: "a", Fleet: "initial", Revision: "recorded", Sequence: 1}
	pod := podWithRecordedCreation(instance, intent)
	pod.Spec.NodeName = "node-a"
	instance.Status.PodCreationSequence = 1
	if err := appendRetiredCreation(instance, &intent); err != nil {
		t.Fatal(err)
	}
	instance.Status.PendingGateRelease = &v1alpha1.GateReleaseIntent{PodUID: string(pod.UID), Pod: intent, StartedAt: metav1.Now(), WithdrawalRequested: true}
	scheme := runtime.NewScheme()
	if err := v1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name   string
		mutate func(*corev1.Pod)
	}{
		{name: "identical Pod"},
		{name: "different UID", mutate: func(pod *corev1.Pod) { pod.UID = "replacement" }},
		{name: "changed provenance", mutate: func(pod *corev1.Pod) { pod.Annotations[creationIntentKey] = "instance/2" }},
		{name: "changed revision", mutate: func(pod *corev1.Pod) { pod.Labels[podRevisionLabel] = "other" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			observation := pod.DeepCopy()
			if tc.mutate != nil {
				tc.mutate(observation)
			}
			live := instance.DeepCopy()
			kube := clientfake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(live).WithObjects(live).Build()
			core := fake.NewSimpleClientset(observation)
			r := &InstanceReconciler{Client: kube, Core: core.CoreV1()}
			_, err := r.reconcileGateRelease(context.Background(), live, "")
			attempt := live.Status.ProvisioningAttempt
			if tc.mutate == nil {
				if err != nil || live.Status.PendingGateRelease != nil || attempt == nil || attempt.PodUID != string(pod.UID) {
					t.Fatalf("released Pod must be tracked as a provisioning attempt: %v", err)
				}
			} else if err == nil || attempt != nil || live.Status.PendingGateRelease == nil {
				t.Fatalf("changed Pod consumed the original release intent: %v", err)
			}
			if len(core.Actions()) != 1 || core.Actions()[0].GetVerb() != "get" {
				t.Fatal("gate recovery must only observe the Pod")
			}
		})
	}
}

func podWithRecordedCreation(instance *v1alpha1.OpenSearchServerless, intent v1alpha1.PodCreationIntent) *corev1.Pod {
	return &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
		Name: intent.Name, Namespace: instance.Namespace, UID: "pod-uid", ResourceVersion: "1",
		OwnerReferences: []metav1.OwnerReference{*metav1.NewControllerRef(instance, v1alpha1.GroupVersion.WithKind("OpenSearchServerless"))},
		Annotations:     map[string]string{creationIntentKey: fmt.Sprintf("%s/%d", instance.UID, intent.Sequence)},
		Labels: map[string]string{workload.InstanceLabel: string(instance.UID), workload.RoleLabel: intent.Role,
			workload.ZoneLabel: intent.Zone, workload.FleetLabel: intent.Fleet, podRevisionLabel: intent.Revision},
	}}
}

func TestAdmittedPodMustPreserveSafetyInvariants(t *testing.T) {
	instance := &v1alpha1.OpenSearchServerless{ObjectMeta: metav1.ObjectMeta{Name: "catalog", Namespace: "n", UID: "owner"}, Spec: v1alpha1.OpenSearchServerlessSpec{
		Image: "image", ServiceAccountName: "runtime", Pools: map[string]v1alpha1.Pool{"search": {JVMHeap: "512Mi", ProtectFromScaleDown: true, Zones: map[string]v1alpha1.Zone{"a": {Replicas: 1}}, Resources: corev1.ResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("1"), corev1.ResourceMemory: resource.MustParse("1Gi")}}}},
	}, Status: v1alpha1.OpenSearchServerlessStatus{PrimaryStore: &v1alpha1.PrimaryStoreStatus{Bucket: "bucket", Region: "region"}}}
	intent := &v1alpha1.PodCreationIntent{Name: "pod", Role: "search", Zone: "a", Fleet: "initial", Revision: "revision", Sequence: 1}
	want, err := renderGatedPod(instance, intent, nil, "", "")
	if err != nil {
		t.Fatal(err)
	}
	pod := want.DeepCopy()
	pod.UID = "pod-uid"
	pod.Spec.RestartPolicy = corev1.RestartPolicyAlways // API default
	if err := validateAdmittedGatedPod(want, pod); err != nil {
		t.Fatalf("valid admission rejected: %v", err)
	}
	withSidecar := want.DeepCopy()
	withSidecar.Spec.Containers = append(withSidecar.Spec.Containers, corev1.Container{Name: "metrics", Image: "metrics:v1", Args: []string{"serve"}, Resources: corev1.ResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("100m")}}})
	withSidecar.Spec.Containers[1].Env = []corev1.EnvVar{{Name: "MODE", Value: "metrics"}}
	withSidecar.Spec.Containers[1].ReadinessProbe = &corev1.Probe{ProbeHandler: corev1.ProbeHandler{Exec: &corev1.ExecAction{Command: []string{"ready"}}}}
	withSidecar.Spec.Containers[1].Resources.Limits = corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("128Mi")}
	admitted := withSidecar.DeepCopy()
	admitted.UID = "pod-uid"
	admitted.Spec.Containers[1].ReadinessProbe.PeriodSeconds = 10 // API default
	admitted.Spec.Containers = append(admitted.Spec.Containers, corev1.Container{Name: "injected", Image: "injected"})
	if err := validateAdmittedGatedPod(withSidecar, admitted); err != nil {
		t.Fatalf("admission addition rejected: %v", err)
	}
	for _, scenario := range []struct {
		name   string
		mutate func(*corev1.Pod)
	}{
		{"removed", func(p *corev1.Pod) { p.Spec.Containers = p.Spec.Containers[:1] }},
		{"image", func(p *corev1.Pod) { p.Spec.Containers[1].Image = "other" }},
		{"args", func(p *corev1.Pod) { p.Spec.Containers[1].Args = []string{"other"} }},
		{"environment", func(p *corev1.Pod) { p.Spec.Containers[1].Env[0].Value = "other" }},
		{"readiness", func(p *corev1.Pod) { p.Spec.Containers[1].ReadinessProbe.Exec.Command = []string{"false"} }},
		{"limit", func(p *corev1.Pod) {
			p.Spec.Containers[1].Resources.Limits[corev1.ResourceMemory] = resource.MustParse("64Mi")
		}},
		{"request", func(p *corev1.Pod) {
			p.Spec.Containers[1].Resources.Requests[corev1.ResourceCPU] = resource.MustParse("1m")
		}},
	} {
		t.Run("sidecar "+scenario.name, func(t *testing.T) {
			changed := admitted.DeepCopy()
			scenario.mutate(changed)
			if err := validateAdmittedGatedPod(withSidecar, changed); err == nil {
				t.Fatal("changed requested sidecar accepted")
			}
		})
	}
	dnsIntent := *intent
	dnsIntent.PeerDNS = true
	dnsPod, err := renderGatedPod(instance, &dnsIntent, nil, "cluster.local", "")
	if err != nil {
		t.Fatal(err)
	}
	if dnsPod.Annotations[peerDomainAnnotation] != "cluster.local" {
		t.Fatal("peer domain not recorded")
	}
	dnsPod.UID = "dns-pod"
	if err := validateAdmittedGatedPod(dnsPod, dnsPod.DeepCopy()); err != nil {
		t.Fatal(err)
	}
	t.Run("keystore without Basic Auth", func(t *testing.T) {
		configured := instance.DeepCopy()
		configured.Spec.Storage.Credentials = &v1alpha1.StorageCredentials{Type: "Keystore", KeystoreSecretRef: &v1alpha1.LocalReference{Name: "runtime"}}
		intent := dnsIntent
		intent.KeystoreUID = "secret-uid"
		pod, err := renderGatedPod(configured, &intent, nil, "cluster.local", "")
		if err != nil {
			t.Fatal(err)
		}
		pod.UID = "keystore-pod"
		if err := validateAdmittedGatedPod(pod, pod.DeepCopy()); err != nil {
			t.Fatalf("valid keystore Pod rejected: %v", err)
		}
		for _, test := range []struct {
			name   string
			mutate func(*corev1.Pod)
		}{
			{"sidecar environment alias", func(p *corev1.Pod) {
				p.Spec.Containers = append(p.Spec.Containers, corev1.Container{Name: "injected", Image: "injected", EnvFrom: []corev1.EnvFromSource{{SecretRef: &corev1.SecretEnvSource{LocalObjectReference: corev1.LocalObjectReference{Name: "runtime"}}}}})
			}},
			{"shadowed keystore file", func(p *corev1.Pod) {
				p.Spec.Containers[0].VolumeMounts = append(p.Spec.Containers[0].VolumeMounts, corev1.VolumeMount{Name: "shadow", MountPath: "/var/lib/serverless/./config/opensearch.keystore"})
			}},
			{"extra keystore mount", func(p *corev1.Pod) {
				p.Spec.Containers[0].VolumeMounts = append(p.Spec.Containers[0].VolumeMounts, corev1.VolumeMount{Name: keystoreVolumeName, MountPath: "/extra", ReadOnly: true})
			}},
		} {
			t.Run(test.name, func(t *testing.T) {
				observed := pod.DeepCopy()
				test.mutate(observed)
				if err := validateAdmittedGatedPod(pod, observed); err == nil {
					t.Fatal("invalid keystore access passed admission checks")
				}
			})
		}
	})
	t.Run("managed Basic Auth", func(t *testing.T) {
		configured := instance.DeepCopyObject().(*v1alpha1.OpenSearchServerless)
		configured.Spec.TLS = &v1alpha1.TLSConfig{HTTP: &v1alpha1.TLSChannel{Source: "Secret", CertificateRef: v1alpha1.LocalReference{Name: "leaf"}, TrustBundleRef: v1alpha1.SecretKeyReference{Name: "roots", Key: "ca.crt"}}}
		configured.Spec.Security = &v1alpha1.SecurityConfig{Provider: serverlessBasicAuth, AuthenticationSecretRef: &v1alpha1.SecretKeyReference{Name: "runtime", Key: "password"}}
		configured.Spec.Storage.Credentials = &v1alpha1.StorageCredentials{Type: "Keystore", KeystoreSecretRef: &v1alpha1.LocalReference{Name: "runtime"}}
		configured.Status.HTTPTLSBaseline = &v1alpha1.HTTPTLSBaseline{Domain: "cluster.local", CertificateName: "leaf", CertificateUID: "leaf-uid", TrustName: "roots", TrustUID: "roots-uid", TrustKey: "ca.crt", CertificateHash: strings.Repeat("a", 64), TrustHash: strings.Repeat("b", 64)}
		configured.Status.BasicAuthBaseline = &v1alpha1.BasicAuthBaseline{SecretName: "runtime", SecretUID: "secret-uid", Key: "password", BundleSHA256: strings.Repeat("c", 64)}
		authIntent := dnsIntent
		authIntent.KeystoreUID = "secret-uid"
		pod, err := renderGatedPod(configured, &authIntent, nil, "cluster.local", "")
		if err != nil {
			t.Fatal(err)
		}
		container := pod.Spec.Containers[0]
		if !slices.Contains(container.ReadinessProbe.Exec.Command, "/var/lib/serverless/config/probe-password") || !slices.ContainsFunc(container.Args, func(arg string) bool {
			return strings.HasPrefix(arg, "-Dserverless.plugins=") && strings.Contains(arg, "org.opensearch.serverless.auth.ServerlessAuthPlugin")
		}) {
			t.Fatal("rendered Pod did not load the provider and its authenticated exec probe")
		}
		pod.UID = "auth-pod"
		if err := validateAdmittedGatedPod(pod, pod.DeepCopy()); err != nil {
			t.Fatalf("expected Basic Auth Pod rejected: %v", err)
		}
		altered := pod.DeepCopy()
		altered.Spec.Containers = append(altered.Spec.Containers, corev1.Container{Name: "injected", Image: "injected", EnvFrom: []corev1.EnvFromSource{{SecretRef: &corev1.SecretEnvSource{LocalObjectReference: corev1.LocalObjectReference{Name: "runtime"}}}}})
		if err := validateAdmittedGatedPod(pod, altered); err == nil {
			t.Fatal("admission let an injected sidecar read the bootstrap Secret")
		}
		altered = pod.DeepCopy()
		altered.Spec.Containers[0].VolumeMounts = append(altered.Spec.Containers[0].VolumeMounts, corev1.VolumeMount{Name: "shadow", MountPath: "/var/lib/serverless/./config/probe-password"})
		if err := validateAdmittedGatedPod(pod, altered); err == nil {
			t.Fatal("admission let an injected mount shadow a credential file")
		}
	})
	withNative := instance.DeepCopyObject().(*v1alpha1.OpenSearchServerless)
	poolWithNative := withNative.Spec.Pools["search"]
	poolWithNative.NativeSidecars = []corev1.Container{{Name: "native", Image: "native-image"}}
	withNative.Spec.Pools["search"] = poolWithNative
	ordered, err := renderGatedPod(withNative, &dnsIntent, nil, "cluster.local", "")
	if err != nil {
		t.Fatal(err)
	}
	ordered.UID = "ordered-pod"
	if err := validateAdmittedGatedPod(ordered, ordered.DeepCopy()); err != nil {
		t.Fatalf("expected init order rejected: %v", err)
	}
	reordered := ordered.DeepCopy()
	reordered.Spec.InitContainers[0], reordered.Spec.InitContainers[1] = reordered.Spec.InitContainers[1], reordered.Spec.InitContainers[0]
	if err := validateAdmittedGatedPod(ordered, reordered); err == nil {
		t.Fatal("native sidecar reordered after DNS preflight")
	}
	added := ordered.DeepCopy()
	added.Spec.InitContainers = append([]corev1.Container{{Name: "injected", Image: "injected"}}, added.Spec.InitContainers...)
	if err := validateAdmittedGatedPod(ordered, added); err != nil {
		t.Fatalf("preserved init order rejected: %v", err)
	}
	withoutPreflight := dnsPod.DeepCopy()
	withoutPreflight.Spec.InitContainers = nil
	if err := validateAdmittedGatedPod(dnsPod, withoutPreflight); err == nil {
		t.Fatal("missing DNS preflight accepted")
	}
	modifiedPreflight := dnsPod.DeepCopy()
	modifiedPreflight.Spec.InitContainers[0].Args = []string{"-wait-peer-dns", "wrong.example"}
	if err := validateAdmittedGatedPod(dnsPod, modifiedPreflight); err == nil {
		t.Fatal("modified DNS preflight accepted")
	}
	if _, err := renderGatedPod(instance, &dnsIntent, nil, "bad domain", ""); err == nil {
		t.Fatal("invalid peer domain accepted")
	}
	r := &InstanceReconciler{DevelopmentPeerDNSImage: "image", DevelopmentClusterDomain: "cluster.local"}
	if _, err := r.peerDNSDomain(instance); err != nil {
		t.Fatal(err)
	}
	r.DevelopmentPeerDNSImage = "other"
	if _, err := r.peerDNSDomain(instance); err == nil {
		t.Fatal("unqualified image accepted")
	}
	if validDNSHost(strings.Repeat("a", 64) + ".local") {
		t.Fatal("overlong DNS label accepted")
	}
	for _, scenario := range []struct {
		name   string
		mutate func(*corev1.Pod)
	}{
		{"removed gate", func(p *corev1.Pod) { p.Spec.SchedulingGates = nil }},
		{"wrong zone", func(p *corev1.Pod) { p.Spec.NodeSelector[corev1.LabelTopologyZone] = "other" }},
		{"wrong fleet", func(p *corev1.Pod) { p.Labels["serverless.opensearch.org/fleet"] = "other" }},
		{"owner name with same UID", func(p *corev1.Pod) { p.OwnerReferences[0].Name = "other" }},
		{"owner API with same UID", func(p *corev1.Pod) { p.OwnerReferences[0].APIVersion = "v1" }},
		{"host network", func(p *corev1.Pod) { p.Spec.HostNetwork = true }},
		{"runtime restart policy", func(p *corev1.Pod) { p.Spec.RestartPolicy = corev1.RestartPolicyNever }},
		{"wrong hostname", func(p *corev1.Pod) { p.Spec.Hostname = "other" }},
		{"wrong subdomain", func(p *corev1.Pod) { p.Spec.Subdomain = "other" }},
		{"implicit token mount", func(p *corev1.Pod) { p.Spec.AutomountServiceAccountToken = nil }},
		{"enabled token mount", func(p *corev1.Pod) { enabled := true; p.Spec.AutomountServiceAccountToken = &enabled }},
		{"modified arguments", func(p *corev1.Pod) { p.Spec.Containers[0].Args = []string{"-Dserverless.roles=ingest"} }},
		{"injected Java environment", func(p *corev1.Pod) {
			p.Spec.Containers[0].EnvFrom = []corev1.EnvFromSource{{ConfigMapRef: &corev1.ConfigMapEnvSource{LocalObjectReference: corev1.LocalObjectReference{Name: "java-options"}}}}
		}},
		{"changed image", func(p *corev1.Pod) { p.Spec.Containers[0].Image = "other" }},
		{"readiness failure threshold", func(p *corev1.Pod) { p.Spec.Containers[0].ReadinessProbe.FailureThreshold = 100 }},
		{"readiness period", func(p *corev1.Pod) { p.Spec.Containers[0].ReadinessProbe.PeriodSeconds = 300 }},
		{"readiness timeout", func(p *corev1.Pod) { p.Spec.Containers[0].ReadinessProbe.TimeoutSeconds = 100 }},
		{"readiness initial delay", func(p *corev1.Pod) { p.Spec.Containers[0].ReadinessProbe.InitialDelaySeconds = 100 }},
		{"lost autoscaler protection", func(p *corev1.Pod) { delete(p.Annotations, "cluster-autoscaler.kubernetes.io/safe-to-evict") }},
		{"weaker resources", func(p *corev1.Pod) {
			p.Spec.Containers[0].Resources.Requests[corev1.ResourceMemory] = resource.MustParse("1Mi")
		}},
		{"sidecar data mount", func(p *corev1.Pod) {
			p.Spec.Containers = append(p.Spec.Containers, corev1.Container{Name: "sidecar", VolumeMounts: []corev1.VolumeMount{{Name: "serverless-data", MountPath: "/data"}}})
		}},
		{"injected runtime data child mount", func(p *corev1.Pod) {
			p.Spec.Containers[0].VolumeMounts = append(p.Spec.Containers[0].VolumeMounts, corev1.VolumeMount{Name: "injected", MountPath: "/var/lib/serverless/data"})
		}},
		{"injected runtime data parent mount", func(p *corev1.Pod) {
			p.Spec.Containers[0].VolumeMounts = append(p.Spec.Containers[0].VolumeMounts, corev1.VolumeMount{Name: "injected", MountPath: "/var/lib"})
		}},
		{"runtime data memory backing", func(p *corev1.Pod) {
			for i := range p.Spec.Volumes {
				if p.Spec.Volumes[i].Name == "serverless-data" {
					p.Spec.Volumes[i].EmptyDir.Medium = corev1.StorageMediumMemory
				}
			}
		}},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			changed := pod.DeepCopy()
			scenario.mutate(changed)
			if err := validateAdmittedGatedPod(want, changed); err == nil {
				t.Fatal("invalid admission accepted")
			}
		})
	}
	t.Run("Java resources after admission defaults", func(t *testing.T) {
		requested := want.DeepCopy()
		requested.Spec.Containers[0].Resources.Requests[corev1.ResourceMemory] = resource.MustParse("256Mi")
		requested.Spec.Containers[0].Resources.Limits = corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("2")}
		observed := requested.DeepCopy()
		observed.UID = "pod-uid"
		limits := observed.Spec.Containers[0].Resources.Limits
		limits[corev1.ResourceMemory] = resource.MustParse("768Mi")
		if err := validateAdmittedGatedPod(requested, observed); err != nil {
			t.Fatalf("compatible default memory limit rejected: %v", err)
		}
		limits[corev1.ResourceMemory] = resource.MustParse("512Mi")
		if err := validateAdmittedGatedPod(requested, observed); err == nil {
			t.Fatal("default memory limit allowed no room outside the Java heap")
		}
		delete(limits, corev1.ResourceMemory)
		limits[corev1.ResourceCPU] = resource.MustParse("1")
		if err := validateAdmittedGatedPod(requested, observed); err == nil {
			t.Fatal("admission silently lowered a configured CPU limit")
		}
	})
	pool := instance.Spec.Pools["search"]
	pool.WarmOnJoin = true
	instance.Spec.Pools["search"] = pool
	warmPod, err := renderGatedPod(instance, intent, nil, "", "")
	if err != nil || !slices.Contains(warmPod.Spec.Containers[0].Args, "-Dserverless.search.warm_on_join=true") {
		t.Fatalf("warm-on-join setting not rendered: %v", err)
	}
	pool.WarmReadiness = "Strict"
	pool.ColdStartPolicy = "AllowOneDegraded"
	instance.Spec.Pools["search"] = pool
	bootstrap := *intent
	bootstrap.WarmupMode = "Bootstrap"
	bootstrapPod, err := renderGatedPod(instance, &bootstrap, nil, "", "")
	if err != nil || slices.Contains(bootstrapPod.Spec.Containers[0].Args, "-Dserverless.search.require_warmup_completion=true") || slices.Contains(bootstrapPod.Spec.Containers[0].ReadinessProbe.Exec.Command, "-require-warmup-completion") {
		t.Fatalf("bootstrap did not use explicitly degraded mode: %v", err)
	}
	strict := *intent
	strict.WarmupMode = "Strict"
	strictPod, err := renderGatedPod(instance, &strict, nil, "", "")
	if err != nil || !slices.Contains(strictPod.Spec.Containers[0].Args, "-Dserverless.search.require_warmup_completion=true") || !slices.Contains(strictPod.Spec.Containers[0].ReadinessProbe.Exec.Command, "-require-warmup-completion") {
		t.Fatalf("strict gates missing: %v", err)
	}
	strictPod.UID = "strict-uid"
	changed := strictPod.DeepCopy()
	changed.Annotations[warmupModeAnnotation] = "Bootstrap"
	if err := validateAdmittedGatedPod(strictPod, changed); err == nil {
		t.Fatal("changed warm-up mode accepted")
	}
}

func TestWebIdentityPodRenderAndAdmission(t *testing.T) {
	instance := &v1alpha1.OpenSearchServerless{ObjectMeta: metav1.ObjectMeta{Name: "catalog", Namespace: "n", UID: "owner"}, Spec: v1alpha1.OpenSearchServerlessSpec{
		Image: "image", ServiceAccountName: "runtime", Storage: v1alpha1.Storage{Credentials: &v1alpha1.StorageCredentials{Type: "WorkloadIdentity", AWSRoleARN: "arn:aws:iam::123456789012:role/store"}}, Pools: map[string]v1alpha1.Pool{"search": {JVMHeap: "512Mi", Zones: map[string]v1alpha1.Zone{"a": {Replicas: 1}}, Resources: corev1.ResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("1"), corev1.ResourceMemory: resource.MustParse("1Gi")}}}},
	}, Status: v1alpha1.OpenSearchServerlessStatus{PrimaryStore: &v1alpha1.PrimaryStoreStatus{Bucket: "bucket", Region: "region"}}}
	intent := &v1alpha1.PodCreationIntent{Name: "pod", Role: "search", Zone: "a", Fleet: "initial", Revision: "revision", Sequence: 1}
	want, err := renderGatedPod(instance, intent, nil, "", "")
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Contains(want.Spec.Containers[0].Args, "-Ds3.client.default.identity_token_file=/var/run/serverless/s3-identity/token") {
		t.Fatal("missing Java token setting")
	}
	observed := want.DeepCopy()
	observed.UID = "pod-uid"
	if err := validateAdmittedGatedPod(want, observed); err != nil {
		t.Fatal(err)
	}
	observed.Spec.Containers = append(observed.Spec.Containers, corev1.Container{Name: "attacker", VolumeMounts: []corev1.VolumeMount{{Name: "serverless-s3-web-identity", MountPath: "/stolen"}}})
	if err := validateAdmittedGatedPod(want, observed); err == nil {
		t.Fatal("admission must reject sidecar access")
	}
}

// Reconcile both provisioning stages without the instance-wide prerequisites.
func (r *InstanceReconciler) reconcileGatedPod(ctx context.Context, instance *v1alpha1.OpenSearchServerless) (ctrl.Result, error) {
	if result, handled, err := r.recoverGatedPreflight(ctx, instance); handled || err != nil {
		return result, err
	}
	return r.advanceGatedPod(ctx, instance)
}
