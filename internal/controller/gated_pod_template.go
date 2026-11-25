package controller

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/huyz0/opensearch-serverless-operator/api/v1alpha1"
	"github.com/huyz0/opensearch-serverless-operator/internal/placement"
	"github.com/huyz0/opensearch-serverless-operator/internal/runtimeconfig"
	"github.com/huyz0/opensearch-serverless-operator/internal/storage"
	"github.com/huyz0/opensearch-serverless-operator/internal/workload"
)

func podNameForSequence(instance *v1alpha1.OpenSearchServerless, sequence int64) string {
	nameHash := sha256.Sum256([]byte(fmt.Sprintf("%s/%d", instance.UID, sequence)))
	prefix := strings.TrimRight(instance.Name, "-.")
	if len(prefix) > 40 {
		prefix = strings.TrimRight(prefix[:40], "-.")
	}
	return fmt.Sprintf("%s-%x", prefix, nameHash[:8])
}

func hasPlacementGate(pod *corev1.Pod) bool {
	for _, gate := range pod.Spec.SchedulingGates {
		if gate.Name == "serverless.opensearch.org/placement" {
			return true
		}
	}
	return false
}

func recoverCreationIntent(instance *v1alpha1.OpenSearchServerless, pod *corev1.Pod, revision, keystoreUID string, profile *placement.Resolved) (*v1alpha1.PodCreationIntent, error) {
	ownerUID, rawSequence, ok := strings.Cut(pod.Annotations[creationIntentKey], "/")
	sequence, err := strconv.ParseInt(rawSequence, 10, 64)
	if !ok || err != nil || sequence <= 0 || sequence > instance.Status.PodCreationSequence || ownerUID != string(instance.UID) || pod.Name != podNameForSequence(instance, sequence) || pod.Labels["serverless.opensearch.org/revision"] != revision || pod.Annotations[keystoreUIDAnnotation] != keystoreUID {
		return nil, fmt.Errorf("Pod creation identity or revision cannot be recovered")
	}
	role := pod.Labels[workload.RoleLabel]
	fleet := pod.Labels[workload.FleetLabel]
	if !fleetAllowedForPod(instance, role, pod.Labels[workload.ZoneLabel], fleet) {
		return nil, fmt.Errorf("Pod fleet differs from recorded serving intent")
	}
	intent := &v1alpha1.PodCreationIntent{Name: pod.Name, Role: role, Zone: pod.Labels[workload.ZoneLabel], Fleet: fleet, Revision: revision, Generation: instance.Generation, Sequence: sequence, WarmupMode: pod.Annotations[warmupModeAnnotation], KeystoreUID: keystoreUID, CapacityProfileUID: pod.Annotations[profileUIDAnnotation], CapacityProfileVersion: pod.Annotations[profileVersionAnnotation], PeerDNS: pod.Annotations[peerDomainAnnotation] != "", PeerTLSLeafUID: pod.Annotations[peerTLSLeafUIDAnnotation], PeerTLSTrustUID: pod.Annotations[peerTLSTrustUIDAnnotation], PeerTLSSnapshotName: pod.Annotations[peerTLSSnapshotNameAnnotation], PeerTLSSnapshotUID: pod.Annotations[peerTLSSnapshotUIDAnnotation]}
	intent.EmergencyEvidenceUID, intent.EmergencyEvidenceVersion = pod.Annotations[emergencyEvidenceUIDAnnotation], pod.Annotations[emergencyEvidenceVersionAnnotation]
	intent.BootstrapReplacementUID = pod.Annotations[bootstrapReplacementAnnotation]
	intent.PluginInstall, err = pluginIntentFromPod(pod)
	if err != nil {
		return nil, err
	}
	if baseline := peerTLSBaselineForFleet(instance, fleet); baseline != nil && intent.PeerTLSLeafUID != "" {
		intent.PeerTLSLeafSHA256 = baseline.LeafSHA256
		intent.PeerTLSTrustSHA256 = baseline.TrustSHA256
	}
	if err := checkProfileIdentity(intent, profile); err != nil {
		return nil, err
	}
	return intent, nil
}

func renderGatedPod(instance *v1alpha1.OpenSearchServerless, intent *v1alpha1.PodCreationIntent, profile *placement.Resolved, domain, qualifiedRuntimeImage string) (*corev1.Pod, error) {
	if intent == nil || intent.PeerDNS != (domain != "") {
		return nil, fmt.Errorf("peer DNS mode differs from recorded Pod intent")
	}
	if config, found := instance.Status.RuntimeByFleet[intent.Fleet]; found {
		view := *instance
		applyFleetRuntime(&view, config)
		instance = &view
	} else if candidate := instance.Status.CandidateFleet; candidate != nil && candidate.Fleet == intent.Fleet && candidate.RuntimeOverride != nil {
		return nil, fmt.Errorf("candidate runtime settings pin is missing")
	}
	pool, exists := instance.Spec.Pools[intent.Role]
	if !exists || instance.Status.PrimaryStore == nil {
		return nil, fmt.Errorf("role or pinned store is missing")
	}
	strict := intent.Role == "search" && pool.WarmReadiness == "Strict"
	if strict && (pool.ColdStartPolicy != "AllowOneDegraded" || (intent.WarmupMode != "Strict" && intent.WarmupMode != "Bootstrap")) {
		return nil, fmt.Errorf("strict search requires an explicit bootstrap policy and persisted warm-up mode")
	}
	if !strict && intent.WarmupMode != "" {
		return nil, fmt.Errorf("unexpected warm-up mode")
	}
	for _, name := range []corev1.ResourceName{corev1.ResourceCPU, corev1.ResourceMemory} {
		quantity, ok := pool.Resources.Requests[name]
		if !ok || quantity.Sign() <= 0 {
			return nil, fmt.Errorf("positive %s request is required for development Pods", name)
		}
	}
	store := instance.Status.PrimaryStore
	webIdentity := instance.Spec.Storage.Credentials != nil && instance.Spec.Storage.Credentials.Type == "WorkloadIdentity"
	peerService, err := workload.PeerDNSService(instance)
	if err != nil {
		return nil, err
	}
	peerHost, err := workload.PeerHostname(intent.Name)
	if err != nil {
		return nil, err
	}
	advertiseHost := ""
	if domain != "" {
		advertiseHost = fmt.Sprintf("%s.%s.%s.svc.%s", peerHost, peerService.Name, instance.Namespace, domain)
		if !validDNSHost(advertiseHost) {
			return nil, fmt.Errorf("invalid peer DNS identity")
		}
	}
	var httpHash string
	httpPins := httpTLSBaselineForFleet(instance, intent.Fleet)
	authPins := instance.Status.BasicAuthBaseline
	authRef, err := basicAuthReference(instance)
	if err != nil {
		return nil, err
	}
	if (authRef == nil) != (authPins == nil) || (authPins != nil && (httpPins == nil || authPins.SecretUID != intent.KeystoreUID)) {
		return nil, fmt.Errorf("Basic Auth requires pinned HTTPS and the intended keystore identity")
	}
	httpConfigured := instance.Spec.TLS != nil && instance.Spec.TLS.HTTP != nil
	if httpConfigured != (httpPins != nil) {
		return nil, fmt.Errorf("HTTP TLS configuration requires the pinned baseline")
	}
	if httpPins != nil {
		if advertiseHost == "" || !httpTLSBaseMatches(instance, domain) ||
			httpPins.CertificateUID == "" || httpPins.TrustUID == "" || httpPins.CertificateHash == "" || httpPins.TrustHash == "" {
			return nil, fmt.Errorf("HTTP TLS baseline does not match the Pod template")
		}
		httpHash = httpPins.CertificateHash
	}
	pod, err := workload.RuntimePod(instance, workload.PodInput{Name: intent.Name, Role: intent.Role, Zone: intent.Zone, Fleet: intent.Fleet, Revision: intent.Revision, IntentKey: creationIntentKey, Intent: fmt.Sprintf("%s/%d", instance.UID, intent.Sequence), Command: []string{"/bin/sh", "/opt/serverless/bin/entrypoint"}, ServingReadinessPort: 9200, WarmupMode: intent.WarmupMode}, runtimeconfig.ManagedInputs{
		NodeName: intent.Name, ClusterName: instance.Name, Role: intent.Role, Zone: intent.Zone, Home: "/var/lib/serverless", ConfigPath: "/var/lib/serverless/config", NetworkHost: runtimeconfig.PodIPReference, JVMHeap: pool.JVMHeap,
		PeerAdvertiseHost:      advertiseHost,
		PeerTLS:                intent.PeerTLSLeafUID != "",
		PeerTLSLeafSHA256:      intent.PeerTLSLeafSHA256,
		PeerTLSTrustSHA256:     intent.PeerTLSTrustSHA256,
		HTTPTLSCertificateHash: httpHash,
		BasicAuth:              authPins != nil,
		S3WebIdentity:          webIdentity,
		Store:                  storage.S3(*store), WarmOnJoin: pool.WarmOnJoin, RequireWarmupCompletion: intent.WarmupMode == "Strict",
	}, startupCatalog(instance.Spec.Image, qualifiedRuntimeImage, intent.Role))
	if err != nil {
		return nil, err
	}
	if strict {
		pod.Annotations[warmupModeAnnotation] = intent.WarmupMode
	}
	if intent.EmergencyEvidenceUID != "" || intent.EmergencyEvidenceVersion != "" {
		if intent.EmergencyEvidenceUID == "" || intent.EmergencyEvidenceVersion == "" || profile == nil {
			return nil, fmt.Errorf("temporary Pod requires pinned emergency evidence and capacity profile")
		}
		if intent.BootstrapReplacementUID == "" {
			pod.Annotations[emergencyCapacityAnnotation] = "true"
		} else {
			pod.Annotations[bootstrapReplacementAnnotation] = intent.BootstrapReplacementUID
		}
		pod.Annotations[emergencyEvidenceUIDAnnotation] = intent.EmergencyEvidenceUID
		pod.Annotations[emergencyEvidenceVersionAnnotation] = intent.EmergencyEvidenceVersion
	}
	pod.Spec.Hostname = peerHost
	pod.Spec.Subdomain = peerService.Name
	if domain != "" {
		pod.Annotations[peerDomainAnnotation] = domain
		pod.Annotations[peerDNSPreflightAnnotation] = "v1"
		pod.Spec.InitContainers = append(pod.Spec.InitContainers, corev1.Container{
			Name: "peer-dns-preflight", Image: instance.Spec.Image,
			Command: []string{"/opt/serverless/bin/health-probe"}, Args: []string{"-wait-peer-dns", advertiseHost},
			Env: []corev1.EnvVar{
				{Name: "POD_IP", ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{APIVersion: "v1", FieldPath: "status.podIP"}}},
				{Name: "POD_NAME", ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{APIVersion: "v1", FieldPath: "metadata.name"}}},
			},
			Resources: corev1.ResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("25m"), corev1.ResourceMemory: resource.MustParse("32Mi"), corev1.ResourceEphemeralStorage: resource.MustParse("32Mi")}},
		})
	}
	if intent.PeerTLSLeafUID != "" || intent.PeerTLSTrustUID != "" {
		if intent.PeerTLSLeafUID == "" || intent.PeerTLSTrustUID == "" || intent.PeerTLSSnapshotName == "" || intent.PeerTLSSnapshotUID == "" || instance.Spec.TLS == nil || instance.Spec.TLS.Transport == nil {
			return nil, fmt.Errorf("complete peer TLS intent is required")
		}
		mode := int32(0440)
		pod.Annotations[peerTLSLeafUIDAnnotation] = intent.PeerTLSLeafUID
		pod.Annotations[peerTLSTrustUIDAnnotation] = intent.PeerTLSTrustUID
		pod.Annotations[peerTLSSnapshotNameAnnotation] = intent.PeerTLSSnapshotName
		pod.Annotations[peerTLSSnapshotUIDAnnotation] = intent.PeerTLSSnapshotUID
		pod.Spec.Volumes = append(pod.Spec.Volumes,
			corev1.Volume{Name: "serverless-peer-cert", VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{SecretName: intent.PeerTLSSnapshotName, DefaultMode: &mode, Items: []corev1.KeyToPath{{Key: corev1.TLSCertKey, Path: "tls.crt"}, {Key: corev1.TLSPrivateKeyKey, Path: "tls.key"}}}}},
			corev1.Volume{Name: "serverless-peer-trust", VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{SecretName: intent.PeerTLSSnapshotName, DefaultMode: &mode, Items: []corev1.KeyToPath{{Key: "ca.crt", Path: "ca.crt"}}}}},
		)
		pod.Spec.Containers[0].VolumeMounts = append(pod.Spec.Containers[0].VolumeMounts, corev1.VolumeMount{Name: "serverless-peer-cert", MountPath: "/var/run/serverless/peer-cert", ReadOnly: true}, corev1.VolumeMount{Name: "serverless-peer-trust", MountPath: "/var/run/serverless/peer-trust", ReadOnly: true})
	}
	if err := checkProfileIdentity(intent, profile); err != nil {
		return nil, err
	}
	if profile != nil {
		pod.Annotations[profileUIDAnnotation] = profile.UID
		pod.Annotations[profileVersionAnnotation] = profile.ResourceVersion
		if err := placement.Apply(pod, profile, string(instance.UID)); err != nil {
			return nil, err
		}
	}
	for _, container := range pod.Spec.Containers[1:] {
		for _, mount := range container.VolumeMounts {
			if protectedRuntimeVolume(mount.Name) {
				return nil, fmt.Errorf("sidecar cannot mount a protected runtime volume")
			}
		}
	}
	for _, container := range pod.Spec.InitContainers {
		if container.RestartPolicy == nil || *container.RestartPolicy != corev1.ContainerRestartPolicyAlways {
			continue
		}
		for _, mount := range container.VolumeMounts {
			if protectedRuntimeVolume(mount.Name) {
				return nil, fmt.Errorf("native sidecar cannot mount a protected runtime volume")
			}
		}
	}
	pod.Spec.Containers[0].Env = append(pod.Spec.Containers[0].Env, corev1.EnvVar{Name: "POD_IP", ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{APIVersion: "v1", FieldPath: "status.podIP"}}})
	pod.Spec.Containers[0].Env = append(pod.Spec.Containers[0].Env, corev1.EnvVar{Name: "POD_NAME", ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{APIVersion: "v1", FieldPath: "metadata.name"}}})
	probeCommand := []string{"/opt/serverless/bin/health-probe", "-role", intent.Role}
	if httpPins != nil {
		addHTTPVolumes(pod, httpPins)
		probeCommand = append(probeCommand, "-tls-ca", runtimeconfig.HTTPServerTrustDir+"/ca.crt", "-tls-ca-sha256", httpPins.TrustHash, "-tls-server-name", advertiseHost)
	}
	if authPins != nil {
		probeCommand = append(probeCommand, "-basic-auth-username", runtimeconfig.BasicAuthUser, "-basic-auth-password-file", "/var/lib/serverless/config/"+runtimeconfig.BasicAuthFile)
		pod.Spec.Containers[0].Env = append(pod.Spec.Containers[0].Env, corev1.EnvVar{Name: runtimeconfig.BasicAuthBundleEnv, Value: authPins.BundleSHA256})
	}
	if intent.WarmupMode == "Strict" {
		probeCommand = append(probeCommand, "-require-warmup-completion")
	}
	pod.Spec.Containers[0].ReadinessProbe = &corev1.Probe{ProbeHandler: corev1.ProbeHandler{Exec: &corev1.ExecAction{Command: probeCommand}}, TimeoutSeconds: 6, PeriodSeconds: 5, SuccessThreshold: 1, FailureThreshold: 1}
	group := int64(10001)
	pod.Spec.SecurityContext = &corev1.PodSecurityContext{FSGroup: &group}
	pod.Spec.Volumes = append(pod.Spec.Volumes, corev1.Volume{Name: "serverless-data", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}}})
	pod.Spec.Containers[0].VolumeMounts = append(pod.Spec.Containers[0].VolumeMounts, corev1.VolumeMount{Name: "serverless-data", MountPath: "/var/lib/serverless"})
	credentials := instance.Spec.Storage.Credentials
	if credentials != nil && credentials.Type == "Keystore" {
		if credentials.KeystoreSecretRef == nil || credentials.KeystoreSecretRef.Name == "" || intent.KeystoreUID == "" {
			return nil, fmt.Errorf("pinned keystore Secret identity is required")
		}
		mode := int32(0440)
		pod.Annotations[keystoreUIDAnnotation] = intent.KeystoreUID
		items := []corev1.KeyToPath{{Key: "opensearch.keystore", Path: "opensearch.keystore"}}
		mountPath := "/var/lib/serverless/config"
		if authPins != nil {
			items = append(items, corev1.KeyToPath{Key: authPins.Key, Path: runtimeconfig.BasicAuthFile})
			mountPath = runtimeconfig.BasicAuthSourceDir
		}
		pod.Spec.Volumes = append(pod.Spec.Volumes, corev1.Volume{Name: keystoreVolumeName, VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{SecretName: credentials.KeystoreSecretRef.Name, DefaultMode: &mode, Items: items}}})
		pod.Spec.Containers[0].VolumeMounts = append(pod.Spec.Containers[0].VolumeMounts, corev1.VolumeMount{Name: keystoreVolumeName, MountPath: mountPath, ReadOnly: true})
	} else if intent.KeystoreUID != "" {
		return nil, fmt.Errorf("unexpected keystore identity")
	}
	if plugin := intent.PluginInstall; plugin != nil {
		if plugin.RequestUID == "" || plugin.PolicyUID == "" || plugin.RequestSHA256 == "" || plugin.PolicySHA256 == "" || plugin.PolicySourceUID == "" || plugin.PolicySourceVersion == "" || (plugin.CredentialsName == "") != (plugin.CredentialsUID == "") {
			return nil, fmt.Errorf("complete plugin snapshot and credential identities are required")
		}
		encoded, err := json.Marshal(plugin)
		if err != nil {
			return nil, err
		}
		pod.Annotations[pluginIntentAnnotation] = string(encoded)
		uid, yes := int64(10001), true
		pod.Spec.Containers[0].SecurityContext = &corev1.SecurityContext{RunAsUser: &uid, RunAsNonRoot: &yes}
		pod, err = withPinnedPluginInstaller(pod, plugin)
		if err != nil {
			return nil, err
		}
	}
	if webIdentity {
		if err := workload.AddS3WebIdentity(pod, instance.Spec.Storage.Credentials.AWSRoleARN); err != nil {
			return nil, err
		}
	}
	if err := validateManagedTLSKeyAccess(instance, pod); err != nil {
		return nil, err
	}
	if intent.PeerTLSSnapshotName != "" {
		if err := validateProtectedSecretAccess(pod, intent.PeerTLSSnapshotName, "serverless-peer-cert", "serverless-peer-trust"); err != nil {
			return nil, err
		}
	}
	if intent.KeystoreUID != "" {
		if err := validateKeystoreMount(pod, pod, &pod.Spec.Containers[0]); err != nil {
			return nil, err
		}
	}
	return pod, nil
}

func protectedRuntimeVolume(name string) bool {
	return name == "serverless-data" || name == keystoreVolumeName || tlsRuntimeVolume(name)
}

func tlsRuntimeVolume(name string) bool {
	switch name {
	case "serverless-peer-cert", "serverless-peer-trust", httpCertificateVolume, httpTrustVolume:
		return true
	default:
		return false
	}
}
