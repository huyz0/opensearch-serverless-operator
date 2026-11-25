package controller

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/url"
	"reflect"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/validation"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/huyz0/opensearch-serverless-operator/api/v1alpha1"
	"github.com/huyz0/opensearch-serverless-operator/internal/java"
	"github.com/huyz0/opensearch-serverless-operator/internal/rollout"
	"github.com/huyz0/opensearch-serverless-operator/internal/workload"
	"github.com/huyz0/opensearch-serverless-operator/pkg/podlifecycle"
)

// DevelopmentWriterRetirementConfig binds one development instance to its
// existing Envoy frontend and preconfigured Java retirement accounts.
type DevelopmentWriterRetirementConfig struct {
	Instance           string `json:"instance"`
	HTTPRoute          string `json:"httpRoute"`
	GatewayNamespace   string `json:"gatewayNamespace"`
	GatewayName        string `json:"gatewayName"`
	GatewayListener    string `json:"gatewayListener"`
	FrontendNamespace  string `json:"frontendNamespace"`
	FrontendService    string `json:"frontendService"`
	IndexURL           string `json:"indexURL"`
	EdgeCredential     string `json:"edgeCredential"`
	ReaderCredential   string `json:"readerCredential"`
	PreparerCredential string `json:"preparerCredential"`
	PlatformKey        string `json:"platformKey"`
	EnableEviction     bool   `json:"enableEviction,omitempty"`
}

func (r *OperationReconciler) ConfigureDevelopmentWriterRetirement(runtime *InstanceReconciler, cfg DevelopmentWriterRetirementConfig) error {
	if runtime == nil || !runtime.DevelopmentObserveEndpoints || !qualifiedRoleProbeImage.MatchString(runtime.DevelopmentServiceProbeImage) {
		return fmt.Errorf("writer retirement requires the qualified ingest probe and endpoint observation")
	}
	for _, name := range []string{cfg.Instance, cfg.HTTPRoute, cfg.GatewayNamespace, cfg.GatewayName, cfg.GatewayListener,
		cfg.FrontendNamespace, cfg.FrontendService, cfg.EdgeCredential, cfg.ReaderCredential, cfg.PreparerCredential} {
		if len(validation.IsDNS1123Subdomain(name)) != 0 {
			return fmt.Errorf("writer retirement requires valid instance, routing and credential names")
		}
	}
	if cfg.EdgeCredential == cfg.ReaderCredential || cfg.EdgeCredential == cfg.PreparerCredential || cfg.ReaderCredential == cfg.PreparerCredential {
		return fmt.Errorf("writer retirement requires three separate credential Secrets")
	}
	key, err := base64.StdEncoding.Strict().DecodeString(cfg.PlatformKey)
	if err != nil || len(key) != ed25519.PublicKeySize {
		return fmt.Errorf("writer retirement requires an Ed25519 public key")
	}
	endpoint, err := url.Parse(cfg.IndexURL)
	if err != nil || endpoint.Scheme != "https" || len(validation.IsDNS1123Subdomain(endpoint.Hostname())) != 0 ||
		endpoint.User != nil || endpoint.Path != "" && endpoint.Path != "/" || endpoint.Fragment != "" ||
		endpoint.RawQuery != "" || endpoint.ForceQuery || endpoint.Port() != "" && endpoint.Port() != "443" {
		return fmt.Errorf("writer retirement requires the root HTTPS index URL")
	}
	r.writerRetirement = developmentWriterRetirement{
		runtime: runtime.writerRuntime(), instanceName: cfg.Instance, routeName: cfg.HTTPRoute,
		gateway:  client.ObjectKey{Namespace: cfg.GatewayNamespace, Name: cfg.GatewayName},
		frontend: client.ObjectKey{Namespace: cfg.FrontendNamespace, Name: cfg.FrontendService},
		listener: cfg.GatewayListener, routeURL: cfg.IndexURL, edgeSecret: cfg.EdgeCredential,
		readerSecret: cfg.ReaderCredential, preparerSecret: cfg.PreparerCredential, platformKey: ed25519.PublicKey(key),
		enableEviction: cfg.EnableEviction,
	}
	return nil
}

// Deployment configuration authorizes one instance and names the production
// route and credentials. Operation authors can only select the victim Pod.
type developmentWriterRetirement struct {
	runtime                  writerRuntime
	instanceName, routeName  string
	gateway, frontend        client.ObjectKey
	listener, routeURL       string
	edgeSecret, readerSecret string
	preparerSecret           string
	platformKey              ed25519.PublicKey
	enableEviction           bool
}

type writerRetirementBinding struct {
	intent            writerPreparationIntent
	access            writerPreparationAccess
	digest            string
	preparerSecretUID string
}

func (b developmentWriterRetirement) observe(ctx context.Context, instance *v1alpha1.OpenSearchServerless, op *v1alpha1.OpenSearchServerlessOperation) (v1alpha1.WriterRetirementStatus, error) {
	var result v1alpha1.WriterRetirementStatus
	bound, err := b.bind(ctx, instance, op)
	if err != nil {
		return result, err
	}
	front := bound.intent
	if replicas, err := rollout.ObserveEnvoyGatewayEndpoints(ctx, b.runtime.core, b.runtime.discovery, front.GatewayService.Namespace, front.GatewayService.Name, front.GatewayServiceUID); err != nil || len(replicas.UIDs) == 0 {
		return result, fmt.Errorf("approved Envoy frontend has no qualified serving replicas")
	}
	pod, err := b.runtime.liveWriterVictim(ctx, instance, bound.intent.Victim)
	if err != nil {
		return result, err
	}
	if b.enableEviction && !workload.PodHasSingleOpenSearchLifetime(pod) {
		return result, errWriterEvictionRestartPolicy
	}
	protection, validUntil, err := b.runtime.qualifyRetiringWriterCapacity(ctx, instance, pod, string(bound.intent.ServiceUID))
	if err != nil {
		return result, err
	}
	ctx, cancel := context.WithDeadline(ctx, validUntil)
	defer cancel()
	httpClient, endpoint, err := b.runtime.developmentPodHTTPTransport(ctx, instance, pod, 9200)
	if err != nil {
		return result, err
	}
	defer httpClient.CloseIdleConnections()
	start := time.Now()
	lease, err := java.GetLeaseWithBasicAuth(ctx, httpClient, endpoint, bound.access.Reader.username, bound.access.Reader.password)
	leaseUntil := lease.EvidenceValidUntil(start, writerDispatchClockMargin)
	if err != nil || lease.NodeName != pod.Name || lease.NodeID == "" || lease.NodeEphemeralID == "" || len(lease.Roles) != 1 ||
		!lease.HasRole("ingest") || !time.Now().Before(leaseUntil) {
		return result, fmt.Errorf("writer stats do not identify a live ingest JVM")
	}
	qualified := false
	for _, observed := range protection.Pods {
		if observed.PodUID == string(pod.UID) && observed.ProcessIncarnation == lease.ProcessIncarnation && lease.ProcessIncarnation != "" {
			qualified = true
			break
		}
	}
	if !qualified {
		return result, fmt.Errorf("writer JVM changed during capacity observation")
	}
	process := java.WriterProcess{NodeID: lease.NodeID, EphemeralID: lease.NodeEphemeralID}
	if b.enableEviction && op.Status.WriterRetirement != nil {
		preflight, cancel := context.WithDeadline(ctx, leaseUntil)
		defer cancel()
		if err := b.runtime.checkPinnedPrimaryStore(preflight, instance); err != nil {
			return result, fmt.Errorf("%w: %w", errWriterPreflightInventory, err)
		}
		if err := checkOpenWriterHeads(preflight, httpClient, endpoint, process, bound.access.Reader.username, bound.access.Reader.password); err != nil {
			return result, err
		}
		if err := b.runtime.checkPinnedPrimaryStore(preflight, instance); err != nil {
			return result, fmt.Errorf("%w: %w", errWriterPreflightInventory, err)
		}
		ctx = preflight
	}
	state, err := java.GetWriterRetirementState(ctx, httpClient, endpoint, process, string(op.UID), bound.access.Preparer.username, bound.access.Preparer.password)
	if err != nil || state != java.WriterAdmissionOpen {
		return result, fmt.Errorf("exact writer is not open for retirement")
	}
	if !fleetPodsUnchanged(ctx, b.runtime.core.Pods(instance.Namespace), string(instance.UID), "ingest", protection.Fleet, protection.PodSnapshot) {
		return result, fmt.Errorf("serving index cohort changed during reservation")
	}
	return v1alpha1.WriterRetirementStatus{
		InstanceGeneration: instance.Generation, ContainerID: runningOpenSearch(pod).ContainerID,
		NodeID: process.NodeID, NodeEphemeralID: process.EphemeralID, ServingFleet: protection.Fleet,
		ServiceUID: string(bound.intent.ServiceUID), RouteUID: string(bound.intent.RouteUID),
		GatewayUID: string(bound.intent.GatewayUID), GatewayServiceUID: string(bound.intent.GatewayServiceUID),
		BindingDigest: bound.digest, PreparerSecretUID: bound.preparerSecretUID,
	}, nil
}

// State readback is scoped to the original JVM and immutable preparer
// credential. The edge, reader and route may have changed since dispatch.
func (b developmentWriterRetirement) readState(ctx context.Context, instance *v1alpha1.OpenSearchServerless, op *v1alpha1.OpenSearchServerlessOperation) (java.WriterRetirementState, error) {
	journal := op.Status.WriterRetirement
	if b.runtime.core == nil || instance.Name != b.instanceName || instance.Status.BasicAuthBaseline == nil || !hasManagedHTTPS(instance) ||
		journal == nil || !journal.WithdrawalArmed || journal.PreparerSecretUID == "" ||
		op.Spec.WriterRetirement == nil || op.Namespace != instance.Namespace || op.Spec.InstanceRef.UID != string(instance.UID) {
		return "", fmt.Errorf("the original managed writer and preparation credential are required")
	}
	credential, err := rollout.ReadRouteCredential(ctx, b.runtime.core, instance, b.preparerSecret)
	if err != nil || credential == nil || string(credential.AuthSecretUID) != journal.PreparerSecretUID {
		return "", fmt.Errorf("original writer preparation credential changed")
	}
	target := op.Spec.WriterRetirement
	victim := podlifecycle.Identity{Namespace: instance.Namespace, OwnerUID: string(instance.UID), Name: target.PodName, UID: target.PodUID}
	pod, err := b.runtime.liveWriterVictim(ctx, instance, victim)
	if err != nil || runningOpenSearch(pod).ContainerID != journal.ContainerID || pod.Labels[workload.FleetLabel] != journal.ServingFleet {
		return "", fmt.Errorf("recorded writer container changed")
	}
	direct, endpoint, err := b.runtime.developmentPodHTTPTransport(ctx, instance, pod, 9200)
	if err != nil {
		return "", err
	}
	defer direct.CloseIdleConnections()
	process := java.WriterProcess{NodeID: journal.NodeID, EphemeralID: journal.NodeEphemeralID}
	return java.GetWriterRetirementState(ctx, direct, endpoint, process, string(op.UID), credential.Username, credential.Password)
}

func (b developmentWriterRetirement) withdraw(ctx context.Context, instance *v1alpha1.OpenSearchServerless, op *v1alpha1.OpenSearchServerlessOperation, recheck func(context.Context) error) (java.WriterRetirementState, error) {
	journal := op.Status.WriterRetirement
	expired := journal != nil && journal.WithdrawalDeadline != nil && !time.Now().Before(journal.WithdrawalDeadline.Time)
	state, err := b.readState(ctx, instance, op)
	if err != nil || state != java.WriterAdmissionOpen || expired {
		return state, err
	}
	bound, err := b.bindJournal(ctx, instance, op)
	if err != nil {
		return "", err
	}
	return b.runtime.withdrawRetiringWriter(ctx, instance, bound.intent, *op.Status.WriterRetirement, bound.access.Preparer, func(ctx context.Context) error {
		current, err := b.bindJournal(ctx, instance, op)
		if err != nil {
			return err
		}
		front := current.intent
		if replicas, err := rollout.ObserveEnvoyGatewayEndpoints(ctx, b.runtime.core, b.runtime.discovery, front.GatewayService.Namespace, front.GatewayService.Name, front.GatewayServiceUID); err != nil || len(replicas.UIDs) == 0 {
			return fmt.Errorf("approved Envoy frontend no longer has qualified serving replicas")
		}
		return recheck(ctx)
	})
}

func (b developmentWriterRetirement) prepare(ctx context.Context, instance *v1alpha1.OpenSearchServerless, op *v1alpha1.OpenSearchServerlessOperation, recheck func(context.Context) error) (java.WriterRetirementState, error) {
	state, err := b.readState(ctx, instance, op)
	if err != nil || state != java.WriterTrafficWithdrawn {
		return state, err
	}
	bound, err := b.bindJournal(ctx, instance, op)
	if err != nil {
		return "", err
	}
	return b.runtime.prepareRetiringWriter(ctx, instance, bound.intent, bound.access, func(ctx context.Context, _ writerPreparationIntent) error {
		if _, err := b.bindJournal(ctx, instance, op); err != nil {
			return err
		}
		return recheck(ctx)
	})
}

func (b developmentWriterRetirement) bindJournal(ctx context.Context, instance *v1alpha1.OpenSearchServerless, op *v1alpha1.OpenSearchServerlessOperation) (writerRetirementBinding, error) {
	journal := op.Status.WriterRetirement
	if journal == nil || !journal.WithdrawalArmed || journal.InstanceGeneration != instance.Generation {
		return writerRetirementBinding{}, fmt.Errorf("exact writer retirement journal is required")
	}
	bound, err := b.bind(ctx, instance, op)
	if err != nil {
		return writerRetirementBinding{}, err
	}
	if !matchesWriterRetirementBinding(bound, journal, instance) {
		return writerRetirementBinding{}, fmt.Errorf("recorded writer route or authorization changed")
	}
	intent := &bound.intent
	pod, err := b.runtime.liveWriterVictim(ctx, instance, intent.Victim)
	if err != nil || runningOpenSearch(pod).ContainerID != journal.ContainerID {
		return writerRetirementBinding{}, fmt.Errorf("recorded writer container changed")
	}
	intent.Process = java.WriterProcess{NodeID: journal.NodeID, EphemeralID: journal.NodeEphemeralID}
	if journal.WithdrawnAt != nil {
		intent.WithdrawnAt = journal.WithdrawnAt.Time
	}
	return bound, nil
}

func matchesWriterRetirementBinding(bound writerRetirementBinding, journal *v1alpha1.WriterRetirementStatus, instance *v1alpha1.OpenSearchServerless) bool {
	intent := bound.intent
	return bound.digest == journal.BindingDigest && bound.preparerSecretUID == journal.PreparerSecretUID && string(intent.ServiceUID) == journal.ServiceUID &&
		string(intent.RouteUID) == journal.RouteUID && string(intent.GatewayUID) == journal.GatewayUID &&
		string(intent.GatewayServiceUID) == journal.GatewayServiceUID && instance.Status.ServingFleetIntentByRole["ingest"] == journal.ServingFleet
}

func (b developmentWriterRetirement) bind(ctx context.Context, instance *v1alpha1.OpenSearchServerless, op *v1alpha1.OpenSearchServerlessOperation) (writerRetirementBinding, error) {
	var result writerRetirementBinding
	r := b.runtime
	if r.reader == nil || r.core == nil || r.discovery == nil || instance.Name != b.instanceName ||
		len(b.platformKey) != ed25519.PublicKeySize || op.Spec.WriterRetirement == nil || op.UID == "" || op.Namespace != instance.Namespace ||
		instance.Status.BasicAuthBaseline == nil || !hasManagedHTTPS(instance) || !r.supportsDevelopmentIngestProbe(instance) {
		return result, fmt.Errorf("writer retirement requires an approved managed HTTPS runtime and deployment binding")
	}
	desired, err := workload.RoleService(instance, "ingest", instance.Status.ServingFleetIntentByRole["ingest"], 9200)
	if err != nil {
		return result, err
	}
	service, err := r.core.Services(instance.Namespace).Get(ctx, desired.Name, metav1.GetOptions{})
	if err != nil || service.UID == "" || service.DeletionTimestamp != nil || !roleServiceShapeMatches(instance, service, desired) ||
		!reflect.DeepEqual(service.Spec.Selector, desired.Spec.Selector) {
		return result, fmt.Errorf("stable index Service does not match the serving fleet")
	}
	route, err := readWriterGatewayObject(ctx, r.reader, "HTTPRoute", client.ObjectKey{Namespace: instance.Namespace, Name: b.routeName})
	if err != nil || !rollout.WriterRouteUsesService(route, service.Name) {
		return result, fmt.Errorf("approved HTTPRoute must send index traffic to the stable Service")
	}
	probeURL, err := rollout.BindAuthenticatedProbeURL(b.routeURL, route)
	if err != nil {
		return result, err
	}
	gateway, err := readWriterGatewayObject(ctx, r.reader, "Gateway", b.gateway)
	if err != nil {
		return result, err
	}
	frontend, err := r.core.Services(b.frontend.Namespace).Get(ctx, b.frontend.Name, metav1.GetOptions{})
	if err != nil || frontend.UID == "" || frontend.DeletionTimestamp != nil {
		return result, fmt.Errorf("approved Envoy frontend Service is unavailable")
	}
	target := op.Spec.WriterRetirement
	intent := writerPreparationIntent{
		Victim:       podlifecycle.Identity{Namespace: instance.Namespace, OwnerUID: string(instance.UID), Name: target.PodName, UID: target.PodUID},
		OperationUID: op.UID, ServiceUID: service.UID, RouteName: route.GetName(), RouteUID: route.GetUID(),
		Gateway: b.gateway, GatewayUID: gateway.GetUID(), GatewayService: b.frontend, GatewayServiceUID: frontend.UID, GatewayListener: b.listener,
	}
	gateway, err = r.readWriterGateway(ctx, intent, route)
	if err != nil {
		return result, err
	}
	if !writerRouteAccepted(route, gateway, b.listener) {
		return result, fmt.Errorf("Envoy has not accepted the current index route and HTTPS listener")
	}
	credentials := make([]*rollout.RouteCredential, 0, 3)
	for _, name := range []string{b.edgeSecret, b.readerSecret, b.preparerSecret} {
		credential, err := rollout.ReadRouteCredential(ctx, r.core, instance, name)
		if err != nil || credential == nil {
			return result, fmt.Errorf("writer retirement requires three immutable scoped credential Secrets")
		}
		for _, previous := range credentials {
			if credential.Username == previous.Username || bytes.Equal(credential.Password, previous.Password) {
				return result, fmt.Errorf("use distinct Envoy probe, Java reader and Java preparer accounts")
			}
		}
		credentials = append(credentials, credential)
	}
	edge, reader, preparer := credentials[0], credentials[1], credentials[2]
	_, bootstrap, err := r.resolveBasicAuth(ctx, instance)
	if err != nil {
		return result, err
	}
	if bytes.Equal(edge.Password, bootstrap) || bytes.Equal(reader.Password, bootstrap) || bytes.Equal(preparer.Password, bootstrap) {
		return result, fmt.Errorf("retirement credentials must differ from the bootstrap credential")
	}
	// Secret data are immutable. Pin UID but allow metadata-only updates.
	// Signed Envoy observations still bind the current edge Secret version.
	auth := [3]rollout.RouteAuthIdentity{edge.RouteAuthIdentity, reader.RouteAuthIdentity, preparer.RouteAuthIdentity}
	for i := range auth {
		auth[i].AuthSecretVersion = ""
	}
	value, err := json.Marshal(struct {
		Intent      writerPreparationIntent
		ProbeURL    string
		Auth        [3]rollout.RouteAuthIdentity
		PlatformKey ed25519.PublicKey
		RouteSpec   any
		GatewaySpec any
	}{
		Intent: intent, ProbeURL: probeURL, Auth: auth,
		PlatformKey: b.platformKey, RouteSpec: route.Object["spec"], GatewaySpec: gateway.Object["spec"],
	})
	if err != nil {
		return result, err
	}
	digest := sha256.Sum256(value)
	return writerRetirementBinding{
		intent: intent, digest: hex.EncodeToString(digest[:]), preparerSecretUID: string(preparer.AuthSecretUID),
		access: writerPreparationAccess{
			Reader:   podHealthCredential{username: reader.Username, password: reader.Password},
			Preparer: podHealthCredential{username: preparer.Username, password: preparer.Password},
			RouteURL: probeURL, RouteAuthSecret: b.edgeSecret, PlatformKey: b.platformKey,
		},
	}, nil
}

func readWriterGatewayObject(ctx context.Context, reader client.Reader, kind string, key client.ObjectKey) (*unstructured.Unstructured, error) {
	object := &unstructured.Unstructured{}
	object.SetGroupVersionKind(schema.GroupVersionKind{Group: "gateway.networking.k8s.io", Version: "v1", Kind: kind})
	if err := reader.Get(ctx, key, object); err != nil {
		return nil, err
	}
	if object.GetUID() == "" || object.GetDeletionTimestamp() != nil {
		return nil, fmt.Errorf("pinned %s is unavailable", kind)
	}
	return object, nil
}

func writerRouteAccepted(route, gateway *unstructured.Unstructured, listener string) bool {
	current := func(status map[string]any, generation int64, required ...string) bool {
		values, _, err := unstructured.NestedSlice(status, "conditions")
		if err != nil || generation <= 0 {
			return false
		}
		for _, condition := range required {
			found := false
			for _, value := range values {
				entry, _ := value.(map[string]any)
				if entry["type"] == condition && entry["status"] == "True" && entry["observedGeneration"] == generation {
					found = true
					break
				}
			}
			if !found {
				return false
			}
		}
		return true
	}
	gatewayStatus, _, _ := unstructured.NestedMap(gateway.Object, "status")
	if !current(gatewayStatus, gateway.GetGeneration(), "Accepted", "Programmed") {
		return false
	}
	listeners, _, _ := unstructured.NestedSlice(gatewayStatus, "listeners")
	programmed := false
	for _, value := range listeners {
		entry, _ := value.(map[string]any)
		if entry["name"] == listener && current(entry, gateway.GetGeneration(), "Accepted", "Programmed", "ResolvedRefs") {
			programmed = true
			break
		}
	}
	if !programmed {
		return false
	}
	parents, _, _ := unstructured.NestedSlice(route.Object, "status", "parents")
	for _, value := range parents {
		parent, _ := value.(map[string]any)
		ref, _, _ := unstructured.NestedMap(parent, "parentRef")
		namespace, explicit, err := unstructured.NestedString(ref, "namespace")
		if !explicit {
			namespace = route.GetNamespace()
		}
		if err == nil && namespace == gateway.GetNamespace() && ref["name"] == gateway.GetName() &&
			ref["sectionName"] == listener && (ref["group"] == nil || ref["group"] == "gateway.networking.k8s.io") &&
			(ref["kind"] == nil || ref["kind"] == "Gateway") && (ref["port"] == nil || ref["port"] == int64(443)) &&
			parent["controllerName"] == "gateway.envoyproxy.io/gatewayclass-controller" && current(parent, route.GetGeneration(), "Accepted", "ResolvedRefs") {
			return true
		}
	}
	return false
}
