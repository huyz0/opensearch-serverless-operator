//go:build envtest

package controller

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	policyv1 "k8s.io/api/policy/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"

	"github.com/huyz0/opensearch-serverless-operator/api/v1alpha1"
	"github.com/huyz0/opensearch-serverless-operator/internal/java"
	"github.com/huyz0/opensearch-serverless-operator/internal/rollout"
	"github.com/huyz0/opensearch-serverless-operator/internal/workload"
)

type candidateRaceTransport func(*http.Request) (*http.Response, error)

func (f candidateRaceTransport) RoundTrip(req *http.Request) (*http.Response, error) { return f(req) }

type failbackReadHook struct {
	client.Reader
	beforeRead func()
}

func (r failbackReadHook) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	r.beforeRead()
	return r.Reader.Get(ctx, key, obj, opts...)
}

func TestRolloutStatusAPIServerConflict(t *testing.T) {
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
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := v1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	c, err := client.New(config, client.Options{Scheme: scheme})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := c.Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "rollout-test"}}); err != nil {
		t.Fatal(err)
	}
	core, err := kubernetes.NewForConfig(config)
	if err != nil {
		t.Fatal(err)
	}
	t.Run("gateway backend lifecycle", func(t *testing.T) {
		testGatewayBackendLifecycle(t, config, c, core)
	})
	t.Run("HTTP leaf rotation staging", func(t *testing.T) {
		testHTTPLeafRotation(t, c, core, "rollout-test")
	})
	t.Run("HTTP CA rollover staging", func(t *testing.T) {
		testHTTPCARollover(t, c, core, "rollout-test")
	})
	t.Run("peer TLS rollover staging", func(t *testing.T) {
		testPeerTLSRollout(t, c, core, "rollout-test")
	})
	t.Run("promoted preview detach", func(t *testing.T) {
		testPromotedPreviewDetach(t, c, core, "rollout-test")
	})
	t.Run("displaced fleet retirement", func(t *testing.T) {
		testDisplacedFleetRetirement(t, c, core, "rollout-test")
	})
	t.Run("namespaced Pod disruption signal", func(t *testing.T) {
		testPodDisruptionSignal(t, c, core, "rollout-test")
	})
	t.Run("disruption orders serving provisioning", func(t *testing.T) {
		testDisruptionOrdersServingProvisioning(t, c, core, "rollout-test")
	})
	t.Run("serving recovery preempts pending preview", func(t *testing.T) {
		testPreviewYieldsToServingRecovery(t, c, core, "rollout-test")
	})
	staged := &v1alpha1.OpenSearchServerless{ObjectMeta: metav1.ObjectMeta{Name: "staged", Namespace: "rollout-test"}, Spec: v1alpha1.OpenSearchServerlessSpec{Image: "example/image@sha256:" + strings.Repeat("a", 64), ServiceAccountName: "runtime", Storage: v1alpha1.Storage{Type: "s3", Bucket: "bucket", Region: "region"}, Pools: map[string]v1alpha1.Pool{"search": {Zones: map[string]v1alpha1.Zone{"a": {Replicas: 1}}}}, Rollout: &v1alpha1.Rollout{Strategy: "BlueGreen", Candidate: &v1alpha1.FleetCandidate{Fleet: "green", Image: "example/image@sha256:" + strings.Repeat("b", 64)}}}}
	t.Run("writer retirement journal", func(t *testing.T) {
		testWriterRetirementJournal(t, c, "rollout-test", staged.Spec, false)
	})
	t.Run("writer retirement journal missing receipt", func(t *testing.T) {
		testWriterRetirementJournal(t, c, "rollout-test", staged.Spec, true)
	})
	t.Run("writer retirement journal partial receipt", func(t *testing.T) {
		testWriterRetirementReleaseEvidence(t, c, "rollout-test", staged.Spec, false)
	})
	t.Run("writer with no heads can retire after a missing receipt", func(t *testing.T) {
		testWriterRetirementReleaseEvidence(t, c, "rollout-test", staged.Spec, true)
	})
	if err := c.Create(ctx, staged); err != nil {
		t.Fatal(err)
	}
	staged.Status.ServingFleetIntentByRole = map[string]string{"search": "blue", "ingest": "blue"}
	if err := c.Status().Update(ctx, staged); err != nil {
		t.Fatal(err)
	}
	beforeCandidate := staged.DeepCopyObject().(*v1alpha1.OpenSearchServerless)
	instanceReconciler := &InstanceReconciler{Client: c, Core: core.CoreV1()}
	if changed, err := instanceReconciler.reconcileCandidatePreview(ctx, staged); err != nil || !changed || staged.Status.CandidateFleet == nil || staged.Status.CandidateFleet.SearchServiceUID != "" {
		t.Fatalf("candidate was not recorded before Services: %#v %v", staged.Status.CandidateFleet, err)
	}
	recovered := &v1alpha1.OpenSearchServerless{}
	if err := c.Get(ctx, types.NamespacedName{Namespace: staged.Namespace, Name: staged.Name}, recovered); err != nil {
		t.Fatal(err)
	}
	if _, err := ensurePreviewServices(ctx, c, core.CoreV1(), beforeCandidate, "green", nil); err == nil || !strings.Contains(err.Error(), "candidate state changed") {
		t.Fatalf("stale instance allowed preview Service creation: %v", err)
	}
	if changed, err := instanceReconciler.reconcileCandidatePreview(ctx, recovered); err != nil || !changed || recovered.Status.CandidateFleet.SearchServiceUID == "" || recovered.Status.CandidateFleet.IngestServiceUID == "" {
		t.Fatalf("preview Services not recorded after restart: %#v %v", recovered.Status.CandidateFleet, err)
	}
	pinnedUID := recovered.Status.CandidateFleet.SearchServiceUID
	recovered.Spec.Rollout.Candidate.Image = "example/image@sha256:" + strings.Repeat("c", 64)
	if err := c.Update(ctx, recovered); err != nil {
		t.Fatal(err)
	}
	if changed, err := instanceReconciler.reconcileCandidatePreview(ctx, recovered); err != nil || !changed || recovered.Status.CandidateFleet.SearchServiceUID != pinnedUID || recovered.Status.CandidateFleet.Image == recovered.Spec.Rollout.Candidate.Image {
		t.Fatalf("changed candidate replaced recorded identity: %#v %v", recovered.Status.CandidateFleet, err)
	}
	recovered.Spec.Rollout.Candidate.Image = recovered.Status.CandidateFleet.Image
	if err := c.Update(ctx, recovered); err != nil {
		t.Fatal(err)
	}
	if changed, err := instanceReconciler.reconcileCandidatePreview(ctx, recovered); err != nil || !changed || recovered.Status.CandidateFleet.Generation != recovered.Generation || recovered.Status.CandidateFleet.SearchServiceUID != pinnedUID {
		t.Fatalf("restored candidate was not safely rebased: %#v %v", recovered.Status.CandidateFleet, err)
	}
	if changed, err := instanceReconciler.reconcileCandidatePreview(ctx, recovered); err != nil || !changed || recovered.Status.CandidateFleet.SearchServiceUID != pinnedUID {
		t.Fatalf("rebased preview identities not rechecked: %#v %v", recovered.Status.CandidateFleet, err)
	}
	recovered.Spec.Rollout.Candidate = nil
	if err := c.Update(ctx, recovered); err != nil {
		t.Fatal(err)
	}
	if changed, err := instanceReconciler.reconcileCandidatePreview(ctx, recovered); err != nil || !changed || !recovered.Status.CandidateFleet.PreviewRetiring {
		t.Fatalf("preview retirement intent missing: %v", err)
	}
	replacedPreview, err := workload.PreviewService(recovered, "search", "green")
	if err != nil {
		t.Fatal(err)
	}
	if err := core.CoreV1().Services(recovered.Namespace).Delete(ctx, replacedPreview.Name, metav1.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	replacement, err := core.CoreV1().Services(recovered.Namespace).Create(ctx, replacedPreview, metav1.CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if string(replacement.UID) == pinnedUID {
		t.Fatal("replacement Service did not receive a new UID")
	}
	if _, err := instanceReconciler.retireCandidatePreviewService(ctx, recovered); err == nil {
		t.Fatal("replacement preview Service accepted for deletion")
	}
	if _, err := core.CoreV1().Services(recovered.Namespace).Get(ctx, replacement.Name, metav1.GetOptions{}); err != nil {
		t.Fatal("replacement preview Service was deleted")
	}
	wired := &v1alpha1.OpenSearchServerless{ObjectMeta: metav1.ObjectMeta{Name: "wired", Namespace: "rollout-test"}, Spec: staged.Spec}
	if err := c.Create(ctx, wired); err != nil {
		t.Fatal(err)
	}
	request := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: wired.Namespace, Name: wired.Name}}
	for i := 0; i < 8; i++ {
		if _, err := instanceReconciler.Reconcile(ctx, request); err != nil {
			t.Fatal(err)
		}
	}
	if err := c.Get(ctx, request.NamespacedName, wired); err != nil {
		t.Fatal(err)
	}
	if wired.Status.CandidateFleet == nil || wired.Status.CandidateFleet.SearchServiceUID == "" || wired.Status.CandidateFleet.IngestServiceUID == "" {
		t.Fatalf("instance reconciliation did not prepare preview Services: %#v", wired.Status.CandidateFleet)
	}
	wired.Status.LastServingSearchProtection = &v1alpha1.ServingSearchProtectionStatus{ObservedGeneration: wired.Generation, Fleet: "initial", Result: "WarmupAndEndpointsObserved", QualifiedPods: 1, ServiceUID: "service", ServiceVersion: "7", ObservedAt: metav1.Now()}
	wired.Status.LastServingIngestProtection = &v1alpha1.ServingIngestProtectionStatus{ObservedGeneration: wired.Generation, Fleet: "initial", Result: "LeaseAndEndpointsObserved", QualifiedPods: 1, ServiceUID: "ingest-service", ServiceVersion: "8", ObservedAt: metav1.Now()}
	wired.Status.LastSearchQualification = &v1alpha1.SearchQualificationObservation{
		ObservedGeneration: wired.Generation, PodName: "search", PodUID: "search-uid", Fleet: "initial", Zone: "a",
		Result: "WarmupCompleted", ObservedAt: metav1.Now(),
		LocalSearchActivity: &v1alpha1.LocalSearchActivity{BlockCacheHits: 11, BlockBytesFetched: 8192},
	}
	if err := c.Status().Update(ctx, wired); err != nil {
		t.Fatal(err)
	}
	if err := c.Get(ctx, request.NamespacedName, wired); err != nil || wired.Status.LastServingSearchProtection == nil || wired.Status.LastServingSearchProtection.ServiceVersion != "7" {
		t.Fatal("serving search diagnostic status was not preserved")
	}
	if wired.Status.LastServingIngestProtection == nil || wired.Status.LastServingIngestProtection.ServiceVersion != "8" {
		t.Fatal("serving ingest diagnostic status was not preserved")
	}
	if wired.Status.LastSearchQualification == nil {
		t.Fatal("search qualification status was not preserved")
	}
	if activity := wired.Status.LastSearchQualification.LocalSearchActivity; activity == nil || *activity != (v1alpha1.LocalSearchActivity{BlockCacheHits: 11, BlockBytesFetched: 8192}) {
		t.Fatalf("search activity diagnostic status was not preserved: %#v", activity)
	}
	wired.Spec.Rollout.Candidate = nil
	if err := c.Update(ctx, wired); err != nil {
		t.Fatal(err)
	}
	if changed, err := instanceReconciler.reconcileCandidatePreview(ctx, wired); err != nil || !changed || !wired.Status.CandidateFleet.PreviewRetiring {
		t.Fatalf("preview retirement was not recorded: %v", err)
	}
	wiredPreview, err := workload.PreviewService(wired, "search", "green")
	if err != nil {
		t.Fatal(err)
	}
	var serviceRaceOnce sync.Once
	var serviceMutationErr error
	serviceRaceConfig := rest.CopyConfig(config)
	serviceRaceConfig.Wrap(func(next http.RoundTripper) http.RoundTripper {
		return candidateRaceTransport(func(req *http.Request) (*http.Response, error) {
			if req.Method == http.MethodDelete && strings.HasSuffix(req.URL.Path, "/services/"+wiredPreview.Name) {
				serviceRaceOnce.Do(func() {
					var service *corev1.Service
					service, serviceMutationErr = core.CoreV1().Services(wired.Namespace).Get(ctx, wiredPreview.Name, metav1.GetOptions{})
					if serviceMutationErr == nil {
						service.Annotations = map[string]string{"concurrent-edit": "true"}
						_, serviceMutationErr = core.CoreV1().Services(wired.Namespace).Update(ctx, service, metav1.UpdateOptions{})
					}
				})
				if serviceMutationErr != nil {
					return nil, serviceMutationErr
				}
			}
			return next.RoundTrip(req)
		})
	})
	serviceRacingCore, err := kubernetes.NewForConfig(serviceRaceConfig)
	if err != nil {
		t.Fatal(err)
	}
	serviceRacingReconciler := &InstanceReconciler{Client: c, Core: serviceRacingCore.CoreV1()}
	if _, err := serviceRacingReconciler.retireCandidatePreviewService(ctx, wired); !apierrors.IsConflict(err) || serviceMutationErr != nil {
		t.Fatalf("expected preview Service version conflict: %v, %v", err, serviceMutationErr)
	}
	if _, err := core.CoreV1().Services(wired.Namespace).Get(ctx, wiredPreview.Name, metav1.GetOptions{}); err != nil {
		t.Fatal("changed preview Service was deleted")
	}
	serviceLostResponse := false
	serviceLostConfig := rest.CopyConfig(config)
	serviceLostConfig.Wrap(func(next http.RoundTripper) http.RoundTripper {
		return candidateRaceTransport(func(req *http.Request) (*http.Response, error) {
			response, err := next.RoundTrip(req)
			if err == nil && req.Method == http.MethodDelete && strings.HasSuffix(req.URL.Path, "/services/"+wiredPreview.Name) && response.StatusCode >= 200 && response.StatusCode < 300 {
				serviceLostResponse = true
				response.Body.Close()
				return nil, io.ErrUnexpectedEOF
			}
			return response, err
		})
	})
	serviceLostCore, err := kubernetes.NewForConfig(serviceLostConfig)
	if err != nil {
		t.Fatal(err)
	}
	serviceRacingReconciler.Core = serviceLostCore.CoreV1()
	if _, err := serviceRacingReconciler.retireCandidatePreviewService(ctx, wired); err == nil || !serviceLostResponse {
		t.Fatalf("expected lost successful Service deletion: %v", err)
	}
	serviceRacingReconciler.Core = core.CoreV1()
	if changed, err := serviceRacingReconciler.retireCandidatePreviewService(ctx, wired); err != nil || !changed {
		t.Fatalf("did not recover and delete other preview Service: %v", err)
	}
	if changed, err := serviceRacingReconciler.retireCandidatePreviewService(ctx, wired); err != nil || changed {
		t.Fatalf("preview Service retirement did not settle: %v", err)
	}
	same := &v1alpha1.OpenSearchServerless{ObjectMeta: metav1.ObjectMeta{Name: "same-image", Namespace: "rollout-test"}, Spec: staged.Spec}
	same.Spec.Rollout = &v1alpha1.Rollout{Strategy: "BlueGreen", MaxPreviewReplicas: 1, Candidate: &v1alpha1.FleetCandidate{Fleet: "green", Image: same.Spec.Image}}
	same.Spec.Pools = map[string]v1alpha1.Pool{"search": {JVMHeap: "512Mi", Zones: map[string]v1alpha1.Zone{"a": {Replicas: 1}}, Resources: corev1.ResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("1"), corev1.ResourceMemory: resource.MustParse("1Gi")}}}}
	if err := c.Create(ctx, same); err != nil {
		t.Fatal(err)
	}
	sameKey := types.NamespacedName{Namespace: same.Namespace, Name: same.Name}
	for i := 0; i < 8; i++ {
		if _, err := instanceReconciler.Reconcile(ctx, ctrl.Request{NamespacedName: sameKey}); err != nil {
			t.Fatal(err)
		}
	}
	if err := c.Get(ctx, sameKey, same); err != nil {
		t.Fatal(err)
	}
	if same.Status.CandidateFleet == nil || same.Status.CandidateFleet.SearchServiceUID == "" {
		t.Fatal("same-image candidate not prepared")
	}
	isController := true
	source := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "source-pod", Namespace: same.Namespace, OwnerReferences: []metav1.OwnerReference{{APIVersion: v1alpha1.GroupVersion.String(), Kind: "OpenSearchServerless", Name: same.Name, UID: same.UID, Controller: &isController}}, Labels: map[string]string{workload.InstanceLabel: string(same.UID), workload.RoleLabel: "search", workload.FleetLabel: "initial", workload.ZoneLabel: "a"}}, Spec: corev1.PodSpec{NodeName: "node-a", Containers: []corev1.Container{{Name: "opensearch", Image: same.Spec.Image}}}}
	if err := c.Create(ctx, source); err != nil {
		t.Fatal(err)
	}
	source.Status.Phase, source.Status.PodIP = corev1.PodRunning, "10.1.0.2"
	source.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}
	if err := c.Status().Update(ctx, source); err != nil {
		t.Fatal(err)
	}
	if _, err := instanceReconciler.reconcileGatedPod(ctx, same); err != nil {
		t.Fatal(err)
	}
	if err := c.Get(ctx, sameKey, same); err != nil {
		t.Fatal(err)
	}
	if same.Status.PendingPodCreation == nil || same.Status.PendingPodCreation.Fleet != "green" {
		t.Fatalf("candidate Pod intent not recorded: %#v", same.Status.PendingPodCreation)
	}
	name := same.Status.PendingPodCreation.Name
	source.Status.Conditions[0].Status = corev1.ConditionFalse
	if err := c.Status().Update(ctx, source); err != nil {
		t.Fatal(err)
	}
	if _, err := instanceReconciler.reconcileGatedPod(ctx, same); err != nil || same.Status.PendingPodCreation != nil ||
		len(same.Status.RetiredPodCreations) != 1 || same.Status.RetiredPodCreations[0].Name != name {
		t.Fatalf("candidate creation reservation did not yield while serving capacity was unready: %v", err)
	}
	source.Status.Conditions[0].Status = corev1.ConditionTrue
	if err := c.Status().Update(ctx, source); err != nil {
		t.Fatal(err)
	}
	if _, err := instanceReconciler.reconcileGatedPod(ctx, same); err != nil {
		t.Fatal(err)
	}
	if intent := same.Status.PendingPodCreation; intent == nil || intent.Name == name {
		t.Fatal("restored serving capacity did not qualify a new preview intent")
	}
	name = same.Status.PendingPodCreation.Name
	if _, err := instanceReconciler.reconcileGatedPod(ctx, same); err != nil {
		t.Fatal(err)
	}
	previewPod, err := core.CoreV1().Pods(same.Namespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil || previewPod.Labels[workload.FleetLabel] != "green" || !hasPlacementGate(previewPod) {
		t.Fatalf("candidate Pod was not created gated: %#v %v", previewPod, err)
	}
	if err := c.Get(ctx, sameKey, same); err != nil {
		t.Fatal(err)
	}
	savedSameSpec := same.DeepCopyObject().(*v1alpha1.OpenSearchServerless).Spec
	same.Spec.Rollout.Candidate = nil
	if err := c.Update(ctx, same); err != nil {
		t.Fatal(err)
	}
	cancelReconciler := &InstanceReconciler{Client: c, Core: core.CoreV1(), Policy: core.PolicyV1(), DevelopmentGatedPods: true}
	cancelRequest := ctrl.Request{NamespacedName: sameKey}
	for i := 0; i < 8; i++ {
		if _, err := cancelReconciler.Reconcile(ctx, cancelRequest); err != nil {
			t.Fatal(err)
		}
		if _, err := core.CoreV1().Pods(same.Namespace).Get(ctx, name, metav1.GetOptions{}); apierrors.IsNotFound(err) {
			break
		}
	}
	if _, err := core.CoreV1().Pods(same.Namespace).Get(ctx, name, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Fatalf("cancelled gated candidate remains: %v", err)
	}
	if err := c.Get(ctx, sameKey, same); err != nil {
		t.Fatal(err)
	}
	if len(same.Status.RetiredPodCreations) != 2 || same.Status.RetiredPodCreations[1].Name != name || same.Status.CandidateFleet == nil {
		t.Fatalf("cancelled candidate identity was not retained: %#v", same.Status)
	}
	// Model a second previously allocated creation whose request is delayed
	// until after the candidate has been retired.
	lateSequence := same.Status.PodCreationSequence + 1
	same.Status.PodCreationSequence = lateSequence
	if err := c.Status().Update(ctx, same); err != nil {
		t.Fatal(err)
	}
	if _, err := core.CoreV1().Pods(same.Namespace).Get(ctx, source.Name, metav1.GetOptions{}); err != nil {
		t.Fatal("serving Pod changed during cancellation")
	}
	for i := 0; i < 12; i++ {
		if _, err := cancelReconciler.Reconcile(ctx, cancelRequest); err != nil {
			t.Fatal(err)
		}
	}
	if err := c.Get(ctx, sameKey, same); err != nil {
		t.Fatal(err)
	}
	if same.Status.CandidateFleet != nil || len(same.Status.RetiredCandidateFleets) != 1 || same.Status.RetiredCandidateFleets[0].Fleet != "green" {
		t.Fatalf("candidate retirement was not retained: %#v", same.Status)
	}
	for _, role := range []string{"search", "ingest"} {
		preview, err := workload.PreviewService(same, role, "green")
		if err != nil {
			t.Fatal(err)
		}
		if _, err := core.CoreV1().Services(same.Namespace).Get(ctx, preview.Name, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
			t.Fatalf("preview Service not removed: %v", err)
		}
	}
	lateName := podNameForSequence(same, lateSequence)
	late := previewPod.DeepCopy()
	late.Name, late.UID, late.ResourceVersion = lateName, "", ""
	late.Annotations[creationIntentKey] = fmt.Sprintf("%s/%d", same.UID, lateSequence)
	late.Status = corev1.PodStatus{}
	if err := c.Create(ctx, late); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 8; i++ {
		if _, err := cancelReconciler.Reconcile(ctx, cancelRequest); err != nil {
			t.Fatal(err)
		}
		if _, err := core.CoreV1().Pods(same.Namespace).Get(ctx, lateName, metav1.GetOptions{}); apierrors.IsNotFound(err) {
			break
		}
	}
	if _, err := core.CoreV1().Pods(same.Namespace).Get(ctx, lateName, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Fatalf("late retired candidate Pod was not withdrawn: %v", err)
	}
	if err := c.Get(ctx, sameKey, same); err != nil {
		t.Fatal(err)
	}
	if len(same.Status.RetiredPodCreations) != 3 {
		t.Fatalf("late Pod name was not retained: %#v", same.Status.RetiredPodCreations)
	}
	latePreview, err := workload.PreviewService(same, "search", "green")
	if err != nil {
		t.Fatal(err)
	}
	latePreview, err = core.CoreV1().Services(same.Namespace).Create(ctx, latePreview, metav1.CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := cancelReconciler.reconcileRetiredCandidateFleets(ctx, same); err == nil {
		t.Fatal("replacement of retired preview Service was accepted")
	}
	if _, err := core.CoreV1().Services(same.Namespace).Get(ctx, latePreview.Name, metav1.GetOptions{}); err != nil {
		t.Fatal("replacement preview Service was removed")
	}
	if err := core.CoreV1().Services(same.Namespace).Delete(ctx, latePreview.Name, metav1.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	same.Spec.Rollout.Candidate = &v1alpha1.FleetCandidate{Fleet: "green", Image: same.Spec.Image}
	if err := c.Update(ctx, same); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 5; i++ {
		if _, err := cancelReconciler.Reconcile(ctx, cancelRequest); err != nil {
			t.Fatal(err)
		}
	}
	preview, err := workload.PreviewService(same, "search", "green")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := core.CoreV1().Services(same.Namespace).Get(ctx, preview.Name, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Fatalf("committed retirement unexpectedly recreated preview Service: %v", err)
	}
	if err := c.Get(ctx, sameKey, same); err != nil {
		t.Fatal(err)
	}
	same.Spec.Rollout.Candidate.Fleet = "orange"
	if err := c.Update(ctx, same); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 8; i++ {
		if _, err := cancelReconciler.Reconcile(ctx, cancelRequest); err != nil {
			t.Fatal(err)
		}
	}
	if err := c.Get(ctx, sameKey, same); err != nil {
		t.Fatal(err)
	}
	if same.Status.CandidateFleet == nil || same.Status.CandidateFleet.Fleet != "orange" {
		t.Fatalf("new fleet did not stage after retirement: %#v", same.Status.CandidateFleet)
	}
	// Exercise normal reconciliation separately. Envtest has no PDB
	// controller, so the test explicitly acknowledges each PDB generation.
	full := &v1alpha1.OpenSearchServerless{ObjectMeta: metav1.ObjectMeta{Name: "full-preview", Namespace: "rollout-test"}, Spec: savedSameSpec}
	pool := full.Spec.Pools["search"]
	pool.Zones = map[string]v1alpha1.Zone{"a": {Replicas: 1, MinServing: 1}}
	full.Spec.Pools = map[string]v1alpha1.Pool{"search": pool}
	if err := c.Create(ctx, full); err != nil {
		t.Fatal(err)
	}
	fullSource := source.DeepCopy()
	fullSource.Name, fullSource.ResourceVersion, fullSource.UID = "full-source", "", ""
	fullSource.OwnerReferences[0].Name, fullSource.OwnerReferences[0].UID = full.Name, full.UID
	fullSource.Labels[workload.InstanceLabel] = string(full.UID)
	fullSource.Status = corev1.PodStatus{}
	if err := c.Create(ctx, fullSource); err != nil {
		t.Fatal(err)
	}
	fullSource.Status.Phase, fullSource.Status.PodIP = corev1.PodRunning, "10.1.0.3"
	fullSource.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}
	if err := c.Status().Update(ctx, fullSource); err != nil {
		t.Fatal(err)
	}
	fullRequest := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: full.Namespace, Name: full.Name}}
	fullReconciler := &InstanceReconciler{Client: c, Core: core.CoreV1(), Policy: core.PolicyV1(), DevelopmentGatedPods: true, DevelopmentReleaseGates: true}
	for i := 0; i < 14; i++ {
		if _, err := fullReconciler.Reconcile(ctx, fullRequest); err != nil {
			t.Fatal(err)
		}
	}
	if err := c.Get(ctx, fullRequest.NamespacedName, full); err != nil {
		t.Fatal(err)
	}
	if full.Status.PendingPodCreation != nil {
		t.Fatal("candidate created before PDB observation")
	}
	seenBudgets := map[string]bool{}
	for attempt := 0; attempt < 12 && len(seenBudgets) < 2; attempt++ {
		budgets, err := core.PolicyV1().PodDisruptionBudgets(full.Namespace).List(ctx, metav1.ListOptions{})
		if err != nil {
			t.Fatal(err)
		}
		for i := range budgets.Items {
			budget := &budgets.Items[i]
			owner := metav1.GetControllerOf(budget)
			if owner == nil || owner.UID != full.UID {
				continue
			}
			fleet := budget.Spec.Selector.MatchLabels[workload.FleetLabel]
			if budget.Spec.Selector.MatchLabels[workload.RoleLabel] != "search" || budget.Spec.Selector.MatchLabels[workload.ZoneLabel] != "a" || budget.Spec.MinAvailable == nil || budget.Spec.MinAvailable.IntVal != 1 {
				t.Fatalf("incorrect candidate PDB: %#v", budget.Spec)
			}
			seenBudgets[fleet] = true
			if budget.Status.ObservedGeneration != budget.Generation {
				budget.Status.ObservedGeneration = budget.Generation
				if _, err := core.PolicyV1().PodDisruptionBudgets(full.Namespace).UpdateStatus(ctx, budget, metav1.UpdateOptions{}); err != nil {
					t.Fatal(err)
				}
			}
		}
		if _, err := fullReconciler.Reconcile(ctx, fullRequest); err != nil {
			t.Fatal(err)
		}
	}
	if !seenBudgets["initial"] || !seenBudgets["green"] || len(seenBudgets) != 2 {
		t.Fatalf("missing serving or candidate PDB: %#v", seenBudgets)
	}
	for i := 0; i < 18; i++ {
		if _, err := fullReconciler.Reconcile(ctx, fullRequest); err != nil {
			t.Fatal(err)
		}
		if err := c.Get(ctx, fullRequest.NamespacedName, full); err != nil {
			t.Fatal(err)
		}
		if full.Status.PendingGateRelease != nil {
			break
		}
	}
	if full.Status.PendingGateRelease == nil {
		t.Fatal("candidate gate release was not recorded")
	}
	if full.Status.CandidateFleet == nil || len(full.Status.CandidateFleet.Capacity) != 1 || full.Status.CandidateFleet.Capacity[0].UnboundPods != 1 || full.Status.CandidateFleet.Capacity[0].Warmth != "Unknown" || full.Status.ZoneCapacityObservedAt == nil {
		t.Fatalf("candidate capacity observation missing or misleading: %#v", full.Status.CandidateFleet)
	}
	gatePodName := full.Status.PendingGateRelease.Pod.Name
	fullSource.Status.Conditions[0].Status = corev1.ConditionFalse
	if err := c.Status().Update(ctx, fullSource); err != nil {
		t.Fatal(err)
	}
	withdrawn := false
	for i := 0; i < 8; i++ {
		if _, err := fullReconciler.Reconcile(ctx, fullRequest); err != nil {
			t.Fatal(err)
		}
		gatePod, err := core.CoreV1().Pods(full.Namespace).Get(ctx, gatePodName, metav1.GetOptions{})
		if apierrors.IsNotFound(err) {
			withdrawn = true
			break
		}
		if err != nil || !hasPlacementGate(gatePod) {
			t.Fatalf("candidate gate released while source was unready: %v", err)
		}
	}
	if !withdrawn {
		t.Fatal("gated candidate did not yield to the serving shortfall")
	}
	fullSource.Status.Conditions[0].Status = corev1.ConditionTrue
	if err := c.Status().Update(ctx, fullSource); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 10; i++ {
		if _, err := fullReconciler.Reconcile(ctx, fullRequest); err != nil {
			t.Fatal(err)
		}
		if err := c.Get(ctx, fullRequest.NamespacedName, full); err != nil {
			t.Fatal(err)
		}
		if full.Status.ProvisioningAttempt != nil {
			break
		}
	}
	if full.Status.ProvisioningAttempt == nil {
		t.Fatalf("candidate gate was not released: %#v", full.Status)
	}
	fullCandidate, err := core.CoreV1().Pods(full.Namespace).Get(ctx, full.Status.ProvisioningAttempt.Name, metav1.GetOptions{})
	if err != nil || fullCandidate.Name == gatePodName || fullCandidate.Labels[workload.FleetLabel] != "green" || hasPlacementGate(fullCandidate) {
		t.Fatalf("candidate release failed: %#v %v", fullCandidate, err)
	}
	full.Spec.Rollout.Candidate = nil
	if err := c.Update(ctx, full); err != nil {
		t.Fatal(err)
	}
	if _, err := fullReconciler.Reconcile(ctx, fullRequest); err != nil {
		t.Fatal(err)
	}
	if err := c.Get(ctx, fullRequest.NamespacedName, full); err != nil {
		t.Fatal(err)
	}
	if full.Status.ProvisioningAttempt == nil || full.Status.ProvisioningAttempt.Reason != "CandidateCancelled" || full.Status.ProvisioningAttempt.Cancellation == nil {
		t.Fatalf("unbound cancellation intent missing: %#v", full.Status.ProvisioningAttempt)
	}
	if _, err := core.CoreV1().Pods(full.Namespace).Get(ctx, fullCandidate.Name, metav1.GetOptions{}); err != nil {
		t.Fatal("candidate was deleted before cancellation intent was observed")
	}
	lostResponse := false
	lostConfig := rest.CopyConfig(config)
	lostConfig.Wrap(func(next http.RoundTripper) http.RoundTripper {
		return candidateRaceTransport(func(req *http.Request) (*http.Response, error) {
			response, err := next.RoundTrip(req)
			if err == nil && req.Method == http.MethodDelete && strings.HasSuffix(req.URL.Path, "/pods/"+fullCandidate.Name) && response.StatusCode >= 200 && response.StatusCode < 300 {
				lostResponse = true
				response.Body.Close()
				return nil, io.ErrUnexpectedEOF
			}
			return response, err
		})
	})
	lostCore, err := kubernetes.NewForConfig(lostConfig)
	if err != nil {
		t.Fatal(err)
	}
	fullReconciler.Core = lostCore.CoreV1()
	if _, err := fullReconciler.Reconcile(ctx, fullRequest); err == nil || !lostResponse {
		t.Fatalf("expected an ambiguous successful withdrawal: %v", err)
	}
	fullReconciler.Core = core.CoreV1()
	for i := 0; i < 8; i++ {
		if _, err := fullReconciler.Reconcile(ctx, fullRequest); err != nil {
			t.Fatal(err)
		}
		if err := c.Get(ctx, fullRequest.NamespacedName, full); err != nil {
			t.Fatal(err)
		}
		if full.Status.ProvisioningAttempt == nil {
			break
		}
	}
	if full.Status.ProvisioningAttempt != nil || len(full.Status.RetiredPodCreations) != 2 || full.Status.RetiredPodCreations[1].Name != fullCandidate.Name {
		t.Fatalf("unbound cancellation did not retain tombstone: %#v", full.Status)
	}
	if _, err := core.CoreV1().Pods(full.Namespace).Get(ctx, fullCandidate.Name, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Fatalf("cancelled unbound Pod remains: %v", err)
	}
	if _, err := core.CoreV1().Pods(full.Namespace).Get(ctx, fullSource.Name, metav1.GetOptions{}); err != nil {
		t.Fatal("serving Pod changed during unbound cancellation")
	}
	for i := 0; i < 14; i++ {
		if _, err := fullReconciler.Reconcile(ctx, fullRequest); err != nil {
			t.Fatal(err)
		}
		if err := c.Get(ctx, fullRequest.NamespacedName, full); err != nil {
			t.Fatal(err)
		}
		if full.Status.CandidateFleet == nil {
			break
		}
	}
	if full.Status.CandidateFleet != nil || len(full.Status.RetiredCandidateFleets) != 1 {
		t.Fatal("empty candidate did not enter retired history")
	}
	candidateBudgetName := disruptionBudgetName(full, "green", "search", "a")
	servingBudgetName := disruptionBudgetName(full, "initial", "search", "a")
	oldBudget, err := core.PolicyV1().PodDisruptionBudgets(full.Namespace).Get(ctx, candidateBudgetName, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if _, changed, err := fullReconciler.reconcileRetiredCandidateFleets(ctx, full); err != nil || !changed || len(full.Status.RetiredCandidateFleets[0].DisruptionBudgets) != 1 {
		t.Fatalf("candidate budget UID not pinned: %v", err)
	}
	var budgetRaceOnce sync.Once
	var budgetMutationErr error
	budgetRaceConfig := rest.CopyConfig(config)
	budgetRaceConfig.Wrap(func(next http.RoundTripper) http.RoundTripper {
		return candidateRaceTransport(func(req *http.Request) (*http.Response, error) {
			if req.Method == http.MethodDelete && strings.HasSuffix(req.URL.Path, "/poddisruptionbudgets/"+candidateBudgetName) {
				budgetRaceOnce.Do(func() {
					var budget *policyv1.PodDisruptionBudget
					budget, budgetMutationErr = core.PolicyV1().PodDisruptionBudgets(full.Namespace).Get(ctx, candidateBudgetName, metav1.GetOptions{})
					if budgetMutationErr == nil {
						budget.Annotations = map[string]string{"concurrent-edit": "true"}
						_, budgetMutationErr = core.PolicyV1().PodDisruptionBudgets(full.Namespace).Update(ctx, budget, metav1.UpdateOptions{})
					}
				})
				if budgetMutationErr != nil {
					return nil, budgetMutationErr
				}
			}
			return next.RoundTrip(req)
		})
	})
	budgetRacingCore, err := kubernetes.NewForConfig(budgetRaceConfig)
	if err != nil {
		t.Fatal(err)
	}
	budgetRacingReconciler := &InstanceReconciler{Client: c, Core: core.CoreV1(), Policy: budgetRacingCore.PolicyV1(), DevelopmentGatedPods: true}
	if _, _, err := budgetRacingReconciler.reconcileRetiredCandidateFleets(ctx, full); !apierrors.IsConflict(err) || budgetMutationErr != nil {
		t.Fatalf("expected PDB version conflict: %v, %v", err, budgetMutationErr)
	}
	if _, err := core.PolicyV1().PodDisruptionBudgets(full.Namespace).Get(ctx, candidateBudgetName, metav1.GetOptions{}); err != nil {
		t.Fatal("changed PDB was deleted")
	}
	budgetLostResponse := false
	budgetLostConfig := rest.CopyConfig(config)
	budgetLostConfig.Wrap(func(next http.RoundTripper) http.RoundTripper {
		return candidateRaceTransport(func(req *http.Request) (*http.Response, error) {
			response, err := next.RoundTrip(req)
			if err == nil && req.Method == http.MethodDelete && strings.HasSuffix(req.URL.Path, "/poddisruptionbudgets/"+candidateBudgetName) && response.StatusCode >= 200 && response.StatusCode < 300 {
				budgetLostResponse = true
				response.Body.Close()
				return nil, io.ErrUnexpectedEOF
			}
			return response, err
		})
	})
	budgetLostCore, err := kubernetes.NewForConfig(budgetLostConfig)
	if err != nil {
		t.Fatal(err)
	}
	budgetRacingReconciler.Policy = budgetLostCore.PolicyV1()
	if _, _, err := budgetRacingReconciler.reconcileRetiredCandidateFleets(ctx, full); err == nil || !budgetLostResponse {
		t.Fatalf("expected a lost successful PDB delete: %v", err)
	}
	if _, changed, err := fullReconciler.reconcileRetiredCandidateFleets(ctx, full); err != nil || changed {
		t.Fatalf("candidate budget deletion did not recover: %v", err)
	}
	replacementBudget, err := workload.DisruptionBudget(full, candidateBudgetName, "green", "search", "a", 1)
	if err != nil {
		t.Fatal(err)
	}
	replacementBudget, err = core.PolicyV1().PodDisruptionBudgets(full.Namespace).Create(ctx, replacementBudget, metav1.CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if replacementBudget.UID == oldBudget.UID {
		t.Fatal("replacement PDB reused old UID")
	}
	if _, _, err := fullReconciler.reconcileRetiredCandidateFleets(ctx, full); err == nil {
		t.Fatal("replacement PDB was accepted for deletion")
	}
	if _, err := core.PolicyV1().PodDisruptionBudgets(full.Namespace).Get(ctx, candidateBudgetName, metav1.GetOptions{}); err != nil {
		t.Fatal("replacement PDB was deleted")
	}
	if _, err := core.PolicyV1().PodDisruptionBudgets(full.Namespace).Get(ctx, servingBudgetName, metav1.GetOptions{}); err != nil {
		t.Fatal("serving PDB was deleted")
	}
	// Bind the candidate after the withdrawal read but before the DELETE
	// reaches the API server, as a scheduler could do concurrently.
	race := &v1alpha1.OpenSearchServerless{ObjectMeta: metav1.ObjectMeta{Name: "cancel-race", Namespace: "rollout-test"}, Spec: savedSameSpec}
	race.Spec.Rollout = &v1alpha1.Rollout{Strategy: "BlueGreen"}
	if err := c.Create(ctx, race); err != nil {
		t.Fatal(err)
	}
	race.Status.ServingFleetIntentByRole = map[string]string{"search": "blue", "ingest": "blue"}
	race.Status.CandidateFleet = &v1alpha1.CandidateFleetStatus{Fleet: "green", Image: race.Spec.Image, Revision: "revision", Generation: race.Generation}
	race.Status.PodCreationSequence = 1
	raceName := podNameForSequence(race, 1)
	racePod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: raceName, Namespace: race.Namespace, OwnerReferences: []metav1.OwnerReference{{APIVersion: v1alpha1.GroupVersion.String(), Kind: "OpenSearchServerless", Name: race.Name, UID: race.UID, Controller: &isController}}, Labels: map[string]string{workload.InstanceLabel: string(race.UID), workload.RoleLabel: "search", workload.ZoneLabel: "a", workload.FleetLabel: "green", "serverless.opensearch.org/revision": "revision"}, Annotations: map[string]string{creationIntentKey: fmt.Sprintf("%s/1", race.UID)}}, Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "opensearch", Image: race.Spec.Image}}}}
	if err := c.Create(ctx, racePod); err != nil {
		t.Fatal(err)
	}
	race.Status.ProvisioningAttempt = &v1alpha1.ProvisioningAttempt{Name: raceName, PodUID: string(racePod.UID), Role: "search", Zone: "a", StartedAt: metav1.Now(), Phase: "Watching"}
	if err := c.Status().Update(ctx, race); err != nil {
		t.Fatal(err)
	}
	racingReconciler := &InstanceReconciler{Client: c, Core: core.CoreV1()}
	if _, changed, err := racingReconciler.reconcileProvisioningAttempt(ctx, race); err != nil || !changed || race.Status.ProvisioningAttempt.Reason != "CandidateCancelled" {
		t.Fatalf("race cancellation intent missing: %v", err)
	}
	var once sync.Once
	var bindErr error
	raceConfig := rest.CopyConfig(config)
	raceConfig.Wrap(func(next http.RoundTripper) http.RoundTripper {
		return candidateRaceTransport(func(req *http.Request) (*http.Response, error) {
			if req.Method == http.MethodDelete && strings.HasSuffix(req.URL.Path, "/pods/"+raceName) {
				once.Do(func() {
					bindErr = core.CoreV1().Pods(race.Namespace).Bind(ctx, &corev1.Binding{ObjectMeta: metav1.ObjectMeta{Name: raceName, UID: racePod.UID}, Target: corev1.ObjectReference{Kind: "Node", Name: "node-race"}}, metav1.CreateOptions{})
				})
				if bindErr != nil {
					return nil, fmt.Errorf("inject scheduler binding: %w", bindErr)
				}
			}
			return next.RoundTrip(req)
		})
	})
	racingCore, err := kubernetes.NewForConfig(raceConfig)
	if err != nil {
		t.Fatal(err)
	}
	racingReconciler.Core = racingCore.CoreV1()
	if _, _, err := racingReconciler.reconcileProvisioningAttempt(ctx, race); !apierrors.IsConflict(err) || bindErr != nil {
		t.Fatalf("expected binding conflict: %v; bind: %v", err, bindErr)
	}
	bound, err := core.CoreV1().Pods(race.Namespace).Get(ctx, raceName, metav1.GetOptions{})
	if err != nil || bound.Spec.NodeName != "node-race" || bound.DeletionTimestamp != nil {
		t.Fatalf("concurrently bound candidate was not retained: %#v %v", bound, err)
	}
	racingReconciler.Core = core.CoreV1()
	if _, changed, err := racingReconciler.reconcileProvisioningAttempt(ctx, race); err != nil || !changed || race.Status.ProvisioningAttempt.Phase != "CheckingStartup" {
		t.Fatalf("bound candidate was not returned to monitoring: %v", err)
	}
	instance := &v1alpha1.OpenSearchServerless{ObjectMeta: metav1.ObjectMeta{Name: "catalog", Namespace: "rollout-test"}, Spec: staged.Spec}
	rolloutConfig := *instance.Spec.Rollout
	rolloutConfig.RollbackWindow = "30m"
	candidateConfig := *rolloutConfig.Candidate
	candidateConfig.Image = instance.Spec.Image
	rolloutConfig.Candidate = &candidateConfig
	instance.Spec.Rollout = &rolloutConfig
	instance.Spec.Pools = map[string]v1alpha1.Pool{
		"search": {Zones: map[string]v1alpha1.Zone{"a": {Replicas: 1, MinServing: 1}, "b": {Replicas: 1, MinServing: 1}}},
		"ingest": {Zones: map[string]v1alpha1.Zone{"a": {Replicas: 1, MinServing: 1}}},
	}
	if err := c.Create(ctx, instance); err != nil {
		t.Fatal(err)
	}
	servingRevision, err := podRevision(instance, "")
	if err != nil {
		t.Fatal(err)
	}
	beforeRollout := instance.Spec
	beforeRollout.Rollout = nil
	sourceRevision, err := hashPodTemplate(beforeRollout, "")
	if err != nil {
		t.Fatal(err)
	}
	op := &v1alpha1.OpenSearchServerlessOperation{ObjectMeta: metav1.ObjectMeta{Name: "promotion", Namespace: "rollout-test"}, Spec: v1alpha1.OperationSpec{InstanceRef: v1alpha1.InstanceReference{Name: "catalog", UID: string(instance.UID)}, Action: "Promote", TargetFleet: "green", ExpectedServingFleetByRole: map[string]string{"search": "blue", "ingest": "blue"}}}
	if err := c.Create(ctx, op); err != nil {
		t.Fatal(err)
	}
	instance.Status.ActiveOperation = &v1alpha1.OperationClaim{Name: op.Name, UID: string(op.UID), Action: "Promote", Generation: op.Generation}
	instance.Status.ServingFleetIntentByRole = map[string]string{"search": "blue", "ingest": "blue"}
	if err := c.Status().Update(ctx, instance); err != nil {
		t.Fatal(err)
	}
	search, err := workload.RoleService(instance, "search", "blue", 9200)
	if err != nil {
		t.Fatal(err)
	}
	ingest, err := workload.RoleService(instance, "ingest", "blue", 9200)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Create(ctx, search); err != nil {
		t.Fatal(err)
	}
	if err := c.Create(ctx, ingest); err != nil {
		t.Fatal(err)
	}
	search.Spec.ExternalIPs = []string{"192.0.2.11"}
	if err := c.Update(ctx, search); err != nil {
		t.Fatal(err)
	}
	if err := ensureRoleServices(ctx, core.CoreV1(), instance); err == nil {
		t.Fatal("stable Service with an external IP was accepted")
	}
	if err := c.Get(ctx, client.ObjectKeyFromObject(search), search); err != nil || len(search.Spec.ExternalIPs) != 1 {
		t.Fatalf("stable Service drift was overwritten: %v, %v", search.Spec.ExternalIPs, err)
	}
	search.Spec.ExternalIPs = nil
	if err := c.Update(ctx, search); err != nil {
		t.Fatal(err)
	}
	previewUIDs, err := ensurePreviewServices(ctx, nil, core.CoreV1(), instance, "green", nil)
	if err != nil || previewUIDs["search"] == "" || previewUIDs["ingest"] == "" || previewUIDs["search"] == previewUIDs["ingest"] {
		t.Fatalf("preview Services not created: %#v %v", previewUIDs, err)
	}
	again, err := ensurePreviewServices(ctx, nil, core.CoreV1(), instance, "green", previewUIDs)
	if err != nil || again["search"] != previewUIDs["search"] || again["ingest"] != previewUIDs["ingest"] {
		t.Fatalf("preview Services not recovered: %#v %v", again, err)
	}
	instance.Status.CandidateFleet = &v1alpha1.CandidateFleetStatus{
		Fleet: "green", Image: instance.Spec.Rollout.Candidate.Image, Revision: servingRevision, Generation: instance.Generation,
		SearchServiceUID: string(previewUIDs["search"]), IngestServiceUID: string(previewUIDs["ingest"]),
	}
	if err := c.Status().Update(ctx, instance); err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 1, 1, 0, 0, 0, 123, time.UTC)
	r := &OperationReconciler{Client: c, Core: core.CoreV1(), Policy: core.PolicyV1(), Now: func() time.Time { return now }}
	candidate := &rollout.CandidateEvidence{InstanceUID: string(instance.UID), OperationUID: string(op.UID), Role: "search", Fleet: "green", Revision: servingRevision, CapacityQualified: true, PeerQualified: true, SecurityQualified: true, RollbackCompatible: true, ObservedAt: now}
	candidates := map[string]*rollout.CandidateEvidence{"search": candidate}
	ingestCandidate := *candidate
	ingestCandidate.Role = "ingest"
	bothQualified := map[string]*rollout.CandidateEvidence{"search": candidate, "ingest": &ingestCandidate}
	if err := r.prepareObservedPromotion(ctx, c, instance, op, bothQualified, time.Minute); err == nil || op.Status.RolloutRoles != nil {
		t.Fatalf("promotion prepared without target budgets: %#v %v", op.Status.RolloutRoles, err)
	}
	budgets := core.PolicyV1().PodDisruptionBudgets(instance.Namespace)
	for _, fleet := range []string{"blue", "green"} {
		for role, pool := range instance.Spec.Pools {
			for zone := range pool.Zones {
				wanted, err := workload.DisruptionBudget(instance, disruptionBudgetName(instance, fleet, role, zone), fleet, role, zone, 1)
				if err != nil {
					t.Fatal(err)
				}
				budget, err := budgets.Create(ctx, wanted, metav1.CreateOptions{})
				if err != nil {
					t.Fatal(err)
				}
				budget.Status = policyv1.PodDisruptionBudgetStatus{
					ObservedGeneration: budget.Generation, DesiredHealthy: 1, CurrentHealthy: 1, ExpectedPods: 1,
					Conditions: []metav1.Condition{{Type: policyv1.DisruptionAllowedCondition, Status: metav1.ConditionFalse, Reason: "InsufficientPods", LastTransitionTime: metav1.NewTime(now)}},
				}
				if _, err := budgets.UpdateStatus(ctx, budget, metav1.UpdateOptions{}); err != nil {
					t.Fatal(err)
				}
			}
		}
	}
	if err := r.prepareObservedPromotion(ctx, c, instance, op, bothQualified, time.Minute); err == nil || op.Status.RolloutRoles != nil {
		t.Fatalf("promotion prepared without source revisions: %#v %v", op.Status.RolloutRoles, err)
	}
	for _, svc := range []*corev1.Service{search, ingest} {
		role := svc.Spec.Selector[workload.RoleLabel]
		for zone := range instance.Spec.Pools[role].Zones {
			name := svc.Name + "-source"
			if zone != "a" {
				name += "-" + zone
			}
			pod := &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: instance.Namespace,
					OwnerReferences: []metav1.OwnerReference{*metav1.NewControllerRef(instance, v1alpha1.GroupVersion.WithKind("OpenSearchServerless"))},
					Labels: map[string]string{
						workload.InstanceLabel: string(instance.UID), workload.RoleLabel: role, workload.ZoneLabel: zone,
						workload.FleetLabel: "blue", podRevisionLabel: sourceRevision,
					}},
				Spec: corev1.PodSpec{NodeName: "fixture-node", Containers: []corev1.Container{{Name: "opensearch", Image: instance.Spec.Image}}},
			}
			pods := core.CoreV1().Pods(instance.Namespace)
			pod, err := pods.Create(ctx, pod, metav1.CreateOptions{})
			if err != nil {
				t.Fatal(err)
			}
			pod.Status.Phase = corev1.PodRunning
			pod.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}
			if _, err := pods.UpdateStatus(ctx, pod, metav1.UpdateOptions{}); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := r.prepareObservedPromotion(ctx, c, instance, op, candidates, time.Minute); err == nil || op.Status.RolloutRoles != nil {
		t.Fatalf("promotion prepared without ingest qualification: %#v %v", op.Status.RolloutRoles, err)
	}
	candidates = bothQualified
	preview, err = workload.PreviewService(instance, "search", "green")
	if err != nil {
		t.Fatal(err)
	}
	preview, err = core.CoreV1().Services(instance.Namespace).Get(ctx, preview.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	preview.Spec.Selector[workload.FleetLabel] = "foreign"
	if _, err := core.CoreV1().Services(instance.Namespace).Update(ctx, preview, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := ensurePreviewServices(ctx, nil, core.CoreV1(), instance, "green", previewUIDs); err == nil {
		t.Fatal("conflicting preview selector accepted")
	}
	preview, err = core.CoreV1().Services(instance.Namespace).Get(ctx, preview.Name, metav1.GetOptions{})
	if err != nil || preview.Spec.Selector[workload.FleetLabel] != "foreign" {
		t.Fatal("conflicting preview Service was overwritten")
	}
	if err := core.CoreV1().Services(instance.Namespace).Delete(ctx, preview.Name, metav1.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := ensurePreviewServices(ctx, nil, core.CoreV1(), instance, "green", previewUIDs); err == nil {
		t.Fatal("missing pinned preview Service accepted")
	}
	if _, err := core.CoreV1().Services(instance.Namespace).Get(ctx, preview.Name, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Fatalf("missing pinned Service was recreated: %v", err)
	}
	if _, err := ensurePreviewServices(ctx, nil, core.CoreV1(), instance, "blue", nil); err == nil {
		t.Fatal("serving fleet accepted as preview")
	}
	stale := op.DeepCopyObject().(*v1alpha1.OpenSearchServerlessOperation)
	if err := r.prepareObservedPromotion(ctx, c, instance, op, candidates, time.Minute); err == nil || op.Status.RolloutRoles != nil {
		t.Fatalf("promotion prepared without the pinned search preview: %#v %v", op.Status.RolloutRoles, err)
	}
	recreated, err := ensurePreviewServices(ctx, nil, core.CoreV1(), instance, "green", nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := r.prepareObservedPromotion(ctx, c, instance, op, candidates, time.Minute); err == nil || op.Status.RolloutRoles != nil {
		t.Fatalf("promotion prepared using a replacement preview: %#v %v", op.Status.RolloutRoles, err)
	}
	instance.Status.CandidateFleet.SearchServiceUID = string(recreated["search"])
	if err := c.Status().Update(ctx, instance); err != nil {
		t.Fatal(err)
	}
	published := prepareWithPublishedEvidence(t, ctx, c, core, instance, op)
	if err := r.prepareObservedPromotion(ctx, c, instance, stale, candidates, time.Minute); !apierrors.IsConflict(err) {
		t.Fatalf("stale write accepted: %v", err)
	}
	observed := &v1alpha1.OpenSearchServerlessOperation{}
	if err := c.Get(ctx, types.NamespacedName{Name: op.Name, Namespace: op.Namespace}, observed); err != nil {
		t.Fatal(err)
	}
	if len(observed.Status.RolloutRoles) != 2 || observed.Status.RolloutRoles["search"].CandidateRevision != servingRevision ||
		observed.Status.RolloutRoles["search"].SourceRevision != sourceRevision || observed.Status.RolloutRoles["ingest"].SourceRevision != sourceRevision ||
		observed.Status.RolloutRoles["search"].ServiceUID != string(search.UID) || observed.Status.RolloutRoles["ingest"].ServiceUID != string(ingest.UID) {
		t.Fatalf("intent was pruned or changed: %#v", observed.Status.RolloutRoles)
	}
	if action, err := r.observeAndAdvanceRollout(ctx, instance, observed, candidates, nil, time.Minute); err != nil || action.Switch.Kind != rollout.PatchService {
		t.Fatalf("live Services did not yield a patch plan: %#v %v", action, err)
	}
	ingest.Spec.Selector[workload.FleetLabel] = "foreign"
	if err := c.Update(ctx, ingest); err != nil {
		t.Fatal(err)
	}
	if action, err := r.observeAndAdvanceRollout(ctx, instance, observed, candidates, nil, time.Minute); err != nil || action.Switch.Kind != rollout.Hold || action.Role != "ingest" {
		t.Fatalf("live selector conflict was not held: %#v %v", action, err)
	}
	ingest.Spec.Selector[workload.FleetLabel] = "blue"
	if err := c.Update(ctx, ingest); err != nil {
		t.Fatal(err)
	}
	key := types.NamespacedName{Name: op.Name, Namespace: op.Namespace}
	readEvidence := func(context.Context, *v1alpha1.OpenSearchServerless, *v1alpha1.OpenSearchServerlessOperation) (rolloutEvidence, error) {
		return rolloutEvidence{Candidates: candidates}, nil
	}
	zoneBudget, err := budgets.Get(ctx, disruptionBudgetName(instance, "green", "search", "b"), metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	zoneBudget.Status.CurrentHealthy = 0
	zoneBudget, err = budgets.UpdateStatus(ctx, zoneBudget, metav1.UpdateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if action, err := r.executePreparedRolloutStep(ctx, c, key, op.UID, readEvidence, time.Minute); err != nil || action.Switch.Kind != rollout.Hold || action.Switch.Reason != "TargetDisruptionBudgetsUnqualified" {
		t.Fatalf("zone below its serving floor did not hold before switch: %#v %v", action, err)
	}
	zoneBudget.Status.CurrentHealthy = 1
	if _, err := budgets.UpdateStatus(ctx, zoneBudget, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	onlySearch := func(context.Context, *v1alpha1.OpenSearchServerless, *v1alpha1.OpenSearchServerlessOperation) (rolloutEvidence, error) {
		return rolloutEvidence{Candidates: map[string]*rollout.CandidateEvidence{"search": candidate}}, nil
	}
	if action, err := r.executePreparedRolloutStep(ctx, c, key, op.UID, onlySearch, time.Minute); err != nil || action.Switch.Kind != rollout.Hold || action.Role != "ingest" || action.Switch.Reason != "CandidateNotQualified" {
		t.Fatalf("lost ingest qualification did not block first switch: %#v %v", action, err)
	}
	instance.Status.CandidateFleet.Revision = "replacement"
	if err := c.Status().Update(ctx, instance); err != nil {
		t.Fatal(err)
	}
	if action, err := r.executePreparedRolloutStep(ctx, c, key, op.UID, readEvidence, time.Minute); err != nil || action.Switch.Kind != rollout.Hold || action.Switch.Reason != "PromotionCandidateChanged" {
		t.Fatalf("replaced candidate revision did not block switch: %#v %v", action, err)
	}
	instance.Status.CandidateFleet.Revision = candidate.Revision
	if err := c.Status().Update(ctx, instance); err != nil {
		t.Fatal(err)
	}
	if current, err := core.CoreV1().Services(instance.Namespace).Get(ctx, search.Name, metav1.GetOptions{}); err != nil || current.Spec.Selector[workload.FleetLabel] != "blue" {
		t.Fatalf("search changed during a hold: %#v %v", current, err)
	}
	changeState := func(ctx context.Context, current *v1alpha1.OpenSearchServerless, _ *v1alpha1.OpenSearchServerlessOperation) (rolloutEvidence, error) {
		current.Annotations = map[string]string{"test": "changed-during-evidence"}
		return rolloutEvidence{Candidates: candidates}, c.Update(ctx, current)
	}
	if action, err := r.executePreparedRolloutStep(ctx, c, key, op.UID, changeState, time.Minute); err != nil || action.Switch.Kind != rollout.Hold || action.Switch.Reason != "RolloutStateChanged" {
		t.Fatalf("concurrent instance update did not block switch: %#v %v", action, err)
	}
	changeStateDuringRecheck := func(_ context.Context, current *v1alpha1.OpenSearchServerless, _ *v1alpha1.OpenSearchServerlessOperation) (rolloutEvidence, error) {
		return rolloutEvidence{Candidates: candidates, revalidate: func(ctx context.Context, _ *rolloutEvidence) {
			current.Annotations["test"] = "changed-during-final-recheck"
			if err := c.Update(ctx, current); err != nil {
				t.Fatal(err)
			}
		}}, nil
	}
	if action, err := r.executePreparedRolloutStep(ctx, c, key, op.UID, changeStateDuringRecheck, time.Minute); err != nil || action.Switch.Kind != rollout.Hold || action.Switch.Reason != "RolloutStateChanged" {
		t.Fatalf("instance update during the final recheck did not block the switch: %#v %v", action, err)
	}
	setSourceRevision := func(revision string) {
		t.Helper()
		pods := core.CoreV1().Pods(instance.Namespace)
		pod, err := pods.Get(ctx, ingest.Name+"-source", metav1.GetOptions{})
		if err != nil {
			t.Fatal(err)
		}
		pod.Labels["serverless.opensearch.org/revision"] = revision
		if _, err := pods.Update(ctx, pod, metav1.UpdateOptions{}); err != nil {
			t.Fatal(err)
		}
	}
	setSourceRevision("different")
	if action, err := r.executePreparedRolloutStep(ctx, c, key, op.UID, readEvidence, time.Minute); err != nil || action.Switch.Kind != rollout.Hold || action.Switch.Reason != "SourceRevisionUnqualified" || action.Role != "ingest" {
		t.Fatalf("changed source revision did not block the first switch: %#v %v", action, err)
	}
	if current, err := core.CoreV1().Services(instance.Namespace).Get(ctx, search.Name, metav1.GetOptions{}); err != nil || current.Spec.Selector[workload.FleetLabel] != "blue" {
		t.Fatalf("source revision hold did not preserve the search selector: %#v %v", current, err)
	}
	setSourceRevision(sourceRevision)
	setSourceZoneReady := func(status corev1.ConditionStatus) {
		t.Helper()
		pods := core.CoreV1().Pods(instance.Namespace)
		pod, err := pods.Get(ctx, search.Name+"-source-b", metav1.GetOptions{})
		if err != nil {
			t.Fatal(err)
		}
		pod.Status.Conditions[0].Status = status
		if _, err := pods.UpdateStatus(ctx, pod, metav1.UpdateOptions{}); err != nil {
			t.Fatal(err)
		}
	}
	setSourceZoneReady(corev1.ConditionFalse)
	if action, err := r.executePreparedRolloutStep(ctx, c, key, op.UID, readEvidence, time.Minute); err != nil ||
		action.Switch.Kind != rollout.Hold || action.Switch.Reason != "SourceRevisionUnqualified" || action.Role != "search" {
		t.Fatalf("loss of source search zone did not hold promotion: %#v %v", action, err)
	}
	setSourceZoneReady(corev1.ConditionTrue)
	setSourceZoneDisruption := func(status corev1.ConditionStatus) {
		t.Helper()
		pods := core.CoreV1().Pods(instance.Namespace)
		pod, err := pods.Get(ctx, search.Name+"-source-b", metav1.GetOptions{})
		if err != nil {
			t.Fatal(err)
		}
		if status == corev1.ConditionTrue {
			pod.Status.Conditions = append(pod.Status.Conditions, corev1.PodCondition{Type: corev1.DisruptionTarget, Status: status, Reason: "PreemptionByScheduler"})
		} else {
			pod.Status.Conditions[len(pod.Status.Conditions)-1].Status = status
		}
		if _, err := pods.UpdateStatus(ctx, pod, metav1.UpdateOptions{}); err != nil {
			t.Fatal(err)
		}
	}
	disruptSourceDuringRecheck := func(ctx context.Context, cluster *v1alpha1.OpenSearchServerless, operation *v1alpha1.OpenSearchServerlessOperation) (rolloutEvidence, error) {
		evidence, err := readEvidence(ctx, cluster, operation)
		evidence.revalidate = func(context.Context, *rolloutEvidence) { setSourceZoneDisruption(corev1.ConditionTrue) }
		return evidence, err
	}
	if action, err := r.executePreparedRolloutStep(ctx, c, key, op.UID, disruptSourceDuringRecheck, time.Minute); err != nil ||
		action.Switch.Kind != rollout.Hold || action.Switch.Reason != "SourceRevisionUnqualified" || action.Role != "search" {
		t.Fatalf("a source disruption after qualification did not hold promotion: %#v %v", action, err)
	}
	setSourceZoneDisruption(corev1.ConditionFalse)
	r.Now = func() time.Time { now = time.Now(); return now }
	r.rolloutSourceProbe = func(ctx context.Context, instance *v1alpha1.OpenSearchServerless, pod *corev1.Pod, role string) error {
		if role == "ingest" {
			return fmt.Errorf("source stats lack the JVM identity")
		}
		return published.sourceProbe(ctx, instance, pod, role)
	}
	if action, err := r.executePreparedRolloutStep(ctx, c, key, op.UID, published.readEvidence, time.Minute); err != nil ||
		action.Switch.Kind != rollout.Hold || action.Switch.Reason != "SourceProcessUnqualified" || action.Role != "ingest" {
		t.Fatalf("unverifiable rollback source did not hold promotion: %#v %v", action, err)
	}
	if current, err := core.CoreV1().Services(instance.Namespace).Get(ctx, search.Name, metav1.GetOptions{}); err != nil || current.Spec.Selector[workload.FleetLabel] != "blue" {
		t.Fatalf("source process hold did not preserve the search selector: %#v %v", current, err)
	}
	r.rolloutSourceProbe = published.sourceProbe
	changeSignedReportDuringRecheck := func(ctx context.Context, cluster *v1alpha1.OpenSearchServerless, operation *v1alpha1.OpenSearchServerlessOperation) (rolloutEvidence, error) {
		evidence, err := published.readEvidence(ctx, cluster, operation)
		if err != nil {
			return evidence, err
		}
		if evidence.Candidates["search"] == nil || evidence.Candidates["ingest"] == nil || evidence.revalidate == nil {
			t.Fatal("the published reports must qualify before simulating a racing update")
		}
		recheck := evidence.revalidate
		evidence.revalidate = func(ctx context.Context, evidence *rolloutEvidence) {
			reports := core.CoreV1().ConfigMaps(cluster.Namespace)
			report, err := reports.Get(ctx, rollout.PlatformEvidenceName(operation.UID, "search"), metav1.GetOptions{})
			if err != nil {
				t.Fatal(err)
			}
			report.Annotations = map[string]string{"test.example.org/changed": "true"}
			if _, err := reports.Update(ctx, report, metav1.UpdateOptions{}); err != nil {
				t.Fatal(err)
			}
			recheck(ctx, evidence)
		}
		return evidence, nil
	}
	if action, err := r.executePreparedRolloutStep(ctx, c, key, op.UID, changeSignedReportDuringRecheck, time.Minute); err != nil ||
		action.Switch.Kind != rollout.Hold || action.Role != "search" {
		t.Fatalf("updated signed search report did not hold promotion: %#v %v", action, err)
	}
	if current, err := core.CoreV1().Services(instance.Namespace).Get(ctx, search.Name, metav1.GetOptions{}); err != nil || current.Spec.Selector[workload.FleetLabel] != "blue" {
		t.Fatalf("changed platform report allowed a Service switch: %#v %v", current, err)
	}
	if action, err := r.executePreparedRolloutStep(ctx, c, key, op.UID, published.readEvidence, time.Minute); err != nil || action.Switch.Kind != rollout.PatchService || action.Role != "search" {
		t.Fatalf("execute search switch: %#v %v", action, err)
	}
	if current, err := core.CoreV1().Services(instance.Namespace).Get(ctx, search.Name, metav1.GetOptions{}); err != nil || current.UID != search.UID || current.Spec.Selector[workload.FleetLabel] != "green" {
		t.Fatalf("search Service did not switch in place: %#v %v", current, err)
	}
	if current, err := core.CoreV1().Services(instance.Namespace).Get(ctx, ingest.Name, metav1.GetOptions{}); err != nil || current.Spec.Selector[workload.FleetLabel] != "blue" {
		t.Fatalf("ingest switched before search convergence: %#v %v", current, err)
	}
	setSourceRevision("different")
	if action, err := r.executePreparedRolloutStep(ctx, c, key, op.UID, published.readEvidence, time.Minute); err != nil || action.Switch.Kind != rollout.RecordApplied {
		t.Fatalf("record observed switch: %#v %v", action, err)
	}
	setSourceRevision(sourceRevision)
	if err := c.Get(ctx, types.NamespacedName{Name: op.Name, Namespace: op.Namespace}, observed); err != nil {
		t.Fatal(err)
	}
	if applied := observed.Status.RolloutRoles["search"].AppliedAt; applied == nil || applied.Time.Before(now) {
		t.Fatalf("serialized applied time predates observation: %#v", applied)
	}
	published.publishEndpoints("search")
	published.oldSearchRoute.Store(true)
	if action, err := r.executePreparedRolloutStep(ctx, c, key, op.UID, published.readEvidence, time.Minute); err != nil ||
		action.Switch.Kind != rollout.Hold || action.Switch.Reason != "RouteNotConverged" {
		t.Fatalf("old search responder did not hold the indexing switch: %#v %v", action, err)
	}
	if current, err := core.CoreV1().Services(instance.Namespace).Get(ctx, ingest.Name, metav1.GetOptions{}); err != nil || current.Spec.Selector[workload.FleetLabel] != "blue" {
		t.Fatalf("indexing switched with a stale search route: %#v %v", current, err)
	}
	published.oldSearchRoute.Store(false)
	changeEndpointSlice := func(serviceName string) {
		t.Helper()
		slices := core.DiscoveryV1().EndpointSlices(instance.Namespace)
		slice, err := slices.Get(ctx, serviceName+"-green", metav1.GetOptions{})
		if err != nil {
			t.Fatal(err)
		}
		slice.Annotations = map[string]string{"test": "changed-after-rollout-probe"}
		if _, err := slices.Update(ctx, slice, metav1.UpdateOptions{}); err != nil {
			t.Fatal(err)
		}
	}
	qualifiedRoutes := func(ctx context.Context, _ *v1alpha1.OpenSearchServerless, _ *v1alpha1.OpenSearchServerlessOperation) (rolloutEvidence, error) {
		routes := make(map[string]*rollout.RouteEvidence, 2)
		for role, name := range map[string]string{"search": search.Name, "ingest": ingest.Name} {
			service, err := core.CoreV1().Services(instance.Namespace).Get(ctx, name, metav1.GetOptions{})
			if err != nil {
				return rolloutEvidence{}, err
			}
			routes[role] = &rollout.RouteEvidence{
				InstanceUID: string(instance.UID), OperationUID: string(op.UID), Role: role,
				Fleet: service.Spec.Selector[workload.FleetLabel], ServiceUID: string(service.UID), ServiceVersion: service.ResourceVersion,
				ObservedAt: now, EndpointsConverged: true, ClientProbePassed: true,
			}
		}
		return rolloutEvidence{Candidates: candidates, Routes: routes}, nil
	}
	for _, want := range []struct {
		role string
		kind rollout.ActionKind
	}{
		{"search", rollout.RecordConverged},
		{"ingest", rollout.PatchService},
		{"ingest", rollout.RecordApplied},
		{"ingest", rollout.RecordConverged},
	} {
		if want.role == "ingest" && want.kind == rollout.PatchService {
			changed := false
			r.rolloutSourceProbe = func(ctx context.Context, i *v1alpha1.OpenSearchServerless, pod *corev1.Pod, role string) error {
				if !changed {
					changed = true
					changeEndpointSlice(search.Name)
				}
				return published.sourceProbe(ctx, i, pod, role)
			}
			action, err := r.executePreparedRolloutStep(ctx, c, key, op.UID, published.readEvidence, time.Minute)
			if err != nil || !changed || action.Role != "search" || action.Switch.Kind != rollout.Hold || action.Switch.Reason != "RouteNotConverged" {
				t.Fatalf("search EndpointSlice change during source checks did not hold ingest: %#v %v", action, err)
			}
			if current, err := core.CoreV1().Services(instance.Namespace).Get(ctx, ingest.Name, metav1.GetOptions{}); err != nil || current.Spec.Selector[workload.FleetLabel] != "blue" {
				t.Fatalf("ingest Service changed despite the invalidated search observation: %#v %v", current, err)
			}
			r.rolloutSourceProbe = published.sourceProbe
		}
		action, err := r.executePreparedRolloutStep(ctx, c, key, op.UID, published.readEvidence, time.Minute)
		if err != nil || action.Role != want.role || action.Switch.Kind != want.kind {
			t.Fatalf("expected %s %s: %#v %v", want.role, want.kind, action, err)
		}
		if want.role == "ingest" && want.kind == rollout.PatchService {
			published.publishEndpoints("ingest")
		}
	}
	changeBeforeCommit := func(ctx context.Context, i *v1alpha1.OpenSearchServerless, o *v1alpha1.OpenSearchServerlessOperation) (rolloutEvidence, error) {
		evidence, err := published.readEvidence(ctx, i, o)
		if err == nil {
			changeEndpointSlice(ingest.Name)
		}
		return evidence, err
	}
	if action, err := r.executePreparedRolloutStep(ctx, c, key, op.UID, changeBeforeCommit, time.Minute); err != nil || action.Role != "ingest" ||
		action.Switch.Kind != rollout.Hold || action.Switch.Reason != "RouteNotConverged" || action.Complete {
		t.Fatalf("commit accepted an EndpointSlice version changed after the probes: %#v %v", action, err)
	}
	withoutRoutes := func(ctx context.Context, i *v1alpha1.OpenSearchServerless, o *v1alpha1.OpenSearchServerlessOperation) (rolloutEvidence, error) {
		evidence, err := published.readEvidence(ctx, i, o)
		evidence.Routes = nil
		return evidence, err
	}
	if action, err := r.executePreparedRolloutStep(ctx, c, key, op.UID, withoutRoutes, time.Minute); err != nil ||
		action.Switch.Kind != rollout.Hold || action.Switch.Reason != "RouteNotConverged" || action.Complete {
		t.Fatalf("commit accepted missing fresh route observations: %#v %v", action, err)
	}
	if action, err := r.executePreparedRolloutStep(ctx, c, key, op.UID, published.readEvidence, time.Minute); err != nil || !action.Complete {
		t.Fatalf("commit serving mapping: %#v %v", action, err)
	}
	published.verifyData()
	r.Now = func() time.Time { return now }
	persisted := &v1alpha1.OpenSearchServerless{}
	if err := c.Get(ctx, client.ObjectKeyFromObject(instance), persisted); err != nil {
		t.Fatal(err)
	}
	if persisted.Status.LastRolloutCommit == nil || *persisted.Status.LastRolloutCommit != *instance.Status.ActiveOperation ||
		persisted.Status.ActiveOperation == nil || *persisted.Status.ActiveOperation != *instance.Status.ActiveOperation ||
		persisted.Status.ServingFleetIntentByRole["search"] != "green" || persisted.Status.ServingFleetIntentByRole["ingest"] != "green" {
		t.Fatalf("inconsistent persisted mapping, claim or commit marker: %#v", persisted.Status)
	}
	for _, role := range []string{"search", "ingest"} {
		if persisted.Status.ServingRevisionByRole[role] != (v1alpha1.ServingRoleRevision{Fleet: "green", Revision: servingRevision}) {
			t.Fatalf("promotion did not pin %s: %#v", role, persisted.Status.ServingRevisionByRole)
		}
	}
	restarted := &OperationReconciler{Client: c, Core: core.CoreV1()}
	if action, err := restarted.executePreparedRolloutStep(ctx, c, key, op.UID, readEvidence, time.Minute); err != nil || !action.Complete {
		t.Fatalf("restart did not recover the committed mapping: %#v %v", action, err)
	}
	target := persisted.Status.RollbackTarget
	if target == nil || target.PromotionUID != string(op.UID) || target.RetainUntil == nil ||
		!target.RetainUntil.Time.Equal(now.Add(30*time.Minute).Truncate(time.Microsecond)) ||
		target.Roles["search"] != (v1alpha1.RollbackRoleTarget{Fleet: "blue", Revision: sourceRevision, ServiceUID: string(search.UID)}) ||
		target.Roles["ingest"] != (v1alpha1.RollbackRoleTarget{Fleet: "blue", Revision: sourceRevision, ServiceUID: string(ingest.UID)}) {
		t.Fatalf("promotion did not preserve both rollback identities and deadline: %#v", target)
	}
	restore := &v1alpha1.OpenSearchServerlessOperation{
		ObjectMeta: metav1.ObjectMeta{Name: "restore-committed", Namespace: instance.Namespace},
		Spec: v1alpha1.OperationSpec{
			InstanceRef: op.Spec.InstanceRef, Action: "Rollback", TargetFleet: "blue", SupersedesOperationUID: string(op.UID),
			ExpectedServingFleetByRole: map[string]string{"search": "green", "ingest": "green"},
		},
	}
	if err := c.Create(ctx, restore); err != nil {
		t.Fatal(err)
	}
	restoreKey := client.ObjectKeyFromObject(restore)
	restarted.Now = func() time.Time { return target.RetainUntil.Time }
	if transferred, err := restarted.prepareRollbackHandoff(ctx, c, restoreKey, restore.UID); err == nil || transferred {
		t.Fatalf("rollback started at the retention deadline: %t %v", transferred, err)
	}
	restarted.Now = func() time.Time { return now }
	if transferred, err := restarted.prepareRollbackHandoff(ctx, c, restoreKey, restore.UID); err != nil || transferred {
		t.Fatalf("expected a staged committed rollback: %t %v", transferred, err)
	}
	restarted.Now = func() time.Time { return target.RetainUntil.Time }
	if transferred, err := restarted.prepareRollbackHandoff(ctx, c, restoreKey, restore.UID); err == nil || transferred {
		t.Fatalf("staged rollback took the claim after expiry: %t %v", transferred, err)
	}
	if released, err := restarted.finalizeCommittedRollout(ctx, c, key, op.UID, "wrong.test"); err == nil || released {
		t.Fatalf("finalized with a different provisionable revision: %t %v", released, err)
	}
	restarted.RolloutReader, restarted.RolloutEvidence = c, readEvidence
	restarted.RolloutPodDomain = func(*v1alpha1.OpenSearchServerless) (string, error) { return "", nil }
	for range 4 {
		if _, err := restarted.Reconcile(ctx, ctrl.Request{NamespacedName: key}); err != nil {
			t.Fatal(err)
		}
	}
	if err := c.Get(ctx, key, observed); err != nil || observed.Status.Phase != "Succeeded" || len(observed.Finalizers) != 0 {
		t.Fatalf("promotion success was not persisted before claim release: %v", err)
	}
	if err := c.Get(ctx, client.ObjectKeyFromObject(instance), persisted); err != nil || persisted.Status.ActiveOperation != nil {
		t.Fatalf("reconcile did not release the committed promotion: %v", err)
	}
	if err := c.Delete(ctx, observed); err != nil {
		t.Fatal(err)
	}
	if err := c.Get(ctx, key, observed); !apierrors.IsNotFound(err) {
		t.Fatalf("completed promotion still exists: %v", err)
	}
	restarted.Now = func() time.Time { return now }
	if transferred, err := restarted.prepareRollbackHandoff(ctx, c, restoreKey, restore.UID); err != nil || !transferred {
		t.Fatalf("committed rollback did not take the claim: %t %v", transferred, err)
	}
	if err := c.Get(ctx, restoreKey, restore); err != nil {
		t.Fatal(err)
	}
	if len(restore.Status.RolloutRoles) != 2 || restore.Status.RolloutRoles["search"].CandidateRevision != sourceRevision ||
		restore.Status.RolloutRoles["ingest"].CandidateRevision != sourceRevision {
		t.Fatalf("rollback did not consume the retained identity: %#v", restore.Status.RolloutRoles)
	}
	rollbackEvidence := func(ctx context.Context, i *v1alpha1.OpenSearchServerless, op *v1alpha1.OpenSearchServerlessOperation) (rolloutEvidence, error) {
		observed, err := qualifiedRoutes(ctx, i, op)
		if err != nil {
			return rolloutEvidence{}, err
		}
		original := make(map[string]*rollout.CandidateEvidence, 2)
		for role, route := range observed.Routes {
			route.OperationUID = string(op.UID)
			evidence := *candidates[role]
			evidence.OperationUID, evidence.Fleet, evidence.Revision = string(op.UID), "blue", sourceRevision
			evidence.ObservedAt = now
			original[role] = &evidence
		}
		return rolloutEvidence{Candidates: original, Routes: observed.Routes}, nil
	}
	for _, kind := range []rollout.ActionKind{rollout.PatchService, rollout.RecordApplied, rollout.RecordConverged, rollout.PatchService, rollout.RecordApplied, rollout.RecordConverged} {
		now = now.Add(time.Second)
		if action, err := r.executePreparedRolloutStep(ctx, c, restoreKey, restore.UID, rollbackEvidence, time.Minute); err != nil || action.Switch.Kind != kind {
			t.Fatalf("committed rollback expected %s: %#v %v", kind, action, err)
		}
	}
	now = now.Add(time.Second)
	if action, err := r.executePreparedRolloutStep(ctx, c, restoreKey, restore.UID, rollbackEvidence, time.Minute); err != nil || !action.Complete {
		t.Fatalf("rollback of committed promotion did not finish: %#v %v", action, err)
	}
	if err := c.Get(ctx, client.ObjectKeyFromObject(instance), persisted); err != nil {
		t.Fatal(err)
	}
	if persisted.Status.RollbackTarget != nil || persisted.Status.LastRolloutCommit == nil || persisted.Status.LastRolloutCommit.UID != string(restore.UID) ||
		persisted.Status.ServingFleetIntentByRole["search"] != "blue" || persisted.Status.ServingFleetIntentByRole["ingest"] != "blue" {
		t.Fatalf("rollback did not clear the target and restore the original mapping: %#v", persisted.Status)
	}
	for _, role := range []string{"search", "ingest"} {
		if persisted.Status.ServingRevisionByRole[role] != (v1alpha1.ServingRoleRevision{Fleet: "blue", Revision: sourceRevision}) {
			t.Fatalf("rollback did not restore the literal %s label: %#v", role, persisted.Status.ServingRevisionByRole)
		}
	}
	for range 4 {
		if _, err := restarted.Reconcile(ctx, ctrl.Request{NamespacedName: restoreKey}); err != nil {
			t.Fatal(err)
		}
	}
	if err := c.Get(ctx, restoreKey, restore); err != nil || restore.Status.Phase != "Succeeded" {
		t.Fatalf("rollback did not finalize with the original Pod template: %v", err)
	}
	if err := c.Get(ctx, client.ObjectKeyFromObject(instance), persisted); err != nil || persisted.Status.ActiveOperation != nil ||
		persisted.Spec.Rollout == nil || persisted.Spec.Rollout.RollbackWindow != instance.Spec.Rollout.RollbackWindow {
		t.Fatalf("rollback did not release its claim with policy intact: %v", err)
	}
	published.verifyRollback()

	t.Run("rollback mixed routes", func(t *testing.T) {
		testMixedRollbackAPIServer(t, ctx, c, core, instance)
	})
	t.Run("held zone frees pending creation slot", func(t *testing.T) {
		testBlockedZoneWithdrawalAPIServer(t, ctx, c, core, instance)
	})
	t.Run("emergency gate revalidates evidence", func(t *testing.T) {
		testEmergencyGateAPIServer(t, ctx, c, core, instance)
	})
}

func testEmergencyGateAPIServer(t *testing.T, ctx context.Context, c client.Client, kube kubernetes.Interface, template *v1alpha1.OpenSearchServerless) {
	t.Helper()
	instance := &v1alpha1.OpenSearchServerless{ObjectMeta: metav1.ObjectMeta{Name: "emergency", Namespace: template.Namespace}, Spec: template.Spec}
	instance.Spec.Rollout = nil
	instance.Spec.Availability = &v1alpha1.Availability{MinServingTotal: map[string]int32{"ingest": 2}, MaxPendingProvisionablePods: 1, MaxConcurrentRetirements: 1,
		EmergencyCapacity: &v1alpha1.EmergencyCapacityPolicy{EvidenceRef: v1alpha1.LocalReference{Name: "emergency-outage"}, MaxTemporaryByRole: map[string]int32{"ingest": 1}, MaxTemporaryPerZone: 1}}
	instance.Spec.Pools = map[string]v1alpha1.Pool{"ingest": {
		JVMHeap: "512Mi", CapacityProfileRef: "emergency-profile", Zones: map[string]v1alpha1.Zone{"a": {Replicas: 1, MinServing: 1}, "b": {Replicas: 1, MinServing: 1}},
		Resources: corev1.ResourceRequirements{Requests: corev1.ResourceList{
			corev1.ResourceCPU: resource.MustParse("1"), corev1.ResourceMemory: resource.MustParse("1Gi"),
			corev1.ResourceEphemeralStorage: resource.MustParse("1Gi"),
		}},
	}}
	if err := c.Create(ctx, instance); err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 1, 1, 0, 1, 0, 0, time.UTC)
	instance.Status.PrimaryStore = &v1alpha1.PrimaryStoreStatus{Bucket: instance.Spec.Storage.Bucket, Region: instance.Spec.Storage.Region}
	instance.Status.ServingFleetIntentByRole = map[string]string{"ingest": "initial", "search": "initial"}
	instance.Status.PodCreationSequence = 1
	if err := c.Status().Update(ctx, instance); err != nil {
		t.Fatal(err)
	}
	cms := kube.CoreV1().ConfigMaps(instance.Namespace)
	immutable := true
	profile, err := cms.Create(ctx, &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "emergency-profile", Namespace: instance.Namespace}, Immutable: &immutable,
		Data: map[string]string{"profile.json": `{"revision":"v1","role":"ingest","zones":["b"],"nodeSelector":{"platform.example/profile":"ingest"},"taint":{"key":"platform.example/dedicated","value":"ingest","effect":"NoSchedule"},"observedAt":"2026-01-01T00:00:00Z","expiresAt":"2026-01-01T00:10:00Z","allocatable":{"cpu":"8","memory":"16Gi","ephemeral-storage":"40Gi","pods":"20"},"reserved":{"cpu":"1","memory":"2Gi","ephemeral-storage":"5Gi","pods":"4"},"headroom":{"cpu":"1","memory":"2Gi","ephemeral-storage":"5Gi","pods":"1"}}`}}, metav1.CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	evidence, err := cms.Create(ctx, &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "emergency-outage", Namespace: instance.Namespace},
		Data: map[string]string{"evidence.json": fmt.Sprintf(`{"instanceUID":%q,"generation":%d,"observedAt":"2026-01-01T00:00:00Z","expiresAt":"2026-01-01T00:04:00Z","unavailableZones":["a"],"fitByRole":{"ingest":{"b":{"additionalPods":1,"profileUID":%q,"profileVersion":%q}}}}`,
			instance.UID, instance.Generation, profile.UID, profile.ResourceVersion)}}, metav1.CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	r := &InstanceReconciler{Client: c, Core: kube.CoreV1(), DevelopmentGatedPods: true, DevelopmentReleaseGates: true, Now: func() time.Time { return now }}
	preOutage, err := kube.CoreV1().Pods(instance.Namespace).Create(ctx, &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "pre-outage-pending", Labels: map[string]string{workload.RoleLabel: "ingest", workload.ZoneLabel: "a"},
			OwnerReferences: []metav1.OwnerReference{*metav1.NewControllerRef(instance, v1alpha1.GroupVersion.WithKind("OpenSearchServerless"))}},
		Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "opensearch", Image: instance.Spec.Image}}},
	}, metav1.CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	instance.Status.ProvisioningAttempt = &v1alpha1.ProvisioningAttempt{Role: "ingest", Zone: "a", Name: preOutage.Name,
		PodUID: string(preOutage.UID), Phase: "Watching", StartedAt: metav1.NewTime(now)}
	if err := c.Status().Update(ctx, instance); err != nil {
		t.Fatal(err)
	}
	if _, changed, err := r.reconcileProvisioningAttempt(ctx, instance); err != nil || !changed || instance.Status.ProvisioningAttempt == nil || instance.Status.ProvisioningAttempt.Reason != "OutageConfirmed" {
		t.Fatalf("claim initial outage withdrawal: changed=%t, error=%v", changed, err)
	}
	confirmed := evidence.Data["evidence.json"]
	evidence.Data["evidence.json"] = strings.Replace(confirmed, `"unavailableZones":["a"]`, `"unavailableZones":[]`, 1)
	evidence, err = cms.Update(ctx, evidence, metav1.UpdateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if _, changed, err := r.reconcileProvisioningAttempt(ctx, instance); err != nil || !changed || instance.Status.ProvisioningAttempt == nil || instance.Status.ProvisioningAttempt.Phase != "Watching" {
		t.Fatalf("cancel withdrawal after outage clears: changed=%t, error=%v", changed, err)
	}
	if _, err := kube.CoreV1().Pods(instance.Namespace).Get(ctx, preOutage.Name, metav1.GetOptions{}); err != nil {
		t.Fatalf("cleared outage removed the original pending Pod: %v", err)
	}
	evidence.Data["evidence.json"] = confirmed
	evidence, err = cms.Update(ctx, evidence, metav1.UpdateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		if _, changed, err := r.reconcileProvisioningAttempt(ctx, instance); err != nil || !changed {
			t.Fatalf("outage withdrawal step %d: changed=%t, error=%v", i, changed, err)
		}
		if i == 0 && (instance.Status.ProvisioningAttempt == nil || instance.Status.ProvisioningAttempt.Reason != "OutageConfirmed") {
			t.Fatal("confirmed outage did not claim withdrawal before the scheduling timeout")
		}
	}
	if instance.Status.ProvisioningAttempt != nil || len(instance.Status.BlockedProvisioningAttempts) != 0 {
		t.Fatal("withdrawal should free the pending slot without a persistent failed-zone hold")
	}
	revision, err := podRevision(instance, "")
	if err != nil {
		t.Fatal(err)
	}
	resolved, err := r.resolveCapacityProfile(ctx, instance, "ingest", "b")
	if err != nil {
		t.Fatal(err)
	}
	regular, err := renderGatedPod(instance, &v1alpha1.PodCreationIntent{
		Name: podNameForSequence(instance, 1), Sequence: 1, Generation: instance.Generation, Role: "ingest", Zone: "b", Fleet: "initial",
		Revision: revision, CapacityProfileUID: resolved.UID, CapacityProfileVersion: resolved.ResourceVersion,
	}, resolved, "", "")
	if err != nil {
		t.Fatal(err)
	}
	regular.Spec.SchedulingGates, regular.Spec.NodeName = nil, "survivor"
	pods := kube.CoreV1().Pods(instance.Namespace)
	regular, err = pods.Create(ctx, regular, metav1.CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	regular.Status.Phase = corev1.PodRunning
	regular.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}
	if _, err := pods.UpdateStatus(ctx, regular, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	if changed, err := r.sampleEmergencyCapacity(ctx, instance); err != nil || !changed || instance.Status.EmergencyCapacityProposal == nil {
		t.Fatalf("record initial emergency proposal: changed=%t, error=%v", changed, err)
	}
	if err := c.Get(ctx, client.ObjectKeyFromObject(instance), instance); err != nil {
		t.Fatal(err)
	}
	if changed, err := r.sampleEmergencyCapacity(ctx, instance); err != nil || changed {
		t.Fatalf("persisted proposal should not requeue the controller without a change: changed=%t, error=%v", changed, err)
	}
	step := func() {
		t.Helper()
		if _, err := r.reconcileGatedPod(ctx, instance); err != nil {
			t.Fatal(err)
		}
	}
	step()
	if instance.Status.ServingRevisionByRole["ingest"] != (v1alpha1.ServingRoleRevision{Fleet: "initial", Revision: revision}) {
		t.Fatal("verified serving Pod did not establish its revision before emergency provisioning")
	}
	for range 3 {
		step()
	}
	if pending := instance.Status.PendingGateRelease; pending == nil || pending.Pod.Zone != "b" || pending.Pod.EmergencyEvidenceVersion != evidence.ResourceVersion {
		t.Fatalf("emergency Pod did not reach the release gate: %#v", pending)
	}
	withdrawn := instance.Status.PendingGateRelease.Pod.Name
	evidence.Annotations = map[string]string{"touch": "new-version"}
	if _, err := cms.Update(ctx, evidence, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	step()
	if instance.Status.PendingGateRelease == nil || !instance.Status.PendingGateRelease.WithdrawalRequested {
		t.Fatal("changed evidence did not request withdrawal")
	}
	for range 7 {
		step()
	}
	if _, err := pods.Get(ctx, withdrawn, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Fatalf("stale emergency Pod was not withdrawn: %v", err)
	}
	attempt := instance.Status.ProvisioningAttempt
	if attempt == nil || attempt.Role != "ingest" || attempt.Zone != "b" || attempt.Name == withdrawn {
		t.Fatalf("fresh emergency Pod was not released: %#v", attempt)
	}
	released, err := pods.Get(ctx, attempt.Name, metav1.GetOptions{})
	if err != nil || hasPlacementGate(released) || released.Annotations[emergencyCapacityAnnotation] != "true" ||
		released.Spec.NodeSelector[corev1.LabelTopologyZone] != "b" {
		t.Fatalf("emergency Pod was not marked and pinned to zone b: %v", err)
	}
	if err := pods.Bind(ctx, &corev1.Binding{ObjectMeta: metav1.ObjectMeta{Name: released.Name, UID: released.UID},
		Target: corev1.ObjectReference{Kind: "Node", Name: "emergency-survivor"}}, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	released, err = pods.Get(ctx, released.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	released.Status.Phase = corev1.PodRunning
	released.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}
	released, err = pods.UpdateStatus(ctx, released, metav1.UpdateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if _, changed, err := r.reconcileProvisioningAttempt(ctx, instance); err != nil || !changed || instance.Status.ProvisioningAttempt != nil {
		t.Fatalf("emergency Pod did not finish startup tracking: %v", err)
	}
	zero := int64(0)
	if err := pods.Delete(ctx, regular.Name, metav1.DeleteOptions{GracePeriodSeconds: &zero}); err != nil {
		t.Fatal(err)
	}
	if changed, err := r.refreshZoneCapacity(ctx, instance); err != nil || !changed {
		t.Fatalf("refresh emergency capacity status: %v", err)
	}
	zone := instance.Status.ZoneCapacity[1]
	if zone.Zone != "b" || zone.EmergencyPods != 1 || zone.EmergencyReady != 1 || zone.KubernetesReady != 1 || zone.ReadyShortfall != 1 {
		t.Fatalf("emergency Pod hid the normal target shortfall: %#v", zone)
	}
	step()
	normal := instance.Status.PendingPodCreation
	if normal == nil || normal.Zone != "b" || normal.EmergencyEvidenceUID != "" {
		t.Fatalf("normal Pod was not replenished while temporary capacity remained: %#v", normal)
	}
	if _, err := pods.Get(ctx, released.Name, metav1.GetOptions{}); err != nil {
		t.Fatalf("replenishing normal capacity removed the emergency Pod: %v", err)
	}
	t.Run("retire after regular endpoints recover", func(t *testing.T) {
		testEmergencyRetirementAPIServer(t, ctx, c, kube, instance, r, released, evidence)
	})
}

func testEmergencyRetirementAPIServer(t *testing.T, ctx context.Context, c client.Client, kube kubernetes.Interface, instance *v1alpha1.OpenSearchServerless, r *InstanceReconciler, victim *corev1.Pod, evidence *corev1.ConfigMap) {
	t.Helper()
	instance.Status.PendingPodCreation = nil
	instance.Status.BlockedProvisioningAttempts = nil
	if err := c.Status().Update(ctx, instance); err != nil {
		t.Fatal(err)
	}
	r.Policy, r.Discovery = kube.PolicyV1(), kube.DiscoveryV1()
	r.APIReader, r.DevelopmentEmergencyFailback, r.DevelopmentServiceProbeImage = c, true, instance.Spec.Image
	leaseUntil := time.Now().Add(time.Minute)
	r.ObserveIngestQualification = func(context.Context, *corev1.Pod) (java.IngestQualification, error) {
		return java.IngestQualification{ProcessIncarnation: "9aa977f3-4985-4cb5-9692-5ad1e3e243fe", LeaseValidUntil: leaseUntil}, nil
	}
	pods := kube.CoreV1().Pods(instance.Namespace)
	service, err := workload.RoleService(instance, "ingest", "initial", 9200)
	if err != nil {
		t.Fatal(err)
	}
	service, err = kube.CoreV1().Services(instance.Namespace).Create(ctx, service, metav1.CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	advertise := func(pod *corev1.Pod, address string) discoveryv1.Endpoint {
		t.Helper()
		pod.Status.PodIP = address
		pod.Status.Phase = corev1.PodRunning
		pod.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}
		pod.Status.ContainerStatuses = []corev1.ContainerStatus{{Name: "opensearch", ContainerID: "containerd://" + pod.Name, Ready: true, State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}}}}
		if _, err := pods.UpdateStatus(ctx, pod, metav1.UpdateOptions{}); err != nil {
			t.Fatal(err)
		}
		ready := true
		zone := pod.Labels[workload.ZoneLabel]
		return discoveryv1.Endpoint{Addresses: []string{address}, NodeName: &pod.Spec.NodeName, Zone: &zone, Conditions: discoveryv1.EndpointConditions{Ready: &ready},
			TargetRef: &corev1.ObjectReference{Kind: "Pod", Namespace: pod.Namespace, Name: pod.Name, UID: pod.UID}}
	}
	endpoints := []discoveryv1.Endpoint{advertise(victim, "10.12.0.2")}
	for i, zone := range []string{"a", "b"} {
		survivor := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "recovered-" + zone, Namespace: victim.Namespace,
			OwnerReferences: victim.OwnerReferences, Labels: map[string]string{
				workload.InstanceLabel: string(instance.UID), workload.RoleLabel: "ingest", workload.FleetLabel: "initial", workload.ZoneLabel: zone,
				"serverless.opensearch.org/revision": victim.Labels["serverless.opensearch.org/revision"],
			}}, Spec: corev1.PodSpec{NodeName: "recovered-node-" + zone, Containers: []corev1.Container{{Name: "opensearch", Image: instance.Spec.Image}}}}
		survivor, err = pods.Create(ctx, survivor, metav1.CreateOptions{})
		if err != nil {
			t.Fatal(err)
		}
		endpoints = append(endpoints, advertise(survivor, fmt.Sprintf("10.12.0.%d", i+3)))
	}
	port, portName, protocol := int32(9200), "http", corev1.ProtocolTCP
	if _, err := r.Discovery.EndpointSlices(instance.Namespace).Create(ctx, &discoveryv1.EndpointSlice{
		ObjectMeta: metav1.ObjectMeta{Name: "emergency-recovery", Labels: map[string]string{
			discoveryv1.LabelServiceName: service.Name, discoveryv1.LabelManagedBy: "endpointslice-controller.k8s.io",
		}, OwnerReferences: []metav1.OwnerReference{*metav1.NewControllerRef(service, corev1.SchemeGroupVersion.WithKind("Service"))}},
		AddressType: discoveryv1.AddressTypeIPv4, Ports: []discoveryv1.EndpointPort{{Name: &portName, Port: &port, Protocol: &protocol}}, Endpoints: endpoints,
	}, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	budget, err := workload.DisruptionBudget(instance, disruptionBudgetName(instance, "initial", "ingest", "b"), "initial", "ingest", "b", 1)
	if err != nil {
		t.Fatal(err)
	}
	budgets := r.Policy.PodDisruptionBudgets(instance.Namespace)
	budget, err = budgets.Create(ctx, budget, metav1.CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	budget.Status = policyv1.PodDisruptionBudgetStatus{ObservedGeneration: budget.Generation, CurrentHealthy: 2, DesiredHealthy: 1, ExpectedPods: 2, DisruptionsAllowed: 1}
	if _, err := budgets.UpdateStatus(ctx, budget, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	evidence, err = r.Core.ConfigMaps(instance.Namespace).Get(ctx, evidence.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	pin := instance.Status.EmergencyFailbackEvidence
	if pin == nil || pin.Name != evidence.Name || pin.UID != string(evidence.UID) {
		t.Fatalf("the creation intent did not retain the report identity: %#v", pin)
	}
	publishClearance := func() {
		t.Helper()
		evidence.Data["evidence.json"] = fmt.Sprintf(`{"instanceUID":%q,"generation":%d,"observedAt":"2026-01-01T00:00:00Z","expiresAt":"2026-01-01T00:04:00Z","unavailableZones":[]}`, instance.UID, instance.Generation)
		updated, err := r.Core.ConfigMaps(instance.Namespace).Update(ctx, evidence, metav1.UpdateOptions{})
		if err != nil {
			t.Fatal(err)
		}
		evidence = updated
	}
	// Reproduce the status of a temporary Pod that predates the report pin.
	instance.Status.EmergencyFailbackEvidence = nil
	if err := c.Status().Update(ctx, instance); err != nil {
		t.Fatal(err)
	}
	publishClearance()
	if _, err := r.sampleEmergencyCapacity(ctx, instance); err != nil {
		t.Fatal(err)
	}
	if _, handled, err := r.reconcileEmergencyRetirement(ctx, instance); err != nil || !handled ||
		instance.Status.EmergencyFailbackEvidence == nil || *instance.Status.EmergencyFailbackEvidence != *pin || instance.Status.EmergencyRetirement != nil {
		t.Fatalf("legacy temporary capacity did not backfill its report before planning retirement: handled=%t, error=%v", handled, err)
	}
	instance.Spec.Availability.EmergencyCapacity = nil
	if err := c.Update(ctx, instance); err != nil {
		t.Fatal(err)
	}
	if !slices.Contains(r.watchedConfigMapNames(instance), evidence.Name) {
		t.Fatal("policy removal stopped the report's ConfigMap watch")
	}
	if proposal, err := r.planEmergencyCapacity(ctx, instance); err != nil || proposal != nil {
		t.Fatalf("removed policy still admits temporary capacity: %#v, %v", proposal, err)
	}
	if _, _, err := r.readFailbackEvidence(ctx, instance); err == nil {
		t.Fatal("policy removal allowed reuse of evidence for the old generation")
	}
	publishClearance()
	replacedReport := instance.DeepCopy()
	replacedReport.Status.EmergencyFailbackEvidence.UID = "previous-configmap-uid"
	if _, _, err := r.readFailbackEvidence(ctx, replacedReport); err == nil || !strings.Contains(err.Error(), "report was replaced") {
		t.Fatalf("a valid clearance under a different report UID must be rejected: %v", err)
	}
	if changed, err := r.sampleEmergencyCapacity(ctx, instance); err != nil || !changed || instance.Status.EmergencyCapacityProposal != nil {
		t.Fatalf("clear the emergency proposal on recovered evidence: changed=%t, error=%v", changed, err)
	}
	stale := instance.DeepCopyObject().(*v1alpha1.OpenSearchServerless)
	if _, handled, err := r.reconcileEmergencyRetirement(ctx, instance); err != nil || !handled {
		t.Fatalf("persist retirement intent: handled=%t, error=%v", handled, err)
	}
	if _, handled, err := r.reconcileEmergencyRetirement(ctx, stale); !handled || !apierrors.IsConflict(err) {
		t.Fatalf("stale controller wrote a competing retirement: handled=%t, error=%v", handled, err)
	}
	restarted := *r
	if err := c.Get(ctx, client.ObjectKeyFromObject(instance), instance); err != nil {
		t.Fatal(err)
	}
	if pending := instance.Status.EmergencyRetirement; pending == nil || pending.PodUID != string(victim.UID) {
		t.Fatalf("retirement intent was not stored with the exact Pod identity: %#v", pending)
	}
	beforeEviction, err := pods.Get(ctx, victim.Name, metav1.GetOptions{})
	if err != nil || beforeEviction.DeletionTimestamp != nil {
		t.Fatalf("planning changed the Pod before eviction: %v", err)
	}
	leaseUntil = time.Now().Add(-time.Second)
	if _, handled, err := restarted.reconcileEmergencyRetirement(ctx, instance); err != nil || !handled {
		t.Fatalf("expired lease evidence did not hold retirement: handled=%t, error=%v", handled, err)
	}
	beforeEviction, err = pods.Get(ctx, victim.Name, metav1.GetOptions{})
	if err != nil || beforeEviction.DeletionTimestamp != nil {
		t.Fatalf("expired evidence allowed an eviction: %v", err)
	}
	leaseUntil = time.Now().Add(time.Minute)
	setSurvivorDisruption := func(active bool) {
		t.Helper()
		survivor, err := pods.Get(ctx, "recovered-b", metav1.GetOptions{})
		if err != nil {
			t.Fatal(err)
		}
		if active {
			survivor.Status.Conditions = append(survivor.Status.Conditions, corev1.PodCondition{Type: corev1.DisruptionTarget, Status: corev1.ConditionTrue, Reason: "PreemptionByScheduler"})
		} else {
			survivor.Status.Conditions[1].Status = corev1.ConditionFalse
		}
		if _, err := pods.UpdateStatus(ctx, survivor, metav1.UpdateOptions{}); err != nil {
			t.Fatal(err)
		}
	}
	restarted.APIReader = failbackReadHook{Reader: c, beforeRead: func() { setSurvivorDisruption(true) }}
	if _, handled, err := restarted.reconcileEmergencyRetirement(ctx, instance); err != nil || !handled {
		t.Fatalf("recheck survivor before eviction: handled=%t, error=%v", handled, err)
	}
	beforeEviction, err = pods.Get(ctx, victim.Name, metav1.GetOptions{})
	if err != nil || beforeEviction.DeletionTimestamp != nil {
		t.Fatalf("disruption after qualification allowed an eviction: %v", err)
	}
	setSurvivorDisruption(false)
	restarted.APIReader = c
	if _, handled, err := restarted.reconcileEmergencyRetirement(ctx, instance); err != nil || !handled {
		t.Fatalf("evict after restart: handled=%t, error=%v", handled, err)
	}
	afterEviction, err := pods.Get(ctx, victim.Name, metav1.GetOptions{})
	if err != nil || afterEviction.DeletionTimestamp == nil || afterEviction.UID != victim.UID {
		t.Fatalf("Kubernetes did not start retiring the exact victim: %v", err)
	}
	budget, err = budgets.Get(ctx, budget.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	disrupted := budget.Status.DisruptedPods[victim.Name]
	if budget.Status.DisruptionsAllowed != 0 || disrupted.IsZero() {
		t.Fatalf("PDB did not account for the retirement: %v", err)
	}
	if _, handled, err := restarted.reconcileEmergencyRetirement(ctx, instance); err != nil || handled || instance.Status.EmergencyRetirement == nil {
		t.Fatalf("terminating Pod should retain its retirement slot and allow provisioning: handled=%t, error=%v", handled, err)
	}
	zero := int64(0)
	if err := pods.Delete(ctx, victim.Name, metav1.DeleteOptions{GracePeriodSeconds: &zero}); err != nil {
		t.Fatal(err)
	}
	if _, handled, err := restarted.reconcileEmergencyRetirement(ctx, instance); err != nil || !handled || instance.Status.EmergencyRetirement != nil {
		t.Fatalf("retirement intent was not cleared after the victim disappeared: handled=%t, error=%v", handled, err)
	}
	if _, handled, err := restarted.reconcileEmergencyRetirement(ctx, instance); err != nil || !handled || instance.Status.EmergencyFailbackEvidence != nil {
		t.Fatalf("the completed failback kept its report pin: handled=%t, error=%v", handled, err)
	}
}

func testBlockedZoneWithdrawalAPIServer(t *testing.T, ctx context.Context, c client.Client, kube kubernetes.Interface, template *v1alpha1.OpenSearchServerless) {
	t.Helper()
	instance := &v1alpha1.OpenSearchServerless{ObjectMeta: metav1.ObjectMeta{Name: "zone-recovery", Namespace: template.Namespace}, Spec: template.Spec}
	instance.Spec.Rollout = nil
	instance.Spec.Availability = &v1alpha1.Availability{MinServingTotal: map[string]int32{"search": 1}}
	requests := corev1.ResourceRequirements{Requests: corev1.ResourceList{
		corev1.ResourceCPU: resource.MustParse("1"), corev1.ResourceMemory: resource.MustParse("1Gi"),
	}}
	instance.Spec.Pools = map[string]v1alpha1.Pool{
		"search": {JVMHeap: "512Mi", Resources: requests, Zones: map[string]v1alpha1.Zone{"a": {Replicas: 1}, "b": {Replicas: 1}}},
		"ingest": {JVMHeap: "512Mi", Resources: requests, Zones: map[string]v1alpha1.Zone{"b": {Replicas: 1, MinServing: 1}}},
	}
	if err := c.Create(ctx, instance); err != nil {
		t.Fatal(err)
	}
	instance.Status.PrimaryStore = &v1alpha1.PrimaryStoreStatus{Bucket: instance.Spec.Storage.Bucket, Region: instance.Spec.Storage.Region}
	instance.Status.ServingFleetIntentByRole = map[string]string{"ingest": "initial", "search": "initial"}
	instance.Status.PodCreationSequence = 1
	instance.Status.BlockedProvisioningAttempts = []v1alpha1.ProvisioningAttempt{{
		Role: "search", Zone: "a", Name: "original-failure", PodUID: "failed-uid", Phase: "Blocked", Reason: "PodGone", StartedAt: metav1.Now(),
	}}
	if err := c.Status().Update(ctx, instance); err != nil {
		t.Fatal(err)
	}
	revision, err := podRevision(instance, "")
	if err != nil {
		t.Fatal(err)
	}
	intent := &v1alpha1.PodCreationIntent{
		Name: podNameForSequence(instance, 1), Sequence: 1, Generation: instance.Generation,
		Role: "search", Zone: "a", Fleet: "initial", Revision: revision,
	}
	pod, err := renderGatedPod(instance, intent, nil, "", "")
	if err != nil {
		t.Fatal(err)
	}
	pods := kube.CoreV1().Pods(instance.Namespace)
	if _, err := pods.Create(ctx, pod, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	r := &InstanceReconciler{Client: c, Core: kube.CoreV1(), DevelopmentGatedPods: true, DevelopmentReleaseGates: true}
	for range 4 {
		if _, err := r.reconcileGatedPod(ctx, instance); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := pods.Get(ctx, pod.Name, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Fatalf("held zone Pod still exists: %v", err)
	}
	if attempt := instance.Status.PendingPodCreation; attempt == nil || attempt.Zone != "b" || attempt.Role != "search" ||
		len(instance.Status.BlockedProvisioningAttempts) != 1 || instance.Status.BlockedProvisioningAttempts[0].Zone != "a" {
		t.Fatalf("surviving zone did not get the freed slot: %#v", instance.Status)
	}
}

func testMixedRollbackAPIServer(t *testing.T, ctx context.Context, c client.Client, k8s kubernetes.Interface, template *v1alpha1.OpenSearchServerless) {
	t.Helper()
	instance := &v1alpha1.OpenSearchServerless{ObjectMeta: metav1.ObjectMeta{Name: "rollback-catalog", Namespace: template.Namespace}, Spec: template.Spec}
	if err := c.Create(ctx, instance); err != nil {
		t.Fatal(err)
	}
	promote := &v1alpha1.OpenSearchServerlessOperation{
		ObjectMeta: metav1.ObjectMeta{Name: "partial-promotion", Namespace: instance.Namespace},
		Spec: v1alpha1.OperationSpec{
			InstanceRef: v1alpha1.InstanceReference{Name: instance.Name, UID: string(instance.UID)}, Action: "Promote", TargetFleet: "green",
			ExpectedServingFleetByRole: map[string]string{"search": "blue", "ingest": "blue"},
		},
	}
	if err := c.Create(ctx, promote); err != nil {
		t.Fatal(err)
	}
	claim := operationClaimFor(promote)
	instance.Status.ActiveOperation = &claim
	instance.Status.ServingFleetIntentByRole = map[string]string{"search": "blue", "ingest": "blue"}
	if err := c.Status().Update(ctx, instance); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC().Truncate(time.Microsecond)
	services := make(map[string]*corev1.Service, 2)
	roles := make(map[string]v1alpha1.RolloutRoleStatus, 2)
	for _, role := range []string{"search", "ingest"} {
		service, err := workload.RoleService(instance, role, "blue", 9200)
		if err != nil {
			t.Fatal(err)
		}
		if err := c.Create(ctx, service); err != nil {
			t.Fatal(err)
		}
		services[role] = service
		roles[role] = v1alpha1.RolloutRoleStatus{ServiceUID: string(service.UID), SourceFleet: "blue", SourceRevision: "original", TargetFleet: "green", CandidateRevision: "candidate", Phase: string(rollout.Prepared)}
		for zone, zoneSpec := range instance.Spec.Pools[role].Zones {
			budgets := k8s.PolicyV1().PodDisruptionBudgets(instance.Namespace)
			budget, err := workload.DisruptionBudget(instance, disruptionBudgetName(instance, "blue", role, zone), "blue", role, zone, zoneSpec.MinServing)
			if err != nil {
				t.Fatal(err)
			}
			budget, err = budgets.Create(ctx, budget, metav1.CreateOptions{})
			if err != nil {
				t.Fatal(err)
			}
			budget.Status = policyv1.PodDisruptionBudgetStatus{
				ObservedGeneration: budget.Generation, DesiredHealthy: zoneSpec.MinServing, CurrentHealthy: zoneSpec.Replicas, ExpectedPods: zoneSpec.Replicas,
				Conditions: []metav1.Condition{{Type: policyv1.DisruptionAllowedCondition, Status: metav1.ConditionFalse, Reason: "InsufficientPods", LastTransitionTime: metav1.NewTime(now)}},
			}
			if _, err := budgets.UpdateStatus(ctx, budget, metav1.UpdateOptions{}); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := prepareRolloutProgress(ctx, c, instance, promote, roles); err != nil {
		t.Fatal(err)
	}
	search := services["search"]
	if err := switchRoleServiceConditional(ctx, k8s.CoreV1(), instance, "search", search.UID, search.ResourceVersion, "blue", "green", nil); err != nil {
		t.Fatal(err)
	}
	rollback := &v1alpha1.OpenSearchServerlessOperation{
		ObjectMeta: metav1.ObjectMeta{Name: "recover-promotion", Namespace: instance.Namespace},
		Spec: v1alpha1.OperationSpec{
			InstanceRef: promote.Spec.InstanceRef, Action: "Rollback", TargetFleet: "blue", SupersedesOperationUID: string(promote.UID),
			ExpectedServingFleetByRole: map[string]string{"search": "green", "ingest": "blue"},
		},
	}
	if err := c.Create(ctx, rollback); err != nil {
		t.Fatal(err)
	}
	key := client.ObjectKeyFromObject(rollback)
	evidence := func(ctx context.Context, _ *v1alpha1.OpenSearchServerless, op *v1alpha1.OpenSearchServerlessOperation) (rolloutEvidence, error) {
		candidates := make(map[string]*rollout.CandidateEvidence, 2)
		routes := make(map[string]*rollout.RouteEvidence, 2)
		for role, expected := range services {
			service, err := k8s.CoreV1().Services(instance.Namespace).Get(ctx, expected.Name, metav1.GetOptions{})
			if err != nil {
				return rolloutEvidence{}, err
			}
			candidates[role] = &rollout.CandidateEvidence{
				InstanceUID: string(instance.UID), OperationUID: string(op.UID), Role: role, Fleet: "blue", Revision: "original", ObservedAt: now,
				CapacityQualified: true, PeerQualified: true, SecurityQualified: true, RollbackCompatible: true,
			}
			routes[role] = &rollout.RouteEvidence{
				InstanceUID: string(instance.UID), OperationUID: string(op.UID), Role: role, Fleet: service.Spec.Selector[workload.FleetLabel],
				ServiceUID: string(service.UID), ServiceVersion: service.ResourceVersion, ObservedAt: now, EndpointsConverged: true, ClientProbePassed: true,
			}
		}
		return rolloutEvidence{Candidates: candidates, Routes: routes}, nil
	}
	r := &OperationReconciler{Client: c, Core: k8s.CoreV1(), Policy: k8s.PolicyV1(), Now: func() time.Time { return now }}
	r.RolloutReader, r.RolloutEvidence = c, evidence
	r.RolloutPodDomain = func(*v1alpha1.OpenSearchServerless) (string, error) { return "", nil }
	for range 3 {
		if _, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: key}); err != nil {
			t.Fatal(err)
		}
	}
	if err := c.Get(ctx, client.ObjectKeyFromObject(instance), instance); err != nil || instance.Status.ActiveOperation == nil || instance.Status.ActiveOperation.UID != string(rollback.UID) {
		t.Fatalf("rollback reconcile did not take the claim: %#v %v", instance.Status.ActiveOperation, err)
	}
	if _, err := r.executePreparedRolloutStep(ctx, c, client.ObjectKeyFromObject(promote), promote.UID, evidence, time.Minute); err == nil {
		t.Fatal("superseded promotion retained authority")
	}
	for _, want := range []struct {
		role string
		kind rollout.ActionKind
	}{
		{"search", rollout.PatchService},
		{"search", rollout.RecordApplied},
		{"search", rollout.RecordConverged},
		{"ingest", rollout.RecordApplied},
		{"ingest", rollout.RecordConverged},
	} {
		now = now.Add(time.Second)
		action, err := r.executePreparedRolloutStep(ctx, c, key, rollback.UID, evidence, time.Minute)
		if err != nil || action.Role != want.role || action.Switch.Kind != want.kind {
			t.Fatalf("expected %s %s: %#v %v", want.role, want.kind, action, err)
		}
	}
	now = now.Add(time.Second)
	if action, err := r.executePreparedRolloutStep(ctx, c, key, rollback.UID, evidence, time.Minute); err != nil || !action.Complete {
		t.Fatalf("mixed rollback did not commit: %#v %v", action, err)
	}
	for role, original := range services {
		current, err := k8s.CoreV1().Services(instance.Namespace).Get(ctx, original.Name, metav1.GetOptions{})
		if err != nil || current.UID != original.UID || current.Spec.Selector[workload.FleetLabel] != "blue" {
			t.Fatalf("%s Service did not return to the original fleet: %#v %v", role, current, err)
		}
		if role == "ingest" && current.ResourceVersion != original.ResourceVersion {
			t.Fatal("ingest Service changed during mixed rollback")
		}
	}
	if err := c.Get(ctx, client.ObjectKeyFromObject(instance), instance); err != nil {
		t.Fatal(err)
	}
	if instance.Status.LastRolloutCommit == nil || *instance.Status.LastRolloutCommit != operationClaimFor(rollback) ||
		instance.Status.ServingFleetIntentByRole["search"] != "blue" || instance.Status.ServingFleetIntentByRole["ingest"] != "blue" {
		t.Fatalf("unexpected serving intent after rollback: %#v", instance.Status)
	}
}
