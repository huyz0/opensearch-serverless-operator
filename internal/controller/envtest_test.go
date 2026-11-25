//go:build envtest

package controller_test

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"errors"
	"math/big"
	"path/filepath"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	typedcore "k8s.io/client-go/kubernetes/typed/core/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/envtest"

	"github.com/huyz0/opensearch-serverless-operator/api/v1alpha1"
	"github.com/huyz0/opensearch-serverless-operator/internal/controller"
	"github.com/huyz0/opensearch-serverless-operator/internal/workload"
	"github.com/huyz0/opensearch-serverless-operator/pkg/podlifecycle"
	podkube "github.com/huyz0/opensearch-serverless-operator/pkg/podlifecycle/kubernetes"
)

type keystoreReadClient struct {
	typedcore.CoreV1Interface
	onRead func(*corev1.Secret)
}

func (c keystoreReadClient) Secrets(namespace string) typedcore.SecretInterface {
	return observedSecretReads{SecretInterface: c.CoreV1Interface.Secrets(namespace), onRead: c.onRead}
}

type observedSecretReads struct {
	typedcore.SecretInterface
	onRead func(*corev1.Secret)
}

func (c observedSecretReads) Get(ctx context.Context, name string, options metav1.GetOptions) (*corev1.Secret, error) {
	secret, err := c.SecretInterface.Get(ctx, name, options)
	if err == nil {
		c.onRead(secret)
	}
	return secret, err
}

type testSettingsBackend struct {
	values       map[string]string
	serviceUID   types.UID
	writes       int
	loseResponse bool
	onRead       func() error
	deferCommit  bool
	commit       func()
}

func (b *testSettingsBackend) Read(context.Context, *v1alpha1.OpenSearchServerless) (controller.SettingsRead, error) {
	if b.onRead != nil {
		if err := b.onRead(); err != nil {
			return controller.SettingsRead{}, err
		}
	}
	values := make(map[string]string, len(b.values))
	for key, value := range b.values {
		values[key] = value
	}
	return controller.SettingsRead{Values: values, ServiceUID: b.serviceUID}, nil
}

func (b *testSettingsBackend) Apply(_ context.Context, _ *v1alpha1.OpenSearchServerless, uid types.UID, patch v1alpha1.ClusterSettingsPatch) error {
	if uid != b.serviceUID {
		return errors.New("Service changed")
	}
	b.writes++
	commit := func() {
		for key, value := range patch.Set {
			b.values[key] = value
		}
		for _, key := range patch.Remove {
			delete(b.values, key)
		}
	}
	if b.deferCommit {
		b.commit = commit
		return errors.New("simulated timeout before server committed")
	}
	commit()
	if b.loseResponse {
		b.loseResponse = false
		return errors.New("simulated lost response")
	}
	return nil
}

func TestAPIServerValidatesInstanceAndStatus(t *testing.T) {
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
	kube, err := client.NewWithWatch(config, client.Options{Scheme: scheme})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	instance := &v1alpha1.OpenSearchServerless{ObjectMeta: metav1.ObjectMeta{Name: "catalog", Namespace: "default"}, Spec: v1alpha1.OpenSearchServerlessSpec{
		Image: "example/image@sha256:" + strings.Repeat("a", 64), ServiceAccountName: "runtime",
		Storage: v1alpha1.Storage{Type: "s3", Bucket: "bucket", Region: "region"},
		Pools:   map[string]v1alpha1.Pool{"search": {Zones: map[string]v1alpha1.Zone{"a": {Replicas: 2, MinServing: 1}}}},
	}}
	if err := kube.Create(ctx, instance); err != nil {
		t.Fatalf("create valid instance: %v", err)
	}
	t.Run("instance admission matches configured shapes", func(t *testing.T) {
		checkInstanceAdmission(t, ctx, kube, instance)
	})
	httpTLS := &v1alpha1.TLSConfig{HTTP: &v1alpha1.TLSChannel{Source: "Secret",
		CertificateRef: v1alpha1.LocalReference{Name: "http-leaf"}, TrustBundleRef: v1alpha1.SecretKeyReference{Name: "http-roots", Key: "ca.crt"}}}
	for _, test := range []struct {
		name          string
		initial, next *v1alpha1.TLSConfig
	}{
		{name: "plaintext-to-https", next: httpTLS},
		{name: "https-to-plaintext", initial: httpTLS},
	} {
		fixture := &v1alpha1.OpenSearchServerless{ObjectMeta: metav1.ObjectMeta{Name: test.name, Namespace: "default"}, Spec: instance.Spec}
		fixture.Spec.TLS = test.initial
		if err := kube.Create(ctx, fixture); err != nil {
			t.Fatal(err)
		}
		fixture.Status.PodCreationSequence = 1
		if err := kube.Status().Update(ctx, fixture); err != nil {
			t.Fatal(err)
		}
		fixture.Spec.TLS = test.next
		if err := kube.Update(ctx, fixture); !apierrors.IsInvalid(err) || !strings.Contains(err.Error(), "HTTP TLS mode cannot change after Pod creation starts") {
			t.Fatalf("started instance allowed an HTTP TLS mode change: %v", err)
		}
	}
	startupAuth := &v1alpha1.SecurityConfig{Provider: "ServerlessBasicAuth", AuthenticationSecretRef: &v1alpha1.SecretKeyReference{Name: "runtime", Key: "bootstrap-password"}}
	for _, test := range []struct {
		name          string
		initial, next *v1alpha1.SecurityConfig
		pinnedOnly    bool
	}{
		{name: "enable-auth-too-late", next: startupAuth},
		{name: "disable-auth-too-late", initial: startupAuth},
		{name: "disable-pinned-auth", initial: startupAuth, pinnedOnly: true},
	} {
		fixture := &v1alpha1.OpenSearchServerless{ObjectMeta: metav1.ObjectMeta{Name: test.name, Namespace: "default"}, Spec: instance.Spec}
		fixture.Spec.TLS = httpTLS
		fixture.Spec.Security = test.initial
		if err := kube.Create(ctx, fixture); err != nil {
			t.Fatal(err)
		}
		if test.pinnedOnly {
			fixture.Status.BasicAuthBaseline = &v1alpha1.BasicAuthBaseline{SecretName: "runtime", SecretUID: "uid", Key: "bootstrap-password", BundleSHA256: strings.Repeat("a", 64)}
		} else {
			fixture.Status.PodCreationSequence = 1
		}
		if err := kube.Status().Update(ctx, fixture); err != nil {
			t.Fatal(err)
		}
		fixture.Spec.Security = test.next
		if err := kube.Update(ctx, fixture); !apierrors.IsInvalid(err) || !strings.Contains(err.Error(), "Managed Basic Auth mode cannot change") {
			t.Fatalf("%s allowed a managed Basic Auth mode change: %v", test.name, err)
		}
	}
	instance.Status.ObservedGeneration = instance.Generation
	instance.Status.LastCandidateIngestObservation = &v1alpha1.CandidateIngestObservationStatus{ObservedGeneration: instance.Generation, Fleet: "green", Revision: "candidate-revision", Result: "LeaseObserved", QualifiedPods: 1, ObservedAt: metav1.Now()}
	instance.Status.LastCandidateSearchObservation = &v1alpha1.CandidateSearchObservationStatus{ObservedGeneration: instance.Generation, Fleet: "green", Revision: "candidate-revision", Result: "WarmthObserved", QualifiedPods: 1, ObservedAt: metav1.Now()}
	instance.Status.LastSearchQualification = &v1alpha1.SearchQualificationObservation{ObservedGeneration: instance.Generation, PodName: "search-a", PodUID: "pod-uid", Fleet: "blue", Zone: "a", Result: "WarmupCompleted", ObservedAt: metav1.Now()}
	if err := kube.Status().Update(ctx, instance); err != nil {
		t.Fatalf("update status: %v", err)
	}
	var observed v1alpha1.OpenSearchServerless
	if err := kube.Get(ctx, types.NamespacedName{Namespace: "default", Name: "catalog"}, &observed); err != nil {
		t.Fatal(err)
	}
	if observed.Status.ObservedGeneration != instance.Generation || observed.Status.LastSearchQualification == nil || observed.Status.LastSearchQualification.PodUID != "pod-uid" {
		t.Fatal("status update was not persisted")
	}
	if observed.Status.LastCandidateIngestObservation == nil || observed.Status.LastCandidateIngestObservation.Revision != "candidate-revision" {
		t.Fatal("candidate ingest observation was not persisted")
	}
	if observed.Status.LastCandidateSearchObservation == nil || observed.Status.LastCandidateSearchObservation.Revision != "candidate-revision" {
		t.Fatal("candidate search observation was not persisted")
	}
	invalid := instance.DeepCopyObject().(*v1alpha1.OpenSearchServerless)
	invalid.Name = "invalid"
	invalid.ResourceVersion = ""
	invalid.UID = ""
	invalid.Spec.Pools["search"].Zones["a"] = v1alpha1.Zone{Replicas: 1, MinServing: 2}
	if err := kube.Create(ctx, invalid); !apierrors.IsInvalid(err) {
		t.Fatalf("expected validation failure, got %v", err)
	}
	reserveInput := &v1alpha1.OpenSearchServerless{ObjectMeta: metav1.ObjectMeta{Name: "reserve-valid", Namespace: "default"}, Spec: instance.Spec}
	reserveInput.Spec.Pools = map[string]v1alpha1.Pool{"search": {Zones: map[string]v1alpha1.Zone{"a": {Replicas: 1, WarmSpare: 1, MinServing: 2}}}}
	if err := kube.Create(ctx, reserveInput); err != nil {
		t.Fatalf("reserve capacity rejected by API server: %v", err)
	}
	overflow := &v1alpha1.OpenSearchServerless{ObjectMeta: metav1.ObjectMeta{Name: "reserve-overflow", Namespace: "default"}, Spec: instance.Spec}
	overflow.Spec.Pools = map[string]v1alpha1.Pool{"search": {Zones: map[string]v1alpha1.Zone{"a": {Replicas: 2147483647, WarmSpare: 1}}}}
	if err := kube.Create(ctx, overflow); !apierrors.IsInvalid(err) {
		t.Fatalf("expected combined replica overflow rejection: %v", err)
	}
	t.Run("operation admission matches action shape", func(t *testing.T) {
		checkOperationActionAdmission(t, ctx, kube, instance)
	})
	operation := &v1alpha1.OpenSearchServerlessOperation{ObjectMeta: metav1.ObjectMeta{Name: "operation", Namespace: "default"}, Spec: v1alpha1.OperationSpec{
		InstanceRef: v1alpha1.InstanceReference{Name: instance.Name, UID: string(instance.UID)}, Action: "Decommission",
	}}
	if err := kube.Create(ctx, operation); err != nil {
		t.Fatalf("create operation: %v", err)
	}
	operation.Spec.InstanceRef.UID = "different-uid"
	if err := kube.Update(ctx, operation); !apierrors.IsInvalid(err) || !strings.Contains(err.Error(), "operation spec is immutable") {
		t.Fatalf("expected an otherwise valid operation edit to fail immutability, got %v", err)
	}
	if err := kube.Get(ctx, types.NamespacedName{Namespace: "default", Name: operation.Name}, operation); err != nil {
		t.Fatal(err)
	}
	secondOperation := &v1alpha1.OpenSearchServerlessOperation{ObjectMeta: metav1.ObjectMeta{Name: "operation-2", Namespace: "default"}, Spec: v1alpha1.OperationSpec{
		InstanceRef: operation.Spec.InstanceRef, Action: "Decommission",
	}}
	if err := kube.Create(ctx, secondOperation); err != nil {
		t.Fatal(err)
	}
	var firstSnapshot, secondSnapshot v1alpha1.OpenSearchServerless
	instanceKey := types.NamespacedName{Namespace: "default", Name: instance.Name}
	if err := kube.Get(ctx, instanceKey, &firstSnapshot); err != nil {
		t.Fatal(err)
	}
	if err := kube.Get(ctx, instanceKey, &secondSnapshot); err != nil {
		t.Fatal(err)
	}
	if err := controller.ClaimOperation(ctx, kube, &firstSnapshot, operation); err != nil {
		t.Fatalf("claim operation: %v", err)
	}
	if err := controller.ClaimOperation(ctx, kube, &secondSnapshot, secondOperation); !apierrors.IsConflict(err) {
		t.Fatalf("stale competing claim must conflict: %v", err)
	}
	if err := kube.Get(ctx, instanceKey, &secondSnapshot); err != nil {
		t.Fatal(err)
	}
	if secondSnapshot.Status.ActiveOperation == nil || secondSnapshot.Status.ActiveOperation.UID != string(operation.UID) {
		t.Fatalf("winning claim did not persist: %#v", secondSnapshot.Status.ActiveOperation)
	}
	if err := controller.ClaimOperation(ctx, kube, &secondSnapshot, secondOperation); err == nil {
		t.Fatal("competing operation acquired an occupied claim")
	}
	if err := controller.ClaimOperation(ctx, kube, &secondSnapshot, operation); err != nil {
		t.Fatalf("winning operation could not recover its claim: %v", err)
	}
	settingsInstance := &v1alpha1.OpenSearchServerless{ObjectMeta: metav1.ObjectMeta{Name: "settings", Namespace: "default"}, Spec: instance.Spec}
	if err := kube.Create(ctx, settingsInstance); err != nil {
		t.Fatal(err)
	}
	settingsInstance.Status.PrimaryStore = &v1alpha1.PrimaryStoreStatus{Bucket: "bucket", Region: "region"}
	settingsInstance.Status.ServingFleetIntentByRole = map[string]string{"search": "initial", "ingest": "initial"}
	meta.SetStatusCondition(&settingsInstance.Status.Conditions, metav1.Condition{Type: "ServicesConfigured", Status: metav1.ConditionTrue, Reason: "Observed", Message: "test"})
	if err := kube.Status().Update(ctx, settingsInstance); err != nil {
		t.Fatal(err)
	}
	settingsOp := &v1alpha1.OpenSearchServerlessOperation{ObjectMeta: metav1.ObjectMeta{Name: "settings-change", Namespace: "default"}, Spec: v1alpha1.OperationSpec{InstanceRef: v1alpha1.InstanceReference{Name: settingsInstance.Name, UID: string(settingsInstance.UID)}, Action: "ApplyClusterSettings", ClusterSettings: &v1alpha1.ClusterSettingsPatch{Set: map[string]string{"serverless.bucket": "off"}}}}
	if err := kube.Create(ctx, settingsOp); err != nil {
		t.Fatal(err)
	}
	backend := &testSettingsBackend{values: map[string]string{"serverless.bucket": "auto"}, serviceUID: "service-uid", loseResponse: true}
	settingsReconciler := controller.OperationReconciler{Client: kube, DevelopmentSettingsImage: settingsInstance.Spec.Image, SettingsBackend: backend}
	settingsKey := types.NamespacedName{Namespace: "default", Name: settingsOp.Name}
	settingsRequest := ctrl.Request{NamespacedName: settingsKey}
	for i := 0; i < 4; i++ {
		if _, err := settingsReconciler.Reconcile(ctx, settingsRequest); err != nil {
			t.Fatal(err)
		}
	}
	if err := kube.Get(ctx, settingsKey, settingsOp); err != nil || settingsOp.Status.Phase != "WritePrepared" || settingsOp.Status.SettingsBase["serverless.bucket"].Value != "auto" {
		t.Fatalf("settings intent did not preserve the base: %v", err)
	}
	if _, err := settingsReconciler.Reconcile(ctx, settingsRequest); err == nil || backend.writes != 1 {
		t.Fatal("expected simulated lost response after one write")
	}
	if _, err := settingsReconciler.Reconcile(ctx, settingsRequest); err != nil {
		t.Fatal(err)
	}
	if err := kube.Get(ctx, settingsKey, settingsOp); err != nil || settingsOp.Status.Phase != "Persisted" || backend.writes != 1 {
		t.Fatalf("ambiguous write did not recover from read-back: %v", err)
	}
	if effective := meta.FindStatusCondition(settingsOp.Status.Conditions, "VerifiedEffective"); effective == nil || effective.Status != metav1.ConditionUnknown {
		t.Fatal("fleet-wide effectiveness was incorrectly asserted")
	}
	settingsInstanceKey := client.ObjectKeyFromObject(settingsInstance)
	if err := kube.Get(ctx, settingsInstanceKey, settingsInstance); err != nil || settingsInstance.Status.ActiveOperation == nil {
		t.Fatalf("settings result must be persisted before its claim is released: %v", err)
	}
	// A new reconciler recovers finalization from API state without an admin backend.
	cleanup := controller.OperationReconciler{Client: kube}
	for i := 0; i < 3; i++ {
		if _, err := cleanup.Reconcile(ctx, settingsRequest); err != nil {
			t.Fatal(err)
		}
	}
	if err := kube.Get(ctx, settingsInstanceKey, settingsInstance); err != nil || settingsInstance.Status.ActiveOperation != nil {
		t.Fatalf("persisted settings kept the instance locked: %v", err)
	}
	if err := kube.Get(ctx, settingsKey, settingsOp); err != nil || len(settingsOp.Finalizers) != 0 || settingsOp.Status.Phase != "Persisted" {
		t.Fatalf("settings finalization changed the outcome or left a finalizer: %v", err)
	}
	cancelOp := &v1alpha1.OpenSearchServerlessOperation{ObjectMeta: metav1.ObjectMeta{Name: "settings-cancel", Namespace: "default"}, Spec: settingsOp.Spec}
	cancelOp.Spec.ClusterSettings = &v1alpha1.ClusterSettingsPatch{Remove: []string{"serverless.bucket"}}
	if err := kube.Create(ctx, cancelOp); err != nil {
		t.Fatal(err)
	}
	cancelKey := client.ObjectKeyFromObject(cancelOp)
	cancelRequest := ctrl.Request{NamespacedName: cancelKey}
	for i := 0; i < 4; i++ {
		if _, err := settingsReconciler.Reconcile(ctx, cancelRequest); err != nil {
			t.Fatal(err)
		}
	}
	if err := kube.Get(ctx, cancelKey, cancelOp); err != nil || cancelOp.Status.Phase != "WritePrepared" || len(cancelOp.Finalizers) != 1 {
		t.Fatalf("a second operation did not acquire the free claim and prepare: %v", err)
	}
	if _, err := settingsReconciler.Reconcile(ctx, settingsRequest); err != nil {
		t.Fatal(err)
	}
	if err := kube.Get(ctx, settingsInstanceKey, settingsInstance); err != nil || settingsInstance.Status.ActiveOperation == nil || settingsInstance.Status.ActiveOperation.UID != string(cancelOp.UID) {
		t.Fatalf("a replay of the persisted operation changed the new claim: %v", err)
	}
	backend.onRead = func() error {
		backend.onRead = nil
		return kube.Delete(ctx, cancelOp)
	}
	if _, err := settingsReconciler.Reconcile(ctx, cancelRequest); err != nil || backend.writes != 1 {
		t.Fatalf("the final check did not prevent sending a write after deletion: %v", err)
	}
	if _, err := cleanup.Reconcile(ctx, cancelRequest); err != nil {
		t.Fatal(err)
	}
	if err := kube.Get(ctx, cancelKey, cancelOp); err != nil || cancelOp.DeletionTimestamp == nil || cancelOp.Status.Phase != "Cancelled" {
		t.Fatalf("cancellation did not record its result before cleanup: %v", err)
	}
	for i := 0; i < 2; i++ {
		if _, err := settingsReconciler.Reconcile(ctx, cancelRequest); err != nil {
			t.Fatal(err)
		}
	}
	if err := kube.Get(ctx, cancelKey, cancelOp); !apierrors.IsNotFound(err) || backend.writes != 1 {
		t.Fatalf("cancelled settings operation was not removed cleanly: %v", err)
	}
	if err := kube.Get(ctx, settingsInstanceKey, settingsInstance); err != nil || settingsInstance.Status.ActiveOperation != nil {
		t.Fatalf("cancelled settings operation left the instance locked: %v", err)
	}
	delayedOp := &v1alpha1.OpenSearchServerlessOperation{ObjectMeta: metav1.ObjectMeta{Name: "settings-delayed", Namespace: "default"}, Spec: settingsOp.Spec}
	delayedOp.Spec.ClusterSettings = &v1alpha1.ClusterSettingsPatch{Set: map[string]string{"serverless.bucket": "auto"}}
	if err := kube.Create(ctx, delayedOp); err != nil {
		t.Fatal(err)
	}
	delayedKey := client.ObjectKeyFromObject(delayedOp)
	delayedRequest := ctrl.Request{NamespacedName: delayedKey}
	backend.deferCommit = true
	for i := 0; i < 4; i++ {
		if _, err := settingsReconciler.Reconcile(ctx, delayedRequest); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := settingsReconciler.Reconcile(ctx, delayedRequest); err == nil || backend.writes != 2 || backend.commit == nil {
		t.Fatal("expected a timed-out write that can still commit on the server")
	}
	if err := kube.Delete(ctx, delayedOp); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		if _, err := settingsReconciler.Reconcile(ctx, delayedRequest); err != nil {
			t.Fatal(err)
		}
	}
	if err := kube.Get(ctx, delayedKey, delayedOp); err != nil || delayedOp.Status.Phase != "Writing" || delayedOp.DeletionTimestamp == nil {
		t.Fatalf("uncertain write was cleared when read-back still showed the base: %v", err)
	}
	if outcome := meta.FindStatusCondition(delayedOp.Status.Conditions, "Ready"); outcome == nil || outcome.Reason != "WriteOutcomeUnknown" {
		t.Fatal("unresolved write outcome was not reported")
	}
	if err := kube.Get(ctx, settingsInstanceKey, settingsInstance); err != nil || settingsInstance.Status.ActiveOperation == nil || settingsInstance.Status.ActiveOperation.UID != string(delayedOp.UID) || backend.writes != 2 {
		t.Fatalf("the unresolved operation lost its claim or replayed the write: %v", err)
	}
	claim := settingsInstance.Status.ActiveOperation
	settingsInstance.Status.ActiveOperation = nil
	if err := kube.Status().Update(ctx, settingsInstance); err != nil {
		t.Fatal(err)
	}
	if _, err := settingsReconciler.Reconcile(ctx, delayedRequest); err != nil {
		t.Fatal(err)
	}
	if err := kube.Get(ctx, delayedKey, delayedOp); err != nil || len(delayedOp.Finalizers) != 1 || delayedOp.Status.Phase != "Writing" {
		t.Fatalf("loss of the claim removed the unresolved write record: %v", err)
	}
	settingsInstance.Status.ActiveOperation = claim
	if err := kube.Status().Update(ctx, settingsInstance); err != nil {
		t.Fatal(err)
	}
	backend.commit()
	backend.deferCommit, backend.commit = false, nil
	if _, err := settingsReconciler.Reconcile(ctx, delayedRequest); err != nil {
		t.Fatal(err)
	}
	if err := kube.Get(ctx, delayedKey, delayedOp); err != nil || delayedOp.Status.Phase != "Persisted" {
		t.Fatalf("the delayed write was not recovered by read-back: %v", err)
	}
	for i := 0; i < 2; i++ {
		if _, err := cleanup.Reconcile(ctx, delayedRequest); err != nil {
			t.Fatal(err)
		}
	}
	if err := kube.Get(ctx, delayedKey, delayedOp); !apierrors.IsNotFound(err) || backend.writes != 2 {
		t.Fatalf("the resolved operation did not finalize without replay: %v", err)
	}
	if err := kube.Get(ctx, settingsInstanceKey, settingsInstance); err != nil || settingsInstance.Status.ActiveOperation != nil {
		t.Fatalf("the resolved delayed write left its claim in place: %v", err)
	}
	claimRace := &v1alpha1.OpenSearchServerlessOperation{ObjectMeta: metav1.ObjectMeta{Name: "settings-claim-race", Namespace: "default"}, Spec: settingsOp.Spec}
	claimRaceRequest := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(claimRace)}
	if err := kube.Create(ctx, claimRace); err != nil {
		t.Fatal(err)
	}
	if _, err := settingsReconciler.Reconcile(ctx, claimRaceRequest); err != nil {
		t.Fatal(err)
	}
	delayedClient := interceptor.NewClient(kube, interceptor.Funcs{
		SubResourcePatch: func(ctx context.Context, c client.Client, subresource string, object client.Object, patch client.Patch, options ...client.SubResourcePatchOption) error {
			if target, ok := object.(*v1alpha1.OpenSearchServerless); ok && target.Status.ActiveOperation != nil {
				if err := kube.Delete(ctx, claimRace); err != nil {
					return err
				}
				for i := 0; i < 2; i++ {
					if _, err := cleanup.Reconcile(ctx, claimRaceRequest); err != nil {
						return err
					}
				}
			}
			return c.SubResource(subresource).Patch(ctx, object, patch, options...)
		},
	})
	oldLeader := controller.OperationReconciler{Client: delayedClient, DevelopmentSettingsImage: settingsReconciler.DevelopmentSettingsImage}
	if _, err := oldLeader.Reconcile(ctx, claimRaceRequest); !apierrors.IsConflict(err) {
		t.Fatalf("a delayed claim patch should conflict after the deletion fence: %v", err)
	}
	if err := kube.Get(ctx, claimRaceRequest.NamespacedName, claimRace); !apierrors.IsNotFound(err) {
		t.Fatalf("unclaimed operation did not delete after fencing: %v", err)
	}
	if err := kube.Get(ctx, settingsInstanceKey, settingsInstance); err != nil || settingsInstance.Status.ActiveOperation != nil {
		t.Fatalf("the stale claim patch left an orphaned claim: %v", err)
	}
	conflictInstance := &v1alpha1.OpenSearchServerless{ObjectMeta: metav1.ObjectMeta{Name: "settings-conflict", Namespace: "default"}, Spec: instance.Spec}
	if err := kube.Create(ctx, conflictInstance); err != nil {
		t.Fatal(err)
	}
	conflictInstance.Status.PrimaryStore = &v1alpha1.PrimaryStoreStatus{Bucket: "bucket", Region: "region"}
	conflictInstance.Status.ServingFleetIntentByRole = map[string]string{"search": "initial", "ingest": "initial"}
	meta.SetStatusCondition(&conflictInstance.Status.Conditions, metav1.Condition{Type: "ServicesConfigured", Status: metav1.ConditionTrue, Reason: "Observed", Message: "test"})
	if err := kube.Status().Update(ctx, conflictInstance); err != nil {
		t.Fatal(err)
	}
	conflictOp := &v1alpha1.OpenSearchServerlessOperation{ObjectMeta: metav1.ObjectMeta{Name: "settings-conflict", Namespace: "default"}, Spec: v1alpha1.OperationSpec{InstanceRef: v1alpha1.InstanceReference{Name: conflictInstance.Name, UID: string(conflictInstance.UID)}, Action: "ApplyClusterSettings", ClusterSettings: &v1alpha1.ClusterSettingsPatch{Set: map[string]string{"serverless.bucket": "off"}}}}
	if err := kube.Create(ctx, conflictOp); err != nil {
		t.Fatal(err)
	}
	conflictBackend := &testSettingsBackend{values: map[string]string{"serverless.bucket": "auto"}, serviceUID: "service-uid"}
	settingsReconciler.SettingsBackend = conflictBackend
	conflictKey := types.NamespacedName{Namespace: "default", Name: conflictOp.Name}
	for i := 0; i < 4; i++ {
		if _, err := settingsReconciler.Reconcile(ctx, ctrl.Request{NamespacedName: conflictKey}); err != nil {
			t.Fatal(err)
		}
	}
	conflictBackend.values["serverless.bucket"] = "external-change"
	if _, err := settingsReconciler.Reconcile(ctx, ctrl.Request{NamespacedName: conflictKey}); err != nil {
		t.Fatal(err)
	}
	if err := kube.Get(ctx, conflictKey, conflictOp); err != nil || conflictOp.Status.Phase != "Conflict" || conflictBackend.writes != 0 {
		t.Fatalf("conflicting settings did not stop the write: %v", err)
	}
	for i := 0; i < 3; i++ {
		if _, err := settingsReconciler.Reconcile(ctx, ctrl.Request{NamespacedName: conflictKey}); err != nil {
			t.Fatal(err)
		}
	}
	if err := kube.Get(ctx, client.ObjectKeyFromObject(conflictInstance), conflictInstance); err != nil || conflictInstance.Status.ActiveOperation != nil || conflictBackend.writes != 0 {
		t.Fatalf("conflicted settings did not release without replay: %v", err)
	}
	core, err := kubernetes.NewForConfig(config)
	if err != nil {
		t.Fatal(err)
	}
	routes := &v1alpha1.OpenSearchServerless{ObjectMeta: metav1.ObjectMeta{Name: "routes", Namespace: "default"}, Spec: instance.Spec}
	if err := kube.Create(ctx, routes); err != nil {
		t.Fatal(err)
	}
	routesKey := types.NamespacedName{Namespace: "default", Name: routes.Name}
	routesRequest := ctrl.Request{NamespacedName: routesKey}
	serviceReconciler := controller.InstanceReconciler{Client: kube, Core: core.CoreV1()}
	if _, err := serviceReconciler.Reconcile(ctx, routesRequest); err != nil {
		t.Fatal(err)
	}
	if _, err := core.CoreV1().Services("default").Get(ctx, "routes-search", metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Fatalf("Service created before recorded intent: %v", err)
	}
	if _, err := serviceReconciler.Reconcile(ctx, routesRequest); err != nil {
		t.Fatal(err)
	}
	searchService, err := core.CoreV1().Services("default").Get(ctx, "routes-search", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	indexService, err := core.CoreV1().Services("default").Get(ctx, "routes-index", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if searchService.Spec.Selector[workload.RoleLabel] != "search" || indexService.Spec.Selector[workload.RoleLabel] != "ingest" || searchService.Spec.Selector[workload.FleetLabel] != "initial" {
		t.Fatal("Services did not select their intended roles and fleet")
	}
	searchService.Spec.Selector[workload.FleetLabel] = "unexpected"
	if _, err := core.CoreV1().Services("default").Update(ctx, searchService, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := serviceReconciler.Reconcile(ctx, routesRequest); err != nil {
		t.Fatal(err)
	}
	if err := kube.Get(ctx, routesKey, routes); err != nil {
		t.Fatal(err)
	}
	serviceCondition := meta.FindStatusCondition(routes.Status.Conditions, "ServicesConfigured")
	if serviceCondition == nil || serviceCondition.Status != metav1.ConditionFalse {
		t.Fatalf("selector conflict not reported: %#v", routes.Status)
	}
	searchService, err = core.CoreV1().Services("default").Get(ctx, "routes-search", metav1.GetOptions{})
	if err != nil || searchService.Spec.Selector[workload.FleetLabel] != "unexpected" {
		t.Fatalf("conflicting selector was overwritten: %v", err)
	}
	gated := &v1alpha1.OpenSearchServerless{ObjectMeta: metav1.ObjectMeta{Name: "gated", Namespace: "default"}, Spec: instance.Spec}
	immutable := true
	keystore, err := core.CoreV1().Secrets("default").Create(ctx, &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "runtime-keystore"}, Immutable: &immutable, Data: map[string][]byte{"opensearch.keystore": []byte("test-only-not-a-real-keystore")}}, metav1.CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	gated.Spec.Storage.Credentials = &v1alpha1.StorageCredentials{Type: "Keystore", KeystoreSecretRef: &v1alpha1.LocalReference{Name: keystore.Name}}
	profileJSON := `{"revision":"v1","role":"search","zones":["zone-a"],"nodeSelector":{"platform.example/profile":"search"},"taint":{"key":"platform.example/dedicated","value":"search","effect":"NoSchedule"},"observedAt":"2025-12-31T23:00:00Z","expiresAt":"2026-01-02T00:00:00Z","allocatable":{"cpu":"4","memory":"8Gi","ephemeral-storage":"20Gi","pods":"20"},"reserved":{"cpu":"1","memory":"1Gi","ephemeral-storage":"1Gi","pods":"3"},"headroom":{"cpu":"1","memory":"1Gi","ephemeral-storage":"1Gi","pods":"1"}}`
	profileCM, err := core.CoreV1().ConfigMaps("default").Create(ctx, &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "search-profile"}, Immutable: &immutable, Data: map[string]string{"profile.json": profileJSON}}, metav1.CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	gated.Spec.Pools = map[string]v1alpha1.Pool{"search": {JVMHeap: "512Mi", WarmOnJoin: true, ProtectFromScaleDown: true, CapacityProfileRef: profileCM.Name, Zones: map[string]v1alpha1.Zone{"zone-a": {Replicas: 1, WarmSpare: 1, MinServing: 1}}, Resources: corev1.ResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("1"), corev1.ResourceMemory: resource.MustParse("1Gi"), corev1.ResourceEphemeralStorage: resource.MustParse("1Gi")}}}}
	if err := kube.Create(ctx, gated); err != nil {
		t.Fatal(err)
	}
	gatedKey := types.NamespacedName{Namespace: "default", Name: gated.Name}
	gatedRequest := ctrl.Request{NamespacedName: gatedKey}
	fakeNow := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	peerServiceForCert, err := workload.PeerDNSService(gated)
	if err != nil {
		t.Fatal(err)
	}
	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	caCert := &x509.Certificate{SerialNumber: big.NewInt(1), NotBefore: fakeNow.Add(-time.Hour), NotAfter: fakeNow.Add(6 * time.Hour), IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign}
	caDER, err := x509.CreateCertificate(rand.Reader, caCert, caCert, &caKey.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	peerKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	peerCert := &x509.Certificate{SerialNumber: big.NewInt(2), DNSNames: []string{"*." + peerServiceForCert.Name + ".default.svc.cluster.local"}, NotBefore: fakeNow.Add(-time.Hour), NotAfter: fakeNow.Add(5 * time.Hour), ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth}}
	peerDER, err := x509.CreateCertificate(rand.Reader, peerCert, caCert, &peerKey.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	peerKeyDER, err := x509.MarshalPKCS8PrivateKey(peerKey)
	if err != nil {
		t.Fatal(err)
	}
	peerLeaf, err := core.CoreV1().Secrets("default").Create(ctx, &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "peer-leaf"}, Type: corev1.SecretTypeTLS, Data: map[string][]byte{corev1.TLSCertKey: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: peerDER}), corev1.TLSPrivateKeyKey: pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: peerKeyDER})}}, metav1.CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	peerTrust, err := core.CoreV1().Secrets("default").Create(ctx, &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "peer-trust"}, Data: map[string][]byte{"ca.crt": pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDER})}}, metav1.CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	gated.Spec.TLS = &v1alpha1.TLSConfig{Transport: &v1alpha1.TLSChannel{Source: "Secret", CertificateRef: v1alpha1.LocalReference{Name: peerLeaf.Name}, TrustBundleRef: v1alpha1.SecretKeyReference{Name: peerTrust.Name, Key: "ca.crt"}}}
	if err := kube.Update(ctx, gated); err != nil {
		t.Fatal(err)
	}
	gatedReconciler := controller.InstanceReconciler{Client: kube, Core: core.CoreV1(), Policy: core.PolicyV1(), DevelopmentGatedPods: true, DevelopmentPendingTimeout: time.Minute, DevelopmentWarmupImage: gated.Spec.Image, DevelopmentPeerDNSImage: gated.Spec.Image, DevelopmentPeerTLSImage: gated.Spec.Image, DevelopmentClusterDomain: "cluster.local", Now: func() time.Time { return fakeNow }}
	for i := 0; i < 6; i++ {
		if _, err := gatedReconciler.Reconcile(ctx, gatedRequest); err != nil {
			t.Fatal(err)
		}
	}
	if err := kube.Get(ctx, gatedKey, gated); err != nil {
		t.Fatal(err)
	}
	if gated.Status.PendingPodCreation != nil {
		t.Fatal("Pod intent created before PDB controller observed the budget")
	}
	budgets, err := core.PolicyV1().PodDisruptionBudgets("default").List(ctx, metav1.ListOptions{})
	if err != nil || len(budgets.Items) != 1 {
		t.Fatalf("expected one role and zone budget: %v", err)
	}
	budget := &budgets.Items[0]
	if budget.Spec.Selector.MatchLabels[workload.RoleLabel] != "search" || budget.Spec.Selector.MatchLabels[workload.ZoneLabel] != "zone-a" {
		t.Fatal("PDB selector does not identify the role and zone")
	}
	if budget.Spec.MinAvailable == nil || budget.Spec.MinAvailable.IntVal != 1 {
		t.Fatal("PDB did not use the configured zonal serving floor")
	}
	budget.Spec.Selector.MatchLabels[workload.ZoneLabel] = "wrong-zone"
	budget, err = core.PolicyV1().PodDisruptionBudgets("default").Update(ctx, budget, metav1.UpdateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := gatedReconciler.Reconcile(ctx, gatedRequest); err != nil {
		t.Fatal(err)
	}
	if err := kube.Get(ctx, gatedKey, gated); err != nil {
		t.Fatal(err)
	}
	budgetCondition := meta.FindStatusCondition(gated.Status.Conditions, "DisruptionBudgetsConfigured")
	if budgetCondition == nil || budgetCondition.Status != metav1.ConditionFalse {
		t.Fatal("conflicting PDB was not reported")
	}
	budget, err = core.PolicyV1().PodDisruptionBudgets("default").Get(ctx, budget.Name, metav1.GetOptions{})
	if err != nil || budget.Spec.Selector.MatchLabels[workload.ZoneLabel] != "wrong-zone" {
		t.Fatalf("conflicting PDB selector was overwritten: %v", err)
	}
	budget.Spec.Selector.MatchLabels[workload.ZoneLabel] = "zone-a"
	budget, err = core.PolicyV1().PodDisruptionBudgets("default").Update(ctx, budget, metav1.UpdateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	budget.Status.ObservedGeneration = budget.Generation
	if _, err := core.PolicyV1().PodDisruptionBudgets("default").UpdateStatus(ctx, budget, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		if _, err := gatedReconciler.Reconcile(ctx, gatedRequest); err != nil {
			t.Fatal(err)
		}
	}
	if err := kube.Get(ctx, gatedKey, gated); err != nil {
		t.Fatal(err)
	}
	if gated.Status.PendingPodCreation == nil || gated.Status.PendingPodCreation.Zone != "zone-a" {
		t.Fatalf("Pod creation intent missing: %#v", gated.Status)
	}
	if len(gated.Status.ServingRevisionByRole) != 0 {
		t.Fatal("a pending first creation established a serving revision before any Pod existed")
	}
	if len(gated.Status.ZoneCapacity) != 1 || gated.Status.ZoneCapacity[0].WarmSpare != 1 || gated.Status.ZoneCapacity[0].WarmReserveStatus != "InsufficientReadyPods" || gated.Status.ZoneCapacity[0].WarmReserveReadyShortfall != 1 || gated.Status.ZoneCapacity[0].ReadyShortfall != 2 || !gated.Status.ZoneCapacity[0].BelowMinServing || gated.Status.ZoneCapacityObservedAt == nil || gated.Status.ZoneCapacityObservedGeneration != gated.Generation {
		t.Fatal("timestamped reserve shortfall and unknown warmth were not reported")
	}
	if gated.Status.PendingPodCreation.KeystoreUID != string(keystore.UID) {
		t.Fatal("keystore identity was not pinned in the creation intent")
	}
	if gated.Status.PendingPodCreation.CapacityProfileUID != string(profileCM.UID) {
		t.Fatal("capacity profile identity was not pinned")
	}
	if gated.Status.PeerTLSBaseline == nil || gated.Status.PeerTLSBaseline.LeafUID != string(peerLeaf.UID) || gated.Status.PendingPodCreation.PeerTLSTrustUID != string(peerTrust.UID) || gated.Status.PendingPodCreation.PeerTLSLeafSHA256 == "" || gated.Status.PendingPodCreation.PeerTLSTrustSHA256 == "" {
		t.Fatal("peer TLS inputs were not pinned before Pod creation")
	}
	intentName := gated.Status.PendingPodCreation.Name
	gatedReconciler.DevelopmentClusterDomain = "changed.local"
	if _, err := gatedReconciler.Reconcile(ctx, gatedRequest); err != nil {
		t.Fatal(err)
	}
	if err := kube.Get(ctx, gatedKey, gated); err != nil || gated.Status.PendingPodCreation != nil || len(gated.Status.RetiredPodCreations) != 1 || gated.Status.RetiredPodCreations[0].Name != intentName {
		t.Fatalf("changed peer domain did not retire the old pending creation: %v", err)
	}
	renewingLeaf := peerLeaf.DeepCopy()
	renewingLeaf.Labels = map[string]string{"issuer-revision": "bootstrap-renewal"}
	renewingLeaf, err = core.CoreV1().Secrets("default").Update(ctx, renewingLeaf, metav1.UpdateOptions{})
	if err != nil || renewingLeaf.ResourceVersion == peerLeaf.ResourceVersion {
		t.Fatalf("issuer did not refresh the source before snapshot creation: %v", err)
	}
	gatedReconciler.DevelopmentClusterDomain = "cluster.local"
	if _, err := core.CoreV1().Pods("default").Get(ctx, intentName, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Fatalf("Pod appeared before creation intent: %v", err)
	}
	for i := 0; i < 5; i++ {
		if _, err := gatedReconciler.Reconcile(ctx, gatedRequest); err != nil {
			t.Fatal(err)
		}
		if err := kube.Get(ctx, gatedKey, gated); err != nil {
			t.Fatal(err)
		}
		if gated.Status.PendingPodCreation != nil {
			break
		}
	}
	if gated.Status.PendingPodCreation == nil || gated.Status.PendingPodCreation.Name == intentName {
		t.Fatal("fresh creation intent was not recorded after restoring peer domain")
	}
	if gated.Status.PeerTLSBaseline == nil || gated.Status.PeerTLSBaseline.LeafResourceVersion != renewingLeaf.ResourceVersion {
		t.Fatal("new bootstrap intent did not observe the renewed source version")
	}
	intentName = gated.Status.PendingPodCreation.Name
	if _, err := gatedReconciler.Reconcile(ctx, gatedRequest); err != nil {
		t.Fatal(err)
	}
	if err := kube.Get(ctx, gatedKey, gated); err != nil || gated.Status.PendingPodCreation == nil || gated.Status.PeerTLSBaseline.SnapshotUID == "" {
		t.Fatalf("snapshot identity was not persisted before Pod creation: %v", err)
	}
	savedIntent := *gated.Status.PendingPodCreation
	stagedSecret, err := core.CoreV1().Secrets("default").Get(ctx, gated.Status.PeerTLSBaseline.SnapshotName, metav1.GetOptions{})
	if err != nil || stagedSecret.Immutable == nil || !*stagedSecret.Immutable || string(stagedSecret.UID) != gated.Status.PeerTLSBaseline.SnapshotUID {
		t.Fatalf("immutable TLS snapshot missing: %v", err)
	}
	if _, err := core.CoreV1().Pods("default").Get(ctx, intentName, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Fatalf("Pod appeared before snapshot identity was persisted: %v", err)
	}
	if _, err := gatedReconciler.Reconcile(ctx, gatedRequest); err != nil {
		t.Fatal(err)
	}
	held, err := core.CoreV1().Pods("default").Get(ctx, intentName, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if len(held.Spec.SchedulingGates) != 1 || held.Spec.NodeSelector[corev1.LabelTopologyZone] != "zone-a" || held.Labels[workload.RoleLabel] != "search" {
		t.Fatalf("incorrect held Pod: %#v", held.Spec)
	}
	if held.Spec.AutomountServiceAccountToken == nil || *held.Spec.AutomountServiceAccountToken {
		t.Fatal("API-server read-back enabled automatic service account token mounting")
	}
	identityPod := held.DeepCopy()
	identityPod.Name = "web-identity-projection-contract"
	identityPod.UID = ""
	identityPod.ResourceVersion = ""
	identityPod.CreationTimestamp = metav1.Time{}
	identityPod.OwnerReferences = nil
	identityPod.Labels = nil
	identityPod.Status = corev1.PodStatus{}
	if err := workload.AddS3WebIdentity(identityPod, "arn:aws:iam::123456789012:role/test"); err != nil {
		t.Fatal(err)
	}
	admittedIdentityPod, err := core.CoreV1().Pods("default").Create(ctx, identityPod, metav1.CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := workload.ValidateS3WebIdentity(identityPod, admittedIdentityPod); err != nil {
		t.Fatalf("API server changed web identity projection: %v", err)
	}
	if held.Annotations["serverless.opensearch.org/keystore-uid"] != string(keystore.UID) {
		t.Fatal("Pod did not record keystore identity")
	}
	if held.Annotations["serverless.opensearch.org/peer-tls-leaf-uid"] != string(peerLeaf.UID) || held.Annotations["serverless.opensearch.org/peer-tls-trust-uid"] != string(peerTrust.UID) {
		t.Fatal("Pod did not record peer TLS Secret identities")
	}
	updatedPeerTrust, err := core.CoreV1().Secrets("default").Get(ctx, peerTrust.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	updatedPeerTrust.Labels = map[string]string{"issuer-revision": "changed"}
	updatedPeerTrust, err = core.CoreV1().Secrets("default").Update(ctx, updatedPeerTrust, metav1.UpdateOptions{})
	if err != nil || updatedPeerTrust.ResourceVersion == gated.Status.PeerTLSBaseline.TrustResourceVersion {
		t.Fatalf("issuer source version did not change: %v", err)
	}
	leafPin := "-Dserverless.peer_tls.expected_leaf_sha256=" + gated.Status.PeerTLSBaseline.LeafSHA256
	trustPin := "-Dserverless.peer_tls.expected_trust_sha256=" + gated.Status.PeerTLSBaseline.TrustSHA256
	leafFound, trustFound := false, false
	for _, arg := range held.Spec.Containers[0].Args {
		if arg == leafPin {
			leafFound = true
		}
		if arg == trustPin {
			trustFound = true
		}
	}
	if !leafFound || !trustFound {
		t.Fatal("approved public TLS material pins missing from Pod arguments")
	}
	if held.Spec.NodeSelector["platform.example/profile"] != "search" || held.Spec.Affinity == nil || len(held.Spec.Tolerations) == 0 {
		t.Fatal("dedicated capacity profile placement was not rendered")
	}
	anti := held.Spec.Affinity.PodAntiAffinity
	if anti == nil || len(anti.RequiredDuringSchedulingIgnoredDuringExecution) != 1 || anti.RequiredDuringSchedulingIgnoredDuringExecution[0].NamespaceSelector == nil || len(anti.RequiredDuringSchedulingIgnoredDuringExecution[0].LabelSelector.MatchExpressions) != 1 || anti.RequiredDuringSchedulingIgnoredDuringExecution[0].LabelSelector.MatchExpressions[0].Key != workload.InstanceLabel || anti.RequiredDuringSchedulingIgnoredDuringExecution[0].LabelSelector.MatchExpressions[0].Operator != metav1.LabelSelectorOpExists {
		t.Fatal("API server lost cross-namespace dedicated host selector")
	}
	if held.Annotations["cluster-autoscaler.kubernetes.io/safe-to-evict"] != "false" {
		t.Fatal("autoscaler protection was not present at creation")
	}
	peerDesired, err := workload.PeerDNSService(gated)
	if err != nil {
		t.Fatal(err)
	}
	peerService, err := core.CoreV1().Services("default").Get(ctx, peerDesired.Name, metav1.GetOptions{})
	if err != nil || peerService.Spec.ClusterIP != "None" || !peerService.Spec.PublishNotReadyAddresses || held.Spec.Subdomain != peerDesired.Name {
		t.Fatalf("headless peer Service or Pod subdomain not persisted: %v", err)
	}
	wantHostname, err := workload.PeerHostname(held.Name)
	if err != nil || held.Spec.Hostname != wantHostname {
		t.Fatalf("unexpected Pod hostname: %v", err)
	}
	wantDNSArgument := "-Dserverless.transport.advertise_host=" + wantHostname + "." + peerDesired.Name + ".default.svc.cluster.local"
	dnsArgumentFound := false
	for _, arg := range held.Spec.Containers[0].Args {
		if arg == wantDNSArgument {
			dnsArgumentFound = true
		}
	}
	if !dnsArgumentFound || held.Annotations["serverless.opensearch.org/peer-dns-domain"] != "cluster.local" {
		t.Fatal("qualified Java peer DNS identity missing")
	}
	peerService.Spec.PublishNotReadyAddresses = false
	peerService, err = core.CoreV1().Services("default").Update(ctx, peerService, metav1.UpdateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	conflictSeen := false
	for i := 0; i < 5; i++ {
		if _, err := gatedReconciler.Reconcile(ctx, gatedRequest); err != nil {
			conflictSeen = true
			break
		}
	}
	if !conflictSeen {
		t.Fatal("conflicting peer Service accepted")
	}
	peerService.Spec.PublishNotReadyAddresses = true
	if _, err := core.CoreV1().Services("default").Update(ctx, peerService, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	if err := kube.Get(ctx, gatedKey, gated); err != nil {
		t.Fatal(err)
	}
	gated.Status.PendingPodCreation = &savedIntent
	if err := kube.Status().Update(ctx, gated); err != nil {
		t.Fatal(err)
	}
	held.Labels[workload.FleetLabel] = "unexpected"
	held, err = core.CoreV1().Pods("default").Update(ctx, held, metav1.UpdateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	conflictSeen = false
	for i := 0; i < 5; i++ {
		if _, err := gatedReconciler.Reconcile(ctx, gatedRequest); err != nil {
			conflictSeen = true
			break
		}
	}
	if !conflictSeen {
		t.Fatal("recovery accepted a Pod with a changed fleet label")
	}
	if err := kube.Get(ctx, gatedKey, gated); err != nil || gated.Status.PendingPodCreation == nil {
		t.Fatalf("invalid Pod cleared its creation intent: %v", err)
	}
	held.Labels[workload.FleetLabel] = savedIntent.Fleet
	held, err = core.CoreV1().Pods("default").Update(ctx, held, metav1.UpdateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := gatedReconciler.Reconcile(ctx, gatedRequest); err != nil {
		t.Fatalf("recover ambiguous create: %v", err)
	}
	recoveredHeld, err := core.CoreV1().Pods("default").Get(ctx, intentName, metav1.GetOptions{})
	if err != nil || recoveredHeld.UID != held.UID {
		t.Fatalf("creation retry did not recover the same Pod: %v", err)
	}
	if _, err := gatedReconciler.Reconcile(ctx, gatedRequest); err != nil {
		t.Fatal(err)
	}
	ownedPods, err := core.CoreV1().Pods("default").List(ctx, metav1.ListOptions{LabelSelector: workload.InstanceLabel + "=" + string(gated.UID)})
	if err != nil || len(ownedPods.Items) != 1 {
		t.Fatalf("held Pod did not bound further creation: %d, %v", len(ownedPods.Items), err)
	}
	gatedReconciler.DevelopmentReleaseGates = true
	if _, err := gatedReconciler.Reconcile(ctx, gatedRequest); err != nil {
		t.Fatal(err)
	}
	if err := kube.Get(ctx, gatedKey, gated); err != nil || gated.Status.PendingGateRelease == nil || gated.Status.PendingGateRelease.PodUID != string(held.UID) {
		t.Fatalf("gate release intent was not persisted: %v", err)
	}
	if gated.Status.SearchWarmupSlot == nil || gated.Status.SearchWarmupSlot.PodUID != string(held.UID) {
		t.Fatal("warm-up slot must be reserved before gate release")
	}
	held, err = core.CoreV1().Pods("default").Get(ctx, intentName, metav1.GetOptions{})
	if err != nil || len(held.Spec.SchedulingGates) != 1 {
		t.Fatalf("gate changed before release intent was persisted: %v", err)
	}
	reads := 0
	gatedReconciler.Core = keystoreReadClient{CoreV1Interface: core.CoreV1(), onRead: func(secret *corev1.Secret) {
		if secret.Name == keystore.Name {
			reads++
			if reads == 2 {
				secret.UID = "recreated-keystore"
			}
		}
	}}
	if _, err := gatedReconciler.Reconcile(ctx, gatedRequest); err == nil || !strings.Contains(err.Error(), "keystore Secret identity changed") || reads != 2 {
		t.Fatalf("replacement keystore was not rejected before gate release: %v (reads=%d)", err, reads)
	}
	gatedReconciler.Core = core.CoreV1()
	if stillHeld, err := core.CoreV1().Pods("default").Get(ctx, intentName, metav1.GetOptions{}); err != nil || len(stillHeld.Spec.SchedulingGates) != 1 {
		t.Fatalf("changed keystore UID let the gate be removed: %v", err)
	}
	if _, err := gatedReconciler.Reconcile(ctx, gatedRequest); err != nil {
		t.Fatal(err)
	}
	held, err = core.CoreV1().Pods("default").Get(ctx, intentName, metav1.GetOptions{})
	if err != nil || len(held.Spec.SchedulingGates) != 0 {
		t.Fatalf("gate was not removed: %v", err)
	}
	if err := kube.Get(ctx, gatedKey, gated); err != nil || gated.Status.PendingGateRelease == nil {
		t.Fatalf("release was not retained pending observation: %v", err)
	}
	if _, err := gatedReconciler.Reconcile(ctx, gatedRequest); err != nil {
		t.Fatal(err)
	}
	if err := kube.Get(ctx, gatedKey, gated); err != nil || gated.Status.PendingGateRelease != nil {
		t.Fatalf("observed release was not completed: %v", err)
	}
	if _, err := gatedReconciler.Reconcile(ctx, gatedRequest); err != nil {
		t.Fatal(err)
	}
	ownedPods, err = core.CoreV1().Pods("default").List(ctx, metav1.ListOptions{LabelSelector: workload.InstanceLabel + "=" + string(gated.UID)})
	if err != nil || len(ownedPods.Items) != 1 {
		t.Fatalf("unbound released Pod did not consume the provisioning slot: %v", err)
	}
	fakeNow = fakeNow.Add(2 * time.Minute)
	if _, err := gatedReconciler.Reconcile(ctx, gatedRequest); err != nil {
		t.Fatal(err)
	}
	if err := kube.Get(ctx, gatedKey, gated); err != nil || gated.Status.ProvisioningAttempt == nil || gated.Status.ProvisioningAttempt.Phase != "Withdrawing" {
		t.Fatalf("timed-out Pod withdrawal intent missing: %v", err)
	}
	if _, err := core.CoreV1().Pods("default").Get(ctx, intentName, metav1.GetOptions{}); err != nil {
		t.Fatalf("Pod deleted before withdrawal intent: %v", err)
	}
	if _, err := gatedReconciler.Reconcile(ctx, gatedRequest); err != nil {
		t.Fatal(err)
	}
	if _, err := gatedReconciler.Reconcile(ctx, gatedRequest); err != nil {
		t.Fatal(err)
	}
	if err := kube.Get(ctx, gatedKey, gated); err != nil || gated.Status.ProvisioningAttempt == nil || gated.Status.ProvisioningAttempt.Phase != "Blocked" {
		t.Fatalf("withdrawal did not hold future attempts: %v", err)
	}
	if _, err := gatedReconciler.Reconcile(ctx, gatedRequest); err != nil {
		t.Fatal(err)
	}
	ownedPods, err = core.CoreV1().Pods("default").List(ctx, metav1.ListOptions{LabelSelector: workload.InstanceLabel + "=" + string(gated.UID)})
	if err != nil || len(ownedPods.Items) != 0 {
		t.Fatalf("blocked provisioning recreated a Pod: %v", err)
	}
	if _, err := gatedReconciler.Reconcile(ctx, gatedRequest); err != nil {
		t.Fatal(err)
	}
	if err := kube.Get(ctx, gatedKey, gated); err != nil || gated.Status.ProvisioningAttempt != nil || len(gated.Status.BlockedProvisioningAttempts) != 1 || gated.Status.BlockedProvisioningAttempts[0].PodUID != string(held.UID) || gated.Status.SearchWarmupSlot != nil {
		t.Fatalf("blocked group is not ready for explicit recovery: %v, status=%#v", err, gated.Status)
	}
	// A retry must clear only the explicitly acknowledged group.
	gated.Status.BlockedProvisioningAttempts = append(gated.Status.BlockedProvisioningAttempts, v1alpha1.ProvisioningAttempt{Name: "other-gone", PodUID: "other-uid", Role: "ingest", Zone: "zone-b", Phase: "Blocked", Reason: "PodGone", StartedAt: metav1.NewTime(fakeNow)})
	if err := kube.Status().Update(ctx, gated); err != nil {
		t.Fatal(err)
	}
	unacknowledged := &v1alpha1.OpenSearchServerlessOperation{ObjectMeta: metav1.ObjectMeta{Name: "retry-unacknowledged", Namespace: "default"}, Spec: v1alpha1.OperationSpec{InstanceRef: v1alpha1.InstanceReference{Name: gated.Name, UID: string(gated.UID)}, Action: "RetryProvisioning", ProvisioningRetry: &v1alpha1.ProvisioningRetry{PodUID: string(held.UID)}}}
	if err := kube.Create(ctx, unacknowledged); !apierrors.IsInvalid(err) {
		t.Fatalf("unacknowledged retry was not rejected: %v", err)
	}
	retryOp := &v1alpha1.OpenSearchServerlessOperation{ObjectMeta: metav1.ObjectMeta{Name: "retry-provisioning", Namespace: "default"}, Spec: v1alpha1.OperationSpec{InstanceRef: v1alpha1.InstanceReference{Name: gated.Name, UID: string(gated.UID)}, Action: "RetryProvisioning", ProvisioningRetry: &v1alpha1.ProvisioningRetry{PodUID: string(held.UID), AcknowledgeNewAttempt: true}}}
	if err := kube.Create(ctx, retryOp); err != nil {
		t.Fatal(err)
	}
	retryKey := types.NamespacedName{Namespace: "default", Name: retryOp.Name}
	retryRequest := ctrl.Request{NamespacedName: retryKey}
	retryReconciler := controller.OperationReconciler{Client: kube, Core: core.CoreV1(), DevelopmentRetryProvisioning: true}
	if _, err := retryReconciler.Reconcile(ctx, retryRequest); err != nil {
		t.Fatal(err)
	} // finalizer
	conflictingPod, err := core.CoreV1().Pods("default").Create(ctx, &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: intentName}, Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "other", Image: "example.invalid/other"}}}}, metav1.CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := retryReconciler.Reconcile(ctx, retryRequest); err != nil {
		t.Fatal(err)
	}
	if err := kube.Get(ctx, retryKey, retryOp); err != nil {
		t.Fatal(err)
	}
	if condition := meta.FindStatusCondition(retryOp.Status.Conditions, "Ready"); condition == nil || condition.Reason != "PodStillExists" {
		t.Fatal("name reuse must block retry")
	}
	if err := core.CoreV1().Pods("default").Delete(ctx, conflictingPod.Name, metav1.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		if _, err := retryReconciler.Reconcile(ctx, retryRequest); err != nil {
			t.Fatal(err)
		}
	}
	if err := kube.Get(ctx, gatedKey, gated); err != nil || gated.Status.ProvisioningAttempt != nil || len(gated.Status.BlockedProvisioningAttempts) != 1 || gated.Status.BlockedProvisioningAttempts[0].PodUID != "other-uid" || gated.Status.ActiveOperation == nil || gated.Status.LastProvisioningRetry == nil || gated.Status.LastProvisioningRetry.OperationUID != string(retryOp.UID) {
		t.Fatalf("retry reset and recovery marker were not committed together: %v", err)
	}
	// Resume with a fresh reconciler after the instance effect was persisted.
	retryReconciler = controller.OperationReconciler{Client: kube, Core: core.CoreV1(), DevelopmentRetryProvisioning: true}
	for i := 0; i < 3; i++ {
		if _, err := retryReconciler.Reconcile(ctx, retryRequest); err != nil {
			t.Fatal(err)
		}
	}
	if err := kube.Get(ctx, retryKey, retryOp); err != nil || retryOp.Status.Phase != "Succeeded" || len(retryOp.Finalizers) != 0 {
		t.Fatalf("retry did not converge after restart: %v", err)
	}
	if err := kube.Get(ctx, gatedKey, gated); err != nil || gated.Status.ActiveOperation != nil {
		t.Fatalf("retry claim was not released: %v", err)
	}
	if err := core.CoreV1().Secrets("default").Delete(ctx, peerLeaf.Name, metav1.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	recreatedLeaf, err := core.CoreV1().Secrets("default").Create(ctx, &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: peerLeaf.Name}, Type: corev1.SecretTypeTLS, Data: map[string][]byte{
		corev1.TLSCertKey: []byte("incomplete renewal"), corev1.TLSPrivateKeyKey: peerLeaf.Data[corev1.TLSPrivateKeyKey],
	}}, metav1.CreateOptions{})
	if err != nil || recreatedLeaf.UID == peerLeaf.UID {
		t.Fatalf("peer Secret was not recreated with a new identity: %v", err)
	}
	var replacement *corev1.Pod
	for i := 0; i < 5; i++ {
		if _, err := gatedReconciler.Reconcile(ctx, gatedRequest); err != nil {
			t.Fatalf("pinned peer snapshot did not survive source replacement: %v", err)
		}
		ownedPods, err = core.CoreV1().Pods("default").List(ctx, metav1.ListOptions{LabelSelector: workload.InstanceLabel + "=" + string(gated.UID)})
		if err != nil {
			t.Fatal(err)
		}
		if len(ownedPods.Items) == 1 {
			replacement = &ownedPods.Items[0]
			break
		}
	}
	if replacement == nil || replacement.UID == held.UID || replacement.Annotations["serverless.opensearch.org/peer-tls-snapshot-uid"] != string(stagedSecret.UID) ||
		replacement.Annotations["serverless.opensearch.org/peer-tls-leaf-uid"] != string(peerLeaf.UID) {
		t.Fatal("replacement Pod did not keep the original peer TLS snapshot")
	}
	for i := 0; i < 5; i++ {
		if _, err := gatedReconciler.Reconcile(ctx, gatedRequest); err != nil {
			t.Fatalf("could not persist replacement gate release: %v", err)
		}
		if err := kube.Get(ctx, gatedKey, gated); err != nil {
			t.Fatal(err)
		}
		if gated.Status.PendingGateRelease != nil {
			break
		}
	}
	if release := gated.Status.PendingGateRelease; release == nil || release.PodUID != string(replacement.UID) || release.WithdrawalRequested {
		t.Fatalf("replacement Pod gate release was not pending: %#v", gated.Status)
	}
	if err := core.CoreV1().Secrets("default").Delete(ctx, stagedSecret.Name, metav1.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := gatedReconciler.Reconcile(ctx, gatedRequest); err == nil || !strings.Contains(err.Error(), "read pinned TLS snapshot") {
		t.Fatalf("gate release should hold when its pinned snapshot is missing: %v", err)
	}
	substitute, err := core.CoreV1().Secrets("default").Create(ctx, &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: stagedSecret.Name, OwnerReferences: stagedSecret.OwnerReferences, Annotations: stagedSecret.Annotations},
		Type:       stagedSecret.Type, Immutable: stagedSecret.Immutable, Data: stagedSecret.Data,
	}, metav1.CreateOptions{})
	if err != nil || substitute.UID == stagedSecret.UID {
		t.Fatalf("could not recreate snapshot under another UID: %v", err)
	}
	if _, err := gatedReconciler.Reconcile(ctx, gatedRequest); err == nil || !strings.Contains(err.Error(), "pinned TLS snapshot identity changed") {
		t.Fatalf("same-name replacement snapshot bypassed the UID pin: %v", err)
	}
	replacement, err = core.CoreV1().Pods("default").Get(ctx, replacement.Name, metav1.GetOptions{})
	if err != nil || len(replacement.Spec.SchedulingGates) == 0 {
		t.Fatalf("replacement was released without its pinned snapshot: %v", err)
	}
	previousTime := fakeNow
	fakeNow = gated.Status.PendingGateRelease.StartedAt.Add(gatedReconciler.DevelopmentPendingTimeout)
	for i := 0; i < 3; i++ {
		if _, err := gatedReconciler.Reconcile(ctx, gatedRequest); err != nil {
			t.Fatalf("expired gate release could not settle with the pinned snapshot unavailable: %v", err)
		}
	}
	if err := kube.Get(ctx, gatedKey, gated); err != nil || gated.Status.PendingGateRelease != nil || gated.Status.SearchWarmupSlot != nil || gated.Status.ProvisioningAttempt != nil {
		t.Fatalf("expired release did not free the provisioning and warm-up slots: %v", err)
	}
	if _, err := core.CoreV1().Pods("default").Get(ctx, replacement.Name, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Fatalf("expired gated replacement was not withdrawn: %v", err)
	}
	fakeNow = previousTime
	deleteInstance := &v1alpha1.OpenSearchServerless{ObjectMeta: metav1.ObjectMeta{Name: "retry-delete", Namespace: "default"}, Spec: instance.Spec}
	if err := kube.Create(ctx, deleteInstance); err != nil {
		t.Fatal(err)
	}
	deleteInstance.Status.ProvisioningAttempt = &v1alpha1.ProvisioningAttempt{Name: "already-gone", PodUID: "old-pod", StartedAt: metav1.NewTime(fakeNow), Phase: "Blocked", Reason: "PodGone"}
	if err := kube.Status().Update(ctx, deleteInstance); err != nil {
		t.Fatal(err)
	}
	deleteOp := &v1alpha1.OpenSearchServerlessOperation{ObjectMeta: metav1.ObjectMeta{Name: "retry-delete", Namespace: "default"}, Spec: v1alpha1.OperationSpec{InstanceRef: v1alpha1.InstanceReference{Name: deleteInstance.Name, UID: string(deleteInstance.UID)}, Action: "RetryProvisioning", ProvisioningRetry: &v1alpha1.ProvisioningRetry{PodUID: "old-pod", AcknowledgeNewAttempt: true}}}
	if err := kube.Create(ctx, deleteOp); err != nil {
		t.Fatal(err)
	}
	deleteRequest := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: deleteOp.Name}}
	for i := 0; i < 3; i++ {
		if _, err := retryReconciler.Reconcile(ctx, deleteRequest); err != nil {
			t.Fatal(err)
		}
	}
	if err := kube.Get(ctx, deleteRequest.NamespacedName, deleteOp); err != nil {
		t.Fatal(err)
	}
	if err := kube.Delete(ctx, deleteOp); err != nil {
		t.Fatal(err)
	}
	if err := kube.Get(ctx, deleteRequest.NamespacedName, deleteOp); err != nil {
		t.Fatal(err)
	}
	if err := kube.Get(ctx, types.NamespacedName{Namespace: "default", Name: deleteInstance.Name}, deleteInstance); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 4; i++ {
		if _, err := retryReconciler.Reconcile(ctx, deleteRequest); err != nil {
			t.Fatal(err)
		}
	}
	if err := kube.Get(ctx, deleteRequest.NamespacedName, deleteOp); !apierrors.IsNotFound(err) {
		t.Fatalf("deleted operation did not finish and release its finalizer: %v", err)
	}
	if err := kube.Get(ctx, types.NamespacedName{Namespace: "default", Name: deleteInstance.Name}, deleteInstance); err != nil || deleteInstance.Status.ActiveOperation != nil || deleteInstance.Status.ProvisioningAttempt != nil {
		t.Fatalf("deleting a claimed operation left the instance locked: %v, status=%#v", err, deleteInstance.Status)
	}
	controlledBound := true
	bound, err := core.CoreV1().Pods("default").Create(ctx, &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "bound-during-withdrawal", Labels: map[string]string{workload.RoleLabel: "ingest", workload.ZoneLabel: "zone-a"},
			OwnerReferences: []metav1.OwnerReference{{APIVersion: v1alpha1.GroupVersion.String(), Kind: "OpenSearchServerless", Name: routes.Name, UID: routes.UID, Controller: &controlledBound}}},
		Spec: corev1.PodSpec{NodeName: "synthetic-node", Containers: []corev1.Container{{Name: "app", Image: "example.invalid/image"}}},
	}, metav1.CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := kube.Get(ctx, routesKey, routes); err != nil {
		t.Fatal(err)
	}
	routes.Status.ProvisioningAttempt = &v1alpha1.ProvisioningAttempt{Name: bound.Name, PodUID: string(bound.UID), Role: "ingest", Zone: "zone-a", StartedAt: metav1.NewTime(fakeNow.Add(-time.Hour)), Phase: "Withdrawing", Reason: "PendingTimeout"}
	if err := kube.Status().Update(ctx, routes); err != nil {
		t.Fatal(err)
	}
	if _, err := gatedReconciler.Reconcile(ctx, routesRequest); err != nil {
		t.Fatal(err)
	}
	if _, err := core.CoreV1().Pods("default").Get(ctx, bound.Name, metav1.GetOptions{}); err != nil {
		t.Fatalf("bound Pod was withdrawn: %v", err)
	}
	if err := kube.Get(ctx, routesKey, routes); err != nil || routes.Status.ProvisioningAttempt == nil || routes.Status.ProvisioningAttempt.Phase != "CheckingStartup" {
		t.Fatalf("bound unready Pod did not retain provisioning slot: %v", err)
	}
	fakeNow = fakeNow.Add(2 * time.Minute)
	if _, err := gatedReconciler.Reconcile(ctx, routesRequest); err != nil {
		t.Fatal(err)
	}
	if err := kube.Get(ctx, routesKey, routes); err != nil || routes.Status.ProvisioningAttempt.Reason != "StartupTimeout" {
		t.Fatal("startup timeout not persisted")
	}
	if _, err := gatedReconciler.Reconcile(ctx, routesRequest); err != nil {
		t.Fatal(err)
	}
	if err := kube.Get(ctx, routesKey, routes); err != nil || routes.Status.ProvisioningAttempt != nil || len(routes.Status.BlockedProvisioningAttempts) != 1 || routes.Status.BlockedProvisioningAttempts[0].Reason != "StartupTimeout" {
		t.Fatal("bound startup hold was not isolated")
	}

	bound.Status.Phase = corev1.PodRunning
	bound.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}
	if _, err := core.CoreV1().Pods("default").UpdateStatus(ctx, bound, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := gatedReconciler.Reconcile(ctx, routesRequest); err != nil {
		t.Fatal(err)
	}
	if err := kube.Get(ctx, routesKey, routes); err != nil || routes.Status.ProvisioningAttempt != nil || len(routes.Status.BlockedProvisioningAttempts) != 0 {
		t.Fatalf("bound Pod did not complete provisioning: %v", err)
	}
	dnsBound, err := core.CoreV1().Pods("default").Create(ctx, &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "bound-dns", Labels: map[string]string{workload.RoleLabel: "ingest", workload.ZoneLabel: "zone-a"},
			Annotations:     map[string]string{"serverless.opensearch.org/peer-dns-domain": "cluster.local", "serverless.opensearch.org/peer-dns-preflight": "v1"},
			OwnerReferences: []metav1.OwnerReference{{APIVersion: v1alpha1.GroupVersion.String(), Kind: "OpenSearchServerless", Name: routes.Name, UID: routes.UID, Controller: &controlledBound}}},
		Spec: corev1.PodSpec{NodeName: "synthetic-node", InitContainers: []corev1.Container{{Name: "peer-dns-preflight", Image: "example.invalid/image"}}, Containers: []corev1.Container{{Name: "app", Image: "example.invalid/image"}}},
	}, metav1.CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := kube.Get(ctx, routesKey, routes); err != nil {
		t.Fatal(err)
	}
	routes.Status.ProvisioningAttempt = &v1alpha1.ProvisioningAttempt{Name: dnsBound.Name, PodUID: string(dnsBound.UID), Role: "ingest", Zone: "zone-a", StartedAt: metav1.NewTime(fakeNow.Add(-time.Hour)), Phase: "Watching", PeerDNS: true}
	if err := kube.Status().Update(ctx, routes); err != nil {
		t.Fatal(err)
	}
	if _, err := gatedReconciler.Reconcile(ctx, routesRequest); err != nil {
		t.Fatal(err)
	}
	if err := kube.Get(ctx, routesKey, routes); err != nil || routes.Status.ProvisioningAttempt == nil || routes.Status.ProvisioningAttempt.Phase != "CheckingPeerDNS" || !routes.Status.ProvisioningAttempt.PeerDNS {
		t.Fatalf("bound DNS preflight was not tracked: %v", err)
	}
	fakeNow = fakeNow.Add(2 * time.Minute)
	if _, err := gatedReconciler.Reconcile(ctx, routesRequest); err != nil {
		t.Fatal(err)
	}
	if err := kube.Get(ctx, routesKey, routes); err != nil || routes.Status.ProvisioningAttempt.Phase != "Blocked" || routes.Status.ProvisioningAttempt.Reason != "PeerDNSPreflightTimeout" {
		t.Fatalf("bound DNS failure did not hold: %v", err)
	}
	if _, err := gatedReconciler.Reconcile(ctx, routesRequest); err != nil {
		t.Fatal(err)
	}
	if err := kube.Get(ctx, routesKey, routes); err != nil || routes.Status.ProvisioningAttempt != nil || len(routes.Status.BlockedProvisioningAttempts) != 1 || routes.Status.BlockedProvisioningAttempts[0].Reason != "PeerDNSPreflightTimeout" {
		t.Fatal("bound DNS hold was not isolated")
	}

	dnsBound, err = core.CoreV1().Pods("default").Get(ctx, dnsBound.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("bound DNS Pod was removed: %v", err)
	}
	dnsBound.Status.Phase = corev1.PodRunning
	dnsBound.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}
	dnsBound.Status.InitContainerStatuses = []corev1.ContainerStatus{{Name: "peer-dns-preflight", State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ExitCode: 0}}}}
	if _, err := core.CoreV1().Pods("default").UpdateStatus(ctx, dnsBound, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := gatedReconciler.Reconcile(ctx, routesRequest); err != nil {
		t.Fatal(err)
	}
	if err := kube.Get(ctx, routesKey, routes); err != nil || routes.Status.ProvisioningAttempt != nil || len(routes.Status.BlockedProvisioningAttempts) != 0 {
		t.Fatalf("completed DNS preflight did not clear hold: %v", err)
	}
	pod, err := core.CoreV1().Pods("default").Create(ctx, &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "pending"}, Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "app", Image: "example.invalid/image"}}}}, metav1.CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	staleVersion := pod.ResourceVersion
	pod.Labels = map[string]string{"updated": "true"}
	if _, err := core.CoreV1().Pods("default").Update(ctx, pod, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	err = core.CoreV1().Pods("default").Delete(ctx, pod.Name, metav1.DeleteOptions{Preconditions: &metav1.Preconditions{UID: &pod.UID, ResourceVersion: &staleVersion}})
	if !apierrors.IsConflict(err) {
		t.Fatalf("expected stale delete precondition conflict, got %v", err)
	}

	controlled := true
	desiredPod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
		Name: "guarded", Namespace: "default", Annotations: map[string]string{"example.com/intent": "intent-1"},
		OwnerReferences: []metav1.OwnerReference{{APIVersion: v1alpha1.GroupVersion.String(), Kind: "OpenSearchServerless", Name: instance.Name, UID: instance.UID, Controller: &controlled}},
	}, Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "app", Image: "example.invalid/image"}}, SchedulingGates: []corev1.PodSchedulingGate{{Name: "example.com/hold"}, {Name: "other.example/hold"}}}}
	created, err := podkube.CreatePod(ctx, core.CoreV1(), desiredPod, "example.com/intent", "intent-1")
	if err != nil {
		t.Fatal(err)
	}
	recovered, err := podkube.CreatePod(ctx, core.CoreV1(), desiredPod, "example.com/intent", "intent-1")
	if err != nil || recovered.UID != created.UID {
		t.Fatalf("creation retry did not recover the same Pod: %v", err)
	}
	conflicting := desiredPod.DeepCopy()
	conflicting.Annotations["example.com/intent"] = "intent-2"
	if _, err := podkube.CreatePod(ctx, core.CoreV1(), conflicting, "example.com/intent", "intent-2"); err == nil {
		t.Fatal("conflicting creation intent was accepted")
	}
	identity := podlifecycle.Identity{Namespace: "default", Name: created.Name, UID: string(created.UID), OwnerUID: string(instance.UID)}
	if err := podkube.ReleaseSchedulingGate(ctx, core.CoreV1(), identity, "example.com/hold"); err != nil {
		t.Fatal(err)
	}
	created, err = core.CoreV1().Pods("default").Get(ctx, created.Name, metav1.GetOptions{})
	if err != nil || len(created.Spec.SchedulingGates) != 1 || created.Spec.SchedulingGates[0].Name != "other.example/hold" {
		t.Fatalf("gate release did not preserve the other gate: %v", err)
	}
	staleIdentity := identity
	staleIdentity.UID = "old-pod"
	if err := podkube.WithdrawUnbound(ctx, core.CoreV1(), staleIdentity); err == nil {
		t.Fatal("withdrawal accepted a stale Pod UID")
	}
	if err := podkube.WithdrawUnbound(ctx, core.CoreV1(), identity); err != nil {
		t.Fatal(err)
	}
	group := int64(10001)
	pluginPod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "with-plugins", Namespace: "default"}, Spec: corev1.PodSpec{SecurityContext: &corev1.PodSecurityContext{FSGroup: &group}, Containers: []corev1.Container{{Name: "opensearch", Image: "example.invalid/java"}}}}
	pluginPod, err = workload.WithPluginInstaller(pluginPod, workload.PluginInstallerInput{
		Image: "example.invalid/installer@sha256:" + strings.Repeat("a", 64), RequestConfigMap: "request", PolicyConfigMap: "policy", CredentialsSecret: "registry", JavaPluginsPath: "/work/plugins", VolumeSizeLimit: resource.MustParse("1Gi"),
		Resources: corev1.ResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("100m"), corev1.ResourceMemory: resource.MustParse("128Mi"), corev1.ResourceEphemeralStorage: resource.MustParse("1Gi")}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := core.CoreV1().Pods("default").Create(ctx, pluginPod, metav1.CreateOptions{}); err != nil {
		t.Fatalf("API server rejected plugin Pod template: %v", err)
	}

	store, err := core.CoreV1().ConfigMaps("default").Create(ctx, &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: "remote-store"},
		Data:       map[string]string{"bucket": "original", "region": "region"},
	}, metav1.CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	referenced := &v1alpha1.OpenSearchServerless{ObjectMeta: metav1.ObjectMeta{Name: "referenced", Namespace: "default"}, Spec: instance.Spec}
	referenced.Spec.Storage = v1alpha1.Storage{Type: "s3", RepositoryRef: &v1alpha1.LocalReference{Name: store.Name}}
	if err := kube.Create(ctx, referenced); err != nil {
		t.Fatal(err)
	}
	reconciler := controller.InstanceReconciler{Client: kube, Core: core.CoreV1()}
	request := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: referenced.Name}}
	if _, err := reconciler.Reconcile(ctx, request); err != nil {
		t.Fatal(err)
	}
	store.Data["bucket"] = "changed"
	if _, err := core.CoreV1().ConfigMaps("default").Update(ctx, store, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := reconciler.Reconcile(ctx, request); err != nil {
		t.Fatal(err)
	}
	if err := kube.Get(ctx, request.NamespacedName, referenced); err != nil {
		t.Fatal(err)
	}
	condition := meta.FindStatusCondition(referenced.Status.Conditions, "Available")
	if referenced.Status.PrimaryStore == nil || referenced.Status.PrimaryStore.Bucket != "original" || condition == nil || condition.Reason != "StorageIdentityChanged" {
		t.Fatalf("repository identity was not preserved: %#v", referenced.Status)
	}
	store, err = core.CoreV1().ConfigMaps("default").Get(ctx, store.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	store.Data["bucket"] = "original"
	store.Data["endpoint"] = "https://user:supersecret@storage.example.test"
	if _, err := core.CoreV1().ConfigMaps("default").Update(ctx, store, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := reconciler.Reconcile(ctx, request); err != nil {
		t.Fatal(err)
	}
	if err := kube.Get(ctx, request.NamespacedName, referenced); err != nil {
		t.Fatal(err)
	}
	condition = meta.FindStatusCondition(referenced.Status.Conditions, "Available")
	statusBytes, err := json.Marshal(referenced.Status)
	if err != nil || condition == nil || condition.Reason != "StorageUnavailable" || referenced.Status.PrimaryStore == nil || referenced.Status.PrimaryStore.Endpoint != "" || strings.Contains(string(statusBytes), "supersecret") {
		t.Fatal("unsafe repository endpoint affected pinned status or leaked into status")
	}
}

func checkOperationActionAdmission(t *testing.T, ctx context.Context, kube client.Client, instance *v1alpha1.OpenSearchServerless) {
	t.Helper()
	ref := v1alpha1.InstanceReference{Name: instance.Name, UID: string(instance.UID)}
	rollout := v1alpha1.OperationSpec{Action: "Promote", TargetFleet: "green", ExpectedServingFleetByRole: map[string]string{"search": "initial", "ingest": "initial"}}
	settings := &v1alpha1.ClusterSettingsPatch{Set: map[string]string{"serverless.bucket": "off"}}
	security := &v1alpha1.SecurityConfigurationChange{CandidateRevision: "next", ExpectedActiveRevision: "current"}
	retry := &v1alpha1.ProvisioningRetry{PodUID: "pod", AcknowledgeNewAttempt: true}
	writer := &v1alpha1.WriterRetirementTarget{PodName: "index", PodUID: "pod"}
	for _, tc := range []struct {
		name, rejection string
		spec            v1alpha1.OperationSpec
	}{
		{name: "promotion", spec: rollout},
		{name: "rollback", spec: v1alpha1.OperationSpec{Action: "Rollback", TargetFleet: "initial",
			ExpectedServingFleetByRole: map[string]string{"search": "green", "ingest": "green"}, SupersedesOperationUID: "promotion"}},
		{name: "decommission", spec: v1alpha1.OperationSpec{Action: "Decommission"}},
		{name: "settings", spec: v1alpha1.OperationSpec{Action: "ApplyClusterSettings", ClusterSettings: settings}},
		{name: "security", spec: v1alpha1.OperationSpec{Action: "ApplySecurityConfiguration", SecurityConfiguration: security}},
		{name: "retry", spec: v1alpha1.OperationSpec{Action: "RetryProvisioning", ProvisioningRetry: retry}},
		{name: "writer", spec: v1alpha1.OperationSpec{Action: "RetireWriter", WriterRetirement: writer}},
		{name: "unknown-action", spec: v1alpha1.OperationSpec{Action: "Unknown"}, rejection: "Unsupported value"},
		{name: "missing-role", spec: v1alpha1.OperationSpec{Action: "Promote", TargetFleet: "green",
			ExpectedServingFleetByRole: map[string]string{"search": "initial", "extra": "initial"}}, rejection: "both expected role assignments"},
		{name: "inapplicable-routing", spec: v1alpha1.OperationSpec{Action: "Decommission", RoutingMode: "WeightedGateway"}, rejection: "routingMode requires"},
		{name: "inapplicable-supersession", spec: v1alpha1.OperationSpec{Action: "Decommission", SupersedesOperationUID: "other"}, rejection: "only rollback"},
		{name: "inapplicable-settings", spec: v1alpha1.OperationSpec{Action: "Decommission", ClusterSettings: settings}, rejection: "clusterSettings is required only"},
		{name: "missing-settings", spec: v1alpha1.OperationSpec{Action: "ApplyClusterSettings"}, rejection: "clusterSettings is required only"},
		{name: "empty-settings", spec: v1alpha1.OperationSpec{Action: "ApplyClusterSettings", ClusterSettings: &v1alpha1.ClusterSettingsPatch{}}, rejection: "patch must not be empty"},
		{name: "missing-security", spec: v1alpha1.OperationSpec{Action: "ApplySecurityConfiguration"}, rejection: "securityConfiguration is required only"},
		{name: "empty-revision", spec: v1alpha1.OperationSpec{Action: "ApplySecurityConfiguration", SecurityConfiguration: &v1alpha1.SecurityConfigurationChange{ExpectedActiveRevision: "current"}}, rejection: "candidateRevision"},
		{name: "missing-retry", spec: v1alpha1.OperationSpec{Action: "RetryProvisioning"}, rejection: "provisioningRetry is required only"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			spec := tc.spec
			spec.InstanceRef = ref
			if err := spec.Validate(); (err == nil) != (tc.rejection == "") {
				t.Fatalf("Go validation returned %v", err)
			}
			operation := &v1alpha1.OpenSearchServerlessOperation{
				ObjectMeta: metav1.ObjectMeta{Name: "admission-" + tc.name, Namespace: instance.Namespace},
				Spec:       spec,
			}
			err := kube.Create(ctx, operation, client.DryRunAll)
			if tc.rejection == "" {
				if err != nil {
					t.Fatalf("API rejected a valid action: %v", err)
				}
			} else if !apierrors.IsInvalid(err) || !strings.Contains(err.Error(), tc.rejection) {
				t.Fatalf("API error = %v, want an invalid request containing %q", err, tc.rejection)
			}
		})
	}
}

func checkInstanceAdmission(t *testing.T, ctx context.Context, kube client.Client, base *v1alpha1.OpenSearchServerless) {
	t.Helper()
	const role = "arn:aws:iam::123456789012:role/store"
	plugin := v1alpha1.Plugin{Name: "analysis-icu", Artifact: "example.com/plugins/analysis-icu@sha256:" + strings.Repeat("a", 64), Roles: []string{"search", "ingest"}}
	for _, tc := range []struct {
		name, rejection string
		change          func(*v1alpha1.OpenSearchServerlessSpec)
	}{
		{name: "aws-role", change: func(spec *v1alpha1.OpenSearchServerlessSpec) {
			spec.Storage.Credentials = &v1alpha1.StorageCredentials{Type: "WorkloadIdentity", AWSRoleARN: role}
		}},
		{name: "keystore", change: func(spec *v1alpha1.OpenSearchServerlessSpec) {
			spec.Storage.Credentials = &v1alpha1.StorageCredentials{Type: "Keystore", KeystoreSecretRef: &v1alpha1.LocalReference{Name: "s3"}}
		}},
		{name: "identity-with-keystore", rejection: "select an AWS role ARN", change: func(spec *v1alpha1.OpenSearchServerlessSpec) {
			spec.Storage.Credentials = &v1alpha1.StorageCredentials{Type: "WorkloadIdentity", AWSRoleARN: role, KeystoreSecretRef: &v1alpha1.LocalReference{Name: "s3"}}
		}},
		{name: "invalid-role-arn", rejection: "awsRoleArn", change: func(spec *v1alpha1.OpenSearchServerlessSpec) {
			spec.Storage.Credentials = &v1alpha1.StorageCredentials{Type: "WorkloadIdentity", AWSRoleARN: "arn:invalid"}
		}},
		{name: "empty-trust-key", rejection: "trustBundleRef.key", change: func(spec *v1alpha1.OpenSearchServerlessSpec) {
			spec.TLS = &v1alpha1.TLSConfig{HTTP: &v1alpha1.TLSChannel{Source: "Secret",
				CertificateRef: v1alpha1.LocalReference{Name: "leaf"}, TrustBundleRef: v1alpha1.SecretKeyReference{Name: "trust"}}}
		}},
		{name: "unknown-pool", rejection: "pools support only search and ingest", change: func(spec *v1alpha1.OpenSearchServerlessSpec) {
			spec.Pools["index"] = spec.Pools["search"]
		}},
		{name: "pool-without-zones", rejection: "zones", change: func(spec *v1alpha1.OpenSearchServerlessSpec) {
			spec.Pools["search"] = v1alpha1.Pool{Zones: map[string]v1alpha1.Zone{}}
		}},
		{name: "strict-without-warmup", rejection: "requires warmOnJoin", change: func(spec *v1alpha1.OpenSearchServerlessSpec) {
			pool := spec.Pools["search"]
			pool.WarmReadiness = "Strict"
			spec.Pools["search"] = pool
		}},
		{name: "warm-ingest", rejection: "warm readiness belongs to search", change: func(spec *v1alpha1.OpenSearchServerlessSpec) {
			pool := spec.Pools["search"]
			pool.WarmOnJoin = true
			spec.Pools["ingest"] = pool
		}},
		{name: "negative-budget", rejection: "maxPendingProvisionablePods", change: func(spec *v1alpha1.OpenSearchServerlessSpec) {
			spec.Availability = &v1alpha1.Availability{MaxPendingProvisionablePods: -1}
		}},
		{name: "rolling-with-candidate", rejection: "candidate requires the BlueGreen strategy", change: func(spec *v1alpha1.OpenSearchServerlessSpec) {
			spec.Rollout = &v1alpha1.Rollout{Strategy: "Rolling", Candidate: &v1alpha1.FleetCandidate{Image: spec.Image, Fleet: "green"}}
		}},
		{name: "oci-plugin", change: func(spec *v1alpha1.OpenSearchServerlessSpec) {
			spec.Plugins = []v1alpha1.Plugin{plugin}
		}},
		{name: "invalid-plugin-name", rejection: "plugins[0].name", change: func(spec *v1alpha1.OpenSearchServerlessSpec) {
			spec.Plugins = []v1alpha1.Plugin{plugin}
			spec.Plugins[0].Name = "../analysis"
		}},
		{name: "candidate-plugin-tag", rejection: "pluginOverride.plugins[0].artifact", change: func(spec *v1alpha1.OpenSearchServerlessSpec) {
			candidatePlugin := plugin
			candidatePlugin.Artifact = "example.com/plugins/analysis-icu:latest"
			spec.Rollout = &v1alpha1.Rollout{Strategy: "BlueGreen", RollbackWindow: "1m", Candidate: &v1alpha1.FleetCandidate{
				Image: spec.Image, Fleet: "green", PluginOverride: &v1alpha1.PluginSet{Plugins: []v1alpha1.Plugin{candidatePlugin}},
			}}
		}},
		{name: "unknown-plugin-role", rejection: "plugins[0].roles[0]", change: func(spec *v1alpha1.OpenSearchServerlessSpec) {
			spec.Plugins = []v1alpha1.Plugin{plugin}
			spec.Plugins[0].Roles = []string{"index"}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			instance := &v1alpha1.OpenSearchServerless{
				ObjectMeta: metav1.ObjectMeta{Name: "instance-admission-" + tc.name, Namespace: base.Namespace},
				Spec:       *base.Spec.DeepCopy(),
			}
			tc.change(&instance.Spec)
			if err := instance.Spec.Validate(); (err == nil) != (tc.rejection == "") {
				t.Fatalf("Go validation returned %v", err)
			}
			err := kube.Create(ctx, instance, client.DryRunAll)
			if tc.rejection == "" {
				if err != nil {
					t.Fatalf("API rejected a valid instance: %v", err)
				}
			} else if !apierrors.IsInvalid(err) || !strings.Contains(err.Error(), tc.rejection) {
				t.Fatalf("API error = %v, want an invalid request containing %q", err, tc.rejection)
			}
		})
	}
}
