package rollout

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	kubernetesfake "k8s.io/client-go/kubernetes/fake"
	clienttesting "k8s.io/client-go/testing"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/huyz0/opensearch-serverless-operator/api/v1alpha1"
	"github.com/huyz0/opensearch-serverless-operator/internal/java"
)

func TestPublishWriterWithdrawalBindsTheLiveJournalAndRejectsOutOfOrderReports(t *testing.T) {
	ctx := context.Background()
	at := time.Now().Add(-4 * time.Second)
	withdrawnAt := metav1.NewMicroTime(at)
	op := &v1alpha1.OpenSearchServerlessOperation{
		ObjectMeta: metav1.ObjectMeta{Name: "retire", Namespace: "app", UID: "operation", Generation: 1, Finalizers: []string{"serverless.opensearch.org/writer-retirement"}},
		Spec: v1alpha1.OperationSpec{Action: "RetireWriter",
			InstanceRef:      v1alpha1.InstanceReference{Name: "catalog", UID: "instance"},
			WriterRetirement: &v1alpha1.WriterRetirementTarget{PodName: "writer", PodUID: "victim"},
		},
		Status: v1alpha1.OperationStatus{WriterRetirement: &v1alpha1.WriterRetirementStatus{
			InstanceGeneration: 2, WithdrawalArmed: true, WithdrawnAt: &withdrawnAt, NodeID: "node", NodeEphemeralID: "jvm",
			ServiceUID: "service", RouteUID: "route", GatewayUID: "gateway", GatewayServiceUID: "frontend", ServingFleet: "initial",
		}},
	}
	instance := &v1alpha1.OpenSearchServerless{
		ObjectMeta: metav1.ObjectMeta{Name: "catalog", Namespace: "app", UID: "instance", Generation: 2},
		Status: v1alpha1.OpenSearchServerlessStatus{
			ActiveOperation:          &v1alpha1.OperationClaim{Name: "retire", UID: "operation", Action: "RetireWriter", Generation: 1},
			ServingFleetIntentByRole: map[string]string{"ingest": "initial"},
		},
	}
	scope := WriterWithdrawalScope{
		RouteAuthIdentity: RouteAuthIdentity{AuthSecretName: "edge", AuthSecretUID: "auth", AuthSecretVersion: "13", AuthProbeURL: "https://index.example"},
		Namespace:         "app", InstanceUID: "instance", InstanceGeneration: 2, OperationUID: "operation",
		VictimPodUID: "victim", VictimPodVersion: "5", Writer: java.WriterProcess{NodeID: "node", EphemeralID: "jvm"},
		ServiceName: "catalog-index", ServiceUID: "service", ServiceVersion: "7", RouteName: "index", RouteUID: "route", RouteVersion: "8",
		GatewayNamespace: "app", GatewayName: "edge", GatewayUID: "gateway", GatewayVersion: "9", GatewayListener: "https", GatewayServiceUID: "frontend",
	}
	report := WriterWithdrawalEvidence{WriterWithdrawalScope: scope, ObservedAt: at.Add(time.Second),
		EndpointVersions: map[types.UID]string{"slice": "11"}, GatewayVersions: map[string]string{"service/frontend": "12"},
		ServingProxyUIDs: []string{"proxy"},
		Proxies:          []WriterProxyReport{{ProxyUID: "proxy", ObservedAt: at.Add(time.Second), LiveBackends: map[types.UID]string{"survivor": "10"}}},
	}
	scheme := runtime.NewScheme()
	if err := v1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	reader := fake.NewClientBuilder().WithScheme(scheme).WithObjects(instance, op).Build()
	kube := kubernetesfake.NewSimpleClientset()
	core := kube.CoreV1()
	public, signer, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	publish := func(evidence WriterWithdrawalEvidence) error {
		return PublishWriterWithdrawalEvidence(ctx, reader, core, "app", "retire", evidence, signer)
	}
	wrong := report
	wrong.Writer.EphemeralID = "previous-jvm"
	if err := publish(wrong); err == nil {
		t.Fatal("published evidence for a different Java process")
	}
	old := report
	old.ObservedAt = at.Add(-time.Second)
	if err := publish(old); err == nil {
		t.Fatal("published an observation before recorded withdrawal")
	}
	name := WriterWithdrawalEvidenceName(op.UID)
	resource := corev1.SchemeGroupVersion.WithResource("configmaps")
	earlier := report
	earlier.ObservedAt = at
	earlierData, err := SignWriterWithdrawalEvidence(earlier, signer)
	if err != nil {
		t.Fatal(err)
	}
	kube.PrependReactor("create", "configmaps", func(clienttesting.Action) (bool, runtime.Object, error) {
		if err := kube.Tracker().Create(resource, &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "app"},
			Data:       earlierData,
		}, "app"); err != nil {
			t.Fatal(err)
		}
		return true, nil, apierrors.NewAlreadyExists(resource.GroupResource(), name)
	})
	if err := publish(report); err != nil {
		t.Fatal(err)
	}
	if err := publish(earlier); err != nil {
		t.Fatal(err)
	}
	cm, err := core.ConfigMaps("app").Get(ctx, WriterWithdrawalEvidenceName(op.UID), metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	got, err := readSignedEvidence[WriterWithdrawalEvidence](writerWithdrawalDomain, cm.Data, public)
	if err != nil || !got.ObservedAt.Equal(report.ObservedAt) {
		t.Fatalf("published report did not retain newest signed observation: %#v %v", got, err)
	}
	op.Status.WriterRetirement.AdmissionClosed = true
	op.Status.WriterRetirement.EvictionArmed = true
	if err := reader.Update(ctx, op); err != nil {
		t.Fatal(err)
	}
	fresh := report
	fresh.ObservedAt = at.Add(2 * time.Second)
	fresh.Proxies = []WriterProxyReport{{ProxyUID: "proxy", ObservedAt: fresh.ObservedAt, LiveBackends: report.Proxies[0].LiveBackends}}
	newer := fresh
	newer.ObservedAt = at.Add(3 * time.Second)
	newer.Proxies = []WriterProxyReport{{ProxyUID: "proxy", ObservedAt: newer.ObservedAt, LiveBackends: report.Proxies[0].LiveBackends}}
	newerData, err := SignWriterWithdrawalEvidence(newer, signer)
	if err != nil {
		t.Fatal(err)
	}
	kube.PrependReactor("update", "configmaps", func(action clienttesting.Action) (bool, runtime.Object, error) {
		competing := action.(clienttesting.UpdateAction).GetObject().(*corev1.ConfigMap).DeepCopy()
		competing.Data = newerData
		if err := kube.Tracker().Update(resource, competing, "app"); err != nil {
			t.Fatal(err)
		}
		return true, nil, apierrors.NewConflict(resource.GroupResource(), name, nil)
	})
	if err := publish(fresh); err != nil {
		t.Fatalf("could not publish fresh proof while eviction was armed: %v", err)
	}
	cm, err = core.ConfigMaps("app").Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	got, err = readSignedEvidence[WriterWithdrawalEvidence](writerWithdrawalDomain, cm.Data, public)
	if err != nil || !got.ObservedAt.Equal(newer.ObservedAt) {
		t.Fatalf("publisher overwrote newer signed observation after conflict: %#v %v", got, err)
	}
	op.Status.WriterRetirement.VictimGone = true
	if err := reader.Update(ctx, op); err != nil {
		t.Fatal(err)
	}
	if err := publish(fresh); err == nil {
		t.Fatal("published withdrawal evidence after confirmation of Pod absence")
	}
}
