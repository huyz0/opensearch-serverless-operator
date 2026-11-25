package rollout

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	kubernetesfake "k8s.io/client-go/kubernetes/fake"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/huyz0/opensearch-serverless-operator/api/v1alpha1"
	"github.com/huyz0/opensearch-serverless-operator/internal/workload"
)

func TestPublisherSignsOnlyVerifiedNodePlacement(t *testing.T) {
	ctx := context.Background()
	instance := &v1alpha1.OpenSearchServerless{
		ObjectMeta: metav1.ObjectMeta{Name: "catalog", Namespace: "n", UID: "instance", Generation: 3},
		Status:     v1alpha1.OpenSearchServerlessStatus{CandidateFleet: &v1alpha1.CandidateFleetStatus{Fleet: "green", Revision: "rev", Generation: 3}},
	}
	op := &v1alpha1.OpenSearchServerlessOperation{
		ObjectMeta: metav1.ObjectMeta{Name: "approve", Namespace: "n", UID: "approval"},
		Spec: v1alpha1.OperationSpec{Action: "Promote", TargetFleet: "green",
			InstanceRef:                v1alpha1.InstanceReference{Name: instance.Name, UID: string(instance.UID)},
			ExpectedServingFleetByRole: map[string]string{"search": "blue", "ingest": "blue"}},
	}
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "search", Namespace: "n", UID: "pod", ResourceVersion: "7",
			OwnerReferences: []metav1.OwnerReference{*metav1.NewControllerRef(instance, v1alpha1.GroupVersion.WithKind("OpenSearchServerless"))},
			Labels: map[string]string{workload.InstanceLabel: "instance", workload.RoleLabel: "search", workload.FleetLabel: "green",
				workload.ZoneLabel: "a", "serverless.opensearch.org/revision": "rev"}},
		Spec: corev1.PodSpec{NodeName: "node"},
	}
	node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node", UID: "node-uid", ResourceVersion: "1", Labels: map[string]string{corev1.LabelTopologyZone: "a"}}}
	scheme := runtime.NewScheme()
	if err := v1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	reader := fake.NewClientBuilder().WithScheme(scheme).WithObjects(instance, op).Build()
	kube := kubernetesfake.NewSimpleClientset(pod, node)
	statement := PlatformEvidence{
		Namespace: "n", InstanceUID: "instance", InstanceGeneration: 3, OperationUID: "approval", Role: "search", Fleet: "green", Revision: "rev",
		ObservedAt: time.Now().Add(-time.Second), PeerQualified: true, SecurityQualified: true, RollbackCompatible: true,
		Pods: []PlatformPod{{UID: "pod", ResourceVersion: "7", NodeName: "node", Zone: "a", ProcessIncarnation: "verified-process"}},
	}
	pub, key, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	publish := func() error {
		return PublishPlatformEvidence(ctx, reader, kube.CoreV1(), "n", "approve", statement, key)
	}
	if err := publish(); err != nil {
		t.Fatal(err)
	}
	name := PlatformEvidenceName("approval", "search")
	cm, err := kube.CoreV1().ConfigMaps("n").Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	first, err := ReadSignedPlatformEvidence(cm.Data, pub)
	if err != nil || first.Pods[0] != statement.Pods[0] {
		t.Fatalf("published statement failed operator verification: %#v %v", first, err)
	}
	node.Labels[corev1.LabelTopologyZone] = "b"
	if _, err := kube.CoreV1().Nodes().Update(ctx, node, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	statement.ObservedAt = time.Now()
	if err := publish(); err == nil {
		t.Fatal("publisher trusted the requested Pod zone after the Node moved")
	}
	cm, err = kube.CoreV1().ConfigMaps("n").Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	last, err := ReadSignedPlatformEvidence(cm.Data, pub)
	if err != nil || !last.ObservedAt.Equal(first.ObservedAt) {
		t.Fatalf("failed verification changed the earlier observation: %#v %v", last, err)
	}
	t.Run("source approval requires a claimed weighted role", func(t *testing.T) {
		node.Labels[corev1.LabelTopologyZone] = "a"
		if _, err := kube.CoreV1().Nodes().Update(ctx, node, metav1.UpdateOptions{}); err != nil {
			t.Fatal(err)
		}
		source := pod.DeepCopy()
		source.Labels[workload.FleetLabel] = "blue"
		source.Labels["serverless.opensearch.org/revision"] = "source-rev"
		current, err := kube.CoreV1().Pods("n").Update(ctx, source, metav1.UpdateOptions{})
		if err != nil {
			t.Fatal(err)
		}
		statement.Fleet, statement.Revision, statement.ObservedAt = "blue", "source-rev", time.Now()
		statement.Pods[0].ResourceVersion = current.ResourceVersion
		if err := publish(); err == nil {
			t.Fatal("selector rollout accepted a source approval")
		}
		if err := reader.Get(ctx, client.ObjectKeyFromObject(op), op); err != nil {
			t.Fatal(err)
		}
		op.Status.WeightedRollout = &v1alpha1.WeightedRolloutStatus{
			InstanceGeneration: 3, Roles: map[string]v1alpha1.WeightedRoleStatus{
				"search": {Revisions: map[string]string{"blue": "source-rev"}, BackendUIDs: map[string]string{"blue": "source-service"}},
			},
		}
		if err := reader.Update(ctx, op); err != nil {
			t.Fatal(err)
		}
		if err := reader.Get(ctx, client.ObjectKeyFromObject(instance), instance); err != nil {
			t.Fatal(err)
		}
		instance.Spec.Rollout = &v1alpha1.Rollout{GatewayBackends: true}
		instance.Status.ActiveOperation = &v1alpha1.OperationClaim{Name: op.Name, UID: string(op.UID), Action: op.Spec.Action, Generation: op.Generation}
		if err := reader.Update(ctx, instance); err != nil {
			t.Fatal(err)
		}
		if err := publish(); err != nil {
			t.Fatal(err)
		}
		sourceName := FleetPlatformEvidenceName("approval", "search", "blue", "green")
		if sourceName == name {
			t.Fatal("source approval would overwrite the target approval")
		}
		sourceCM, err := kube.CoreV1().ConfigMaps("n").Get(ctx, sourceName, metav1.GetOptions{})
		if err != nil {
			t.Fatal(err)
		}
		signed, err := ReadSignedPlatformEvidence(sourceCM.Data, pub)
		if err != nil || signed.Fleet != "blue" || signed.Revision != "source-rev" {
			t.Fatalf("unexpected source approval: %#v, %v", signed, err)
		}
	})
}
