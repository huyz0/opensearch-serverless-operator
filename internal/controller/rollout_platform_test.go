package controller

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/huyz0/opensearch-serverless-operator/api/v1alpha1"
	"github.com/huyz0/opensearch-serverless-operator/internal/rollout"
	"github.com/huyz0/opensearch-serverless-operator/internal/workload"
)

func TestSignedRolloutPlatformCheckBindsCurrentPlacement(t *testing.T) {
	ctx := context.Background()
	instance := &v1alpha1.OpenSearchServerless{ObjectMeta: metav1.ObjectMeta{Namespace: "n", Name: "catalog", UID: "instance", Generation: 3}}
	operation := &v1alpha1.OpenSearchServerlessOperation{
		ObjectMeta: metav1.ObjectMeta{Namespace: instance.Namespace, UID: "operation"},
		Spec:       v1alpha1.OperationSpec{TargetFleet: "green", InstanceRef: v1alpha1.InstanceReference{Name: instance.Name, UID: string(instance.UID)}},
	}
	pod := corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{UID: "pod", ResourceVersion: "42", Labels: map[string]string{workload.ZoneLabel: "zone-a"}},
		Spec:       corev1.PodSpec{NodeName: "node-a"},
	}
	observed := candidateRoleObservation{
		Fleet: "green", Revision: "revision", PodSnapshot: []corev1.Pod{pod},
		Pods: []searchPodQualification{{PodUID: "pod", ProcessIncarnation: "process"}},
	}
	statement := rollout.PlatformEvidence{
		Namespace: "n", InstanceUID: "instance", InstanceGeneration: 3, OperationUID: "operation", Role: "search",
		Fleet: "green", Revision: "revision", ObservedAt: time.Now().UTC(),
		PeerQualified: true, SecurityQualified: true, RollbackCompatible: true,
		Pods: []rollout.PlatformPod{{UID: "pod", ResourceVersion: "42", NodeName: "node-a", Zone: "zone-a", ProcessIncarnation: "process"}},
	}
	key, signer, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	cm := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: rollout.PlatformEvidenceName(operation.UID, "search"), Namespace: "n", UID: "report", ResourceVersion: "1"}}
	publish := func(s rollout.PlatformEvidence) {
		t.Helper()
		raw, err := json.Marshal(s)
		if err != nil {
			t.Fatal(err)
		}
		signature := ed25519.Sign(signer, append([]byte(rollout.PlatformEvidenceDomain), raw...))
		cm.Data = map[string]string{"evidence.json": string(raw), "signature": base64.StdEncoding.EncodeToString(signature)}
	}
	publish(statement)
	k8s := fake.NewSimpleClientset(cm)
	check := signedRolloutPlatformCheck(k8s.CoreV1(), key)
	initial, err := check(ctx, instance, operation, "search", observed)
	if err != nil || !initial.observedAt.Equal(statement.ObservedAt) || !initial.unchanged(ctx) {
		t.Fatalf("current signed statement rejected: %v %v", initial.observedAt, err)
	}
	cm.ResourceVersion = "2"
	if _, err := k8s.CoreV1().ConfigMaps("n").Update(ctx, cm, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	if initial.unchanged(ctx) {
		t.Fatal("the original observation survived a report update")
	}
	for _, tc := range []struct {
		name   string
		change func(*rollout.PlatformEvidence)
	}{
		{"wrong zone", func(s *rollout.PlatformEvidence) { s.Pods[0].Zone = "zone-b" }},
		{"different Pod version", func(s *rollout.PlatformEvidence) { s.Pods[0].ResourceVersion = "43" }},
		{"restarted Java process", func(s *rollout.PlatformEvidence) { s.Pods[0].ProcessIncarnation = "other" }},
		{"different approval", func(s *rollout.PlatformEvidence) { s.OperationUID = "other" }},
		{"expired observation", func(s *rollout.PlatformEvidence) { s.ObservedAt = s.ObservedAt.Add(-time.Minute) }},
		{"incompatible rollback", func(s *rollout.PlatformEvidence) { s.RollbackCompatible = false }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			changed := statement
			changed.Pods = append([]rollout.PlatformPod(nil), statement.Pods...)
			tc.change(&changed)
			publish(changed)
			if _, err := k8s.CoreV1().ConfigMaps("n").Update(ctx, cm, metav1.UpdateOptions{}); err != nil {
				t.Fatal(err)
			}
			if _, err := check(ctx, instance, operation, "search", observed); err == nil {
				t.Fatal("invalid statement accepted")
			}
		})
	}
	publish(statement)
	cm.Data["evidence.json"] += " "
	if _, err := k8s.CoreV1().ConfigMaps("n").Update(ctx, cm, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := check(ctx, instance, operation, "search", observed); err == nil {
		t.Fatal("modified statement accepted without a new signature")
	}
	t.Run("fenced backend trust", func(t *testing.T) {
		state := instance.DeepCopyObject().(*v1alpha1.OpenSearchServerless)
		state.Status.HTTPTLSBaseline = &v1alpha1.HTTPTLSBaseline{TrustHash: "original"}
		state.Status.HTTPTrustRotationUsed = true
		state.Status.HTTPFleetCertificates = map[string]v1alpha1.HTTPFleetCertificate{"green": {TrustName: "overlap", TrustKey: "ca", TrustUID: "overlap-uid", TrustSHA256: "overlap-hash"}}
		op := operation.DeepCopyObject().(*v1alpha1.OpenSearchServerlessOperation)
		op.Spec.Action = "Promote"
		op.Spec.ExpectedServingFleetByRole = map[string]string{"search": "blue"}
		for _, sample := range []struct {
			hash            string
			fenced, accepts bool
		}{
			{"", false, false}, {"original", true, false}, {"overlap-hash", false, false}, {"overlap-hash", true, true},
		} {
			claim := statement
			claim.HTTPTrustSHA256, claim.HTTPTrustFenced = sample.hash, sample.fenced
			publish(claim)
			if _, err := k8s.CoreV1().ConfigMaps("n").Update(ctx, cm, metav1.UpdateOptions{}); err != nil {
				t.Fatal(err)
			}
			if _, err := check(ctx, state, op, "search", observed); (err == nil) != sample.accepts {
				t.Fatalf("backend trust %+v accepted incorrectly: %v", sample, err)
			}
		}
		op.Spec.Action, op.Spec.TargetFleet = "Rollback", "blue"
		op.Spec.ExpectedServingFleetByRole["search"] = "green"
		if got := transitionHTTPTrustHash(state, op, "search"); got != "overlap-hash" {
			t.Fatalf("rollback expected the wrong effective bundle: %s", got)
		}
		op.Spec.ExpectedServingFleetByRole["search"] = "blue"
		op.Status.WeightedRollout = &v1alpha1.WeightedRolloutStatus{Roles: map[string]v1alpha1.WeightedRoleStatus{
			"search": {Revisions: map[string]string{"blue": "blue-rev", "green": "green-rev"}},
		}}
		if got := transitionHTTPTrustHash(state, op, "search"); got != "overlap-hash" {
			t.Fatalf("weighted rollback skipped trust for the nonzero backend before stable cutover: %s", got)
		}
		op.Status.WeightedRollout = nil
		op.Spec.Action, op.Spec.TargetFleet = "Promote", "amber"
		op.Spec.ExpectedServingFleetByRole["search"] = "green"
		state.Status.HTTPFleetCertificates["amber"] = state.Status.HTTPFleetCertificates["green"]
		if got := transitionHTTPTrustHash(state, op, "search"); got != "overlap-hash" {
			t.Fatalf("same-bundle promotion after a CA change skipped its trust check: %s", got)
		}
	})

	instance.ResourceVersion = "2"
	instance.Spec.Rollout = &v1alpha1.Rollout{GatewayBackends: true}
	operation.Name, operation.Generation, operation.ResourceVersion = "promote", 1, "3"
	operation.Finalizers = []string{rolloutOperationFinalizer}
	operation.Spec.Action = "Promote"
	claim := operationClaimFor(operation)
	instance.Status.ActiveOperation = &claim
	operation.Status.WeightedRollout = &v1alpha1.WeightedRolloutStatus{
		InstanceGeneration: instance.Generation,
		Roles: map[string]v1alpha1.WeightedRoleStatus{
			"search": {Revisions: map[string]string{"blue": "revision"}, BackendUIDs: map[string]string{"blue": "backend"}},
		},
	}
	statement.Fleet, observed.Fleet = "blue", "blue"
	publish(statement)
	source := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name: rollout.FleetPlatformEvidenceName(operation.UID, "search", "blue", "green"), Namespace: instance.Namespace, UID: "source-report", ResourceVersion: "1",
		},
		Data: cm.Data,
	}
	if _, err := k8s.CoreV1().ConfigMaps(instance.Namespace).Create(ctx, source, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := check(ctx, instance, operation, "search", observed); err != nil {
		t.Fatalf("signed source approval for the weighted journal was rejected: %v", err)
	}
	operation.Status.WeightedRollout.Roles["search"].Revisions["blue"] = "different-revision"
	if _, err := check(ctx, instance, operation, "search", observed); err == nil {
		t.Fatal("source approval survived a change to the pinned revision")
	}
}
