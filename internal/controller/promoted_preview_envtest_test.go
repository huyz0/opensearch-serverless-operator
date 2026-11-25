//go:build envtest

package controller

import (
	"context"
	"reflect"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/kubernetes"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/huyz0/opensearch-serverless-operator/api/v1alpha1"
	"github.com/huyz0/opensearch-serverless-operator/internal/workload"
)

func testPromotedPreviewDetach(t *testing.T, c client.Client, kube kubernetes.Interface, ns string) {
	t.Helper()
	ctx := context.Background()
	image := "example/image@sha256:" + strings.Repeat("b", 64)
	instance := &v1alpha1.OpenSearchServerless{
		ObjectMeta: metav1.ObjectMeta{Name: "detach-promoted", Namespace: ns},
		Spec: v1alpha1.OpenSearchServerlessSpec{
			Image: image, ServiceAccountName: "runtime",
			Storage: v1alpha1.Storage{Type: "s3", Bucket: "bucket", Region: "region"},
			Pools:   map[string]v1alpha1.Pool{"search": {Zones: map[string]v1alpha1.Zone{"a": {Replicas: 1}}}},
			Rollout: &v1alpha1.Rollout{Strategy: "BlueGreen", GatewayBackends: true},
		},
	}
	if err := c.Create(ctx, instance); err != nil {
		t.Fatal(err)
	}
	core := kube.CoreV1()
	createService := func(service *corev1.Service, err error) *corev1.Service {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
		actual, err := core.Services(ns).Create(ctx, service, metav1.CreateOptions{})
		if err != nil {
			t.Fatal(err)
		}
		return actual
	}
	search := createService(workload.RoleService(instance, "search", "green", 9200))
	ingest := createService(workload.RoleService(instance, "ingest", "blue", 9200))
	searchPreview := createService(workload.PreviewService(instance, "search", "green"))
	ingestPreview := createService(workload.PreviewService(instance, "ingest", "green"))
	greenBackend := createService(workload.GatewayBackendService(instance, "ingest", "green"))
	blueBackend := createService(workload.GatewayBackendService(instance, "ingest", "blue"))
	owner := *metav1.NewControllerRef(instance, v1alpha1.GroupVersion.WithKind("OpenSearchServerless"))
	pod, err := core.Pods(ns).Create(ctx, &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
		Name: "detach-green-pod", OwnerReferences: []metav1.OwnerReference{owner},
		Labels: map[string]string{workload.InstanceLabel: string(instance.UID), workload.FleetLabel: "green"},
	}, Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "opensearch", Image: image}}}}, metav1.CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	min := intstr.FromInt32(1)
	budget, err := kube.PolicyV1().PodDisruptionBudgets(ns).Create(ctx, &policyv1.PodDisruptionBudget{ObjectMeta: metav1.ObjectMeta{
		Name: "detach-green-pdb", OwnerReferences: []metav1.OwnerReference{owner},
	}, Spec: policyv1.PodDisruptionBudgetSpec{MinAvailable: &min, Selector: &metav1.LabelSelector{MatchLabels: pod.Labels}}}, metav1.CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	instance.Status.ServingFleetIntentByRole = map[string]string{"search": "green", "ingest": "green"}
	instance.Status.CandidateFleet = &v1alpha1.CandidateFleetStatus{Fleet: "green", Image: image, Revision: "promoted", Generation: instance.Generation, SearchServiceUID: string(searchPreview.UID), IngestServiceUID: string(ingestPreview.UID)}
	instance.Status.LastRolloutCommit = &v1alpha1.OperationClaim{Name: "promote", UID: "promotion", Generation: 1, Action: "Promote"}
	for _, role := range []string{"search", "ingest"} {
		pinServingRevision(instance, role, "green", "promoted")
	}
	instance.Status.RollbackTarget = &v1alpha1.RollbackTarget{Weighted: true, PromotionUID: "promotion", Roles: map[string]v1alpha1.RollbackRoleTarget{
		"search": {Fleet: "blue", Revision: "old", ServiceUID: string(search.UID)},
		"ingest": {Fleet: "blue", Revision: "old", ServiceUID: string(ingest.UID)},
	}}
	instance.Status.HTTPFleetCertificates = map[string]v1alpha1.HTTPFleetCertificate{"green": {Name: "leaf-green", UID: "leaf", PEMSHA256: strings.Repeat("a", 64), LeafSHA256: strings.Repeat("b", 64)}}
	if err := c.Status().Update(ctx, instance); err != nil {
		t.Fatal(err)
	}
	keep := instance.DeepCopyObject().(*v1alpha1.OpenSearchServerless).Status
	r := &InstanceReconciler{Client: c, Core: core, Policy: kube.PolicyV1()}
	if changed, err := r.retireCancelledCandidateGated(ctx, instance); err != nil || changed {
		t.Fatalf("gated preview cancellation intercepted serving alias cleanup: %v %v", changed, err)
	}
	if _, err := r.reconcileCandidatePreview(ctx, instance); err != nil || instance.Status.CandidateFleet == nil {
		t.Fatalf("detach did not hold when one stable selector disagreed: %v", err)
	}
	ingest.Spec.Selector[workload.FleetLabel] = "green"
	if _, err := core.Services(ns).Update(ctx, ingest, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	if changed, err := r.reconcileCandidatePreview(ctx, instance); err != nil || !changed || instance.Status.CandidateFleet != nil {
		t.Fatalf("serving preview was not detached: %v %v", changed, err)
	}
	if err := c.Get(ctx, client.ObjectKeyFromObject(instance), instance); err != nil {
		t.Fatal(err)
	}
	if len(instance.Status.RetiredCandidateFleets) != 1 || !instance.Status.RetiredCandidateFleets[0].PreviewOnly ||
		!reflect.DeepEqual(instance.Status.RollbackTarget, keep.RollbackTarget) || !reflect.DeepEqual(instance.Status.HTTPFleetCertificates, keep.HTTPFleetCertificates) {
		t.Fatalf("cleanup intent lost fleet or certificate pins: %#v", instance.Status)
	}
	r = &InstanceReconciler{Client: c, Core: core}
	for range 2 {
		if _, changed, err := r.reconcileRetiredCandidateFleets(ctx, instance); err != nil || !changed {
			t.Fatalf("resumed alias cleanup did not progress: %v %v", changed, err)
		}
	}
	for _, service := range []*corev1.Service{searchPreview, ingestPreview} {
		if _, err := core.Services(ns).Get(ctx, service.Name, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
			t.Fatalf("preview alias still exists: %v", err)
		}
	}
	for _, service := range []*corev1.Service{search, ingest, blueBackend, greenBackend} {
		if observed, err := core.Services(ns).Get(ctx, service.Name, metav1.GetOptions{}); err != nil || observed.UID != service.UID {
			t.Fatalf("cleanup changed stable Service or Gateway backend %s: %v", service.Name, err)
		}
	}
	if actual, err := core.Pods(ns).Get(ctx, pod.Name, metav1.GetOptions{}); err != nil || actual.UID != pod.UID {
		t.Fatalf("cleanup changed the serving Pod: %v", err)
	}
	if actual, err := kube.PolicyV1().PodDisruptionBudgets(ns).Get(ctx, budget.Name, metav1.GetOptions{}); err != nil || actual.UID != budget.UID {
		t.Fatalf("cleanup changed the serving budget: %v", err)
	}
	replacement := createService(workload.PreviewService(instance, "search", "green"))
	if _, changed, err := r.reconcileRetiredCandidateFleets(ctx, instance); err != nil || !changed {
		t.Fatalf("alias UID conflict was not recorded: %v %v", changed, err)
	}
	if condition := meta.FindStatusCondition(instance.Status.Conditions, "PromotedPreviewAliasesClean"); condition == nil || condition.Reason != "AliasCleanupBlocked" {
		t.Fatalf("replacement alias conflict was not reported: %#v", condition)
	}
	if _, changed, err := r.reconcileRetiredCandidateFleets(ctx, instance); err != nil || changed {
		t.Fatalf("alias conflict held unrelated reconciliation: %v %v", changed, err)
	}
	if actual, err := core.Services(ns).Get(ctx, replacement.Name, metav1.GetOptions{}); err != nil || actual.UID != replacement.UID {
		t.Fatalf("cleanup changed the replacement alias: %v", err)
	}
	instance.Spec.Rollout.Candidate = &v1alpha1.FleetCandidate{Fleet: "amber", Image: image}
	if err := c.Update(ctx, instance); err != nil {
		t.Fatal(err)
	}
	if _, err := r.reconcileCandidatePreview(ctx, instance); err != nil || instance.Status.CandidateFleet != nil {
		t.Fatalf("admitted a third fleet while rollback remains retained: %v", err)
	}
	if condition := meta.FindStatusCondition(instance.Status.Conditions, "PreviewServicesConfigured"); condition == nil || condition.Reason != "RollbackFleetRetained" {
		t.Fatalf("missing rollback retention condition: %#v", condition)
	}
	instance.Status.RollbackTarget = nil
	instance.Status.ServingFleetIntentByRole = map[string]string{"search": "blue", "ingest": "blue"}
	if err := c.Status().Update(ctx, instance); err != nil {
		t.Fatal(err)
	}
	if _, err := r.reconcileCandidatePreview(ctx, instance); err != nil || instance.Status.CandidateFleet != nil {
		t.Fatalf("admitted another fleet following rollback with the detached fleet still present: %v", err)
	}
	if condition := meta.FindStatusCondition(instance.Status.Conditions, "PreviewServicesConfigured"); condition == nil || condition.Reason != "DetachedFleetRetained" {
		t.Fatalf("missing detached fleet retention condition: %#v", condition)
	}
}
