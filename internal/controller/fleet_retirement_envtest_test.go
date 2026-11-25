//go:build envtest

package controller

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"fmt"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/huyz0/opensearch-serverless-operator/api/v1alpha1"
	"github.com/huyz0/opensearch-serverless-operator/internal/rollout"
	"github.com/huyz0/opensearch-serverless-operator/internal/workload"
)

func testDisplacedFleetRetirement(t *testing.T, c client.Client, kube kubernetes.Interface, ns string) {
	t.Helper()
	ctx := context.Background()
	key, signer, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	_, wrongSigner, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	for _, action := range []string{"Promote", "Rollback"} {
		t.Run(action, func(t *testing.T) {
			image := "example/image@sha256:" + strings.Repeat("b", 64)
			instance := &v1alpha1.OpenSearchServerless{ObjectMeta: metav1.ObjectMeta{Name: "retire-" + strings.ToLower(action), Namespace: ns},
				Spec: v1alpha1.OpenSearchServerlessSpec{Image: image, ServiceAccountName: "runtime",
					Storage: v1alpha1.Storage{Type: "s3", Bucket: "bucket", Region: "region"},
					Pools:   map[string]v1alpha1.Pool{"search": {Zones: map[string]v1alpha1.Zone{"a": {Replicas: 1}}}},
					Rollout: &v1alpha1.Rollout{Strategy: "BlueGreen", MaxPreviewReplicas: 3, Candidate: &v1alpha1.FleetCandidate{Fleet: "amber", Image: image}},
				},
			}
			if err := c.Create(ctx, instance); err != nil {
				t.Fatal(err)
			}
			update := func() {
				t.Helper()
				if err := c.Status().Update(ctx, instance); err != nil {
					t.Fatal(err)
				}
			}
			commitUID := types.UID("commit-" + strings.ToLower(action))
			instance.Status.LastRolloutCommit = &v1alpha1.OperationClaim{Name: "completed", UID: string(commitUID), Action: action, Generation: 1}
			instance.Status.ServingFleetIntentByRole = map[string]string{"search": "live", "ingest": "live"}
			want := v1alpha1.FleetRoleRevisions{Search: "search-revision", Ingest: "ingest-revision"}
			if action == "Promote" {
				future := metav1.NewMicroTime(time.Now().Add(time.Minute))
				instance.Status.RollbackTarget = &v1alpha1.RollbackTarget{PromotionUID: string(commitUID), RetainUntil: &future, Roles: map[string]v1alpha1.RollbackRoleTarget{
					"search": {Fleet: "old", Revision: want.Search, ServiceUID: "search-uid"},
					"ingest": {Fleet: "old", Revision: want.Ingest, ServiceUID: "ingest-uid"},
				}}
				instance.Status.RetiredCandidateFleets = []v1alpha1.RetiredCandidateFleet{{Fleet: "old", Revision: "previous-shared-alias", PreviewOnly: true}}
			} else {
				instance.Spec.Rollout.Candidate = nil
				status := instance.Status
				if err := c.Update(ctx, instance); err != nil {
					t.Fatal(err)
				}
				instance.Status = status
				instance.Status.CandidateFleet = &v1alpha1.CandidateFleetStatus{Fleet: "old", Image: image, Revision: "old-revision", Generation: instance.Generation,
					SearchServiceUID: "search-preview", IngestServiceUID: "ingest-preview"}
				instance.Status.ServingFleetIntentByRole = map[string]string{"search": "old", "ingest": "old"}
				pinServingRevision(instance, "search", "old", want.Search)
				pinServingRevision(instance, "ingest", "old", want.Ingest)
				rollback := &v1alpha1.OpenSearchServerlessOperation{Spec: v1alpha1.OperationSpec{Action: "Rollback", TargetFleet: "live"}}
				detached := instance.DeepCopyObject().(*v1alpha1.OpenSearchServerless)
				detached.Status.CandidateFleet = nil
				detached.Status.RetiredCandidateFleets = []v1alpha1.RetiredCandidateFleet{{Fleet: "old", Revision: "old-revision", PreviewOnly: true}}
				markRolloutCandidateServed(detached, rollback)
				archived := detached.Status.RetiredCandidateFleets[0]
				if got, valid := v1alpha1.ResolveFleetRoleRevisions(archived.Revision, archived.RevisionsByRole); !valid || got != want {
					t.Fatalf("rollback lost role revisions for the already detached fleet: %#v", archived)
				}
				markRolloutCandidateServed(instance, rollback)
				instance.Status.ServingFleetIntentByRole = map[string]string{"search": "live", "ingest": "live"}
				for _, role := range []string{"search", "ingest"} {
					pinServingRevision(instance, role, "live", "restored")
				}
			}
			pin := v1alpha1.HTTPFleetCertificate{Name: "leaf", UID: "leaf-uid", PEMSHA256: strings.Repeat("a", 64), LeafSHA256: strings.Repeat("b", 64)}
			core := kube.CoreV1()
			r := &InstanceReconciler{Client: c, Core: core, retirementEvidenceKey: key, retirementInstanceName: instance.Name}
			if action == "Promote" {
				pin = installNextRotationFixture(t, ctx, r, instance, image)
			}
			instance.Status.HTTPFleetCertificates = map[string]v1alpha1.HTTPFleetCertificate{"old": pin, "live": pin}
			update()
			if action == "Rollback" {
				if err := ensureRoleServices(ctx, core, instance); err != nil {
					t.Fatal(err)
				}
				if changed, err := r.reconcileCandidatePreview(ctx, instance); err != nil || !changed || instance.Status.CandidateFleet != nil ||
					len(instance.Status.RetiredCandidateFleets) != 1 || !instance.Status.RetiredCandidateFleets[0].PreviewOnly {
					t.Fatalf("rollback before preview detach bypassed signed retirement: %v %v", changed, err)
				}
			}
			if !strings.Contains(strings.Join(r.watchedConfigMapNames(instance), " "), rollout.FleetRetirementEvidenceName(commitUID)) {
				t.Fatal("retirement ConfigMap does not enqueue the instance")
			}
			evidence := rollout.FleetRetirementEvidence{Namespace: ns, InstanceUID: instance.UID, InstanceGeneration: instance.Generation,
				CommitUID: commitUID, Fleet: "old", RevisionsByRole: &want, Phase: rollout.RetirementReserve, ObservedAt: time.Now()}
			if action == "Promote" {
				if err := rollout.PublishFleetRetirementEvidence(ctx, core, instance, evidence, signer); err == nil {
					t.Fatal("published retirement during the active rollback window")
				}
				past := metav1.NewMicroTime(time.Now().Add(-time.Minute))
				instance.Status.RollbackTarget.RetainUntil = &past
				update()
				invalid := evidence
				invalid.RevisionsByRole = &v1alpha1.FleetRoleRevisions{Search: "ingest-revision", Ingest: "search-revision"}
				if err := rollout.PublishFleetRetirementEvidence(ctx, core, instance, invalid, signer); err == nil {
					t.Fatal("published a reservation with swapped roles")
				}
				invalid.Revision, invalid.RevisionsByRole = "search-revision", nil
				if err := rollout.PublishFleetRetirementEvidence(ctx, core, instance, invalid, signer); err == nil {
					t.Fatal("a shared revision qualified a mixed fleet")
				}
			}
			if changed, err := r.reconcileFleetRetirement(ctx, instance); err != nil || changed {
				t.Fatalf("empty Pod inventory alone retired the fleet: %v %v", changed, err)
			}
			if err := rollout.PublishFleetRetirementEvidence(ctx, core, instance, evidence, wrongSigner); err != nil {
				t.Fatal(err)
			}
			if _, err := r.reconcileFleetRetirement(ctx, instance); err != nil {
				t.Fatal(err)
			}
			if cond := meta.FindStatusCondition(instance.Status.Conditions, "FleetRetirement"); cond == nil || cond.Reason != "RetirementEvidenceUnqualified" {
				t.Fatalf("wrong signer qualified: %#v", cond)
			}
			complete := evidence
			complete.Phase, complete.TrafficDrained, complete.ProcessesFenced = rollout.RetirementComplete, true, true
			if err := rollout.PublishFleetRetirementEvidence(ctx, core, instance, complete, signer); err == nil {
				t.Fatal("published completed fencing without a controller reservation")
			}
			pod, err := core.Pods(ns).Create(ctx, &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: instance.Name + "-old", Labels: map[string]string{
				workload.FleetLabel: "old",
			}, OwnerReferences: []metav1.OwnerReference{*metav1.NewControllerRef(instance, v1alpha1.GroupVersion.WithKind("OpenSearchServerless"))}},
				Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "opensearch", Image: image}}}}, metav1.CreateOptions{})
			if err != nil {
				t.Fatal(err)
			}
			if err := rollout.PublishFleetRetirementEvidence(ctx, core, instance, evidence, signer); err != nil {
				t.Fatal(err)
			}
			operation := &v1alpha1.OpenSearchServerlessOperation{ObjectMeta: metav1.ObjectMeta{Name: instance.Name + "-account", Namespace: ns},
				Spec: v1alpha1.OperationSpec{Action: "ApplySecurityConfiguration", InstanceRef: v1alpha1.InstanceReference{Name: instance.Name, UID: string(instance.UID)},
					SecurityConfiguration: &v1alpha1.SecurityConfigurationChange{CandidateRevision: "next", ExpectedActiveRevision: "current"}},
			}
			if err := c.Create(ctx, operation); err != nil {
				t.Fatal(err)
			}
			staleReserve := instance.DeepCopyObject().(*v1alpha1.OpenSearchServerless)
			if err := ClaimOperation(ctx, c, instance, operation); err != nil {
				t.Fatal(err)
			}
			if _, err := r.reconcileFleetRetirement(ctx, staleReserve); !apierrors.IsConflict(err) {
				t.Fatalf("stale retirement reservation overtook an account claim: %v", err)
			}
			if changed, err := r.reconcileFleetRetirement(ctx, instance); err != nil || changed {
				t.Fatalf("active account claim did not hold retirement: %v %v", changed, err)
			}
			instance.Status.ActiveOperation = nil
			update()
			staleClaim := instance.DeepCopyObject().(*v1alpha1.OpenSearchServerless)
			if changed, err := r.reconcileFleetRetirement(ctx, instance); err != nil || !changed || instance.Status.FleetRetirement == nil {
				t.Fatalf("reserve retirement before drain: %v %v", changed, err)
			}
			if err := ClaimOperation(ctx, c, staleClaim, operation); !apierrors.IsConflict(err) {
				t.Fatalf("in-flight account claim overtook reservation: %v", err)
			}
			if err := ClaimOperation(ctx, c, instance, operation); err == nil || !strings.Contains(err.Error(), "fleet retirement reservation") {
				t.Fatalf("fresh account claim was not held by the reservation: %v", err)
			}
			retry := &v1alpha1.OpenSearchServerlessOperation{ObjectMeta: metav1.ObjectMeta{Name: instance.Name + "-retry", Namespace: ns},
				Spec: v1alpha1.OperationSpec{Action: "RetryProvisioning", InstanceRef: operation.Spec.InstanceRef,
					ProvisioningRetry: &v1alpha1.ProvisioningRetry{PodUID: "withdrawn-serving-pod", AcknowledgeNewAttempt: true}},
			}
			if err := c.Create(ctx, retry); err != nil {
				t.Fatal(err)
			}
			if err := ClaimOperation(ctx, c, instance, retry); err != nil {
				t.Fatalf("retirement blocked recovery of serving provisioning: %v", err)
			}
			instance.Status.ActiveOperation = nil
			update()
			if action == "Promote" {
				rollback := &v1alpha1.OpenSearchServerlessOperation{ObjectMeta: metav1.ObjectMeta{Name: instance.Name + "-rollback", Namespace: ns},
					Spec: v1alpha1.OperationSpec{Action: "Rollback", InstanceRef: operation.Spec.InstanceRef, SupersedesOperationUID: string(commitUID),
						TargetFleet: "old", ExpectedServingFleetByRole: map[string]string{"search": "live", "ingest": "live"}},
				}
				if err := c.Create(ctx, rollback); err != nil {
					t.Fatal(err)
				}
				routes := &OperationReconciler{Client: c}
				if changed, err := routes.prepareReleasedPromotionRollback(ctx, c, instance, rollback); err == nil || changed || !strings.Contains(err.Error(), "retirement is reserved") {
					t.Fatalf("selector rollback bypassed the reservation: %v %v", changed, err)
				}
				if changed, err := routes.prepareWeightedRollbackHandoff(ctx, c, client.ObjectKeyFromObject(rollback), rollback.UID, nil); err == nil || changed || !strings.Contains(err.Error(), "retirement is reserved") {
					t.Fatalf("weighted rollback bypassed the reservation: %v %v", changed, err)
				}
			}
			reserved := instance.Status.FleetRetirement
			if got, valid := v1alpha1.ResolveFleetRoleRevisions(reserved.Revision, reserved.RevisionsByRole); !valid || got != want {
				t.Fatalf("reservation did not preserve the exact role revisions: %#v", reserved)
			}
			if len(reserved.PodUIDs) != 1 || reserved.PodUIDs[0] != string(pod.UID) {
				t.Fatalf("displaced Pod is missing from the reservation inventory: %#v", reserved.PodUIDs)
			}
			pod.Labels[workload.FleetLabel] = "live"
			if _, err := core.Pods(ns).Update(ctx, pod, metav1.UpdateOptions{}); err != nil {
				t.Fatal(err)
			}
			complete.ReservationUID, complete.ObservedAt = "wrong-token", time.Now()
			if err := rollout.PublishFleetRetirementEvidence(ctx, core, instance, complete, signer); err == nil {
				t.Fatal("completed fencing was accepted with a different reservation UID")
			}
			complete.ReservationUID = reserved.UID
			if action == "Promote" {
				invalid := complete
				invalid.RevisionsByRole = &v1alpha1.FleetRoleRevisions{Search: want.Ingest, Ingest: want.Search}
				if err := rollout.PublishFleetRetirementEvidence(ctx, core, instance, invalid, signer); err == nil {
					t.Fatal("published completion with swapped roles")
				}
			}
			if err := rollout.PublishFleetRetirementEvidence(ctx, core, instance, complete, signer); err != nil {
				t.Fatal(err)
			}
			if _, err := r.reconcileFleetRetirement(ctx, instance); err != nil {
				t.Fatal(err)
			}
			if cond := meta.FindStatusCondition(instance.Status.Conditions, "FleetRetirement"); cond == nil || cond.Reason != "RetiredFleetPodsRemain" {
				t.Fatalf("signed statement overrode a remaining relabeled Pod: %#v", cond)
			}
			zero := int64(0)
			if err := core.Pods(ns).Delete(ctx, pod.Name, metav1.DeleteOptions{GracePeriodSeconds: &zero, Preconditions: &metav1.Preconditions{UID: &pod.UID}}); err != nil {
				t.Fatal(err)
			}
			if changed, err := r.reconcileFleetRetirement(ctx, instance); err != nil || !changed {
				t.Fatalf("fenced, empty fleet did not retire: %v %v", changed, err)
			}
			if err := c.Get(ctx, client.ObjectKeyFromObject(instance), instance); err != nil {
				t.Fatal(err)
			}
			if instance.Status.RollbackTarget != nil || instance.Status.FleetRetirement != nil || len(instance.Status.RetiredCandidateFleets) != 1 || instance.Status.RetiredCandidateFleets[0].PreviewOnly ||
				instance.Status.RetiredCandidateFleets[0].Fleet != "old" || len(instance.Status.HTTPFleetCertificates) != 1 || instance.Status.HTTPFleetCertificates["live"] != pin {
				t.Fatalf("retirement did not retain the old fleet fence and live pin: %#v", instance.Status)
			}
			archived := instance.Status.RetiredCandidateFleets[0]
			if got, valid := v1alpha1.ResolveFleetRoleRevisions(archived.Revision, archived.RevisionsByRole); !valid || got != want {
				t.Fatalf("archived fleet did not preserve the exact role revisions: %#v", archived)
			}
			if changed, err := r.reconcileFleetRetirement(ctx, instance); err != nil || changed {
				t.Fatalf("recorded retirement did not survive retry: %v %v", changed, err)
			}
			if err := ClaimOperation(ctx, c, instance, operation); err != nil {
				t.Fatalf("completed retirement did not release the operation lock: %v", err)
			}
			instance.Status.ActiveOperation = nil
			update()
			if action == "Promote" {
				if changed, err := r.reconcileCandidatePreview(ctx, instance); err != nil || !changed || instance.Status.CandidateFleet == nil ||
					instance.Status.CandidateFleet.Fleet != "amber" || instance.Status.HTTPFleetCertificates["amber"].LeafSHA256 == pin.LeafSHA256 ||
					instance.Status.HTTPFleetCertificates["amber"].Name != "retirement-amber" {
					t.Fatalf("retirement did not permit staging a second leaf on amber: %#v %v", instance.Status.CandidateFleet, err)
				}
				r.Policy, r.DevelopmentGatedPods = kube.PolicyV1(), true
				instance.Status.PodCreationSequence = 1
				update()
				late, err := core.Pods(ns).Create(ctx, &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
					Name: podNameForSequence(instance, 1), Annotations: map[string]string{creationIntentKey: fmt.Sprintf("%s/1", instance.UID)},
					Labels:          map[string]string{workload.InstanceLabel: string(instance.UID), workload.FleetLabel: "old", workload.RoleLabel: "search", workload.ZoneLabel: "a", "serverless.opensearch.org/revision": want.Ingest},
					OwnerReferences: []metav1.OwnerReference{*metav1.NewControllerRef(instance, v1alpha1.GroupVersion.WithKind("OpenSearchServerless"))},
				}, Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "opensearch", Image: image}}, SchedulingGates: []corev1.PodSchedulingGate{{Name: "serverless.opensearch.org/placement"}}}}, metav1.CreateOptions{})
				if err != nil {
					t.Fatal(err)
				}
				if _, _, err := r.reconcileRetiredCandidateFleets(ctx, instance); err == nil {
					t.Fatal("a late search Pod was accepted with the archived ingest revision")
				}
				late.Labels["serverless.opensearch.org/revision"] = want.Search
				if _, err := core.Pods(ns).Update(ctx, late, metav1.UpdateOptions{}); err != nil {
					t.Fatal(err)
				}
				if _, changed, err := r.reconcileRetiredCandidateFleets(ctx, instance); err != nil || !changed || len(instance.Status.RetiredPodCreations) != 1 || instance.Status.RetiredPodCreations[0].Revision != want.Search {
					t.Fatalf("late Pod with its own role revision was not recorded for gated withdrawal: %v %v", changed, err)
				}
			}
		})
	}
}

func installNextRotationFixture(t *testing.T, ctx context.Context, r *InstanceReconciler, instance *v1alpha1.OpenSearchServerless, image string) v1alpha1.HTTPFleetCertificate {
	t.Helper()
	const domain = "cluster.local"
	r.DevelopmentClusterDomain, r.DevelopmentHTTPTLSImage, r.DevelopmentPeerDNSImage = domain, image, image
	identity, err := httpPodIdentity(instance, podNameForSequence(instance, 1), domain)
	if err != nil {
		t.Fatal(err)
	}
	ca, leaves := rotationTestLeaves(t, "*."+strings.SplitN(identity, ".", 2)[1], "backend.test")
	immutable := true
	create := func(name string, kind corev1.SecretType, data map[string][]byte) {
		t.Helper()
		if _, err := r.Core.Secrets(instance.Namespace).Create(ctx, &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: name}, Immutable: &immutable, Type: kind, Data: data,
		}, metav1.CreateOptions{}); err != nil {
			t.Fatal(err)
		}
	}
	create("retirement-ca", corev1.SecretTypeOpaque, map[string][]byte{"ca.crt": ca})
	create("retirement-blue", corev1.SecretTypeTLS, leaves[0])
	create("retirement-green", corev1.SecretTypeTLS, leaves[2])
	create("retirement-amber", corev1.SecretTypeTLS, leaves[3])
	instance.Spec.TLS = &v1alpha1.TLSConfig{HTTP: &v1alpha1.TLSChannel{Source: "Secret",
		CertificateRef: v1alpha1.LocalReference{Name: "retirement-blue"}, TrustBundleRef: v1alpha1.SecretKeyReference{Name: "retirement-ca", Key: "ca.crt"}}}
	instance.Spec.Rollout.Candidate.HTTPCertificateRef = &v1alpha1.LocalReference{Name: "retirement-amber"}
	status := instance.Status
	if err := r.Update(ctx, instance); err != nil {
		t.Fatal(err)
	}
	instance.Status = status
	base, err := r.resolveHTTPTLS(ctx, instance, domain)
	if err != nil {
		t.Fatal(err)
	}
	instance.Status.HTTPTLSBaseline = &base.baseline
	trust, roots, err := r.readHTTPTrustForFleet(ctx, instance, domain, "")
	if err != nil {
		t.Fatal(err)
	}
	green, err := r.readHTTPLeaf(ctx, instance, "retirement-green", trust, "ca.crt", roots, domain)
	if err != nil {
		t.Fatal(err)
	}
	pin := green.baseline
	return v1alpha1.HTTPFleetCertificate{Name: pin.CertificateName, UID: pin.CertificateUID, PEMSHA256: pin.CertificateHash, LeafSHA256: pin.LeafSHA256}
}
