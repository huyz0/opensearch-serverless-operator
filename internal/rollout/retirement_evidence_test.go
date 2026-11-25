package rollout

import (
	"crypto/ed25519"
	"crypto/rand"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/huyz0/opensearch-serverless-operator/api/v1alpha1"
)

func TestSharedRetirementRevisionSurvivesUpgrade(t *testing.T) {
	key, signer, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	expired := metav1.NewMicroTime(now.Add(-time.Minute))
	instance := &v1alpha1.OpenSearchServerless{ObjectMeta: metav1.ObjectMeta{Namespace: "test", UID: "instance", Generation: 4}}
	instance.Status.ServingFleetIntentByRole = map[string]string{"search": "green", "ingest": "green"}
	instance.Status.LastRolloutCommit = &v1alpha1.OperationClaim{UID: "promotion", Action: "Promote"}
	instance.Status.RollbackTarget = &v1alpha1.RollbackTarget{PromotionUID: "promotion", RetainUntil: &expired, Roles: map[string]v1alpha1.RollbackRoleTarget{
		"search": {Fleet: "blue", Revision: "shared"}, "ingest": {Fleet: "blue", Revision: "shared"},
	}}
	instance.Status.FleetRetirement = &v1alpha1.FleetRetirementReservation{UID: "reserved", CommitUID: "promotion", Fleet: "blue", Revision: "shared", ReservedAt: metav1.NewMicroTime(now.Add(-time.Second))}
	evidence := FleetRetirementEvidence{Namespace: "test", InstanceUID: "instance", InstanceGeneration: 4, CommitUID: "promotion", Fleet: "blue", Revision: "shared",
		Phase: RetirementComplete, ReservationUID: "reserved", ObservedAt: now, TrafficDrained: true, ProcessesFenced: true}
	data, err := signEvidence(retirementEvidenceDomain, evidence, signer)
	if err != nil {
		t.Fatal(err)
	}
	read, err := ReadSignedFleetRetirementEvidence(data, key)
	if err != nil || !read.Qualifies(instance, now) {
		t.Fatalf("legacy fixture with one revision was rejected: %v", err)
	}
	evidence.RevisionsByRole = &v1alpha1.FleetRoleRevisions{Search: "shared", Ingest: "shared"}
	if evidence.Qualifies(instance, now) {
		t.Fatal("two revision representations were accepted")
	}
}
