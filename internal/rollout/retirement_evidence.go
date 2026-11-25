package rollout

import (
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"fmt"
	"time"

	"k8s.io/apimachinery/pkg/types"
	typedcore "k8s.io/client-go/kubernetes/typed/core/v1"

	"github.com/huyz0/opensearch-serverless-operator/api/v1alpha1"
)

const retirementEvidenceDomain = "opensearch-serverless-fleet-retirement/v2\n"

const (
	RetirementReserve  = "Reserve"
	RetirementComplete = "Complete"
)

// FleetRetirementEvidence requests a reservation or records completion under it.
// An absent Pod object alone cannot establish that a JVM has stopped.
type FleetRetirementEvidence struct {
	Namespace          string                       `json:"namespace"`
	InstanceUID        types.UID                    `json:"instanceUID"`
	InstanceGeneration int64                        `json:"instanceGeneration"`
	CommitUID          types.UID                    `json:"commitUID"`
	Fleet              string                       `json:"fleet"`
	Revision           string                       `json:"revision,omitempty"`
	RevisionsByRole    *v1alpha1.FleetRoleRevisions `json:"revisionsByRole,omitempty"`
	Phase              string                       `json:"phase"`
	ReservationUID     string                       `json:"reservationUID,omitempty"`
	ObservedAt         time.Time                    `json:"observedAt"`
	TrafficDrained     bool                         `json:"trafficDrained"`
	ProcessesFenced    bool                         `json:"processesFenced"`
}

func FleetRetirementEvidenceName(uid types.UID) string {
	digest := sha256.Sum256([]byte(uid))
	return fmt.Sprintf("fleet-retirement-%x", digest[:12])
}

func ParseFleetRetirementEvidence(raw string) (FleetRetirementEvidence, error) {
	return parseEvidence[FleetRetirementEvidence](raw)
}

func ReadSignedFleetRetirementEvidence(data map[string]string, key ed25519.PublicKey) (FleetRetirementEvidence, error) {
	return readSignedEvidence[FleetRetirementEvidence](retirementEvidenceDomain, data, key)
}

// FleetRetirementTarget returns the displaced fleet after its rollback is ineligible.
func FleetRetirementTarget(instance *v1alpha1.OpenSearchServerless, now time.Time) (fleet string, revisions v1alpha1.FleetRoleRevisions) {
	commit := instance.Status.LastRolloutCommit
	serving := instance.Status.ServingFleetIntentByRole["search"]
	if instance.UID == "" || instance.Namespace == "" || commit == nil || commit.UID == "" || serving == "" || serving != instance.Status.ServingFleetIntentByRole["ingest"] ||
		instance.Status.ActiveOperation != nil || instance.Status.CandidateFleet != nil || instance.DeletionTimestamp != nil {
		return "", v1alpha1.FleetRoleRevisions{}
	}
	if target := instance.Status.RollbackTarget; target != nil {
		search, ingest := target.Roles["search"], target.Roles["ingest"]
		if commit.Action == "Promote" && target.PromotionUID == commit.UID && target.RetainUntil != nil && !now.Before(target.RetainUntil.Time) &&
			search.Fleet != "" && search.Fleet != serving && search.Fleet == ingest.Fleet && search.Revision != "" && ingest.Revision != "" {
			return search.Fleet, v1alpha1.FleetRoleRevisions{Search: search.Revision, Ingest: ingest.Revision}
		}
		return "", v1alpha1.FleetRoleRevisions{}
	}
	if commit.Action == "Rollback" {
		for _, retired := range instance.Status.RetiredCandidateFleets {
			if !retired.PreviewOnly || retired.Fleet == serving {
				continue
			}
			roles, valid := v1alpha1.ResolveFleetRoleRevisions(retired.Revision, retired.RevisionsByRole)
			if fleet != "" || retired.Fleet == "" || !valid {
				return "", v1alpha1.FleetRoleRevisions{}
			}
			fleet, revisions = retired.Fleet, roles
		}
	}
	return fleet, revisions
}

func (e FleetRetirementEvidence) Qualifies(instance *v1alpha1.OpenSearchServerless, now time.Time) bool {
	fleet, revisions := FleetRetirementTarget(instance, now)
	observed, valid := v1alpha1.ResolveFleetRoleRevisions(e.Revision, e.RevisionsByRole)
	if fleet == "" || e.Fleet != fleet || !valid || observed != revisions ||
		e.Namespace != instance.Namespace || e.InstanceUID != instance.UID || e.InstanceGeneration != instance.Generation ||
		e.CommitUID != types.UID(instance.Status.LastRolloutCommit.UID) || e.ObservedAt.IsZero() || e.ObservedAt.After(now) || now.Sub(e.ObservedAt) > PlatformEvidenceMaxAge {
		return false
	}
	reserved := instance.Status.FleetRetirement
	switch e.Phase {
	case RetirementReserve:
		return reserved == nil && e.ReservationUID == "" && !e.TrafficDrained && !e.ProcessesFenced
	case RetirementComplete:
		if reserved == nil {
			return false
		}
		pinned, valid := v1alpha1.ResolveFleetRoleRevisions(reserved.Revision, reserved.RevisionsByRole)
		return valid && pinned == revisions && reserved.UID != "" && e.ReservationUID == reserved.UID && reserved.CommitUID == string(e.CommitUID) &&
			reserved.Fleet == fleet && !reserved.ReservedAt.IsZero() && !e.ObservedAt.Before(reserved.ReservedAt.Time) &&
			e.TrafficDrained && e.ProcessesFenced
	}
	return false
}

func PublishFleetRetirementEvidence(ctx context.Context, core typedcore.CoreV1Interface, instance *v1alpha1.OpenSearchServerless, evidence FleetRetirementEvidence, signer ed25519.PrivateKey) error {
	if !evidence.Qualifies(instance, time.Now()) {
		return fmt.Errorf("retirement evidence does not qualify the displaced fleet")
	}
	data, err := signEvidence(retirementEvidenceDomain, evidence, signer)
	if err != nil {
		return err
	}
	return publishEvidenceConfigMap(ctx, core.ConfigMaps(instance.Namespace), instance.Namespace, FleetRetirementEvidenceName(evidence.CommitUID), data, func(current map[string]string) bool {
		previous, err := ReadSignedFleetRetirementEvidence(current, signer.Public().(ed25519.PublicKey))
		return err == nil && previous.Qualifies(instance, time.Now()) && !evidence.ObservedAt.After(previous.ObservedAt)
	})
}
