package rollout

import (
	"context"
	"crypto/ed25519"
	"fmt"
	"slices"
	"time"

	"k8s.io/apimachinery/pkg/types"
	typedcore "k8s.io/client-go/kubernetes/typed/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/huyz0/opensearch-serverless-operator/api/v1alpha1"
	"github.com/huyz0/opensearch-serverless-operator/internal/java"
)

// PublishWriterWithdrawalEvidence signs a trusted observer's report only for
// the live withdrawal journal. The manager verifies the signature and checks
// the route, frontend and survivor inventory independently before mutation.
func PublishWriterWithdrawalEvidence(ctx context.Context, reader client.Reader, core typedcore.CoreV1Interface, namespace, operationName string, evidence WriterWithdrawalEvidence, signer ed25519.PrivateKey) error {
	if reader == nil || core == nil || namespace == "" || operationName == "" || len(signer) != ed25519.PrivateKeySize || namespace != evidence.Namespace {
		return fmt.Errorf("writer withdrawal evidence requires a named operation and signing identity")
	}
	var op v1alpha1.OpenSearchServerlessOperation
	if err := reader.Get(ctx, client.ObjectKey{Namespace: namespace, Name: operationName}, &op); err != nil {
		return err
	}
	var instance v1alpha1.OpenSearchServerless
	if err := reader.Get(ctx, client.ObjectKey{Namespace: namespace, Name: op.Spec.InstanceRef.Name}, &instance); err != nil {
		return err
	}
	journal := op.Status.WriterRetirement
	target := op.Spec.WriterRetirement
	claim := v1alpha1.OperationClaim{Name: op.Name, Action: "RetireWriter", UID: string(op.UID), Generation: op.Generation}
	if op.Spec.Validate() != nil || op.Spec.Action != claim.Action || op.UID != evidence.OperationUID || op.DeletionTimestamp != nil ||
		!slices.Contains(op.Finalizers, "serverless.opensearch.org/writer-retirement") ||
		journal == nil || !journal.WithdrawalArmed || journal.WithdrawnAt == nil || journal.VictimGone || target == nil ||
		instance.UID != evidence.InstanceUID || string(instance.UID) != op.Spec.InstanceRef.UID || instance.DeletionTimestamp != nil ||
		instance.Generation != journal.InstanceGeneration || instance.Generation != evidence.InstanceGeneration ||
		instance.Status.ActiveOperation == nil || *instance.Status.ActiveOperation != claim ||
		instance.Status.ServingFleetIntentByRole["ingest"] != journal.ServingFleet ||
		evidence.VictimPodUID != types.UID(target.PodUID) || evidence.Writer != (java.WriterProcess{NodeID: journal.NodeID, EphemeralID: journal.NodeEphemeralID}) ||
		evidence.ServiceUID != types.UID(journal.ServiceUID) || evidence.RouteUID != types.UID(journal.RouteUID) ||
		evidence.GatewayUID != types.UID(journal.GatewayUID) || evidence.GatewayServiceUID != types.UID(journal.GatewayServiceUID) {
		return fmt.Errorf("observation differs from the live writer withdrawal journal")
	}
	observed := GatewayReplicaObservation{Versions: evidence.GatewayVersions, UIDs: map[string]bool{}}
	for _, uid := range evidence.ServingProxyUIDs {
		observed.UIDs[uid] = true
	}
	var survivors map[types.UID]string
	if len(evidence.Proxies) != 0 {
		survivors = evidence.Proxies[0].LiveBackends
	}
	if _, err := evidence.verify(evidence.WriterWithdrawalScope, evidence.EndpointVersions, survivors, observed, journal.WithdrawnAt.Time, time.Now()); err != nil {
		return err
	}
	data, err := SignWriterWithdrawalEvidence(evidence, signer)
	if err != nil {
		return err
	}
	return publishEvidenceConfigMap(ctx, core.ConfigMaps(namespace), namespace, WriterWithdrawalEvidenceName(op.UID), data, func(current map[string]string) bool {
		previous, err := readSignedEvidence[WriterWithdrawalEvidence](writerWithdrawalDomain, current, signer.Public().(ed25519.PublicKey))
		return err == nil && previous.OperationUID == op.UID && !evidence.ObservedAt.After(previous.ObservedAt)
	})
}
