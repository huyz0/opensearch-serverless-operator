package rollout

import (
	"context"
	"crypto/ed25519"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	typedcore "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/huyz0/opensearch-serverless-operator/api/v1alpha1"
	"github.com/huyz0/opensearch-serverless-operator/internal/workload"
)

// PublishPlatformEvidence checks live rollout and Node identities before
// signing a statement supplied by the trusted platform verifier. The verifier
// is responsible for peer health, security, and compatibility conclusions.
func PublishPlatformEvidence(ctx context.Context, reader client.Reader, core typedcore.CoreV1Interface, namespace, operationName string, evidence PlatformEvidence, signer ed25519.PrivateKey) error {
	if reader == nil || core == nil || namespace == "" || operationName == "" || len(signer) != ed25519.PrivateKeySize ||
		evidence.Namespace != namespace || evidence.InstanceUID == "" || evidence.OperationUID == "" ||
		(evidence.Role != "search" && evidence.Role != "ingest") || evidence.Fleet == "" || evidence.Revision == "" ||
		!evidence.PeerQualified || !evidence.SecurityQualified || !evidence.RollbackCompatible ||
		len(evidence.Pods) == 0 || len(evidence.Pods) > 512 || !platformObservationCurrent(evidence.ObservedAt) {
		return fmt.Errorf("current platform approval and matching rollout scope are required")
	}
	var operation v1alpha1.OpenSearchServerlessOperation
	if err := reader.Get(ctx, client.ObjectKey{Namespace: namespace, Name: operationName}, &operation); err != nil {
		return err
	}
	if err := operation.Spec.Validate(); err != nil {
		return err
	}
	if operation.UID != evidence.OperationUID || operation.DeletionTimestamp != nil ||
		(operation.Spec.Action != "Promote" && operation.Spec.Action != "Rollback") ||
		operation.Spec.InstanceRef.UID != string(evidence.InstanceUID) {
		return fmt.Errorf("operation differs from the supplied platform approval")
	}
	var instance v1alpha1.OpenSearchServerless
	if err := reader.Get(ctx, client.ObjectKey{Namespace: namespace, Name: operation.Spec.InstanceRef.Name}, &instance); err != nil {
		return err
	}
	if instance.UID != evidence.InstanceUID || instance.Generation != evidence.InstanceGeneration || instance.DeletionTimestamp != nil {
		return fmt.Errorf("instance differs from the supplied platform approval")
	}
	if weighted := operation.Status.WeightedRollout; weighted != nil {
		role, ok := weighted.Roles[evidence.Role]
		claim := instance.Status.ActiveOperation
		if !ok || instance.Spec.Rollout == nil || !instance.Spec.Rollout.GatewayBackends ||
			weighted.InstanceGeneration != instance.Generation || role.Revisions[evidence.Fleet] != evidence.Revision ||
			role.BackendUIDs[evidence.Fleet] == "" || claim == nil || claim.Name != operation.Name ||
			claim.UID != string(operation.UID) || claim.Action != operation.Spec.Action || claim.Generation != operation.Generation {
			return fmt.Errorf("weighted fleet differs from the claimed rollout approval")
		}
	} else if operation.Spec.TargetFleet != evidence.Fleet {
		return fmt.Errorf("operation differs from the supplied platform approval")
	} else if operation.Status.RolloutRoles != nil {
		role, ok := operation.Status.RolloutRoles[evidence.Role]
		if !ok || role.TargetFleet != evidence.Fleet || role.CandidateRevision != evidence.Revision {
			return fmt.Errorf("prepared role differs from the supplied platform approval")
		}
	} else {
		candidate := instance.Status.CandidateFleet
		if operation.Spec.Action != "Promote" || candidate == nil || candidate.PreviewRetiring || candidate.Generation != instance.Generation ||
			candidate.Fleet != evidence.Fleet || candidate.Revision != evidence.Revision {
			return fmt.Errorf("staged candidate differs from the supplied platform approval")
		}
	}
	if err := verifyPlatformPlacement(ctx, core, &instance, evidence); err != nil {
		return err
	}
	var latestOperation v1alpha1.OpenSearchServerlessOperation
	if err := reader.Get(ctx, client.ObjectKeyFromObject(&operation), &latestOperation); err != nil {
		return err
	}
	var latestInstance v1alpha1.OpenSearchServerless
	if err := reader.Get(ctx, client.ObjectKeyFromObject(&instance), &latestInstance); err != nil {
		return err
	}
	if latestOperation.UID != operation.UID || latestOperation.ResourceVersion != operation.ResourceVersion ||
		latestInstance.UID != instance.UID || latestInstance.ResourceVersion != instance.ResourceVersion ||
		!platformObservationCurrent(evidence.ObservedAt) {
		return fmt.Errorf("rollout state or platform approval changed during verification")
	}
	data, err := SignPlatformEvidence(evidence, signer)
	if err != nil {
		return err
	}
	name := FleetPlatformEvidenceName(evidence.OperationUID, evidence.Role, evidence.Fleet, operation.Spec.TargetFleet)
	return publishEvidenceConfigMap(ctx, core.ConfigMaps(namespace), namespace, name, data, func(current map[string]string) bool {
		previous, err := ReadSignedPlatformEvidence(current, signer.Public().(ed25519.PublicKey))
		return err == nil && previous.OperationUID == evidence.OperationUID &&
			previous.Role == evidence.Role && previous.Fleet == evidence.Fleet && !evidence.ObservedAt.After(previous.ObservedAt)
	})
}

func publishEvidenceConfigMap(ctx context.Context, configMaps typedcore.ConfigMapInterface, namespace, name string, data map[string]string, keepExisting func(map[string]string) bool) error {
	return retry.OnError(retry.DefaultRetry, func(err error) bool {
		return apierrors.IsConflict(err) || apierrors.IsAlreadyExists(err)
	}, func() error {
		current, err := configMaps.Get(ctx, name, metav1.GetOptions{})
		if apierrors.IsNotFound(err) {
			_, err = configMaps.Create(ctx, &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace}, Data: data}, metav1.CreateOptions{})
			return err
		}
		if err != nil {
			return err
		}
		if current.DeletionTimestamp != nil {
			return fmt.Errorf("rollout evidence ConfigMap is terminating")
		}
		if keepExisting(current.Data) {
			return nil
		}
		current.Data = data
		_, err = configMaps.Update(ctx, current, metav1.UpdateOptions{})
		return err
	})
}

func platformObservationCurrent(at time.Time) bool {
	now := time.Now()
	return !at.IsZero() && !at.After(now) && now.Sub(at) <= PlatformEvidenceMaxAge
}

func verifyPlatformPlacement(ctx context.Context, core typedcore.CoreV1Interface, instance *v1alpha1.OpenSearchServerless, evidence PlatformEvidence) error {
	selector := labels.SelectorFromSet(labels.Set{workload.InstanceLabel: string(instance.UID), workload.RoleLabel: evidence.Role, workload.FleetLabel: evidence.Fleet}).String()
	list := func() (*corev1.PodList, error) {
		return core.Pods(instance.Namespace).List(ctx, metav1.ListOptions{LabelSelector: selector, Limit: 513})
	}
	pods, err := list()
	if err != nil {
		return err
	}
	if pods.Continue != "" || len(pods.Items) != len(evidence.Pods) {
		return fmt.Errorf("live Pod cohort differs from the platform approval")
	}
	attested := make(map[types.UID]PlatformPod, len(evidence.Pods))
	for _, pod := range evidence.Pods {
		if _, duplicate := attested[pod.UID]; duplicate || pod.UID == "" || pod.ResourceVersion == "" || pod.NodeName == "" || pod.Zone == "" || pod.ProcessIncarnation == "" {
			return fmt.Errorf("platform approval contains duplicate or incomplete Pod identities")
		}
		attested[pod.UID] = pod
	}
	nodes := make(map[string]*corev1.Node)
	versions := make(map[types.UID]string, len(pods.Items))
	for _, pod := range pods.Items {
		approved, ok := attested[pod.UID]
		owner := metav1.GetControllerOf(&pod)
		if !ok || owner == nil || owner.UID != instance.UID || owner.Name != instance.Name || owner.Kind != "OpenSearchServerless" ||
			owner.APIVersion != v1alpha1.GroupVersion.String() || pod.DeletionTimestamp != nil || pod.ResourceVersion != approved.ResourceVersion ||
			pod.Spec.NodeName != approved.NodeName || pod.Labels[workload.ZoneLabel] != approved.Zone ||
			pod.Labels["serverless.opensearch.org/revision"] != evidence.Revision {
			return fmt.Errorf("Pod %q differs from the platform approval", pod.Name)
		}
		node := nodes[approved.NodeName]
		if node == nil {
			node, err = core.Nodes().Get(ctx, approved.NodeName, metav1.GetOptions{})
			if err != nil {
				return err
			}
			nodes[approved.NodeName] = node
		}
		if node.UID == "" || node.ResourceVersion == "" || node.DeletionTimestamp != nil || node.Labels[corev1.LabelTopologyZone] != approved.Zone {
			return fmt.Errorf("Node %q does not confirm the approved zone", approved.NodeName)
		}
		versions[pod.UID] = pod.ResourceVersion
	}
	for name, first := range nodes {
		current, err := core.Nodes().Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			return err
		}
		if current.UID != first.UID || current.ResourceVersion != first.ResourceVersion {
			return fmt.Errorf("Node %q changed during placement verification", name)
		}
	}
	currentPods, err := list()
	if err != nil {
		return err
	}
	if currentPods.Continue != "" || len(currentPods.Items) != len(versions) {
		return fmt.Errorf("Pod cohort changed during placement verification")
	}
	for _, pod := range currentPods.Items {
		if versions[pod.UID] != pod.ResourceVersion || pod.ResourceVersion == "" {
			return fmt.Errorf("Pod %q changed during placement verification", pod.Name)
		}
		delete(versions, pod.UID)
	}
	return nil
}
