package controller

import (
	"context"
	"fmt"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	kubefake "k8s.io/client-go/kubernetes/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/huyz0/opensearch-serverless-operator/api/v1alpha1"
	"github.com/huyz0/opensearch-serverless-operator/internal/java"
	"github.com/huyz0/opensearch-serverless-operator/internal/workload"
	"github.com/huyz0/opensearch-serverless-operator/pkg/podlifecycle"
)

func TestWriterPeerWithdrawalRequiresTheWholeUnchangedCohort(t *testing.T) {
	instance := &v1alpha1.OpenSearchServerless{
		ObjectMeta: metav1.ObjectMeta{Name: "catalog", Namespace: "tenant", UID: "owner", ResourceVersion: "1"},
		Status:     v1alpha1.OpenSearchServerlessStatus{ServingFleetIntentByRole: map[string]string{"ingest": "blue", "search": "blue"}},
	}
	owner := *metav1.NewControllerRef(instance, v1alpha1.GroupVersion.WithKind("OpenSearchServerless"))
	makePod := func(name, role, fleet, ip string) *corev1.Pod {
		return &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name: name, Namespace: instance.Namespace, UID: types.UID(name), ResourceVersion: "1", OwnerReferences: []metav1.OwnerReference{owner},
				Labels: map[string]string{workload.InstanceLabel: string(instance.UID), workload.RoleLabel: role, workload.FleetLabel: fleet},
			},
			Spec: corev1.PodSpec{NodeName: "node"},
			Status: corev1.PodStatus{Phase: corev1.PodRunning, PodIP: ip,
				ContainerStatuses: []corev1.ContainerStatus{{Name: "opensearch", ContainerID: "containerd://" + name,
					State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}}}},
			},
		}
	}
	victim := podlifecycle.Identity{Namespace: instance.Namespace, Name: "victim", UID: "victim", OwnerUID: string(instance.UID)}
	target := java.WriterProcess{NodeID: "victim-node", EphemeralID: "victim-epoch"}
	scheme := runtime.NewScheme()
	if err := v1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	for _, scenario := range []string{"converged", "stale preview", "restarted peer", "duplicate node", "expired sample"} {
		t.Run(scenario, func(t *testing.T) {
			ctx := context.Background()
			preview := makePod("preview", "search", "green", "10.0.0.4")
			kube := kubefake.NewSimpleClientset(
				makePod("victim", "ingest", "blue", "10.0.0.2"),
				makePod("survivor", "ingest", "blue", "10.0.0.3"),
				preview,
			)
			r := &InstanceReconciler{APIReader: fake.NewClientBuilder().WithScheme(scheme).WithObjects(instance).Build(), Core: kube.CoreV1()}
			earliest := time.Now().Add(5 * time.Second)
			evidence, err := r.writerRuntime().observeWriterPeerCohort(ctx, instance, victim, target, func(ctx context.Context, uid types.UID, peer runtimeCohortPod) (writerPeerObservation, error) {
				result := writerPeerObservation{process: java.WriterProcess{NodeID: peer.Name + "-node", EphemeralID: "epoch"}, validUntil: earliest.Add(time.Minute)}
				if peer.Name == victim.Name {
					t.Error("the retiring writer is not its own observer")
				}
				if uid == preview.UID {
					result.validUntil = earliest
					switch scenario {
					case "stale preview":
						return writerPeerObservation{}, fmt.Errorf("withdrawal marker not observed")
					case "restarted peer":
						newPod := preview.DeepCopy()
						newPod.ResourceVersion = "2"
						newPod.Status.ContainerStatuses[0].RestartCount++
						if _, err := r.Core.Pods(instance.Namespace).UpdateStatus(ctx, newPod, metav1.UpdateOptions{}); err != nil {
							return writerPeerObservation{}, err
						}
					case "duplicate node":
						result.process.NodeID = "survivor-node"
					case "expired sample":
						result.validUntil = time.Now().Add(-time.Second)
					}
				}
				return result, nil
			})
			if scenario == "converged" {
				if err != nil || !evidence.validUntil.Equal(earliest) || len(evidence.pods) != 3 {
					t.Fatalf("cohort valid until %v, %v, want %v", evidence.validUntil, err, earliest)
				}
			} else if err == nil || !evidence.validUntil.IsZero() || len(evidence.pods) != 0 {
				t.Fatalf("cohort incorrectly qualified until %v, %v", evidence.validUntil, err)
			}
		})
	}
}
