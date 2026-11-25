package controller

import (
	"context"
	"errors"
	"testing"

	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	policyv1 "k8s.io/api/policy/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	clienttest "k8s.io/client-go/testing"

	"github.com/huyz0/opensearch-serverless-operator/api/v1alpha1"
	"github.com/huyz0/opensearch-serverless-operator/internal/workload"
	"github.com/huyz0/opensearch-serverless-operator/pkg/podlifecycle"
)

func TestWriterEvictionRechecksRoutingAfterEdgeEvidence(t *testing.T) {
	ctx := context.Background()
	instance := &v1alpha1.OpenSearchServerless{ObjectMeta: metav1.ObjectMeta{Name: "catalog", Namespace: "app", UID: "owner"}}
	instance.Status.ServingFleetIntentByRole = map[string]string{"ingest": "blue"}
	namespace := instance.Namespace
	makePod := func(name, address string) *corev1.Pod {
		return &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace, UID: types.UID(name), ResourceVersion: "1",
				OwnerReferences: []metav1.OwnerReference{*metav1.NewControllerRef(instance, v1alpha1.GroupVersion.WithKind("OpenSearchServerless"))},
				Labels:          map[string]string{workload.InstanceLabel: string(instance.UID), workload.RoleLabel: "ingest", workload.FleetLabel: "blue"}},
			Spec: corev1.PodSpec{NodeName: "node"},
			Status: corev1.PodStatus{Phase: corev1.PodRunning, PodIP: address, ContainerStatuses: []corev1.ContainerStatus{{
				Name: "opensearch", ContainerID: name + "-container", State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}},
			}}},
		}
	}
	for _, change := range []string{"none", "survivor", "slice", "service", "Envoy report", "claim"} {
		t.Run(change, func(t *testing.T) {
			victim, survivor := makePod("victim", "10.1.0.1"), makePod("survivor", "10.1.0.2")
			service := &corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: "index", Namespace: namespace, UID: "service", ResourceVersion: "1"}}
			slice := &discoveryv1.EndpointSlice{ObjectMeta: metav1.ObjectMeta{Name: "slice", Namespace: namespace, UID: "slice", ResourceVersion: "1",
				Labels: map[string]string{discoveryv1.LabelServiceName: service.Name}}}
			kube := fake.NewSimpleClientset(victim, survivor, service, slice)
			r := (&InstanceReconciler{Core: kube.CoreV1(), Discovery: kube.DiscoveryV1()}).writerRuntime()
			cohort, err := listRuntimeCohort(ctx, r.core.Pods(namespace), instance)
			if err != nil {
				t.Fatal(err)
			}
			observed := writerEvictionObservation{
				peers: writerPeerCohortEvidence{pods: cohort},
				endpoints: roleEndpointObservation{ServiceName: service.Name, ServiceUID: service.UID, ServiceVersion: service.ResourceVersion,
					SliceVersions: map[string]endpointSliceVersion{slice.Name: {uid: slice.UID, version: slice.ResourceVersion}}},
				edge: writerEnvoyObservation{unchanged: func(ctx context.Context) error {
					switch change {
					case "survivor":
						survivor.ResourceVersion = "2"
						_, err := r.core.Pods(namespace).Update(ctx, survivor, metav1.UpdateOptions{})
						return err
					case "slice":
						slice.ResourceVersion = "2"
						_, err := r.discovery.EndpointSlices(namespace).Update(ctx, slice, metav1.UpdateOptions{})
						return err
					case "service":
						service.ResourceVersion = "2"
						_, err := r.core.Services(namespace).Update(ctx, service, metav1.UpdateOptions{})
						return err
					case "Envoy report":
						return errors.New("Envoy evidence changed")
					default:
						return nil
					}
				}},
			}
			identity := podlifecycle.Identity{Name: victim.Name, Namespace: namespace, UID: string(victim.UID), OwnerUID: string(instance.UID)}
			err = r.evictObservedWriter(ctx, instance, identity, victim.ResourceVersion, observed, func(context.Context) error {
				if change == "claim" {
					return errors.New("operation claim changed")
				}
				return nil
			})
			if (err == nil) != (change == "none") {
				t.Fatalf("unexpected eviction result: %v", err)
			}
			sent := false
			for _, action := range kube.Actions() {
				if action.GetSubresource() == "eviction" {
					sent = true
					eviction := action.(clienttest.CreateAction).GetObject().(*policyv1.Eviction)
					preconditions := eviction.DeleteOptions.Preconditions
					if preconditions == nil || *preconditions.UID != victim.UID || *preconditions.ResourceVersion != victim.ResourceVersion {
						t.Fatal("writer eviction did not retain the exact Pod preconditions")
					}
				}
			}
			if sent != (change == "none") {
				t.Fatalf("unexpected eviction dispatch: %t", sent)
			}
		})
	}
}

func TestWriterEvictionReadbackOnlyConfirmsExactPodAbsence(t *testing.T) {
	instance := &v1alpha1.OpenSearchServerless{ObjectMeta: metav1.ObjectMeta{Name: "catalog", Namespace: "app"}}
	op := &v1alpha1.OpenSearchServerlessOperation{Spec: v1alpha1.OperationSpec{
		WriterRetirement: &v1alpha1.WriterRetirementTarget{PodName: "writer", PodUID: "retired"},
	}}
	for _, tc := range []struct {
		name        string
		uid         types.UID
		terminating bool
		wantGone    bool
	}{
		{name: "same UID", uid: "retired"},
		{name: "terminating UID", uid: "retired", terminating: true},
		{name: "same name different UID", uid: "replacement", wantGone: true},
		{name: "not found", wantGone: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var objects []runtime.Object
			if tc.uid != "" {
				pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "writer", Namespace: "app", UID: tc.uid}}
				if tc.terminating {
					now := metav1.Now()
					pod.DeletionTimestamp = &now
				}
				objects = append(objects, pod)
			}
			kube := fake.NewSimpleClientset(objects...)
			backend := developmentWriterRetirement{instanceName: "catalog", runtime: (&InstanceReconciler{Core: kube.CoreV1()}).writerRuntime()}
			gone, err := backend.writerVictimAbsent(context.Background(), instance, op)
			if gone != tc.wantGone || err != nil {
				t.Fatalf("gone = %v, error = %v", gone, err)
			}
			actions := kube.Actions()
			if len(actions) != 1 || actions[0].GetVerb() != "get" {
				t.Fatalf("expected a single read-only action, got %v", actions)
			}
		})
	}
	kube := fake.NewSimpleClientset()
	kube.PrependReactor("get", "pods", func(clienttest.Action) (bool, runtime.Object, error) {
		return true, nil, apierrors.NewServiceUnavailable("Pod read temporarily unavailable")
	})
	backend := developmentWriterRetirement{instanceName: "catalog", runtime: (&InstanceReconciler{Core: kube.CoreV1()}).writerRuntime()}
	gone, err := backend.writerVictimAbsent(context.Background(), instance, op)
	if gone || !apierrors.IsServiceUnavailable(err) {
		t.Fatalf("an unavailable API must not prove absence: gone = %v, error = %v", gone, err)
	}
}
