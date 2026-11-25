package controller

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/huyz0/opensearch-serverless-operator/api/v1alpha1"
	"github.com/huyz0/opensearch-serverless-operator/internal/workload"
	"github.com/huyz0/opensearch-serverless-operator/pkg/podlifecycle"
)

func TestWriterTrafficWithdrawalRequiresSurvivorCoverageAndVictimAbsence(t *testing.T) {
	yes, no := true, false
	instance := &v1alpha1.OpenSearchServerless{
		ObjectMeta: metav1.ObjectMeta{Name: "catalog", Namespace: "n", UID: "owner"},
		Status:     v1alpha1.OpenSearchServerlessStatus{ServingFleetIntentByRole: map[string]string{"ingest": "blue"}},
	}
	service, err := workload.RoleService(instance, "ingest", "blue", 9200)
	if err != nil {
		t.Fatal(err)
	}
	service.UID, service.ResourceVersion = "service", "1"
	service.Spec.Type, service.Spec.ClusterIP = corev1.ServiceTypeClusterIP, "10.0.0.1"
	survivor := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "survivor", Namespace: instance.Namespace, UID: "survivor-uid", ResourceVersion: "1",
			OwnerReferences: []metav1.OwnerReference{{UID: instance.UID, Name: instance.Name, Kind: "OpenSearchServerless", APIVersion: v1alpha1.GroupVersion.String(), Controller: &yes}},
			Labels:          map[string]string{workload.InstanceLabel: string(instance.UID), workload.RoleLabel: "ingest", workload.FleetLabel: "blue", workload.ZoneLabel: "a"},
		},
		Spec: corev1.PodSpec{NodeName: "node"},
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning, PodIP: "10.1.0.2",
			Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}},
		},
	}
	victim := survivor.DeepCopy()
	victim.Name, victim.UID, victim.Status.PodIP = "victim", "victim-uid", "10.1.0.3"
	victim.Status.Conditions[0].Status = corev1.ConditionFalse
	identity := podlifecycle.Identity{Namespace: instance.Namespace, Name: victim.Name, UID: string(victim.UID), OwnerUID: string(instance.UID)}
	port, name, protocol := int32(9200), "http", corev1.ProtocolTCP
	slice := &discoveryv1.EndpointSlice{
		ObjectMeta: metav1.ObjectMeta{
			Name: "slice", Namespace: instance.Namespace, UID: "slice-uid", ResourceVersion: "1",
			OwnerReferences: []metav1.OwnerReference{{UID: service.UID, Name: service.Name, Kind: "Service", APIVersion: "v1", Controller: &yes}},
			Labels:          map[string]string{discoveryv1.LabelServiceName: service.Name, discoveryv1.LabelManagedBy: "endpointslice-controller.k8s.io"},
		},
		AddressType: discoveryv1.AddressTypeIPv4,
		Ports:       []discoveryv1.EndpointPort{{Name: &name, Port: &port, Protocol: &protocol}},
		Endpoints: []discoveryv1.Endpoint{
			{Addresses: []string{survivor.Status.PodIP}, Conditions: discoveryv1.EndpointConditions{Ready: &yes, Serving: &yes},
				TargetRef: &corev1.ObjectReference{Kind: "Pod", Namespace: survivor.Namespace, Name: survivor.Name, UID: survivor.UID}},
			{Addresses: []string{victim.Status.PodIP}, Conditions: discoveryv1.EndpointConditions{Ready: &no, Serving: &no},
				TargetRef: &corev1.ObjectReference{Kind: "Pod", Namespace: victim.Namespace, Name: victim.Name, UID: victim.UID}},
		},
	}
	for _, test := range []struct {
		name string
		edit func(*corev1.Pod, *discoveryv1.EndpointSlice)
		pass bool
	}{
		{name: "explicitly not serving", pass: true},
		{name: "endpoint absent", edit: func(_ *corev1.Pod, e *discoveryv1.EndpointSlice) { e.Endpoints = e.Endpoints[:1] }, pass: true},
		{name: "stale Ready endpoint", edit: func(_ *corev1.Pod, e *discoveryv1.EndpointSlice) { e.Endpoints[1].Conditions.Ready = &yes }},
		{name: "still serving by IP", edit: func(_ *corev1.Pod, e *discoveryv1.EndpointSlice) {
			e.Endpoints[1].TargetRef = nil
			e.Endpoints[1].Conditions.Serving = &yes
		}},
		{name: "unknown serving", edit: func(_ *corev1.Pod, e *discoveryv1.EndpointSlice) { e.Endpoints[1].Conditions.Serving = nil }},
		{name: "victim still Ready", edit: func(p *corev1.Pod, _ *discoveryv1.EndpointSlice) {
			p.Status.Conditions[0].Status = corev1.ConditionTrue
		}},
		{name: "victim replaced", edit: func(p *corev1.Pod, _ *discoveryv1.EndpointSlice) { p.UID = "next" }},
		{name: "no surviving capacity", edit: func(_ *corev1.Pod, e *discoveryv1.EndpointSlice) { e.Endpoints = e.Endpoints[1:] }},
	} {
		t.Run(test.name, func(t *testing.T) {
			pod, endpoints := victim.DeepCopy(), slice.DeepCopy()
			if test.edit != nil {
				test.edit(pod, endpoints)
			}
			kube := fake.NewSimpleClientset(service.DeepCopy(), survivor.DeepCopy(), pod, endpoints)
			r := &InstanceReconciler{Core: kube.CoreV1(), Discovery: kube.DiscoveryV1()}
			observed, err := r.writerRuntime().observeWriterTrafficWithdrawal(context.Background(), instance, identity, service.UID,
				map[string]int{"a": 1}, map[types.UID]struct{}{survivor.UID: {}})
			if (err == nil) != test.pass {
				t.Fatalf("withdrawal observation = %+v, %v", observed, err)
			}
		})
	}
}
