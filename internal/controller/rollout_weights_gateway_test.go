package controller

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
)

func TestWeightedEvidenceRequiresCurrentGatewayPods(t *testing.T) {
	const namespace = "gateway"
	ready := true
	service := &corev1.Service{ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: "edge", UID: "service", ResourceVersion: "1"}}
	proxy := func(name string) *corev1.Pod {
		return &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: name, UID: types.UID(name), ResourceVersion: "1"},
			Spec:       corev1.PodSpec{NodeName: "node"},
			Status:     corev1.PodStatus{Phase: corev1.PodRunning, Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}},
		}
	}
	endpoint := func(pod *corev1.Pod) discoveryv1.Endpoint {
		return discoveryv1.Endpoint{
			TargetRef:  &corev1.ObjectReference{Kind: "Pod", Namespace: namespace, Name: pod.Name, UID: pod.UID},
			Conditions: discoveryv1.EndpointConditions{Ready: &ready},
		}
	}
	a, b := proxy("proxy-a"), proxy("proxy-b")
	slice := &discoveryv1.EndpointSlice{
		ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: "edge-1", UID: "slice", ResourceVersion: "1",
			OwnerReferences: []metav1.OwnerReference{{APIVersion: "v1", Kind: "Service", Name: service.Name, UID: service.UID, Controller: &ready}},
			Labels:          map[string]string{discoveryv1.LabelServiceName: service.Name, discoveryv1.LabelManagedBy: "endpointslice-controller.k8s.io"}},
		Endpoints: []discoveryv1.Endpoint{endpoint(a), endpoint(b)},
	}
	kube := fake.NewSimpleClientset(service, slice, a, b)
	ctx := context.Background()
	gateway := weightedGatewayFrontend{namespace: namespace, name: service.Name, uid: service.UID}
	reportUnchanged := func(context.Context) bool { return true }
	bind := func(expected ...string) (func(context.Context) bool, error) {
		return gateway.bindEvidence(ctx, kube.CoreV1(), kube.DiscoveryV1(), expected, reportUnchanged)
	}
	check, err := bind(string(a.UID), string(b.UID))
	if err != nil || !check(ctx) {
		t.Fatalf("current cohort did not qualify: %v", err)
	}
	if err := kube.CoreV1().Pods(namespace).Delete(ctx, a.Name, metav1.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	if check(ctx) {
		t.Fatal("the original proof survived a Pod deletion while EndpointSlice still advertised it")
	}
	if _, err := bind(string(a.UID), string(b.UID)); err == nil {
		t.Fatal("the stale report qualified after the proxy disappeared")
	}
	replacement := proxy("proxy-c")
	if _, err := kube.CoreV1().Pods(namespace).Create(ctx, replacement, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	slice.Endpoints = []discoveryv1.Endpoint{endpoint(replacement), endpoint(b)}
	slice.ResourceVersion = "2"
	if _, err := kube.DiscoveryV1().EndpointSlices(namespace).Update(ctx, slice, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := bind(string(a.UID), string(b.UID)); err == nil {
		t.Fatal("a replacement with the same replica count qualified the old proof")
	}
	if check, err := bind(string(b.UID), string(replacement.UID)); err != nil || !check(ctx) {
		t.Fatalf("fresh proof did not qualify the replacement: %v", err)
	}
}
