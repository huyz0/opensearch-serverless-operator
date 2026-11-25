package controller

import (
	"context"
	"fmt"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	clientfake "sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/huyz0/opensearch-serverless-operator/api/v1alpha1"
	"github.com/huyz0/opensearch-serverless-operator/internal/workload"
)

func TestRetiredCreationCannotDeleteChangedOrActivePod(t *testing.T) {
	instance := &v1alpha1.OpenSearchServerless{ObjectMeta: metav1.ObjectMeta{Name: "catalog", Namespace: "n", UID: "owner"}, Status: v1alpha1.OpenSearchServerlessStatus{PodCreationSequence: 1}}
	item := v1alpha1.RetiredPodCreation{Name: podNameForSequence(instance, 1), Sequence: 1, Role: "search", Zone: "a", Fleet: "initial", Revision: "old", PodUID: "pod-uid"}
	instance.Status.RetiredPodCreations = []v1alpha1.RetiredPodCreation{item}
	controller := true
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: item.Name, Namespace: "n", UID: "pod-uid", ResourceVersion: "2", OwnerReferences: []metav1.OwnerReference{{APIVersion: v1alpha1.GroupVersion.String(), Kind: "OpenSearchServerless", Name: instance.Name, UID: instance.UID, Controller: &controller}}, Annotations: map[string]string{creationIntentKey: fmt.Sprintf("%s/%d", instance.UID, item.Sequence)}, Labels: map[string]string{workload.InstanceLabel: string(instance.UID), workload.RoleLabel: item.Role, workload.ZoneLabel: item.Zone, workload.FleetLabel: item.Fleet, "serverless.opensearch.org/revision": item.Revision}}, Spec: corev1.PodSpec{SchedulingGates: []corev1.PodSchedulingGate{{Name: "serverless.opensearch.org/placement"}}}}
	for _, test := range []struct {
		name           string
		changePod      func(*corev1.Pod)
		changeInstance func(*v1alpha1.OpenSearchServerless)
	}{
		{"different UID", func(p *corev1.Pod) { p.UID = "other" }, func(*v1alpha1.OpenSearchServerless) {}},
		{"released gate", func(p *corev1.Pod) { p.Spec.SchedulingGates = nil }, func(*v1alpha1.OpenSearchServerless) {}},
		{"bound", func(p *corev1.Pod) { p.Spec.NodeName = "node" }, func(*v1alpha1.OpenSearchServerless) {}},
		{"unlabelled unpinned Pod", func(p *corev1.Pod) { delete(p.Labels, workload.InstanceLabel) }, func(i *v1alpha1.OpenSearchServerless) { i.Status.RetiredPodCreations[0].PodUID = "" }},
		{"foreign unpinned Pod", func(p *corev1.Pod) { p.OwnerReferences[0].UID = "other" }, func(i *v1alpha1.OpenSearchServerless) { i.Status.RetiredPodCreations[0].PodUID = "" }},
		{"active creation", func(*corev1.Pod) {}, func(i *v1alpha1.OpenSearchServerless) {
			i.Status.PendingPodCreation = &v1alpha1.PodCreationIntent{Name: item.Name}
		}},
		{"active attempt", func(*corev1.Pod) {}, func(i *v1alpha1.OpenSearchServerless) {
			i.Status.ProvisioningAttempt = &v1alpha1.ProvisioningAttempt{Name: item.Name}
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			changedPod := pod.DeepCopy()
			changedInstance := instance.DeepCopyObject().(*v1alpha1.OpenSearchServerless)
			test.changePod(changedPod)
			test.changeInstance(changedInstance)
			core := fake.NewSimpleClientset(changedPod)
			r := &InstanceReconciler{Core: core.CoreV1()}
			if _, _, err := r.reconcileRetiredPodCreations(context.Background(), changedInstance); err == nil {
				t.Fatal("unsafe retirement was accepted")
			}
			for _, action := range core.Actions() {
				if action.GetVerb() == "delete" {
					t.Fatal("unsafe retirement deleted a Pod")
				}
			}
		})
	}
}

func TestRetiredPodWatchMapsUnownedNameWithinNamespace(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := v1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	first := &v1alpha1.OpenSearchServerless{ObjectMeta: metav1.ObjectMeta{Name: "first", Namespace: "n"}, Status: v1alpha1.OpenSearchServerlessStatus{RetiredPodCreations: []v1alpha1.RetiredPodCreation{{Name: "late"}}}}
	other := &v1alpha1.OpenSearchServerless{ObjectMeta: metav1.ObjectMeta{Name: "other", Namespace: "other"}, Status: v1alpha1.OpenSearchServerlessStatus{RetiredPodCreations: []v1alpha1.RetiredPodCreation{{Name: "late"}}}}
	kube := clientfake.NewClientBuilder().WithScheme(scheme).WithIndex(&v1alpha1.OpenSearchServerless{}, retiredPodNameIndex, retiredPodNames).WithObjects(first, other).Build()
	r := &InstanceReconciler{Client: kube}
	requests := r.instancesForRetiredPod(context.Background(), &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "late", Namespace: "n"}})
	if len(requests) != 1 || requests[0].Namespace != "n" || requests[0].Name != "first" {
		t.Fatalf("unexpected retired-name watch requests: %v", requests)
	}
	if requests := r.instancesForRetiredPod(context.Background(), &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "unknown", Namespace: "n"}}); len(requests) != 0 {
		t.Fatal("unrelated Pod triggered a retired-name request")
	}
}
