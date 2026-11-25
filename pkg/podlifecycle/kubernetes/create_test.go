package kubernetes_test

import (
	"context"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	kubetesting "k8s.io/client-go/testing"

	"github.com/huyz0/opensearch-serverless-operator/pkg/podlifecycle/kubernetes"
)

func TestCreatePodRecoversOnlyMatchingIntent(t *testing.T) {
	controller := true
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
		Namespace: "n", Name: "p", UID: "pod-uid", Annotations: map[string]string{"intent": "revision-1"},
		OwnerReferences: []metav1.OwnerReference{{APIVersion: "workload.example/v1", Kind: "Workload", Name: "owner", UID: "owner-uid", Controller: &controller}},
	}}
	client := fake.NewSimpleClientset(pod)
	desired := pod.DeepCopy()
	desired.UID = ""
	created, err := kubernetes.CreatePod(context.Background(), client.CoreV1(), desired, "intent", "revision-1")
	if err != nil || created.UID != pod.UID {
		t.Fatalf("expected existing Pod after retry: pod=%v err=%v", created, err)
	}
	desired.Annotations["intent"] = "revision-2"
	if _, err := kubernetes.CreatePod(context.Background(), client.CoreV1(), desired, "intent", "revision-2"); err == nil {
		t.Fatal("conflicting intent must not adopt existing Pod")
	}
	desired.Annotations["intent"] = "revision-1"
	desired.OwnerReferences[0].Name = "new-owner-name"
	if _, err := kubernetes.CreatePod(context.Background(), client.CoreV1(), desired, "intent", "revision-1"); err == nil {
		t.Fatal("a matching owner UID did not prove the rest of the controller reference")
	}
}

func TestCreatePodVerifiesSuccessfulAdmission(t *testing.T) {
	controller := true
	desired := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
		Name: "p", Namespace: "n", Annotations: map[string]string{"intent": "revision-1"},
		OwnerReferences: []metav1.OwnerReference{{APIVersion: "workload.example/v1", Kind: "Workload", Name: "owner", UID: "owner-uid", Controller: &controller}},
	}}
	for _, tc := range []struct {
		name   string
		mutate func(*corev1.Pod)
	}{
		{name: "matching response"},
		{name: "changed owner", mutate: func(pod *corev1.Pod) { pod.OwnerReferences[0].Name = "other" }},
		{name: "changed intent", mutate: func(pod *corev1.Pod) { pod.Annotations["intent"] = "other" }},
		{name: "absent UID", mutate: func(pod *corev1.Pod) { pod.UID = "" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client := fake.NewSimpleClientset()
			client.PrependReactor("create", "pods", func(action kubetesting.Action) (bool, runtime.Object, error) {
				response := action.(kubetesting.CreateAction).GetObject().(*corev1.Pod).DeepCopy()
				response.UID = "created"
				if tc.mutate != nil {
					tc.mutate(response)
				}
				return true, response, nil
			})
			pod, err := kubernetes.CreatePod(context.Background(), client.CoreV1(), desired, "intent", "revision-1")
			if tc.mutate == nil {
				if err != nil || pod == nil || pod.UID != "created" {
					t.Fatalf("matching response rejected: %v", err)
				}
			} else if err == nil || pod != nil {
				t.Fatal("changed create response was accepted")
			}
		})
	}
}

func TestCreatePodDoesNotRecoverTerminatingPod(t *testing.T) {
	controller := true
	now := metav1.NewTime(time.Now())
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
		Namespace: "n", Name: "p", UID: "pod-uid", DeletionTimestamp: &now,
		Annotations:     map[string]string{"intent": "revision-1"},
		OwnerReferences: []metav1.OwnerReference{{UID: "owner-uid", Controller: &controller}},
	}}
	desired := pod.DeepCopy()
	desired.UID = ""
	desired.DeletionTimestamp = nil
	client := fake.NewSimpleClientset(pod)
	if _, err := kubernetes.CreatePod(context.Background(), client.CoreV1(), desired, "intent", "revision-1"); err == nil {
		t.Fatal("a terminating Pod must not satisfy a creation intent")
	}
}
