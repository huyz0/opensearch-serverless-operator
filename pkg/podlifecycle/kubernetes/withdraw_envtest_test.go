//go:build envtest

package kubernetes_test

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"sigs.k8s.io/controller-runtime/pkg/envtest"

	"github.com/huyz0/opensearch-serverless-operator/pkg/podlifecycle"
	podkube "github.com/huyz0/opensearch-serverless-operator/pkg/podlifecycle/kubernetes"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestWithdrawUnboundBindingRaceOnAPIServer(t *testing.T) {
	environment := &envtest.Environment{}
	config, err := environment.Start()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := environment.Stop(); err != nil {
			t.Error(err)
		}
	})
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	client, err := kubernetes.NewForConfig(config)
	if err != nil {
		t.Fatal(err)
	}
	controller := true
	pod, err := client.CoreV1().Pods("default").Create(ctx, &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "binding-race", OwnerReferences: []metav1.OwnerReference{{APIVersion: "example.org/v1", Kind: "Workload", Name: "owner", UID: "owner-uid", Controller: &controller}}},
		Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: "app", Image: "example.invalid/app"}}},
	}, metav1.CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	var once sync.Once
	var bindingErr error
	config.Wrap(func(next http.RoundTripper) http.RoundTripper {
		return roundTripFunc(func(req *http.Request) (*http.Response, error) {
			if req.Method == http.MethodDelete && strings.HasSuffix(req.URL.Path, "/pods/binding-race") {
				once.Do(func() {
					bindingErr = client.CoreV1().Pods("default").Bind(ctx, &corev1.Binding{ObjectMeta: metav1.ObjectMeta{Name: pod.Name, UID: pod.UID}, Target: corev1.ObjectReference{Kind: "Node", Name: "node-a"}}, metav1.CreateOptions{})
				})
				if bindingErr != nil {
					return nil, fmt.Errorf("inject binding: %w", bindingErr)
				}
			}
			return next.RoundTrip(req)
		})
	})
	racing, err := kubernetes.NewForConfig(config)
	if err != nil {
		t.Fatal(err)
	}
	err = podkube.WithdrawUnboundAtVersion(ctx, racing.CoreV1(), podlifecycle.Identity{Namespace: "default", Name: pod.Name, UID: string(pod.UID), OwnerUID: "owner-uid"}, pod.ResourceVersion)
	if bindingErr != nil {
		t.Fatal(bindingErr)
	}
	if !apierrors.IsConflict(err) {
		t.Fatalf("expected version conflict after concurrent binding, got %v", err)
	}
	observed, err := client.CoreV1().Pods("default").Get(ctx, pod.Name, metav1.GetOptions{})
	if err != nil || observed.UID != pod.UID || observed.Spec.NodeName != "node-a" || observed.DeletionTimestamp != nil {
		t.Fatalf("bound Pod was not preserved: %#v, %v", observed, err)
	}
}

func TestWithdrawGatedReleaseRaceOnAPIServer(t *testing.T) {
	environment := &envtest.Environment{}
	config, err := environment.Start()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := environment.Stop(); err != nil {
			t.Error(err)
		}
	})
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	client, err := kubernetes.NewForConfig(config)
	if err != nil {
		t.Fatal(err)
	}
	controller := true
	pod, err := client.CoreV1().Pods("default").Create(ctx, &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "gate-race", OwnerReferences: []metav1.OwnerReference{{APIVersion: "example.org/v1", Kind: "Workload", Name: "owner", UID: "owner-uid", Controller: &controller}}}, Spec: corev1.PodSpec{SchedulingGates: []corev1.PodSchedulingGate{{Name: "example.org/gate"}}, Containers: []corev1.Container{{Name: "app", Image: "example.invalid/app"}}}}, metav1.CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	var once sync.Once
	var releaseErr error
	config.Wrap(func(next http.RoundTripper) http.RoundTripper {
		return roundTripFunc(func(req *http.Request) (*http.Response, error) {
			if req.Method == http.MethodDelete && strings.HasSuffix(req.URL.Path, "/pods/gate-race") {
				once.Do(func() {
					var current *corev1.Pod
					current, releaseErr = client.CoreV1().Pods("default").Get(ctx, pod.Name, metav1.GetOptions{})
					if releaseErr == nil {
						current.Spec.SchedulingGates = nil
						_, releaseErr = client.CoreV1().Pods("default").Update(ctx, current, metav1.UpdateOptions{})
					}
				})
				if releaseErr != nil {
					return nil, fmt.Errorf("inject gate release: %w", releaseErr)
				}
			}
			return next.RoundTrip(req)
		})
	})
	racing, err := kubernetes.NewForConfig(config)
	if err != nil {
		t.Fatal(err)
	}
	err = podkube.WithdrawGatedAtVersion(ctx, racing.CoreV1(), podlifecycle.Identity{Namespace: "default", Name: pod.Name, UID: string(pod.UID), OwnerUID: "owner-uid"}, "example.org/gate", pod.ResourceVersion)
	if releaseErr != nil {
		t.Fatal(releaseErr)
	}
	if !apierrors.IsConflict(err) {
		t.Fatalf("expected conflict after concurrent release, got %v", err)
	}
	observed, err := client.CoreV1().Pods("default").Get(ctx, pod.Name, metav1.GetOptions{})
	if err != nil || observed.UID != pod.UID || len(observed.Spec.SchedulingGates) != 0 || observed.DeletionTimestamp != nil {
		t.Fatalf("released Pod was not preserved: %#v, %v", observed, err)
	}
}
