//go:build envtest

package kubernetes_test

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
	"sigs.k8s.io/controller-runtime/pkg/envtest"

	"github.com/huyz0/opensearch-serverless-operator/pkg/podlifecycle"
	podkube "github.com/huyz0/opensearch-serverless-operator/pkg/podlifecycle/kubernetes"
)

func TestEvictionPDBAndLostResponseOnAPIServer(t *testing.T) {
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
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	defer cancel()
	client, err := kubernetes.NewForConfig(config)
	if err != nil {
		t.Fatal(err)
	}
	controlled := true
	template := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "eviction-target", Labels: map[string]string{"app": "eviction-test"}, Finalizers: []string{"example.org/hold"}, OwnerReferences: []metav1.OwnerReference{{APIVersion: "example.org/v1", Kind: "Workload", Name: "owner", UID: "owner-uid", Controller: &controlled}}}, Spec: corev1.PodSpec{NodeName: "node-a", Containers: []corev1.Container{{Name: "app", Image: "example.invalid/app"}}}}
	pod, err := client.CoreV1().Pods("default").Create(ctx, template, metav1.CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	pod.Status.Phase = corev1.PodRunning
	pod.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}
	pod, err = client.CoreV1().Pods("default").UpdateStatus(ctx, pod, metav1.UpdateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	min := intstr.FromInt(1)
	pdb, err := client.PolicyV1().PodDisruptionBudgets("default").Create(ctx, &policyv1.PodDisruptionBudget{ObjectMeta: metav1.ObjectMeta{Name: "eviction-test"}, Spec: policyv1.PodDisruptionBudgetSpec{MinAvailable: &min, Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "eviction-test"}}}}, metav1.CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	// Envtest runs no PDB controller: provide the observed budget explicitly.
	pdb.Status = policyv1.PodDisruptionBudgetStatus{ObservedGeneration: pdb.Generation, CurrentHealthy: 1, DesiredHealthy: 1, ExpectedPods: 1, DisruptionsAllowed: 0}
	pdb, err = client.PolicyV1().PodDisruptionBudgets("default").UpdateStatus(ctx, pdb, metav1.UpdateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	identity := podlifecycle.Identity{Namespace: "default", Name: pod.Name, UID: string(pod.UID), OwnerUID: "owner-uid"}
	if err := podkube.Evict(ctx, client.CoreV1(), identity); !apierrors.IsTooManyRequests(err) {
		t.Fatalf("expected PDB rejection, got %v", err)
	}
	observed, err := client.CoreV1().Pods("default").Get(ctx, pod.Name, metav1.GetOptions{})
	if err != nil || observed.DeletionTimestamp != nil || observed.Status.Conditions[0].Status != corev1.ConditionTrue {
		t.Fatalf("denied Pod did not remain ready: %#v %v", observed, err)
	}

	pdb.Status.DisruptionsAllowed = 1
	pdb.Status.CurrentHealthy = 2
	pdb.Status.ExpectedPods = 2
	if _, err := client.PolicyV1().PodDisruptionBudgets("default").UpdateStatus(ctx, pdb, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	lostResponse := false
	config.Wrap(func(next http.RoundTripper) http.RoundTripper {
		return roundTripFunc(func(req *http.Request) (*http.Response, error) {
			resp, err := next.RoundTrip(req)
			if err == nil && req.Method == http.MethodPost && strings.HasSuffix(req.URL.Path, "/pods/eviction-target/eviction") && resp.StatusCode >= 200 && resp.StatusCode < 300 {
				lostResponse = true
				resp.Body.Close()
				return nil, io.ErrUnexpectedEOF
			}
			return resp, err
		})
	})
	intercepted, err := kubernetes.NewForConfig(config)
	if err != nil {
		t.Fatal(err)
	}
	if err := podkube.Evict(ctx, intercepted.CoreV1(), identity); err == nil || !lostResponse {
		t.Fatalf("expected a lost successful eviction response, got %v", err)
	}
	observed, err = client.CoreV1().Pods("default").Get(ctx, pod.Name, metav1.GetOptions{})
	if err != nil || observed.UID != pod.UID || observed.DeletionTimestamp == nil {
		t.Fatalf("eviction was not observed after lost response: %#v %v", observed, err)
	}
	if err := podkube.Evict(ctx, client.CoreV1(), identity); err == nil {
		t.Fatal("terminating victim was evicted again")
	}
	observed.Finalizers = nil
	if _, err := client.CoreV1().Pods("default").Update(ctx, observed, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	// Envtest has no kubelet to complete the termination grace period.
	zero := int64(0)
	if err := client.CoreV1().Pods("default").Delete(ctx, pod.Name, metav1.DeleteOptions{GracePeriodSeconds: &zero}); err != nil && !apierrors.IsNotFound(err) {
		t.Fatal(err)
	}
	if err := wait.PollUntilContextTimeout(ctx, 50*time.Millisecond, 5*time.Second, true, func(ctx context.Context) (bool, error) {
		_, err := client.CoreV1().Pods("default").Get(ctx, pod.Name, metav1.GetOptions{})
		if apierrors.IsNotFound(err) {
			return true, nil
		}
		return false, err
	}); err != nil {
		t.Fatal(err)
	}
	replacement, err := client.CoreV1().Pods("default").Create(ctx, template, metav1.CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if replacement.UID == pod.UID {
		t.Fatal("replacement did not get a new UID")
	}
	if err := podkube.Evict(ctx, client.CoreV1(), identity); err == nil {
		t.Fatal("stale identity accepted for replacement")
	}
	replacement, err = client.CoreV1().Pods("default").Get(ctx, pod.Name, metav1.GetOptions{})
	if err != nil || replacement.DeletionTimestamp != nil {
		t.Fatalf("replacement was modified: %#v %v", replacement, err)
	}
	// Model a replacement created after the controller's final GET. An unready
	// Pod can take the IfHealthyBudget shortcut, which refreshes the requested
	// resource version on some servers. The original UID must still win.
	replacement.Status.Phase = corev1.PodRunning
	replacement.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionFalse}}
	if _, err := client.CoreV1().Pods("default").UpdateStatus(ctx, replacement, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	err = client.CoreV1().Pods("default").EvictV1(ctx, &policyv1.Eviction{
		ObjectMeta: metav1.ObjectMeta{Name: replacement.Name, Namespace: replacement.Namespace},
		DeleteOptions: &metav1.DeleteOptions{Preconditions: &metav1.Preconditions{
			UID: &pod.UID, ResourceVersion: &pod.ResourceVersion,
		}},
	})
	if !apierrors.IsConflict(err) {
		t.Fatalf("expected an exact UID conflict for the unready replacement, got %v", err)
	}
	replacement, err = client.CoreV1().Pods("default").Get(ctx, pod.Name, metav1.GetOptions{})
	if err != nil || replacement.DeletionTimestamp != nil {
		t.Fatalf("unready replacement was modified: %#v %v", replacement, err)
	}
}
