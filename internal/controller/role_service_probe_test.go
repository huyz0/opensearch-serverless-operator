package controller

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	kubernetesfake "k8s.io/client-go/kubernetes/fake"
	clienttesting "k8s.io/client-go/testing"

	"github.com/huyz0/opensearch-serverless-operator/api/v1alpha1"
	"github.com/huyz0/opensearch-serverless-operator/internal/workload"
)

type serviceProbeTransport func(*http.Request) (*http.Response, error)

func (f serviceProbeTransport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestDevelopmentRoleServiceProbeBindsReplyToFleetAndService(t *testing.T) {
	ctx := context.Background()
	image := "registry/runtime@sha256:" + strings.Repeat("a", 64)
	instance := &v1alpha1.OpenSearchServerless{ObjectMeta: metav1.ObjectMeta{Name: "catalog", Namespace: "n", UID: "owner"}, Spec: v1alpha1.OpenSearchServerlessSpec{Image: image}}
	service, err := workload.RoleService(instance, "search", "green", 9200)
	if err != nil {
		t.Fatal(err)
	}
	service.UID = "service"
	service.ResourceVersion = "10"
	service.Spec.Type = corev1.ServiceTypeClusterIP
	service.Spec.ClusterIP = "10.0.0.1"
	controller := true
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "pod", Namespace: "n", UID: "pod-uid", ResourceVersion: "20", OwnerReferences: []metav1.OwnerReference{{APIVersion: v1alpha1.GroupVersion.String(), Kind: "OpenSearchServerless", Name: "catalog", UID: "owner", Controller: &controller}}, Labels: map[string]string{workload.InstanceLabel: "owner", workload.RoleLabel: "search", workload.FleetLabel: "green", workload.ZoneLabel: "a"}}, Spec: corev1.PodSpec{NodeName: "node"}, Status: corev1.PodStatus{Phase: corev1.PodRunning, PodIP: "10.1.0.2", Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}}}
	core := kubernetesfake.NewSimpleClientset(service, pod)
	body := `{"node":"java-id","name":"pod","roles":["search","ingest"],"lease":{"valid_now":true,"expires_in_millis":60000,"ttl_millis":60000}}`
	changed := false
	redirect := false
	requests := 0
	client := &http.Client{Transport: serviceProbeTransport(func(req *http.Request) (*http.Response, error) {
		requests++
		if redirect {
			return &http.Response{StatusCode: http.StatusFound, Header: http.Header{"Location": []string{"http://foreign.invalid/"}}, Body: io.NopCloser(strings.NewReader("")), Request: req}, nil
		}
		if req.URL.String() != "http://catalog-search.n.svc:9200/_serverless/stats" {
			return nil, fmt.Errorf("unexpected Service endpoint")
		}
		if changed {
			observed, err := core.CoreV1().Services("n").Get(ctx, service.Name, metav1.GetOptions{})
			if err != nil {
				return nil, err
			}
			observed.ResourceVersion = "11"
			if _, err = core.CoreV1().Services("n").Update(ctx, observed, metav1.UpdateOptions{}); err != nil {
				return nil, err
			}
		}
		return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader(body)), Header: make(http.Header), Request: req}, nil
	})}
	probe := func(allowed map[types.UID]struct{}) (roleServiceProbe, error) {
		return probeDevelopmentRoleService(ctx, core.CoreV1(), instance, image, "search", "green", "service", allowed, client, 5*time.Second)
	}
	out, err := probe(map[types.UID]struct{}{"pod-uid": {}})
	if err != nil || out.PodUID != "pod-uid" || out.NodeID != "java-id" || out.ServiceVersion != "10" {
		t.Fatalf("valid Service response rejected: %#v %v", out, err)
	}
	disrupted := pod.DeepCopy()
	disrupted.Status.Conditions = append(disrupted.Status.Conditions, corev1.PodCondition{Type: corev1.DisruptionTarget, Status: corev1.ConditionTrue})
	if _, err := core.CoreV1().Pods("n").UpdateStatus(ctx, disrupted, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := probe(map[types.UID]struct{}{"pod-uid": {}}); err == nil {
		t.Fatal("a valid reply from a Ready Pod marked for disruption was accepted")
	}
	if _, err := core.CoreV1().Pods("n").UpdateStatus(ctx, pod, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := probe(map[types.UID]struct{}{"other": {}}); err == nil {
		t.Fatal("unqualified responding Pod accepted")
	}
	body = `{"node":"java-id","name":"pod","roles":["search","ingest"],"lease":{"valid_now":true,"expires_in_millis":1000,"ttl_millis":60000}}`
	if _, err := probe(map[types.UID]struct{}{"pod-uid": {}}); err == nil {
		t.Fatal("expiring lease accepted")
	}
	body = `{"node":"java-id","name":"pod","roles":["search","ingest"],"lease":{"valid_now":true,"expires_in_millis":60000,"ttl_millis":60000}}`
	redirect = true
	priorRequests := requests
	if _, err := probe(map[types.UID]struct{}{"pod-uid": {}}); err == nil || requests != priorRequests+1 {
		t.Fatal("redirect escaped the intended Service")
	}
	redirect = false
	changePod := true
	podReads := 0
	core.PrependReactor("get", "pods", func(action clienttesting.Action) (bool, runtime.Object, error) {
		if !changePod {
			return false, nil, nil
		}
		podReads++
		if podReads == 2 {
			changedPod := pod.DeepCopy()
			changedPod.ResourceVersion = "21"
			return true, changedPod, nil
		}
		return false, nil, nil
	})
	if _, err := probe(map[types.UID]struct{}{"pod-uid": {}}); err == nil {
		t.Fatal("Pod changed during probe")
	}
	changePod = false
	changed = true
	if _, err := probe(map[types.UID]struct{}{"pod-uid": {}}); err == nil {
		t.Fatal("Service changed during probe")
	}
	instance.Spec.Security = &v1alpha1.SecurityConfig{Provider: "security"}
	if _, err := probe(map[types.UID]struct{}{"pod-uid": {}}); err == nil {
		t.Fatal("unqualified security provider accepted")
	}
}
