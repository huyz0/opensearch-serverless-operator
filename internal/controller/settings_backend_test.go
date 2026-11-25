package controller

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/huyz0/opensearch-serverless-operator/api/v1alpha1"
	"github.com/huyz0/opensearch-serverless-operator/internal/workload"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (fn roundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) { return fn(request) }

func TestSettingsBackendUsesOwnedRoleServiceIdentity(t *testing.T) {
	instance := &v1alpha1.OpenSearchServerless{ObjectMeta: metav1.ObjectMeta{Name: "catalog", Namespace: "n", UID: "owner"}, Status: v1alpha1.OpenSearchServerlessStatus{ServingFleetIntentByRole: map[string]string{"ingest": "blue"}}}
	service, err := workload.RoleService(instance, "ingest", "blue", 9200)
	if err != nil {
		t.Fatal(err)
	}
	service.UID, service.ResourceVersion = "service-uid", "1"
	service.Spec.Type = corev1.ServiceTypeClusterIP
	service.Spec.ClusterIP = "10.0.0.1"
	core := fake.NewSimpleClientset(service).CoreV1()
	requests := 0
	transport := roundTripFunc(func(request *http.Request) (*http.Response, error) {
		requests++
		if request.URL.Host != "catalog-index.n.svc:9200" {
			t.Errorf("unexpected admin endpoint %s", request.URL.Host)
		}
		body := `{"persistent":{"serverless.bucket":"auto"}}`
		if request.Method == http.MethodPut {
			body = `{}`
		}
		return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader(body)), Header: make(http.Header)}, nil
	})
	backend := developmentHTTPSettings{core: core, http: &http.Client{Transport: transport}}
	observed, err := backend.Read(context.Background(), instance)
	if err != nil || observed.ServiceUID != service.UID || observed.Values["serverless.bucket"] != "auto" {
		t.Fatalf("unexpected observation: %+v, %v", observed, err)
	}
	if err := backend.Apply(context.Background(), instance, observed.ServiceUID, v1alpha1.ClusterSettingsPatch{Set: map[string]string{"serverless.bucket": "off"}}); err != nil {
		t.Fatal(err)
	}
	service.Spec.Selector[workload.FleetLabel] = "other"
	if _, err := core.Services("n").Update(context.Background(), service, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	if err := backend.Apply(context.Background(), instance, observed.ServiceUID, v1alpha1.ClusterSettingsPatch{Set: map[string]string{"serverless.bucket": "off"}}); err == nil {
		t.Fatal("modified Service selector must block writes")
	}
	if requests != 2 {
		t.Fatalf("unexpected settings request count %d", requests)
	}
}

func TestSettingsBackendRejectsServiceChangeDuringRequest(t *testing.T) {
	for _, method := range []string{"GET", "PUT"} {
		t.Run(method, func(t *testing.T) {
			instance := &v1alpha1.OpenSearchServerless{ObjectMeta: metav1.ObjectMeta{Name: "catalog", Namespace: "n", UID: "owner"}, Status: v1alpha1.OpenSearchServerlessStatus{ServingFleetIntentByRole: map[string]string{"ingest": "blue"}}}
			service, err := workload.RoleService(instance, "ingest", "blue", 9200)
			if err != nil {
				t.Fatal(err)
			}
			service.UID, service.ResourceVersion = "service-uid", "1"
			service.Spec.Type, service.Spec.ClusterIP = corev1.ServiceTypeClusterIP, "10.0.0.1"
			core := fake.NewSimpleClientset(service).CoreV1()
			backend := developmentHTTPSettings{core: core, http: &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
				changed := service.DeepCopy()
				changed.ResourceVersion = "2"
				if _, err := core.Services("n").Update(context.Background(), changed, metav1.UpdateOptions{}); err != nil {
					t.Fatal(err)
				}
				body := `{"persistent":{"serverless.bucket":"auto"}}`
				if method == "PUT" {
					body = `{}`
				}
				return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader(body)), Header: make(http.Header)}, nil
			})}}
			if method == "GET" {
				if _, err := backend.Read(context.Background(), instance); err == nil {
					t.Fatal("read accepted a changed Service")
				}
			} else if err := backend.Apply(context.Background(), instance, service.UID, v1alpha1.ClusterSettingsPatch{Set: map[string]string{"serverless.bucket": "off"}}); err == nil {
				t.Fatal("write reported a definitive success after Service change")
			}
		})
	}
}
