package controller

import (
	"context"
	"github.com/huyz0/opensearch-serverless-operator/api/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"testing"
	"time"
)

func TestArchiveWithdrawnAttemptPreservesGroupAndIdentity(t *testing.T) {
	ctx := context.Background()
	scheme := runtime.NewScheme()
	if err := v1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	instance := &v1alpha1.OpenSearchServerless{ObjectMeta: metav1.ObjectMeta{Name: "instance", Namespace: "n", UID: "owner"}, Status: v1alpha1.OpenSearchServerlessStatus{ProvisioningAttempt: &v1alpha1.ProvisioningAttempt{Name: "pod", PodUID: "uid", Role: "search", Zone: "a", Phase: "Blocked", Reason: "PodGone", StartedAt: metav1.NewTime(time.Now()), PluginInstall: &v1alpha1.PluginInstallIntent{RequestUID: "snapshot"}}}}
	kube := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(instance).WithObjects(instance).Build()
	r := &InstanceReconciler{Client: kube}
	if _, changed, err := r.archiveBlockedAttempt(ctx, instance); err != nil || !changed {
		t.Fatalf("archive: %v", err)
	}
	if err := kube.Get(ctx, types.NamespacedName{Namespace: "n", Name: "instance"}, instance); err != nil {
		t.Fatal(err)
	}
	if instance.Status.ProvisioningAttempt != nil || len(instance.Status.BlockedProvisioningAttempts) != 1 || instance.Status.BlockedProvisioningAttempts[0].PluginInstall.RequestUID != "snapshot" {
		t.Fatal("archived identity was lost")
	}
	copy := instance.DeepCopyObject().(*v1alpha1.OpenSearchServerless)
	copy.Status.BlockedProvisioningAttempts[0].PluginInstall.RequestUID = "changed"
	if instance.Status.BlockedProvisioningAttempts[0].PluginInstall.RequestUID != "snapshot" {
		t.Fatal("nested blocked state was not copied")
	}
	instance.Status.ProvisioningAttempt = &v1alpha1.ProvisioningAttempt{Name: "other", PodUID: "uid2", Role: "search", Zone: "a", Phase: "Blocked", Reason: "PodGone", StartedAt: metav1.NewTime(time.Now())}
	if _, changed, err := r.archiveBlockedAttempt(ctx, instance); err == nil || changed {
		t.Fatal("duplicate group accepted")
	}
	instance.Status.ProvisioningAttempt.Role = ""
	if _, changed, err := r.archiveBlockedAttempt(ctx, instance); err != nil || changed {
		t.Fatal("legacy attempt must remain globally held")
	}
}

func TestTerminalHoldBlocksOnlyItsRoleAndZone(t *testing.T) {
	instance := &v1alpha1.OpenSearchServerless{Status: v1alpha1.OpenSearchServerlessStatus{BlockedProvisioningAttempts: []v1alpha1.ProvisioningAttempt{{Name: "terminal", PodUID: "uid", Role: "search", Zone: "a", Phase: "Blocked", Reason: "PodTerminal", StartedAt: metav1.NewTime(time.Now())}}}}
	groups, err := blockedProvisioningGroups(instance)
	if err != nil || !groups[provisioningGroup{"search", "a"}] || groups[provisioningGroup{"search", "b"}] || groups[provisioningGroup{"ingest", "a"}] {
		t.Fatalf("terminal group isolation failed: %v", err)
	}
}
