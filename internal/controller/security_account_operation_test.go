package controller

import (
	"context"
	"fmt"
	"maps"
	"testing"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/huyz0/opensearch-serverless-operator/api/v1alpha1"
)

type simulatedAccountStore struct {
	uid      string
	cohort   map[string]string
	desired  bool
	dispatch int
}

func (store *simulatedAccountStore) resolve(context.Context, *v1alpha1.OpenSearchServerless, *v1alpha1.SecurityAccountChange) (securityAccountInputs, error) {
	return securityAccountInputs{sources: v1alpha1.SecurityAccountSources{PreviousSecretUID: "prior", DesiredSecretUID: store.uid}}, nil
}

func (store *simulatedAccountStore) verify(_ context.Context, _ *v1alpha1.OpenSearchServerless, _ securityAccountInputs, _ string, desired bool) (map[string]string, error) {
	if store.desired != desired {
		return nil, fmt.Errorf("state cannot be verified")
	}
	return maps.Clone(store.cohort), nil
}

func (store *simulatedAccountStore) apply(context.Context, *v1alpha1.OpenSearchServerless, securityAccountInputs, string) error {
	store.dispatch++
	store.desired = true
	return fmt.Errorf("response lost")
}

func TestSecurityAccountOperationRecovery(t *testing.T) {
	for _, scenario := range []string{"lost response", "lost dispatch", "missing process", "recreated Secret", "new process before dispatch", "new process after dispatch"} {
		t.Run(scenario, func(t *testing.T) {
			ctx := context.Background()
			scheme := runtime.NewScheme()
			if err := v1alpha1.AddToScheme(scheme); err != nil {
				t.Fatal(err)
			}
			instance := &v1alpha1.OpenSearchServerless{ObjectMeta: metav1.ObjectMeta{Name: "catalog", Namespace: "n", UID: "instance", Generation: 1}}
			op := &v1alpha1.OpenSearchServerlessOperation{ObjectMeta: metav1.ObjectMeta{Name: "rotate", Namespace: "n", UID: "op", Generation: 1}, Spec: v1alpha1.OperationSpec{
				Action: "ApplySecurityConfiguration", InstanceRef: v1alpha1.InstanceReference{Name: instance.Name, UID: string(instance.UID)},
				SecurityConfiguration: &v1alpha1.SecurityConfigurationChange{CandidateRevision: "second", ExpectedActiveRevision: "initial", Account: &v1alpha1.SecurityAccountChange{Username: "application",
					PreviousPasswordRef: &v1alpha1.SecretKeyReference{Name: "old", Key: "password"}, DesiredPasswordRef: &v1alpha1.SecretKeyReference{Name: "new", Key: "password"}}},
			}}
			kube := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(instance, op).WithObjects(instance, op).Build()
			store := &simulatedAccountStore{uid: "desired", cohort: map[string]string{"ingest": "a", "search": "b"}}
			r := &OperationReconciler{Client: kube, accountBackend: store}
			key := client.ObjectKeyFromObject(op)
			step := func() {
				t.Helper()
				if _, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: key}); err != nil {
					t.Fatal(err)
				}
				if err := kube.Get(ctx, key, op); err != nil {
					t.Fatal(err)
				}
				if err := kube.Get(ctx, client.ObjectKeyFromObject(instance), instance); err != nil {
					t.Fatal(err)
				}
			}
			for range 3 {
				step()
			}
			if op.Status.Phase != accountPrepared || store.dispatch != 0 || instance.Status.ActiveOperation == nil {
				t.Fatalf("account dispatch was not prepared: phase=%s writes=%d", op.Status.Phase, store.dispatch)
			}
			if scenario == "new process before dispatch" {
				store.cohort["preview"] = "c"
				step()
				condition := meta.FindStatusCondition(op.Status.Conditions, "Ready")
				if op.Status.Phase != "Conflict" || condition == nil || condition.Reason != "CohortChangedBeforeDispatch" || store.dispatch != 0 {
					t.Fatal("changed cohort did not block dispatch")
				}
				return
			}
			if scenario == "lost dispatch" {
				op.Status.Phase = accountWriting
				if err := kube.Status().Update(ctx, op); err != nil {
					t.Fatal(err)
				}
				if err := kube.Delete(ctx, op); err != nil {
					t.Fatal(err)
				}
			} else {
				step()
				if op.Status.Phase != accountWriting || store.dispatch != 1 {
					t.Fatal("write was not reserved and sent exactly once")
				}
			}
			if scenario == "missing process" {
				delete(store.cohort, "search")
			}
			if scenario == "recreated Secret" {
				store.uid = "replacement"
			}
			if scenario == "new process after dispatch" {
				store.cohort["preview"] = "c"
			}
			step()
			if scenario == "lost response" || scenario == "new process after dispatch" {
				if instance.Status.ActiveSecurityRevision != "second/op" || instance.Status.ActiveOperation == nil || op.Status.Phase != accountWriting {
					t.Fatal("commit receipt did not retain the claim for restart recovery")
				}
				r = &OperationReconciler{Client: kube}
				for range 3 {
					step()
				}
				if op.Status.Phase != "Succeeded" || instance.Status.ActiveOperation != nil || len(op.Finalizers) != 0 || store.dispatch != 1 {
					t.Fatal("persisted receipt did not recover without a second write")
				}
				return
			}
			step()
			wantReason := map[string]string{"lost dispatch": "WriteOutcomeUnknown", "missing process": "ConsumerMissing", "recreated Secret": "InputIdentityChanged"}[scenario]
			condition := meta.FindStatusCondition(op.Status.Conditions, "Ready")
			if op.Status.Phase != accountWriting || condition == nil || condition.Reason != wantReason || instance.Status.ActiveSecurityRevision != "" || instance.Status.ActiveOperation == nil || len(op.Finalizers) == 0 {
				t.Fatalf("unresolved dispatch advanced: phase=%s reason=%#v", op.Status.Phase, condition)
			}
			if (scenario == "lost dispatch" && store.dispatch != 0) || (scenario != "lost dispatch" && store.dispatch != 1) {
				t.Fatal("reconciliation replayed an unresolved write")
			}
			if scenario == "lost dispatch" && op.DeletionTimestamp == nil {
				t.Fatal("the finalizer did not retain an operation with an unresolved write")
			}
		})
	}
}
