package v1alpha1_test

import (
	"testing"

	"github.com/huyz0/opensearch-serverless-operator/api/v1alpha1"
)

func TestRetryProvisioningRequiresAcknowledgedPod(t *testing.T) {
	spec := v1alpha1.OperationSpec{InstanceRef: v1alpha1.InstanceReference{Name: "instance", UID: "uid"}, Action: "RetryProvisioning", ProvisioningRetry: &v1alpha1.ProvisioningRetry{PodUID: "pod", AcknowledgeNewAttempt: true}}
	if err := spec.Validate(); err != nil {
		t.Fatal(err)
	}
	spec.ProvisioningRetry.AcknowledgeNewAttempt = false
	if err := spec.Validate(); err == nil {
		t.Fatal("retry without acknowledgement accepted")
	}
	spec.ProvisioningRetry.AcknowledgeNewAttempt = true
	spec.ProvisioningRetry.PodUID = ""
	if err := spec.Validate(); err == nil {
		t.Fatal("retry without old Pod UID accepted")
	}
}

func TestOperationValidation(t *testing.T) {
	spec := v1alpha1.OperationSpec{
		InstanceRef: v1alpha1.InstanceReference{Name: "catalog", UID: "uid"}, Action: "Promote", TargetFleet: "green",
		ExpectedServingFleetByRole: map[string]string{"search": "blue", "ingest": "blue"},
	}
	if err := spec.Validate(); err != nil {
		t.Fatal(err)
	}
	delete(spec.ExpectedServingFleetByRole, "ingest")
	if err := spec.Validate(); err == nil {
		t.Fatal("promotion without both expected role assignments must fail")
	}
	spec.ExpectedServingFleetByRole["ingest"] = "blue"
	spec.SupersedesOperationUID = "other"
	if err := spec.Validate(); err == nil {
		t.Fatal("promotion must not supersede an operation")
	}
	spec.Action = "Rollback"
	if err := spec.Validate(); err != nil {
		t.Fatal(err)
	}
}

func TestWeightedPromotionTrafficValidation(t *testing.T) {
	base := v1alpha1.OperationSpec{
		InstanceRef: v1alpha1.InstanceReference{Name: "catalog", UID: "uid"},
		Action:      "Promote", TargetFleet: "green", RoutingMode: "WeightedGateway",
		ExpectedServingFleetByRole: map[string]string{"search": "blue", "ingest": "blue"},
	}
	for _, test := range []struct {
		name         string
		weights      []int
		action, mode string
		valid        bool
	}{
		{name: "strictly increasing", weights: []int{2, 25, 99}, action: "Promote", mode: "WeightedGateway", valid: true},
		{name: "admission already automatic", weights: []int{1, 25}, action: "Promote", mode: "WeightedGateway"},
		{name: "cutover already automatic", weights: []int{25, 100}, action: "Promote", mode: "WeightedGateway"},
		{name: "duplicate", weights: []int{20, 20}, action: "Promote", mode: "WeightedGateway"},
		{name: "descending", weights: []int{45, 20}, action: "Promote", mode: "WeightedGateway"},
		{name: "selector mode", weights: []int{25}, action: "Promote"},
		{name: "rollback derives its own steps", weights: []int{25}, action: "Rollback", mode: "WeightedGateway"},
	} {
		t.Run(test.name, func(t *testing.T) {
			spec := base
			spec.Action, spec.RoutingMode = test.action, test.mode
			spec.WeightedTraffic = &v1alpha1.WeightedPromotionTraffic{Ingest: test.weights}
			if err := spec.Validate(); (err == nil) != test.valid {
				t.Fatalf("Validate() = %v, want valid=%v", err, test.valid)
			}
		})
	}
}

func TestClusterSettingsPatchValidation(t *testing.T) {
	spec := v1alpha1.OperationSpec{InstanceRef: v1alpha1.InstanceReference{Name: "catalog", UID: "uid"}, Action: "ApplyClusterSettings",
		ClusterSettings: &v1alpha1.ClusterSettingsPatch{Set: map[string]string{"a": "value"}, Remove: []string{"b"}},
	}
	if err := spec.Validate(); err != nil {
		t.Fatal(err)
	}
	spec.ClusterSettings.Remove = []string{"a"}
	if err := spec.Validate(); err == nil {
		t.Fatal("setting and removing the same key must fail")
	}
}

func TestSecurityRevisionValidation(t *testing.T) {
	spec := v1alpha1.OperationSpec{InstanceRef: v1alpha1.InstanceReference{Name: "catalog", UID: "uid"}, Action: "ApplySecurityConfiguration", SecurityConfiguration: &v1alpha1.SecurityConfigurationChange{CandidateRevision: "r2", ExpectedActiveRevision: "r1"}}
	if err := spec.Validate(); err != nil {
		t.Fatal(err)
	}
	spec.SecurityConfiguration.ExpectedActiveRevision = ""
	if err := spec.Validate(); err == nil {
		t.Fatal("expected active revision is required")
	}
}
