package plugins

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"regexp"
	"sort"
	"strings"

	"k8s.io/apimachinery/pkg/util/validation"
	"oras.land/oras-go/v2/registry"

	"github.com/huyz0/opensearch-serverless-operator/api/v1alpha1"
)

// Policy is the independently supplied approval and compatibility input.
type Policy struct {
	ApprovedArtifacts []string `json:"approvedArtifacts"`
	OpenSearchVersion string   `json:"openSearchVersion"`
	JavaVersion       string   `json:"javaVersion"`
	MaxBytes          int64    `json:"maxBytes"`
	MaxFiles          int      `json:"maxFiles"`
	// ApprovedFleetTransitions qualify old and new plugin sets to coexist
	// and the old set to serve again after writes made by the new set.
	ApprovedFleetTransitions []FleetTransition `json:"approvedFleetTransitions,omitempty"`
}

type FleetTransition struct {
	From string `json:"from"`
	To   string `json:"to"`
}

// Resolution contains public, immutable inputs for one role's installer.
// Credentials are referenced by name and their contents are never returned.
type Resolution struct {
	RequestJSON       []byte
	RequestSHA256     string
	PolicySHA256      string
	CredentialsSecret string
}

var manifestDigest = regexp.MustCompile(`^sha256:[a-f0-9]{64}$`)

func validArtifact(value string) bool {
	ref, err := registry.ParseReference(value)
	return err == nil && strings.Contains(value, "@sha256:") && manifestDigest.MatchString(ref.Reference)
}

// ResolveRole validates a bounded policy and selects a deterministic plugin
// request for one role. The policy's authority, source identity, input
// mounting and image compatibility must be verified by its caller.
func ResolveRole(config []v1alpha1.Plugin, role string, policyJSON []byte) (*Resolution, error) {
	if (role != "search" && role != "ingest") || len(config) > 256 {
		return nil, fmt.Errorf("unsupported plugin role or plugin count")
	}
	policy, err := decodePolicy(policyJSON)
	if err != nil {
		return nil, err
	}
	if policy.OpenSearchVersion == "" || policy.JavaVersion == "" || policy.MaxBytes <= 0 || policy.MaxBytes > 1<<30 || policy.MaxFiles <= 0 || policy.MaxFiles > 10000 || len(policy.ApprovedArtifacts) == 0 || len(policy.ApprovedArtifacts) > 1024 {
		return nil, fmt.Errorf("plugin policy compatibility or limits are invalid")
	}
	approved := make(map[string]bool, len(policy.ApprovedArtifacts))
	for _, artifact := range policy.ApprovedArtifacts {
		if !validArtifact(artifact) || approved[artifact] {
			return nil, fmt.Errorf("policy contains invalid or duplicate artifact")
		}
		approved[artifact] = true
	}
	var selected []Installation
	seen := map[string]bool{}
	credentials := ""
	for _, plugin := range config {
		include := len(plugin.Roles) == 0
		roles := map[string]bool{}
		for _, candidate := range plugin.Roles {
			if (candidate != "search" && candidate != "ingest") || roles[candidate] {
				return nil, fmt.Errorf("invalid or duplicate plugin role")
			}
			roles[candidate] = true
			if candidate == role {
				include = true
			}
		}
		if !include {
			continue
		}
		if !v1alpha1.ValidPluginName(plugin.Name) || seen[plugin.Name] || !validArtifact(plugin.Artifact) || !approved[plugin.Artifact] {
			return nil, fmt.Errorf("plugin selection is invalid, duplicated, reserved or unapproved")
		}
		seen[plugin.Name] = true
		if plugin.RegistryCredentialsRef != nil {
			name := plugin.RegistryCredentialsRef.Name
			if name == "" || len(validation.IsDNS1123Subdomain(name)) != 0 || (credentials != "" && credentials != name) {
				return nil, fmt.Errorf("selected plugins require a single valid credentials Secret")
			}
			credentials = name
		}
		selected = append(selected, Installation{Name: plugin.Name, Artifact: plugin.Artifact})
	}
	if len(selected) == 0 {
		return nil, nil
	}
	sort.Slice(selected, func(i, j int) bool { return selected[i].Name < selected[j].Name })
	request, err := json.Marshal(selected)
	if err != nil {
		return nil, err
	}
	return &Resolution{RequestJSON: request, RequestSHA256: fmt.Sprintf("%x", sha256.Sum256(request)), PolicySHA256: fmt.Sprintf("%x", sha256.Sum256(policyJSON)), CredentialsSecret: credentials}, nil
}

func decodePolicy(raw []byte) (Policy, error) {
	var policy Policy
	if len(raw) == 0 || len(raw) > 1<<20 {
		return policy, fmt.Errorf("plugin policy is missing or too large")
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&policy); err != nil {
		return policy, fmt.Errorf("invalid plugin policy: %w", err)
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return policy, fmt.Errorf("plugin policy contains trailing data")
	}
	return policy, nil
}

// FleetDigest combines the two canonical role request hashes. An empty role
// uses an empty string, distinct from any plugin request.
func FleetDigest(ingest, search string) string {
	return fmt.Sprintf("sha256:%x", sha256.Sum256([]byte(fmt.Sprintf("%s\n%s\n", ingest, search))))
}

func QualifyFleetTransition(raw []byte, from, to string) error {
	if from == to {
		return nil
	}
	policy, err := decodePolicy(raw)
	if err != nil {
		return err
	}
	if len(policy.ApprovedFleetTransitions) > 128 {
		return fmt.Errorf("too many approved plugin fleet transitions")
	}
	seen := make(map[FleetTransition]bool, len(policy.ApprovedFleetTransitions))
	for _, transition := range policy.ApprovedFleetTransitions {
		if !manifestDigest.MatchString(transition.From) || !manifestDigest.MatchString(transition.To) ||
			transition.From == transition.To || seen[transition] {
			return fmt.Errorf("invalid or duplicate approved plugin fleet transition")
		}
		seen[transition] = true
	}
	if !seen[FleetTransition{From: from, To: to}] {
		return fmt.Errorf("plugin coexistence and rollback are not approved for fleet transition %s to %s", from, to)
	}
	return nil
}
