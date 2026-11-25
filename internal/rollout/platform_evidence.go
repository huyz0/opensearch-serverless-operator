package rollout

import (
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"time"

	"k8s.io/apimachinery/pkg/types"
)

const PlatformEvidenceDomain = "opensearch-serverless-rollout-evidence/v1\n"
const PlatformEvidenceMaxAge = 30 * time.Second
const PlatformEvidenceMaxBytes = 512 << 10

type PlatformEvidence struct {
	Namespace          string    `json:"namespace"`
	InstanceUID        types.UID `json:"instanceUID"`
	InstanceGeneration int64     `json:"instanceGeneration"`
	OperationUID       types.UID `json:"operationUID"`
	Role               string    `json:"role"`
	Fleet              string    `json:"fleet"`
	Revision           string    `json:"revision"`
	ObservedAt         time.Time `json:"observedAt"`
	PeerQualified      bool      `json:"peerQualified"`
	SecurityQualified  bool      `json:"securityQualified"`
	RollbackCompatible bool      `json:"rollbackCompatible"`
	// HTTPTrustSHA256 attests the effective client-facing backend trust bundle for the transition.
	HTTPTrustSHA256 string `json:"httpTrustSHA256,omitempty"`
	// HTTPTrustFenced attests platform exclusion of competing backend trust writes.
	HTTPTrustFenced bool          `json:"httpTrustFenced,omitempty"`
	Pods            []PlatformPod `json:"pods"`
}

type PlatformPod struct {
	UID                types.UID `json:"uid"`
	ResourceVersion    string    `json:"resourceVersion"`
	NodeName           string    `json:"nodeName"`
	Zone               string    `json:"zone"`
	ProcessIncarnation string    `json:"processIncarnation"`
}

func PlatformEvidenceName(uid types.UID, role string) string {
	digest := sha256.Sum256([]byte(uid))
	return fmt.Sprintf("rollout-evidence-%x-%s", digest[:12], role)
}

// FleetPlatformEvidenceName keeps the operation target's established name
// while allowing evidence for multiple weighted fleets under the same role.
func FleetPlatformEvidenceName(uid types.UID, role, fleet, target string) string {
	name := PlatformEvidenceName(uid, role)
	if fleet == target {
		return name
	}
	digest := sha256.Sum256([]byte(fleet))
	return fmt.Sprintf("%s-%x", name, digest[:6])
}

func ParsePlatformEvidence(raw string) (PlatformEvidence, error) {
	return parseEvidence[PlatformEvidence](raw)
}

func ReadSignedPlatformEvidence(data map[string]string, key ed25519.PublicKey) (PlatformEvidence, error) {
	return readSignedEvidence[PlatformEvidence](PlatformEvidenceDomain, data, key)
}

func SignPlatformEvidence(evidence PlatformEvidence, signer ed25519.PrivateKey) (map[string]string, error) {
	return signEvidence(PlatformEvidenceDomain, evidence, signer)
}

func parseEvidence[T any](raw string) (T, error) {
	var evidence, empty T
	if len(raw) == 0 || len(raw) > PlatformEvidenceMaxBytes {
		return empty, fmt.Errorf("rollout platform evidence is missing or too large")
	}
	decoder := json.NewDecoder(strings.NewReader(raw))
	decoder.DisallowUnknownFields()
	if !strings.HasPrefix(strings.TrimSpace(raw), "{") || decoder.Decode(&evidence) != nil || decoder.Decode(new(any)) != io.EOF {
		return empty, fmt.Errorf("rollout platform evidence must be one v1 JSON object")
	}
	return evidence, nil
}

func readSignedEvidence[T any](domain string, data map[string]string, key ed25519.PublicKey) (T, error) {
	var empty T
	raw, encoded := data["evidence.json"], data["signature"]
	if len(key) != ed25519.PublicKeySize || len(raw) == 0 || len(raw) > PlatformEvidenceMaxBytes ||
		len(encoded) != base64.StdEncoding.EncodedLen(ed25519.SignatureSize) {
		return empty, fmt.Errorf("rollout platform evidence or signature is invalid")
	}
	signature, err := base64.StdEncoding.Strict().DecodeString(encoded)
	if err != nil || !ed25519.Verify(key, []byte(domain+raw), signature) {
		return empty, fmt.Errorf("rollout platform evidence signature is invalid")
	}
	return parseEvidence[T](raw)
}

func signEvidence(domain string, evidence any, signer ed25519.PrivateKey) (map[string]string, error) {
	if len(signer) != ed25519.PrivateKeySize {
		return nil, fmt.Errorf("Ed25519 signing key is required")
	}
	raw, err := json.Marshal(evidence)
	if err != nil || len(raw) > PlatformEvidenceMaxBytes {
		return nil, fmt.Errorf("cannot encode bounded rollout platform evidence")
	}
	signature := ed25519.Sign(signer, append([]byte(domain), raw...))
	return map[string]string{"evidence.json": string(raw), "signature": base64.StdEncoding.EncodeToString(signature)}, nil
}
