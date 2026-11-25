package security_test

import (
	"testing"

	"github.com/huyz0/opensearch-serverless-operator/internal/security"
)

func TestAppliedRequiresCurrentProcessAcknowledgement(t *testing.T) {
	consumers := []security.Consumer{{PodUID: "pod-a", ProcessID: "new-process"}, {PodUID: "pod-b", ProcessID: "process-b"}}
	acks := []security.Acknowledgement{{PodUID: "pod-a", ProcessID: "old-process", Revision: "r2"}, {PodUID: "pod-b", ProcessID: "process-b", Revision: "r2"}}
	applied, err := security.Applied("r2", consumers, acks)
	if err != nil || applied {
		t.Fatalf("stale process acknowledgement must not qualify: applied=%v err=%v", applied, err)
	}
	acks = append(acks, security.Acknowledgement{PodUID: "pod-a", ProcessID: "new-process", Revision: "r2"})
	applied, err = security.Applied("r2", consumers, acks)
	if err != nil || !applied {
		t.Fatalf("current acknowledgements should qualify: applied=%v err=%v", applied, err)
	}
}
