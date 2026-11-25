package java

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"time"
)

// ObserveWriterWithdrawalView samples the observer's live cached placement
// view. Zero means the exact withdrawal has not been observed.
func ObserveWriterWithdrawalView(ctx context.Context, client *http.Client, baseURL string, observer, writer WriterProcess, username string, password []byte) (time.Duration, error) {
	if observer.NodeID == writer.NodeID {
		return 0, fmt.Errorf("writer withdrawal requires a distinct observing node")
	}
	path, err := writerProcessPath("/_serverless/writer_withdrawal_view/", observer)
	if err != nil {
		return 0, err
	}
	path, err = writerProcessPath(path+"/", writer)
	if err != nil {
		return 0, err
	}
	status, body, err := requestWriterEvidence(ctx, client, baseURL, username, password, http.MethodGet, path, nil)
	if err != nil {
		return 0, err
	}
	if status != http.StatusOK {
		return 0, fmt.Errorf("writer withdrawal view returned HTTP %d", status)
	}
	var reply struct {
		Version   int           `json:"version"`
		Observer  WriterProcess `json:"observer"`
		Writer    WriterProcess `json:"writer"`
		Withdrawn *bool         `json:"withdrawn"`
		Remaining *int64        `json:"lease_remaining_millis"`
	}
	if err := json.Unmarshal(body, &reply); err != nil {
		return 0, fmt.Errorf("decode writer withdrawal view: %w", err)
	}
	if reply.Version != 1 || reply.Observer != observer || reply.Writer != writer || reply.Withdrawn == nil || reply.Remaining == nil ||
		*reply.Remaining < 0 || *reply.Remaining > math.MaxInt64/int64(time.Millisecond) || *reply.Withdrawn != (*reply.Remaining > 0) {
		return 0, fmt.Errorf("writer withdrawal view has an unexpected process, version or state")
	}
	return time.Duration(*reply.Remaining) * time.Millisecond, nil
}
