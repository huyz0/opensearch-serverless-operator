package java

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"time"
)

// WriterRetirementState describes admission on one exact Java process.
type WriterRetirementState string

const (
	WriterAdmissionOpen    WriterRetirementState = "open"
	WriterTrafficWithdrawn WriterRetirementState = "withdrawn"
	WriterAdmissionClosing WriterRetirementState = "closing"
	WriterAdmissionClosed  WriterRetirementState = "closed"
)

const writerRetirementPath = "/_serverless/writer_retirement/"
const writerWithdrawalPath = "/_serverless/writer_withdrawal/"

// ErrWriterRetirementConflict means Java refused a stale process or another operation owns admission.
var ErrWriterRetirementConflict = errors.New("writer retirement conflicts with the current process or operation")

// WithdrawWriterTraffic changes local readiness and publishes an assignment
// withdrawal in the Java lease. Existing owners remain reachable over peer
// transport. The reply precedes peer, Kubernetes and proxy convergence.
func WithdrawWriterTraffic(ctx context.Context, client *http.Client, baseURL string, target WriterProcess, operationID, username string, password []byte) (WriterRetirementState, error) {
	return writerRetirementState(ctx, client, baseURL, target, operationID, username, password, http.MethodPost, writerWithdrawalPath, "")
}

// WithdrawWriterTrafficBefore requires Java to accept the withdrawal before
// the deadline. The caller subtracts any allowance for clock skew.
func WithdrawWriterTrafficBefore(ctx context.Context, client *http.Client, baseURL string, target WriterProcess, operationID, username string, password []byte, deadline time.Time) (WriterRetirementState, error) {
	if !time.Now().Before(deadline) {
		return "", fmt.Errorf("writer withdrawal deadline has expired")
	}
	ctx, cancel := context.WithDeadline(ctx, deadline)
	defer cancel()
	suffix := "/withdraw_before/" + strconv.FormatInt(deadline.UnixMilli(), 10)
	return writerRetirementState(ctx, client, baseURL, target, operationID, username, password, http.MethodPost, writerWithdrawalPath, suffix)
}

// PrepareWriterRetirement closes new acquisitions on the exact process after withdrawal. A caller must
// reserve the operation before dispatch and observe the same operation after
// an ambiguous response. Closing is permanent for the Java process.
func PrepareWriterRetirement(ctx context.Context, client *http.Client, baseURL string, target WriterProcess, operationID, username string, password []byte) (WriterRetirementState, error) {
	return writerRetirementState(ctx, client, baseURL, target, operationID, username, password, http.MethodPost, writerRetirementPath, "")
}

// PrepareWriterRetirementBefore uses the timed Java handler. The caller's
// deadline must include its clock-skew margin.
func PrepareWriterRetirementBefore(ctx context.Context, client *http.Client, baseURL string, target WriterProcess, operationID, username string, password []byte, deadline time.Time) (WriterRetirementState, error) {
	if !time.Now().Before(deadline) {
		return "", fmt.Errorf("writer preparation deadline has expired")
	}
	ctx, cancel := context.WithDeadline(ctx, deadline)
	defer cancel()
	suffix := "/prepare_before/" + strconv.FormatInt(deadline.UnixMilli(), 10)
	return writerRetirementState(ctx, client, baseURL, target, operationID, username, password, http.MethodPost, writerRetirementPath, suffix)
}

// GetWriterRetirementState observes the same reserved operation without closing admission.
func GetWriterRetirementState(ctx context.Context, client *http.Client, baseURL string, target WriterProcess, operationID, username string, password []byte) (WriterRetirementState, error) {
	return writerRetirementState(ctx, client, baseURL, target, operationID, username, password, http.MethodGet, writerRetirementPath, "")
}

func writerRetirementState(ctx context.Context, client *http.Client, baseURL string, target WriterProcess, operationID, username string, password []byte, method, prefix, suffix string) (WriterRetirementState, error) {
	if !writerProcessID.MatchString(operationID) {
		return "", fmt.Errorf("valid writer retirement operation ID is required")
	}
	path, err := writerProcessPath(prefix, target)
	if err != nil {
		return "", err
	}
	status, body, err := requestWriterEvidence(ctx, client, baseURL, username, password, method, path+"/"+operationID+suffix, nil)
	if err != nil {
		return "", err
	}
	if status == http.StatusConflict {
		var failure struct {
			Error struct {
				Type string `json:"type"`
			} `json:"error"`
		}
		if json.Unmarshal(body, &failure) == nil &&
			(failure.Error.Type == "writer_process_changed" || failure.Error.Type == "writer_admission_already_reserved") {
			return "", ErrWriterRetirementConflict
		}
	}
	if status != http.StatusOK {
		return "", fmt.Errorf("writer retirement endpoint returned HTTP %d", status)
	}
	var reply struct {
		Version int `json:"version"`
		WriterProcess
		OperationID string                `json:"operation_id"`
		State       WriterRetirementState `json:"state"`
	}
	if err := json.Unmarshal(body, &reply); err != nil {
		return "", fmt.Errorf("decode writer retirement state: %w", err)
	}
	if reply.Version != 1 || reply.WriterProcess != target || reply.OperationID != operationID {
		return "", fmt.Errorf("writer retirement response does not match the exact process and operation")
	}
	switch reply.State {
	case WriterAdmissionClosing, WriterAdmissionClosed:
		return reply.State, nil
	case WriterTrafficWithdrawn:
		if method == http.MethodGet || prefix == writerWithdrawalPath {
			return reply.State, nil
		}
	case WriterAdmissionOpen:
		if method == http.MethodGet {
			return reply.State, nil
		}
	}
	return "", fmt.Errorf("invalid writer retirement state for %s", method)
}
