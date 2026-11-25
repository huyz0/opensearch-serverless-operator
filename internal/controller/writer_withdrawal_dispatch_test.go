package controller

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/huyz0/opensearch-serverless-operator/internal/java"
)

func TestWriterWithdrawalDispatch(t *testing.T) {
	for _, test := range []struct {
		name     string
		observed java.WriterRetirementState
		fail     string
		calls    []string
	}{
		{"qualified", java.WriterAdmissionOpen, "", []string{"state", "qualify", "recheck", "withdraw"}},
		{"recover publication", java.WriterTrafficWithdrawn, "", []string{"state"}},
		{"observe open after cutoff", java.WriterAdmissionOpen, "", []string{"state"}},
		{"publication pending", "", "state", []string{"state"}},
		{"insufficient capacity", java.WriterAdmissionOpen, "qualify", []string{"state", "qualify"}},
		{"claim changed", java.WriterAdmissionOpen, "recheck", []string{"state", "qualify", "recheck"}},
		{"lease expired", java.WriterAdmissionOpen, "expired", []string{"state", "qualify", "recheck"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			var calls []string
			step := func(name string) error {
				calls = append(calls, name)
				if name == test.fail {
					return errors.New("observation failed")
				}
				return nil
			}
			cutoff := time.Now().Add(time.Minute)
			wantState := java.WriterTrafficWithdrawn
			if test.name == "observe open after cutoff" {
				cutoff = time.Now().Add(-time.Second)
				wantState = java.WriterAdmissionOpen
			}
			got, err := executeWriterWithdrawal(context.Background(), writerWithdrawalChecks{
				cutoff: cutoff,
				state:  func(context.Context) (java.WriterRetirementState, error) { return test.observed, step("state") },
				withdraw: func(context.Context) (java.WriterRetirementState, error) {
					return java.WriterTrafficWithdrawn, step("withdraw")
				},
				qualify: func(context.Context) (time.Time, error) {
					if test.fail == "expired" {
						return time.Time{}, step("qualify")
					}
					return time.Now().Add(time.Minute), step("qualify")
				},
				recheck: func(context.Context) error { return step("recheck") },
			})
			if !reflect.DeepEqual(calls, test.calls) || (err != nil) != (test.fail != "") || (err == nil && got != wantState) {
				t.Fatalf("withdrawal: state %q, error %v, calls %v, want %v", got, err, calls, test.calls)
			}
		})
	}
}
