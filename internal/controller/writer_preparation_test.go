package controller

import (
	"context"
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/huyz0/opensearch-serverless-operator/internal/java"
)

func TestWriterPreparationRequiresEveryGateAndRecoversAmbiguousPrepare(t *testing.T) {
	const (
		withdrawn = java.WriterTrafficWithdrawn
		closed    = java.WriterAdmissionClosed
	)
	held := errors.New("hold")
	for _, scenario := range []struct {
		name   string
		states []java.WriterRetirementState
		fail   string
		want   []string
	}{
		{name: "qualified", states: []java.WriterRetirementState{withdrawn, withdrawn}, want: []string{"state", "peer", "edge", "state", "reservation", "prepare"}},
		{name: "peer stale", states: []java.WriterRetirementState{withdrawn}, fail: "peer", want: []string{"state", "peer"}},
		{name: "proxy stale", states: []java.WriterRetirementState{withdrawn}, fail: "edge", want: []string{"state", "peer", "edge"}},
		{name: "victim changed", states: []java.WriterRetirementState{withdrawn, java.WriterAdmissionOpen}, want: []string{"state", "peer", "edge", "state"}},
		{name: "reservation changed", states: []java.WriterRetirementState{withdrawn, withdrawn}, fail: "reservation", want: []string{"state", "peer", "edge", "state", "reservation"}},
		{name: "short peer lease", states: []java.WriterRetirementState{withdrawn, withdrawn}, fail: "expired", want: []string{"state", "peer", "edge", "state", "reservation"}},
		{name: "expired final proof", states: []java.WriterRetirementState{withdrawn, withdrawn}, fail: "expired final proof", want: []string{"state", "peer", "edge", "state", "reservation"}},
		{name: "recover timeout by reading closed", states: []java.WriterRetirementState{closed}, want: []string{"state"}},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			var calls []string
			var read int
			appendCall := func(step string) error {
				calls = append(calls, step)
				if scenario.fail == step {
					return held
				}
				return nil
			}
			deadline := func(step string) func(context.Context) (time.Time, error) {
				return func(context.Context) (time.Time, error) {
					if step == "peer" && scenario.fail == "expired" {
						return time.Now().Add(-time.Second), appendCall(step)
					}
					return time.Now().Add(time.Minute), appendCall(step)
				}
			}
			state, err := executeWriterPreparation(context.Background(), writerPreparationChecks{
				state: func(context.Context) (java.WriterRetirementState, error) {
					current := scenario.states[read]
					read++
					return current, appendCall("state")
				},
				peer: deadline("peer"), edge: deadline("edge"),
				recheck: func(context.Context) (time.Time, error) {
					if scenario.fail == "expired final proof" {
						return time.Now().Add(-time.Second), appendCall("reservation")
					}
					return time.Now().Add(time.Minute), appendCall("reservation")
				},
				prepare: func(ctx context.Context) (java.WriterRetirementState, error) {
					if _, ok := ctx.Deadline(); !ok {
						t.Error("prepare must be bounded by evidence expiry")
					}
					return closed, appendCall("prepare")
				},
			})
			if !slices.Equal(calls, scenario.want) || (err != nil) != (scenario.fail != "") {
				t.Fatalf("calls = %v, state = %q, err = %v, want %v", calls, state, err, scenario.want)
			}
		})
	}
}
