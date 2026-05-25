//go:build e2e || integration

// Package testutil provides shared test helpers for e2e tests that depend on
// internal SchemaBot packages (client, state). For helpers with zero internal
// dependencies, use pkg/e2eutil instead.
package testutil

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/apitypes"
	"github.com/block/schemabot/pkg/cmd/client"
	"github.com/block/schemabot/pkg/state"
)

// ProgressTimeout is the per-request timeout for progress API polling.
const ProgressTimeout = 5 * time.Second

// PollDeadline is the maximum time to wait when polling for a state transition.
const PollDeadline = 30 * time.Second

// PollInterval is the default sleep between polling attempts for state
// transitions observed via the progress API.
const PollInterval = 300 * time.Millisecond

// Poll repeatedly invokes cond every interval until cond returns true or the
// timeout expires. On timeout the test fails with failureMsg's return value,
// which is computed lazily so callers can reference state captured by cond via
// closure (e.g. the most recent observed value).
func Poll(t *testing.T, timeout, interval time.Duration, cond func() bool, failureMsg func() string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(interval)
	}
	require.Fail(t, "timeout", failureMsg())
}

// FetchProgress calls the progress API by apply ID and returns the
// full ProgressResponse with its State normalized.
func FetchProgress(endpoint, applyID string) (*apitypes.ProgressResponse, error) {
	ctx, cancel := context.WithTimeout(context.Background(), ProgressTimeout)
	defer cancel()
	result, err := client.GetProgressCtx(ctx, endpoint, applyID)
	if err != nil {
		return nil, err
	}
	result.State = state.NormalizeState(result.State)
	return result, nil
}

// WaitForState polls the progress API by apply ID until the overall state
// matches expectedState or the timeout expires.
func WaitForState(t *testing.T, endpoint, applyID, expectedState string, timeout time.Duration) {
	t.Helper()
	expected := state.NormalizeState(expectedState)
	var lastState string
	Poll(t, timeout, PollInterval,
		func() bool {
			result, err := FetchProgress(endpoint, applyID)
			if err != nil {
				return false
			}
			lastState = result.State
			return result.State == expected
		},
		func() string {
			return fmt.Sprintf("timeout waiting for apply %s state %q, last state: %q", applyID, expectedState, lastState)
		},
	)
}

// WaitForAnyState polls the progress API by apply ID until any of the expected
// states matches or timeout expires. Returns the matched state string.
func WaitForAnyState(t *testing.T, endpoint, applyID string, expectedStates []string, timeout time.Duration) string {
	t.Helper()
	expected := make([]string, len(expectedStates))
	for i, s := range expectedStates {
		expected[i] = state.NormalizeState(s)
	}
	var lastState, matched string
	Poll(t, timeout, PollInterval,
		func() bool {
			result, err := FetchProgress(endpoint, applyID)
			if err != nil {
				return false
			}
			lastState = result.State
			for i, exp := range expected {
				if lastState == exp {
					matched = expectedStates[i]
					return true
				}
			}
			return false
		},
		func() string {
			return fmt.Sprintf("timeout waiting for apply %s any of states %v, last state: %q", applyID, expectedStates, lastState)
		},
	)
	return matched
}
