//go:build integration

package integration

import (
	"testing"
	"time"

	"github.com/block/schemabot/e2e/testutil"
)

// waitForState polls the progress API by apply ID until the overall state
// matches expectedState or the timeout expires.
func waitForState(t *testing.T, endpoint, applyID, expectedState string, timeout time.Duration) {
	t.Helper()
	testutil.WaitForState(t, endpoint, applyID, expectedState, timeout)
}
