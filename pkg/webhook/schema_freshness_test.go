package webhook

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	ghclient "github.com/block/schemabot/pkg/github"
)

func TestAssertSchemaStillCurrent_MatchProceeds(t *testing.T) {
	h, comments, _ := newTestHandler(t)

	schema := &ghclient.SchemaRequestResult{
		Repository: "octocat/hello-world",
		Database:   "testdb",
		Type:       "mysql",
		HeadSHA:    "sha-abc",
	}

	rejected := h.assertSchemaStillCurrent(
		t.Context(),
		"octocat/hello-world", 1, int64(12345),
		schema,
		"sha-abc",
		"staging",
		"alice",
		"apply-confirm",
	)

	assert.False(t, rejected, "matching SHAs must not reject")

	select {
	case body := <-comments:
		t.Fatalf("no comment should be posted on a match; got: %s", body)
	case <-time.After(100 * time.Millisecond):
	}
}

func TestAssertSchemaStillCurrent_MismatchRejectsAndPostsComment(t *testing.T) {
	h, comments, _ := newTestHandler(t)

	schema := &ghclient.SchemaRequestResult{
		Repository: "octocat/hello-world",
		Database:   "testdb",
		Type:       "mysql",
		HeadSHA:    "discovery-sha",
	}

	rejected := h.assertSchemaStillCurrent(
		t.Context(),
		"octocat/hello-world", 1, int64(12345),
		schema,
		"current-sha",
		"staging",
		"alice",
		"apply-confirm",
	)

	require.True(t, rejected, "differing SHAs must reject")

	select {
	case body := <-comments:
		assert.Contains(t, body, "Rejected", "title should call out rejection")
		assert.Contains(t, body, "new commits since discovery", "should explain why")
		assert.Contains(t, body, "discovery-sha", "must include discovery SHA")
		assert.Contains(t, body, "current-sha", "must include current SHA")
		assert.Contains(t, body, "testdb", "must include database")
		assert.Contains(t, body, "staging", "must include environment")
		assert.Contains(t, body, "schemabot apply-confirm -e staging", "retry hint must use the action and env")
		assert.Contains(t, body, "alice", "must attribute the requester")
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for rejection comment")
	}
}

func TestMetricActionKey(t *testing.T) {
	cases := map[string]string{
		"plan":          "plan",
		"apply":         "apply",
		"apply-confirm": "apply_confirm",
	}
	for in, want := range cases {
		assert.Equal(t, want, metricActionKey(in), "input=%q", in)
	}
}
