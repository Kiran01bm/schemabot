//go:build integration

package webhook

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	gh "github.com/google/go-github/v68/github"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/spirit/pkg/utils"

	ghclient "github.com/block/schemabot/pkg/github"
	"github.com/block/schemabot/pkg/storage"
)

func TestE2EApplyWithChanges(t *testing.T) {
	dbName := "webhook_apply_changes"
	svc := setupE2EService(t, dbName)

	mux := http.NewServeMux()
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)

	client := gh.NewClient(nil)
	client.BaseURL, _ = url.Parse(server.URL + "/")

	schemabotConfig := fmt.Sprintf("database: %s\ntype: mysql\n", dbName)
	schemaFiles := map[string]string{
		"users.sql": "CREATE TABLE `users` (\n  `id` bigint unsigned NOT NULL AUTO_INCREMENT,\n  `name` varchar(255) NOT NULL,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;",
	}

	result := setupFakeGitHubForPlan(t, mux, schemaFiles, schemabotConfig, dbName)

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	installClient := ghclient.NewInstallationClient(client, logger)
	factory := &fakeClientFactory{client: installClient}

	h := NewHandler(svc, factory, nil, logger)

	req := buildWebhookRequest(t, webhookPayloadOpts{
		comment: "schemabot apply -e staging",
		isPR:    true,
	}, nil)

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	require.Equal(t, http.StatusOK, rr.Code)
	assert.Contains(t, rr.Body.String(), "apply started")

	// The apply handler runs as a goroutine — wait for the apply plan confirmation comment
	select {
	case body := <-result.comments:
		assert.Contains(t, body, "Schema Change Plan (Apply)")
		assert.Contains(t, body, "CREATE TABLE")
		assert.Contains(t, body, dbName)
		assert.Contains(t, body, "Lock acquired by")
		assert.Contains(t, body, "schemabot apply-confirm -e staging")
		assert.Contains(t, body, "schemabot unlock")
	case <-time.After(30 * time.Second):
		t.Fatal("timed out waiting for apply plan comment")
	}

	// Verify lock was acquired
	lock, err := svc.Storage().Locks().Get(t.Context(), dbName, "mysql")
	require.NoError(t, err)
	require.NotNil(t, lock, "expected lock to be acquired")
	assert.Equal(t, "octocat/hello-world#1", lock.Owner)
	assert.Equal(t, "octocat/hello-world", lock.Repository)
	assert.Equal(t, 1, lock.PullRequest)

	// Clean up the lock so it doesn't leak to other tests sharing the same PR
	t.Cleanup(func() {
		_ = svc.Storage().Locks().ForceRelease(context.WithoutCancel(t.Context()), dbName, "mysql")
	})

	// Verify check run was created with action_required
	select {
	case cr := <-result.checkRuns:
		assert.Contains(t, cr.Name, "SchemaBot")
		assert.Equal(t, "completed", cr.Status)
		assert.Equal(t, "action_required", cr.Conclusion)
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for check run")
	}
}

func TestE2EApplyNoChanges(t *testing.T) {
	dbName := "webhook_apply_nochanges"
	svc := setupE2EService(t, dbName)

	// Pre-create the table so there are no changes
	seedTargetTable(t, dbName,
		"CREATE TABLE `users` (\n  `id` bigint unsigned NOT NULL AUTO_INCREMENT,\n  `name` varchar(255) NOT NULL,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci")

	mux := http.NewServeMux()
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)

	client := gh.NewClient(nil)
	client.BaseURL, _ = url.Parse(server.URL + "/")

	schemabotConfig := fmt.Sprintf("database: %s\ntype: mysql\n", dbName)
	schemaFiles := map[string]string{
		"users.sql": "CREATE TABLE `users` (\n  `id` bigint unsigned NOT NULL AUTO_INCREMENT,\n  `name` varchar(255) NOT NULL,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;",
	}

	result := setupFakeGitHubForPlan(t, mux, schemaFiles, schemabotConfig, dbName)

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	installClient := ghclient.NewInstallationClient(client, logger)
	factory := &fakeClientFactory{client: installClient}

	h := NewHandler(svc, factory, nil, logger)

	req := buildWebhookRequest(t, webhookPayloadOpts{
		comment: "schemabot apply -e staging",
		isPR:    true,
	}, nil)

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	require.Equal(t, http.StatusOK, rr.Code)

	// No changes → should post regular plan comment (not apply plan), no lock acquired
	select {
	case body := <-result.comments:
		assert.Contains(t, body, "No schema changes detected")
		assert.NotContains(t, body, "Lock acquired")
	case <-time.After(30 * time.Second):
		t.Fatal("timed out waiting for comment")
	}

	// Verify no lock was acquired
	lock, err := svc.Storage().Locks().Get(t.Context(), dbName, "mysql")
	require.NoError(t, err)
	assert.Nil(t, lock, "expected no lock when there are no changes")
}

func TestE2EApplyLockConflictDifferentPR(t *testing.T) {
	dbName := "webhook_apply_conflict"
	svc := setupE2EService(t, dbName)

	// Pre-acquire a lock from a different PR
	err := svc.Storage().Locks().Acquire(t.Context(), &storage.Lock{
		DatabaseName: dbName,
		DatabaseType: "mysql",
		Repository:   "other-org/other-repo",
		PullRequest:  42,
		Owner:        "other-org/other-repo#42",
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = svc.Storage().Locks().ForceRelease(context.WithoutCancel(t.Context()), dbName, "mysql")
	})

	mux := http.NewServeMux()
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)

	client := gh.NewClient(nil)
	client.BaseURL, _ = url.Parse(server.URL + "/")

	schemabotConfig := fmt.Sprintf("database: %s\ntype: mysql\n", dbName)
	schemaFiles := map[string]string{
		"users.sql": "CREATE TABLE `users` (\n  `id` bigint unsigned NOT NULL AUTO_INCREMENT,\n  `name` varchar(255) NOT NULL,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;",
	}

	result := setupFakeGitHubForPlan(t, mux, schemaFiles, schemabotConfig, dbName)

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	installClient := ghclient.NewInstallationClient(client, logger)
	factory := &fakeClientFactory{client: installClient}

	h := NewHandler(svc, factory, nil, logger)

	req := buildWebhookRequest(t, webhookPayloadOpts{
		comment: "schemabot apply -e staging",
		isPR:    true,
	}, nil)

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	require.Equal(t, http.StatusOK, rr.Code)

	// Should post "blocked by other PR" comment
	select {
	case body := <-result.comments:
		assert.Contains(t, body, "Apply Blocked")
		assert.Contains(t, body, "other-org/other-repo#42")
		assert.Contains(t, body, "https://github.com/other-org/other-repo/pull/42")
	case <-time.After(30 * time.Second):
		t.Fatal("timed out waiting for blocked comment")
	}
}

func TestE2EApplyConfirmNoLock(t *testing.T) {
	dbName := "webhook_confirm_nolock"
	svc := setupE2EService(t, dbName)

	mux := http.NewServeMux()
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)

	client := gh.NewClient(nil)
	client.BaseURL, _ = url.Parse(server.URL + "/")

	schemabotConfig := fmt.Sprintf("database: %s\ntype: mysql\n", dbName)
	schemaFiles := map[string]string{
		"users.sql": "CREATE TABLE `users` (\n  `id` bigint unsigned NOT NULL AUTO_INCREMENT,\n  `name` varchar(255) NOT NULL,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;",
	}

	result := setupFakeGitHubForPlan(t, mux, schemaFiles, schemabotConfig, dbName)

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	installClient := ghclient.NewInstallationClient(client, logger)
	factory := &fakeClientFactory{client: installClient}

	h := NewHandler(svc, factory, nil, logger)

	// apply-confirm without a prior apply (no lock)
	req := buildWebhookRequest(t, webhookPayloadOpts{
		comment: "schemabot apply-confirm -e staging",
		isPR:    true,
	}, nil)

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	require.Equal(t, http.StatusOK, rr.Code)

	// Should post "no lock found" comment
	select {
	case body := <-result.comments:
		assert.Contains(t, body, "No Lock Found")
		assert.Contains(t, body, "schemabot apply -e staging")
	case <-time.After(30 * time.Second):
		t.Fatal("timed out waiting for no-lock comment")
	}
}

func TestE2EUnlock(t *testing.T) {
	dbName := "webhook_unlock"
	svc := setupE2EService(t, dbName)

	// Acquire a lock from this PR
	err := svc.Storage().Locks().Acquire(t.Context(), &storage.Lock{
		DatabaseName: dbName,
		DatabaseType: "mysql",
		Repository:   "octocat/hello-world",
		PullRequest:  1,
		Owner:        "octocat/hello-world#1",
	})
	require.NoError(t, err)

	mux := http.NewServeMux()
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)

	client := gh.NewClient(nil)
	client.BaseURL, _ = url.Parse(server.URL + "/")

	// For unlock, we still need PR info for the check run
	mux.HandleFunc("GET /repos/octocat/hello-world/pulls/1", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(gh.PullRequest{
			Head: &gh.PullRequestBranch{
				Ref: new("feature-branch"),
				SHA: new("abc123"),
			},
			Base: &gh.PullRequestBranch{
				Ref: new("main"),
				SHA: new("def456"),
			},
			User: &gh.User{Login: new("testuser")},
		})
	})

	comments := make(chan string, 10)
	reactions := make(chan string, 10)
	checkRuns := make(chan checkRunCapture, 10)

	mux.HandleFunc("POST /repos/octocat/hello-world/issues/1/comments", func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Body string `json:"body"`
		}
		_ = json.NewDecoder(r.Body).Decode(&body)
		comments <- body.Body
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(map[string]any{"id": 99})
	})
	mux.HandleFunc("POST /repos/octocat/hello-world/issues/comments/42/reactions", func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Content string `json:"content"`
		}
		_ = json.NewDecoder(r.Body).Decode(&body)
		reactions <- body.Content
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(map[string]any{"id": 1})
	})
	mux.HandleFunc("POST /repos/octocat/hello-world/check-runs", func(w http.ResponseWriter, r *http.Request) {
		var body checkRunCapture
		_ = json.NewDecoder(r.Body).Decode(&body)
		checkRuns <- body
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(map[string]any{"id": 1})
	})

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	installClient := ghclient.NewInstallationClient(client, logger)
	factory := &fakeClientFactory{client: installClient}

	h := NewHandler(svc, factory, nil, logger)

	req := buildWebhookRequest(t, webhookPayloadOpts{
		comment: "schemabot unlock",
		isPR:    true,
	}, nil)

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	require.Equal(t, http.StatusOK, rr.Code)
	assert.Contains(t, rr.Body.String(), "unlock started")

	// Should post "lock released" comment
	select {
	case body := <-comments:
		assert.Contains(t, body, "Lock Released")
		assert.Contains(t, body, dbName)
		assert.Contains(t, body, "@testuser")
	case <-time.After(30 * time.Second):
		t.Fatal("timed out waiting for unlock comment")
	}

	// Verify lock was released
	lock, err := svc.Storage().Locks().Get(t.Context(), dbName, "mysql")
	require.NoError(t, err)
	assert.Nil(t, lock, "expected lock to be released")

	// Verify check run updated to neutral
	select {
	case cr := <-checkRuns:
		assert.Equal(t, "completed", cr.Status)
		assert.Equal(t, "neutral", cr.Conclusion)
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for check run")
	}
}

// TestE2EUnlockDoesNotPassAggregateWithPendingChanges verifies that releasing
// a PR lock does not make the aggregate SchemaBot check pass while the PR still
// has unapplied schema changes.
func TestE2EUnlockDoesNotPassAggregateWithPendingChanges(t *testing.T) {
	dbName := "webhook_unlock_pending"
	svc := setupE2EService(t, dbName)
	ctx := t.Context()

	// Seed the state after `schemabot apply` has planned work and acquired the
	// PR-owned lock, but before those changes have been applied.
	err := svc.Storage().Locks().Acquire(ctx, &storage.Lock{
		DatabaseName: dbName,
		DatabaseType: "mysql",
		Repository:   "octocat/hello-world",
		PullRequest:  1,
		Owner:        "octocat/hello-world#1",
	})
	require.NoError(t, err)

	// Both the per-database stored check state and the visible aggregate check
	// are action_required because schema changes are still waiting for apply.
	require.NoError(t, svc.Storage().Checks().Upsert(ctx, &storage.Check{
		Repository:   "octocat/hello-world",
		PullRequest:  1,
		HeadSHA:      "abc123",
		Environment:  "staging",
		DatabaseType: "mysql",
		DatabaseName: dbName,
		CheckRunID:   42,
		HasChanges:   true,
		Status:       checkStatusCompleted,
		Conclusion:   checkConclusionActionRequired,
	}))
	require.NoError(t, svc.Storage().Checks().Upsert(ctx, &storage.Check{
		Repository:   "octocat/hello-world",
		PullRequest:  1,
		HeadSHA:      "abc123",
		Environment:  aggregateSentinel,
		DatabaseType: aggregateSentinel,
		DatabaseName: aggregateSentinel,
		CheckRunID:   43,
		HasChanges:   true,
		Status:       checkStatusCompleted,
		Conclusion:   checkConclusionActionRequired,
	}))

	mux := http.NewServeMux()
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)

	client := gh.NewClient(nil)
	client.BaseURL, _ = url.Parse(server.URL + "/")

	// The current PR commit is still the same commit that owns the pending plan.
	mux.HandleFunc("GET /repos/octocat/hello-world/pulls/1", func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(gh.PullRequest{
			Head: &gh.PullRequestBranch{
				Ref: new("feature-branch"),
				SHA: new("abc123"),
			},
			Base: &gh.PullRequestBranch{
				Ref: new("main"),
				SHA: new("def456"),
			},
			User: &gh.User{Login: new("testuser")},
		})
	})

	comments := make(chan string, 10)
	checkRuns := make(chan checkRunCapture, 10)

	mux.HandleFunc("POST /repos/octocat/hello-world/issues/1/comments", func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Body string `json:"body"`
		}
		_ = json.NewDecoder(r.Body).Decode(&body)
		comments <- body.Body
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(map[string]any{"id": 99})
	})
	mux.HandleFunc("POST /repos/octocat/hello-world/issues/comments/42/reactions", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(map[string]any{"id": 1})
	})
	mux.HandleFunc("POST /repos/octocat/hello-world/check-runs", func(w http.ResponseWriter, r *http.Request) {
		var body checkRunCapture
		_ = json.NewDecoder(r.Body).Decode(&body)
		checkRuns <- body
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(map[string]any{"id": 1})
	})

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	installClient := ghclient.NewInstallationClient(client, logger)
	factory := &fakeClientFactory{client: installClient}
	h := NewHandler(svc, factory, nil, logger)

	req := buildWebhookRequest(t, webhookPayloadOpts{
		comment: "schemabot unlock",
		isPR:    true,
	}, nil)

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	require.Equal(t, http.StatusOK, rr.Code)

	select {
	case body := <-comments:
		assert.Contains(t, body, "Lock Released")
	case <-time.After(webhookIntegrationPollDeadline):
		t.Fatal("timed out waiting for unlock comment")
	}

	lock, err := svc.Storage().Locks().Get(ctx, dbName, "mysql")
	require.NoError(t, err)
	assert.Nil(t, lock)

	// Unlock updates the command-specific "SchemaBot Apply" check to neutral,
	// but must not make the aggregate safety gate pass.
	select {
	case cr := <-checkRuns:
		assert.Contains(t, cr.Name, "SchemaBot Apply")
		assert.Equal(t, checkStatusCompleted, cr.Status)
		assert.Equal(t, checkConclusionNeutral, cr.Conclusion)
	case <-time.After(webhookIntegrationCheckRunDeadline):
		t.Fatal("timed out waiting for unlock check run")
	}

	check, err := svc.Storage().Checks().Get(ctx, "octocat/hello-world", 1, "staging", "mysql", dbName)
	require.NoError(t, err)
	require.NotNil(t, check)
	assert.Equal(t, checkConclusionActionRequired, check.Conclusion)

	aggregate, err := svc.Storage().Checks().Get(ctx, "octocat/hello-world", 1, aggregateSentinel, aggregateSentinel, aggregateSentinel)
	require.NoError(t, err)
	require.NotNil(t, aggregate)
	assert.Equal(t, checkConclusionActionRequired, aggregate.Conclusion)
}

func TestE2EApplyConfirmExecutesApply(t *testing.T) {
	dbName := "webhook_confirm_apply"
	svc := setupE2EService(t, dbName)

	// Seed a check record (simulating a prior plan that created the check run)
	seedCheck(t, svc, dbName, "staging", "action_required")

	// Acquire a lock from this PR (simulating a prior `apply` command)
	err := svc.Storage().Locks().Acquire(t.Context(), &storage.Lock{
		DatabaseName: dbName,
		DatabaseType: "mysql",
		Repository:   "octocat/hello-world",
		PullRequest:  1,
		Owner:        "octocat/hello-world#1",
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = svc.Storage().Locks().ForceRelease(context.WithoutCancel(t.Context()), dbName, "mysql")
	})

	mux := http.NewServeMux()
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)

	client := gh.NewClient(nil)
	client.BaseURL, _ = url.Parse(server.URL + "/")

	schemabotConfig := fmt.Sprintf("database: %s\ntype: mysql\n", dbName)
	schemaFiles := map[string]string{
		"users.sql": "CREATE TABLE `users` (\n  `id` bigint unsigned NOT NULL AUTO_INCREMENT,\n  `name` varchar(255) NOT NULL,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;",
	}

	result := setupFakeGitHubForPlan(t, mux, schemaFiles, schemabotConfig, dbName)

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	installClient := ghclient.NewInstallationClient(client, logger)
	factory := &fakeClientFactory{client: installClient}

	h := NewHandler(svc, factory, nil, logger)

	req := buildWebhookRequest(t, webhookPayloadOpts{
		comment: "schemabot apply-confirm -e staging",
		isPR:    true,
	}, nil)

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	require.Equal(t, http.StatusOK, rr.Code)
	assert.Contains(t, rr.Body.String(), "apply-confirm started")

	// The apply may complete synchronously (instant DDL) or asynchronously.
	// For a CREATE TABLE, it completes fast — the sync handler catches it and
	// posts the summary directly without an "In Progress" comment.
	//
	// Wait for either "Schema Change In Progress" or "Schema Change Applied/Failed"
	// as the first comment.
	select {
	case body := <-result.comments:
		hasProgress := strings.Contains(body, "Schema Change In Progress")
		hasApplied := strings.Contains(body, "Schema Change Applied")
		hasFailed := strings.Contains(body, "Schema Change Failed")
		assert.True(t, hasProgress || hasApplied || hasFailed,
			"expected progress or summary comment, got: %s", body[:min(len(body), 200)])

		// If we got a progress comment, wait for the summary too
		if hasProgress {
			select {
			case summary := <-result.comments:
				assert.True(t,
					strings.Contains(summary, "Schema Change Applied") || strings.Contains(summary, "Schema Change Failed"),
					"expected summary comment")
			case <-time.After(60 * time.Second):
				t.Fatal("timed out waiting for summary comment")
			}
		}
	case <-time.After(30 * time.Second):
		t.Fatal("timed out waiting for any apply comment")
	}

	// Verify apply was created in storage
	applies, err := svc.Storage().Applies().GetByPR(t.Context(), "octocat/hello-world", 1)
	require.NoError(t, err)
	var ourApply *storage.Apply
	for _, a := range applies {
		if a.Database == dbName {
			ourApply = a
			break
		}
	}
	require.NotNil(t, ourApply, "expected an apply record for database %s", dbName)
	assert.Equal(t, dbName, ourApply.Database)
	assert.Equal(t, "mysql", ourApply.DatabaseType)
	assert.Equal(t, "staging", ourApply.Environment)
	assert.Contains(t, ourApply.Caller, "github:testuser@octocat/hello-world#1")

	// After terminal state, the check run should be updated in storage.
	require.Eventually(t, func() bool {
		check, err := svc.Storage().Checks().Get(t.Context(), "octocat/hello-world", 1, "staging", "mysql", dbName)
		if err != nil || check == nil {
			return false
		}
		return check.Conclusion == "success"
	}, 30*time.Second, 200*time.Millisecond,
		"expected check to transition to success after apply completion")
}

func TestE2EUnlockNoLocks(t *testing.T) {
	dbName := "webhook_unlock_none"
	svc := setupE2EService(t, dbName)

	mux := http.NewServeMux()
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)

	client := gh.NewClient(nil)
	client.BaseURL, _ = url.Parse(server.URL + "/")

	comments := make(chan string, 10)
	reactions := make(chan string, 10)

	mux.HandleFunc("POST /repos/octocat/hello-world/issues/1/comments", func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Body string `json:"body"`
		}
		_ = json.NewDecoder(r.Body).Decode(&body)
		comments <- body.Body
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(map[string]any{"id": 99})
	})
	mux.HandleFunc("POST /repos/octocat/hello-world/issues/comments/42/reactions", func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Content string `json:"content"`
		}
		_ = json.NewDecoder(r.Body).Decode(&body)
		reactions <- body.Content
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(map[string]any{"id": 1})
	})

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	installClient := ghclient.NewInstallationClient(client, logger)
	factory := &fakeClientFactory{client: installClient}

	h := NewHandler(svc, factory, nil, logger)

	req := buildWebhookRequest(t, webhookPayloadOpts{
		comment: "schemabot unlock",
		isPR:    true,
	}, nil)

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	require.Equal(t, http.StatusOK, rr.Code)

	select {
	case body := <-comments:
		assert.Contains(t, body, "No Locks Found")
	case <-time.After(30 * time.Second):
		t.Fatal("timed out waiting for no-locks comment")
	}
}

func TestE2EApplySamePRActiveApply(t *testing.T) {
	dbName := "webhook_apply_active"
	svc := setupE2EService(t, dbName)

	// Acquire a lock from this PR
	err := svc.Storage().Locks().Acquire(t.Context(), &storage.Lock{
		DatabaseName: dbName,
		DatabaseType: "mysql",
		Repository:   "octocat/hello-world",
		PullRequest:  1,
		Owner:        "octocat/hello-world#1",
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = svc.Storage().Locks().ForceRelease(context.WithoutCancel(t.Context()), dbName, "mysql")
	})

	// Get the lock to obtain its ID
	lock, err := svc.Storage().Locks().Get(t.Context(), dbName, "mysql")
	require.NoError(t, err)

	// Create a non-terminal apply record linked to this lock
	_, err = svc.Storage().Applies().Create(t.Context(), &storage.Apply{
		ApplyIdentifier: "apply-active-test-001",
		LockID:          lock.ID,
		PlanID:          0,
		Database:        dbName,
		DatabaseType:    "mysql",
		Repository:      "octocat/hello-world",
		PullRequest:     1,
		Environment:     "staging",
		Engine:          "spirit",
		State:           "running",
	})
	require.NoError(t, err)

	mux := http.NewServeMux()
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)

	client := gh.NewClient(nil)
	client.BaseURL, _ = url.Parse(server.URL + "/")

	schemabotConfig := fmt.Sprintf("database: %s\ntype: mysql\n", dbName)
	schemaFiles := map[string]string{
		"users.sql": "CREATE TABLE `users` (\n  `id` bigint unsigned NOT NULL AUTO_INCREMENT,\n  `name` varchar(255) NOT NULL,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;",
	}

	result := setupFakeGitHubForPlan(t, mux, schemaFiles, schemabotConfig, dbName)

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	installClient := ghclient.NewInstallationClient(client, logger)
	factory := &fakeClientFactory{client: installClient}

	h := NewHandler(svc, factory, nil, logger)

	req := buildWebhookRequest(t, webhookPayloadOpts{
		comment: "schemabot apply -e staging",
		isPR:    true,
	}, nil)

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	require.Equal(t, http.StatusOK, rr.Code)

	select {
	case body := <-result.comments:
		assert.Contains(t, body, "Apply Already In Progress")
		assert.Contains(t, body, "apply-active-test-001")
		assert.Contains(t, body, "running")
	case <-time.After(30 * time.Second):
		t.Fatal("timed out waiting for active-apply comment")
	}
}

func TestE2EApplyConfirmLockConflict(t *testing.T) {
	dbName := "webhook_confirm_conflict"
	svc := setupE2EService(t, dbName)

	// Acquire a lock from a different PR
	err := svc.Storage().Locks().Acquire(t.Context(), &storage.Lock{
		DatabaseName: dbName,
		DatabaseType: "mysql",
		Repository:   "other-org/other-repo",
		PullRequest:  42,
		Owner:        "other-org/other-repo#42",
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = svc.Storage().Locks().ForceRelease(context.WithoutCancel(t.Context()), dbName, "mysql")
	})

	mux := http.NewServeMux()
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)

	client := gh.NewClient(nil)
	client.BaseURL, _ = url.Parse(server.URL + "/")

	schemabotConfig := fmt.Sprintf("database: %s\ntype: mysql\n", dbName)
	schemaFiles := map[string]string{
		"users.sql": "CREATE TABLE `users` (\n  `id` bigint unsigned NOT NULL AUTO_INCREMENT,\n  `name` varchar(255) NOT NULL,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;",
	}

	result := setupFakeGitHubForPlan(t, mux, schemaFiles, schemabotConfig, dbName)

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	installClient := ghclient.NewInstallationClient(client, logger)
	factory := &fakeClientFactory{client: installClient}

	h := NewHandler(svc, factory, nil, logger)

	req := buildWebhookRequest(t, webhookPayloadOpts{
		comment: "schemabot apply-confirm -e staging",
		isPR:    true,
	}, nil)

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	require.Equal(t, http.StatusOK, rr.Code)

	select {
	case body := <-result.comments:
		assert.Contains(t, body, "Apply Blocked")
		assert.Contains(t, body, "other-org/other-repo#42")
	case <-time.After(30 * time.Second):
		t.Fatal("timed out waiting for blocked comment")
	}
}

func TestE2EApplyConfirmNoChanges(t *testing.T) {
	dbName := "webhook_confirm_nochange"
	svc := setupE2EService(t, dbName)

	// Pre-create the table so there are no changes when re-planning
	seedTargetTable(t, dbName,
		"CREATE TABLE `users` (\n  `id` bigint unsigned NOT NULL AUTO_INCREMENT,\n  `name` varchar(255) NOT NULL,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci")

	// Acquire a lock from this PR (simulating a prior `apply` command)
	err := svc.Storage().Locks().Acquire(t.Context(), &storage.Lock{
		DatabaseName: dbName,
		DatabaseType: "mysql",
		Repository:   "octocat/hello-world",
		PullRequest:  1,
		Owner:        "octocat/hello-world#1",
	})
	require.NoError(t, err)

	mux := http.NewServeMux()
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)

	client := gh.NewClient(nil)
	client.BaseURL, _ = url.Parse(server.URL + "/")

	schemabotConfig := fmt.Sprintf("database: %s\ntype: mysql\n", dbName)
	schemaFiles := map[string]string{
		"users.sql": "CREATE TABLE `users` (\n  `id` bigint unsigned NOT NULL AUTO_INCREMENT,\n  `name` varchar(255) NOT NULL,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;",
	}

	result := setupFakeGitHubForPlan(t, mux, schemaFiles, schemabotConfig, dbName)

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	installClient := ghclient.NewInstallationClient(client, logger)
	factory := &fakeClientFactory{client: installClient}

	h := NewHandler(svc, factory, nil, logger)

	req := buildWebhookRequest(t, webhookPayloadOpts{
		comment: "schemabot apply-confirm -e staging",
		isPR:    true,
	}, nil)

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	require.Equal(t, http.StatusOK, rr.Code)

	// Should post "no changes" and release lock
	select {
	case body := <-result.comments:
		assert.Contains(t, body, "No Changes Detected")
		assert.Contains(t, body, "already up to date")
	case <-time.After(30 * time.Second):
		t.Fatal("timed out waiting for no-changes comment")
	}

	// Lock release happens asynchronously after the comment is posted —
	// poll until the goroutine completes the ForceRelease call.
	require.Eventually(t, func() bool {
		lock, err := svc.Storage().Locks().Get(t.Context(), dbName, "mysql")
		return err == nil && lock == nil
	}, 5*time.Second, 100*time.Millisecond, "expected lock to be released after no-changes confirm")
}

func TestE2EUnlockBlockedByActiveApply(t *testing.T) {
	dbName := "webhook_unlock_blocked"
	svc := setupE2EService(t, dbName)

	// Acquire a lock from this PR
	err := svc.Storage().Locks().Acquire(t.Context(), &storage.Lock{
		DatabaseName: dbName,
		DatabaseType: "mysql",
		Repository:   "octocat/hello-world",
		PullRequest:  1,
		Owner:        "octocat/hello-world#1",
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = svc.Storage().Locks().ForceRelease(context.WithoutCancel(t.Context()), dbName, "mysql")
	})

	// Get the lock to obtain its ID
	lock, err := svc.Storage().Locks().Get(t.Context(), dbName, "mysql")
	require.NoError(t, err)

	// Create a non-terminal apply record
	_, err = svc.Storage().Applies().Create(t.Context(), &storage.Apply{
		ApplyIdentifier: "apply-blocked-unlock-001",
		LockID:          lock.ID,
		PlanID:          0,
		Database:        dbName,
		DatabaseType:    "mysql",
		Repository:      "octocat/hello-world",
		PullRequest:     1,
		Environment:     "staging",
		Engine:          "spirit",
		State:           "running",
	})
	require.NoError(t, err)

	mux := http.NewServeMux()
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)

	client := gh.NewClient(nil)
	client.BaseURL, _ = url.Parse(server.URL + "/")

	comments := make(chan string, 10)
	reactions := make(chan string, 10)

	mux.HandleFunc("POST /repos/octocat/hello-world/issues/1/comments", func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Body string `json:"body"`
		}
		_ = json.NewDecoder(r.Body).Decode(&body)
		comments <- body.Body
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(map[string]any{"id": 99})
	})
	mux.HandleFunc("POST /repos/octocat/hello-world/issues/comments/42/reactions", func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Content string `json:"content"`
		}
		_ = json.NewDecoder(r.Body).Decode(&body)
		reactions <- body.Content
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(map[string]any{"id": 1})
	})

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	installClient := ghclient.NewInstallationClient(client, logger)
	factory := &fakeClientFactory{client: installClient}

	h := NewHandler(svc, factory, nil, logger)

	req := buildWebhookRequest(t, webhookPayloadOpts{
		comment: "schemabot unlock",
		isPR:    true,
	}, nil)

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	require.Equal(t, http.StatusOK, rr.Code)

	select {
	case body := <-comments:
		assert.Contains(t, body, "Cannot Unlock")
		assert.Contains(t, body, "apply-blocked-unlock-001")
		assert.Contains(t, body, "running")
	case <-time.After(30 * time.Second):
		t.Fatal("timed out waiting for cannot-unlock comment")
	}

	// Lock should still be held
	existingLock, err := svc.Storage().Locks().Get(t.Context(), dbName, "mysql")
	require.NoError(t, err)
	assert.NotNil(t, existingLock, "expected lock to still be held")
}

func TestE2EApplyStaleLockReacquire(t *testing.T) {
	dbName := "webhook_apply_stale"
	svc := setupE2EService(t, dbName)

	// Acquire a stale lock from the same PR (no active applies)
	err := svc.Storage().Locks().Acquire(t.Context(), &storage.Lock{
		DatabaseName: dbName,
		DatabaseType: "mysql",
		Repository:   "octocat/hello-world",
		PullRequest:  1,
		Owner:        "octocat/hello-world#1",
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = svc.Storage().Locks().ForceRelease(context.WithoutCancel(t.Context()), dbName, "mysql")
	})

	mux := http.NewServeMux()
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)

	client := gh.NewClient(nil)
	client.BaseURL, _ = url.Parse(server.URL + "/")

	schemabotConfig := fmt.Sprintf("database: %s\ntype: mysql\n", dbName)
	schemaFiles := map[string]string{
		"users.sql": "CREATE TABLE `users` (\n  `id` bigint unsigned NOT NULL AUTO_INCREMENT,\n  `name` varchar(255) NOT NULL,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;",
	}

	result := setupFakeGitHubForPlan(t, mux, schemaFiles, schemabotConfig, dbName)

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	installClient := ghclient.NewInstallationClient(client, logger)
	factory := &fakeClientFactory{client: installClient}

	h := NewHandler(svc, factory, nil, logger)

	req := buildWebhookRequest(t, webhookPayloadOpts{
		comment: "schemabot apply -e staging",
		isPR:    true,
	}, nil)

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	require.Equal(t, http.StatusOK, rr.Code)

	// Should succeed: release stale lock, re-plan, acquire new lock, post confirmation
	select {
	case body := <-result.comments:
		assert.Contains(t, body, "Schema Change Plan (Apply)")
		assert.Contains(t, body, "Lock acquired by")
		assert.Contains(t, body, "schemabot apply-confirm -e staging")
	case <-time.After(30 * time.Second):
		t.Fatal("timed out waiting for apply plan comment")
	}

	// Verify a lock is still held (the new one)
	lock, err := svc.Storage().Locks().Get(t.Context(), dbName, "mysql")
	require.NoError(t, err)
	require.NotNil(t, lock, "expected lock to be re-acquired")
	assert.Equal(t, "octocat/hello-world#1", lock.Owner)
}

// TestE2EApplyProductionBlockedByStagingFirst verifies that production apply is
// blocked when staging has unapplied changes.
func TestE2EApplyProductionBlockedByStagingFirst(t *testing.T) {
	dbName := "webhook_staging_first"
	svc := setupE2EService(t, dbName)

	// Seed a staging check that is NOT success (action_required — unapplied changes)
	seedCheck(t, svc, dbName, "staging", "action_required")

	mux := http.NewServeMux()
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)

	client := gh.NewClient(nil)
	client.BaseURL, _ = url.Parse(server.URL + "/")

	// Client config can enable environments, but cannot control promotion order.
	// Production listed before staging must still be gated by staging.
	schemabotConfig := fmt.Sprintf("database: %s\ntype: mysql\nenvironments:\n  - production\n  - staging\n", dbName)
	schemaFiles := map[string]string{
		"users.sql": "CREATE TABLE `users` (\n  `id` bigint unsigned NOT NULL AUTO_INCREMENT,\n  `name` varchar(255) NOT NULL,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;",
	}

	result := setupFakeGitHubForPlan(t, mux, schemaFiles, schemabotConfig, dbName)

	h := newE2EHandler(t, svc, client)

	// Try to apply production — should be blocked
	req := buildWebhookRequest(t, webhookPayloadOpts{
		comment: "schemabot apply -e production",
		isPR:    true,
	}, nil)

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	require.Equal(t, http.StatusOK, rr.Code)

	select {
	case body := <-result.comments:
		assert.Contains(t, body, "Apply Blocked")
		assert.Contains(t, body, "Staging")
		assert.Contains(t, body, "schemabot apply -e staging")
	case <-time.After(30 * time.Second):
		t.Fatal("timed out waiting for blocked comment")
	}
}

// TestE2EApplyUsesCustomServerEnvironmentOrder verifies that environment_order
// in server config controls promotion order. The repo lists staging before
// production, but this server config makes production the prior environment.
func TestE2EApplyUsesCustomServerEnvironmentOrder(t *testing.T) {
	dbName := "webhook_custom_env_order"
	svc := setupE2EService(t, dbName)
	svc.Config().EnvironmentOrder = []string{"production", "staging"}

	seedCheck(t, svc, dbName, "production", "action_required")

	mux := http.NewServeMux()
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)

	client := gh.NewClient(nil)
	client.BaseURL, _ = url.Parse(server.URL + "/")

	schemabotConfig := fmt.Sprintf("database: %s\ntype: mysql\nenvironments:\n  - staging\n  - production\n", dbName)
	schemaFiles := map[string]string{
		"users.sql": "CREATE TABLE `users` (\n  `id` bigint unsigned NOT NULL AUTO_INCREMENT,\n  `name` varchar(255) NOT NULL,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;",
	}

	result := setupFakeGitHubForPlan(t, mux, schemaFiles, schemabotConfig, dbName)
	h := newE2EHandler(t, svc, client)

	req := buildWebhookRequest(t, webhookPayloadOpts{
		comment: "schemabot apply -e staging",
		isPR:    true,
	}, nil)

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	require.Equal(t, http.StatusOK, rr.Code)

	select {
	case body := <-result.comments:
		assert.Contains(t, body, "Apply Blocked")
		assert.Contains(t, body, "Production")
		assert.Contains(t, body, "Apply production first")
	case <-time.After(30 * time.Second):
		t.Fatal("timed out waiting for custom-order blocked comment")
	}
}

// TestE2EApplyProductionAllowedWhenStagingSuccess verifies that production apply
// is allowed when staging check is success.
func TestE2EApplyProductionAllowedWhenStagingSuccess(t *testing.T) {
	dbName := "webhook_staging_ok"
	svc := setupE2EService(t, dbName)

	// Seed a staging check that IS success
	seedCheck(t, svc, dbName, "staging", "success")

	mux := http.NewServeMux()
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)

	client := gh.NewClient(nil)
	client.BaseURL, _ = url.Parse(server.URL + "/")

	schemabotConfig := fmt.Sprintf("database: %s\ntype: mysql\nenvironments:\n  - staging\n  - production\n", dbName)
	schemaFiles := map[string]string{
		"users.sql": "CREATE TABLE `users` (\n  `id` bigint unsigned NOT NULL AUTO_INCREMENT,\n  `name` varchar(255) NOT NULL,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;",
	}

	result := setupFakeGitHubForPlan(t, mux, schemaFiles, schemabotConfig, dbName)

	h := newE2EHandler(t, svc, client)

	// Try to apply production — should be allowed (staging is success)
	req := buildWebhookRequest(t, webhookPayloadOpts{
		comment: "schemabot apply -e production",
		isPR:    true,
	}, nil)

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	require.Equal(t, http.StatusOK, rr.Code)
	assert.Contains(t, rr.Body.String(), "apply started")

	// Should NOT get a "blocked" message — the staging-first check passed.
	// The apply may fail for other reasons (e.g., no production deployment in test)
	// but the point is it wasn't blocked by staging-first enforcement.
	select {
	case body := <-result.comments:
		assert.NotContains(t, body, "Apply Blocked",
			"production apply should not be blocked when staging is success")
	case <-time.After(30 * time.Second):
		t.Fatal("timed out waiting for comment")
	}

	// Clean up lock
	t.Cleanup(func() {
		_ = svc.Storage().Locks().ForceRelease(context.WithoutCancel(t.Context()), dbName, "mysql")
	})
}

// TestE2EApplyAutoConfirmExecutes verifies that `schemabot apply -e staging -y`
// plans, acquires a lock, and executes the apply in a single command.
func TestE2EApplyAutoConfirmExecutes(t *testing.T) {
	dbName := "webhook_auto_confirm"
	svc := setupE2EService(t, dbName)

	// Seed a check record with the correct HEAD SHA so the SHA verification passes
	seedCheck(t, svc, dbName, "staging", checkConclusionActionRequired)

	mux := http.NewServeMux()
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)

	client := gh.NewClient(nil)
	client.BaseURL, _ = url.Parse(server.URL + "/")

	schemabotConfig := fmt.Sprintf("database: %s\ntype: mysql\n", dbName)
	schemaFiles := map[string]string{
		"users.sql": "CREATE TABLE `users` (\n  `id` bigint unsigned NOT NULL AUTO_INCREMENT,\n  `name` varchar(255) NOT NULL,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;",
	}

	result := setupFakeGitHubForPlan(t, mux, schemaFiles, schemabotConfig, dbName)
	h := newE2EHandler(t, svc, client)

	req := buildWebhookRequest(t, webhookPayloadOpts{
		comment: "schemabot apply -e staging -y",
		isPR:    true,
	}, nil)

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	require.Equal(t, http.StatusOK, rr.Code)

	// First comment: plan with "Applying automatically"
	select {
	case body := <-result.comments:
		assert.Contains(t, body, "Schema Change Plan (Apply)")
		assert.Contains(t, body, "Applying automatically")
		assert.Contains(t, body, "-y")
	case <-time.After(30 * time.Second):
		t.Fatal("timed out waiting for auto-confirm plan comment")
	}

	// Wait for summary comment (skip progress comment posted by observer)
	require.Eventually(t, func() bool {
		select {
		case body := <-result.comments:
			return strings.Contains(body, "Schema Change Applied") || strings.Contains(body, "Schema Change Failed")
		default:
			return false
		}
	}, 30*time.Second, 200*time.Millisecond,
		"expected apply summary comment")
}

// TestE2EApplyAutoConfirmRejectsWhenHEADAdvanced verifies that `apply -y`
// rejects (does NOT apply, releases the lock) when the PR HEAD advances
// between discovery (cached FetchPullRequest) and the fresh
// FetchPullRequestNoCache fetch inside the auto-confirm branch. The user
// must re-run the command so discovery picks up the new HEAD.
func TestE2EApplyAutoConfirmRejectsWhenHEADAdvanced(t *testing.T) {
	dbName := "webhook_auto_confirm_stale"
	svc := setupE2EService(t, dbName)

	mux := http.NewServeMux()
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)

	client := gh.NewClient(nil)
	client.BaseURL, _ = url.Parse(server.URL + "/")

	schemabotConfig := fmt.Sprintf("database: %s\ntype: mysql\n", dbName)
	schemaFiles := map[string]string{
		"users.sql": "CREATE TABLE `users` (\n  `id` bigint unsigned NOT NULL AUTO_INCREMENT,\n  `name` varchar(255) NOT NULL,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;",
	}

	result := setupFakeGitHubForPlan(t, mux, schemaFiles, schemabotConfig, dbName)
	// Discovery (cached FetchPullRequest) sees abc123; the NoCache fetch
	// inside the auto-confirm branch sees a new commit at newsha456.
	result.HeadSHAs = []string{"abc123", "newsha456"}
	h := newE2EHandler(t, svc, client)

	req := buildWebhookRequest(t, webhookPayloadOpts{
		comment: "schemabot apply -e staging -y",
		isPR:    true,
	}, nil)

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	require.Equal(t, http.StatusOK, rr.Code)

	// Drain all comments posted during the delivery: we expect at least one
	// rejection comment and zero "Applying automatically" comments.
	deadline := time.After(30 * time.Second)
	var rejection string
	for {
		select {
		case body := <-result.comments:
			assert.NotContains(t, body, "Applying automatically", "apply must not auto-confirm on stale schema")
			if strings.Contains(body, "Rejected") && strings.Contains(body, "new commits since discovery") {
				rejection = body
			}
		case <-deadline:
			t.Fatal("timed out waiting for rejection comment")
		}
		if rejection != "" {
			break
		}
	}

	assert.Contains(t, rejection, "abc123", "rejection must show discovery SHA")
	assert.Contains(t, rejection, "newsha456", "rejection must show current SHA")
	assert.Contains(t, rejection, "schemabot apply -e staging", "retry hint must reference the env")

	// Lock acquired earlier in handleApplyCommand must be released so the user
	// can re-run `schemabot apply` cleanly. The release runs in the handler
	// goroutine after postComment returns, so poll briefly.
	require.Eventually(t, func() bool {
		lock, err := svc.Storage().Locks().Get(t.Context(), dbName, "mysql")
		return err == nil && lock == nil
	}, 5*time.Second, 50*time.Millisecond, "lock must be released after stale-schema rejection")

	// No apply record should exist — executeApply must not have been reached.
	applies, err := svc.Storage().Applies().GetByPR(t.Context(), "octocat/hello-world", 1)
	require.NoError(t, err)
	for _, a := range applies {
		assert.NotEqual(t, dbName, a.Database, "no apply for %s should have been started", dbName)
	}
}

// TestE2EApplyConfirmRejectsWhenHEADAdvanced verifies that `apply-confirm`
// rejects (does NOT apply, releases the lock) when the PR HEAD advances
// between discovery (cached FetchPullRequest) and the fresh
// FetchPullRequestNoCache fetch used by the checks gate.
func TestE2EApplyConfirmRejectsWhenHEADAdvanced(t *testing.T) {
	dbName := "webhook_confirm_stale"
	svc := setupE2EService(t, dbName)

	// apply-confirm requires a pre-existing lock from a prior `apply`.
	require.NoError(t, svc.Storage().Locks().Acquire(t.Context(), &storage.Lock{
		DatabaseName: dbName,
		DatabaseType: "mysql",
		Repository:   "octocat/hello-world",
		PullRequest:  1,
		Owner:        "octocat/hello-world#1",
	}))
	t.Cleanup(func() {
		_ = svc.Storage().Locks().ForceRelease(context.WithoutCancel(t.Context()), dbName, "mysql")
	})

	mux := http.NewServeMux()
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)

	client := gh.NewClient(nil)
	client.BaseURL, _ = url.Parse(server.URL + "/")

	schemabotConfig := fmt.Sprintf("database: %s\ntype: mysql\n", dbName)
	schemaFiles := map[string]string{
		"users.sql": "CREATE TABLE `users` (\n  `id` bigint unsigned NOT NULL AUTO_INCREMENT,\n  `name` varchar(255) NOT NULL,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;",
	}

	result := setupFakeGitHubForPlan(t, mux, schemaFiles, schemabotConfig, dbName)
	result.HeadSHAs = []string{"abc123", "newsha456"}
	h := newE2EHandler(t, svc, client)

	req := buildWebhookRequest(t, webhookPayloadOpts{
		comment: "schemabot apply-confirm -e staging",
		isPR:    true,
	}, nil)

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	require.Equal(t, http.StatusOK, rr.Code)

	select {
	case body := <-result.comments:
		assert.Contains(t, body, "Rejected")
		assert.Contains(t, body, "new commits since discovery")
		assert.Contains(t, body, "abc123", "must show discovery SHA")
		assert.Contains(t, body, "newsha456", "must show current SHA")
		assert.Contains(t, body, "schemabot apply-confirm -e staging", "retry hint must show full retry command")
		assert.NotContains(t, body, "Schema Change In Progress", "apply must not have started")
		assert.NotContains(t, body, "Schema Change Applied", "apply must not have completed")
	case <-time.After(30 * time.Second):
		t.Fatal("timed out waiting for rejection comment")
	}

	// Lock must be released after rejection so the user can re-run `apply`.
	// The release runs in the handler goroutine after postComment returns,
	// so poll briefly.
	require.Eventually(t, func() bool {
		lock, err := svc.Storage().Locks().Get(t.Context(), dbName, "mysql")
		return err == nil && lock == nil
	}, 5*time.Second, 50*time.Millisecond, "lock must be released after stale-schema rejection")

	applies, err := svc.Storage().Applies().GetByPR(t.Context(), "octocat/hello-world", 1)
	require.NoError(t, err)
	for _, a := range applies {
		assert.NotEqual(t, dbName, a.Database, "no apply for %s should have been started", dbName)
	}
}

// TestE2EApplyManualRejectsWhenHEADAdvanced verifies that `schemabot apply`
// (without -y) rejects and releases the lock when the PR HEAD advances
// between discovery and the freshness check, instead of posting a stale
// confirmation plan that the user might `apply-confirm` against. Symmetric
// to TestE2EApplyAutoConfirmRejectsWhenHEADAdvanced but covers the manual
// (non-auto-confirm) path that aparajon flagged in PR #134 review.
//
// Without this guard, a fresh discovery at apply-confirm time would see the
// new HEAD and the confirm-time freshness check would pass — but the plan
// the user reviewed was rendered for the old commit.
func TestE2EApplyManualRejectsWhenHEADAdvanced(t *testing.T) {
	dbName := "webhook_manual_apply_stale"
	svc := setupE2EService(t, dbName)

	mux := http.NewServeMux()
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)

	client := gh.NewClient(nil)
	client.BaseURL, _ = url.Parse(server.URL + "/")

	schemabotConfig := fmt.Sprintf("database: %s\ntype: mysql\n", dbName)
	schemaFiles := map[string]string{
		"users.sql": "CREATE TABLE `users` (\n  `id` bigint unsigned NOT NULL AUTO_INCREMENT,\n  `name` varchar(255) NOT NULL,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;",
	}

	result := setupFakeGitHubForPlan(t, mux, schemaFiles, schemabotConfig, dbName)
	// Discovery (cached FetchPullRequest) sees abc123; the NoCache fetch
	// just before posting the manual plan sees a new commit at newsha456.
	result.HeadSHAs = []string{"abc123", "newsha456"}
	h := newE2EHandler(t, svc, client)

	req := buildWebhookRequest(t, webhookPayloadOpts{
		comment: "schemabot apply -e staging",
		isPR:    true,
	}, nil)

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	require.Equal(t, http.StatusOK, rr.Code)

	// Drain comments looking for the rejection. The handler must not post a
	// plan comment (with or without the confirmation footer).
	deadline := time.After(30 * time.Second)
	var rejection string
	for rejection == "" {
		select {
		case body := <-result.comments:
			assert.NotContains(t, body, "Schema Change Plan", "manual apply must not post a stale plan comment")
			assert.NotContains(t, body, "Applying automatically", "manual apply must never auto-confirm")
			if strings.Contains(body, "Rejected") && strings.Contains(body, "new commits since discovery") {
				rejection = body
			}
		case <-deadline:
			t.Fatal("timed out waiting for rejection comment")
		}
	}

	assert.Contains(t, rejection, "abc123", "rejection must show discovery SHA")
	assert.Contains(t, rejection, "newsha456", "rejection must show current SHA")
	assert.Contains(t, rejection, "schemabot apply -e staging", "retry hint must reference the env")

	// Lock acquired earlier in handleApplyCommand must be released so the
	// user can re-run cleanly. Release runs after postComment; poll briefly.
	require.Eventually(t, func() bool {
		lock, err := svc.Storage().Locks().Get(t.Context(), dbName, "mysql")
		return err == nil && lock == nil
	}, 5*time.Second, 50*time.Millisecond, "lock must be released after stale-schema rejection")
}

// TestE2EApplyConfirmStaleSchemaPreservesOtherPRLock verifies that when
// `apply-confirm` runs on PR #1 and the schema-freshness check rejects, the
// per-target lock held by a different PR (#999) is NOT released. Using
// owner-scoped Release rather than ForceRelease ensures stale-schema
// rejections cannot inadvertently clear an unrelated PR's lock.
func TestE2EApplyConfirmStaleSchemaPreservesOtherPRLock(t *testing.T) {
	dbName := "webhook_confirm_stale_otherlock"
	svc := setupE2EService(t, dbName)

	// Pre-seed a lock owned by a different PR (#999).
	otherOwner := "octocat/hello-world#999"
	require.NoError(t, svc.Storage().Locks().Acquire(t.Context(), &storage.Lock{
		DatabaseName: dbName,
		DatabaseType: "mysql",
		Repository:   "octocat/hello-world",
		PullRequest:  999,
		Owner:        otherOwner,
	}))
	t.Cleanup(func() {
		_ = svc.Storage().Locks().ForceRelease(context.WithoutCancel(t.Context()), dbName, "mysql")
	})

	mux := http.NewServeMux()
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)

	client := gh.NewClient(nil)
	client.BaseURL, _ = url.Parse(server.URL + "/")

	schemabotConfig := fmt.Sprintf("database: %s\ntype: mysql\n", dbName)
	schemaFiles := map[string]string{
		"users.sql": "CREATE TABLE `users` (\n  `id` bigint unsigned NOT NULL AUTO_INCREMENT,\n  `name` varchar(255) NOT NULL,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;",
	}

	result := setupFakeGitHubForPlan(t, mux, schemaFiles, schemabotConfig, dbName)
	// HEAD advances between cached discovery and NoCache fetch, triggering
	// the stale-schema rejection on PR #1.
	result.HeadSHAs = []string{"abc123", "newsha456"}
	h := newE2EHandler(t, svc, client)

	// Webhook delivery for PR #1 — but the lock is held by PR #999.
	req := buildWebhookRequest(t, webhookPayloadOpts{
		comment: "schemabot apply-confirm -e staging",
		isPR:    true,
	}, nil)

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	require.Equal(t, http.StatusOK, rr.Code)

	select {
	case body := <-result.comments:
		assert.Contains(t, body, "Rejected", "stale-schema rejection must still post the rejection comment")
		assert.Contains(t, body, "new commits since discovery")
	case <-time.After(30 * time.Second):
		t.Fatal("timed out waiting for rejection comment")
	}

	// The other PR's lock must remain intact for the full polling window —
	// long enough for the handler's owner-scoped Release attempt to land
	// (and silently no-op as ErrLockNotOwned) before t.Cleanup tears down
	// the containers. require.Never both asserts the lock is preserved and
	// synchronises with the async handler so we don't race shutdown.
	require.Never(t, func() bool {
		lock, err := svc.Storage().Locks().Get(t.Context(), dbName, "mysql")
		return err != nil || lock == nil || lock.Owner != otherOwner
	}, 1*time.Second, 50*time.Millisecond, "other PR's lock must remain intact after stale-schema rejection on PR #1")
}

// TestE2EPlanRejectsWhenHEADAdvanced verifies that `schemabot plan` rejects
// (does NOT post a plan comment rendered against stale schema files) when
// the PR HEAD advances between discovery and the freshness check.
func TestE2EPlanRejectsWhenHEADAdvanced(t *testing.T) {
	dbName := "webhook_plan_stale"
	svc := setupE2EService(t, dbName)

	mux := http.NewServeMux()
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)

	client := gh.NewClient(nil)
	client.BaseURL, _ = url.Parse(server.URL + "/")

	schemabotConfig := fmt.Sprintf("database: %s\ntype: mysql\n", dbName)
	schemaFiles := map[string]string{
		"users.sql": "CREATE TABLE `users` (\n  `id` bigint unsigned NOT NULL AUTO_INCREMENT,\n  `name` varchar(255) NOT NULL,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;",
	}

	result := setupFakeGitHubForPlan(t, mux, schemaFiles, schemabotConfig, dbName)
	result.HeadSHAs = []string{"abc123", "newsha456"}
	h := newE2EHandler(t, svc, client)

	req := buildWebhookRequest(t, webhookPayloadOpts{
		comment: "schemabot plan -e staging",
		isPR:    true,
	}, nil)

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	require.Equal(t, http.StatusOK, rr.Code)

	select {
	case body := <-result.comments:
		assert.Contains(t, body, "Rejected")
		assert.Contains(t, body, "new commits since discovery")
		assert.Contains(t, body, "abc123")
		assert.Contains(t, body, "newsha456")
		assert.Contains(t, body, "schemabot plan -e staging")
		assert.NotContains(t, body, "Schema Change Plan", "stale plan comment must not be posted")
	case <-time.After(30 * time.Second):
		t.Fatal("timed out waiting for rejection comment")
	}
}

// TestE2EApplyAutoConfirmReGatesAgainstFreshHEADBeforeApply verifies that the
// `apply -y` auto-confirm branch re-evaluates the PR checks gate against the
// fresh HEAD immediately before executeApply, even when the HEAD has not
// moved (so the schema-freshness check passes). This catches the case where
// a required check transitioned to failing on the same SHA — e.g. CI re-ran
// red, or a new required check was added — between the discovery-time early
// gate and the apply itself.
func TestE2EApplyAutoConfirmReGatesAgainstFreshHEADBeforeApply(t *testing.T) {
	dbName := "webhook_auto_confirm_regate"
	svc := setupE2EService(t, dbName)

	mux := http.NewServeMux()
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)

	client := gh.NewClient(nil)
	client.BaseURL, _ = url.Parse(server.URL + "/")

	schemabotConfig := fmt.Sprintf("database: %s\ntype: mysql\n", dbName)
	schemaFiles := map[string]string{
		"users.sql": "CREATE TABLE `users` (\n  `id` bigint unsigned NOT NULL AUTO_INCREMENT,\n  `name` varchar(255) NOT NULL,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;",
	}

	result := setupFakeGitHubForPlan(t, mux, schemaFiles, schemabotConfig, dbName)
	// HEAD does not advance — both /pulls/1 calls return abc123 (default).
	// The schema-freshness check therefore passes; the re-gate is the only
	// thing that can still block the apply.

	// GraphQL handler: PASS on the early gate call, FAIL on the fresh-HEAD
	// re-gate call. Without the re-gate, this transition would be invisible
	// to `apply -y` and the apply would proceed against red CI.
	var graphqlCalls atomic.Int64
	passing := rollupGraphQLHandler(nil)
	failing := rollupGraphQLHandler([]rollupNode{
		{
			Typename:   "CheckRun",
			Name:       "ci/lint",
			Status:     "COMPLETED",
			Conclusion: "FAILURE",
			AppSlug:    "ci-bot",
		},
	})
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if graphqlCalls.Add(1) == 1 {
			passing(w, r)
			return
		}
		failing(w, r)
	})
	result.GraphQLHandler.Store(&handler)

	h := newE2EHandler(t, svc, client)

	req := buildWebhookRequest(t, webhookPayloadOpts{
		comment: "schemabot apply -e staging -y",
		isPR:    true,
	}, nil)

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	require.Equal(t, http.StatusOK, rr.Code)

	// Expect a "Apply Blocked … checks failing" comment from the re-gate.
	// Drain comments looking for the blocking message; fail if anything
	// indicates the apply started.
	deadline := time.After(30 * time.Second)
	var blocked string
	for blocked == "" {
		select {
		case body := <-result.comments:
			assert.NotContains(t, body, "Schema Change In Progress", "apply must not have started")
			assert.NotContains(t, body, "Schema Change Applied", "apply must not have completed")
			if strings.Contains(body, "Apply Blocked") && strings.Contains(body, "ci/lint") {
				blocked = body
			}
		case <-deadline:
			t.Fatal("timed out waiting for fresh-HEAD checks-gate block comment")
		}
	}

	assert.Contains(t, blocked, "failure", "block comment must show the failing conclusion")
	assert.Contains(t, blocked, "staging", "retry hint must reference the env")

	// Re-gate must have actually happened — at least two GraphQL calls
	// (early gate + re-gate). The singleflight does not memoise across
	// sequential calls so the second call hits the mock.
	assert.GreaterOrEqual(t, graphqlCalls.Load(), int64(2),
		"expected at least 2 GraphQL calls (early gate + fresh-HEAD re-gate)")

	// Lock must be released so the user can re-run `apply -y` once checks recover.
	require.Eventually(t, func() bool {
		lock, err := svc.Storage().Locks().Get(t.Context(), dbName, "mysql")
		return err == nil && lock == nil
	}, 5*time.Second, 50*time.Millisecond, "lock must be released after fresh-HEAD checks-gate block")

	// No apply record — executeApply must not have been reached.
	applies, err := svc.Storage().Applies().GetByPR(t.Context(), "octocat/hello-world", 1)
	require.NoError(t, err)
	for _, a := range applies {
		assert.NotEqual(t, dbName, a.Database, "no apply for %s should have been started", dbName)
	}
}

// TestE2EApplyAutoConfirmFreshHEADBlockPreservesOtherPRLock verifies that when
// the fresh-HEAD checks gate blocks `apply -y` and the per-target lock has
// changed owner to a different PR between acquisition and the block, the
// owner-scoped Release leaves the other PR's lock intact. ForceRelease here
// would inadvertently clear that lock and remove the concurrency safety gate.
func TestE2EApplyAutoConfirmFreshHEADBlockPreservesOtherPRLock(t *testing.T) {
	dbName := "webhook_auto_confirm_regate_otherlock"
	svc := setupE2EService(t, dbName)

	mux := http.NewServeMux()
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)

	client := gh.NewClient(nil)
	client.BaseURL, _ = url.Parse(server.URL + "/")

	schemabotConfig := fmt.Sprintf("database: %s\ntype: mysql\n", dbName)
	schemaFiles := map[string]string{
		"users.sql": "CREATE TABLE `users` (\n  `id` bigint unsigned NOT NULL AUTO_INCREMENT,\n  `name` varchar(255) NOT NULL,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;",
	}

	result := setupFakeGitHubForPlan(t, mux, schemaFiles, schemabotConfig, dbName)
	// HEAD does not advance — schema-freshness check passes; the re-gate is
	// the only thing that can still block.

	otherOwner := "octocat/hello-world#999"
	t.Cleanup(func() {
		_ = svc.Storage().Locks().ForceRelease(context.WithoutCancel(t.Context()), dbName, "mysql")
	})

	// GraphQL handler: PASS on the early gate call; on the re-gate call,
	// FIRST swap the lock owner to a different PR (#999), THEN return FAILURE.
	// The swap simulates an unrelated `schemabot unlock` + another PR
	// acquiring the lock in the window between our acquire and our re-gate.
	var graphqlCalls atomic.Int64
	passing := rollupGraphQLHandler(nil)
	failing := rollupGraphQLHandler([]rollupNode{
		{
			Typename:   "CheckRun",
			Name:       "ci/lint",
			Status:     "COMPLETED",
			Conclusion: "FAILURE",
			AppSlug:    "ci-bot",
		},
	})
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if graphqlCalls.Add(1) == 1 {
			passing(w, r)
			return
		}
		// Re-gate call: swap the lock to PR #999 before responding FAILURE.
		// ForceRelease then re-Acquire is the closest analogue to "another
		// caller cleared and reacquired" within a single test process.
		_ = svc.Storage().Locks().ForceRelease(r.Context(), dbName, "mysql")
		_ = svc.Storage().Locks().Acquire(r.Context(), &storage.Lock{
			DatabaseName: dbName,
			DatabaseType: "mysql",
			Repository:   "octocat/hello-world",
			PullRequest:  999,
			Owner:        otherOwner,
		})
		failing(w, r)
	})
	result.GraphQLHandler.Store(&handler)

	h := newE2EHandler(t, svc, client)

	req := buildWebhookRequest(t, webhookPayloadOpts{
		comment: "schemabot apply -e staging -y",
		isPR:    true,
	}, nil)

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	require.Equal(t, http.StatusOK, rr.Code)

	// Drain comments until the blocking message lands; fail if the apply started.
	deadline := time.After(30 * time.Second)
	var blocked string
	for blocked == "" {
		select {
		case body := <-result.comments:
			assert.NotContains(t, body, "Schema Change In Progress", "apply must not have started")
			assert.NotContains(t, body, "Schema Change Applied", "apply must not have completed")
			if strings.Contains(body, "Apply Blocked") && strings.Contains(body, "ci/lint") {
				blocked = body
			}
		case <-deadline:
			t.Fatal("timed out waiting for fresh-HEAD checks-gate block comment")
		}
	}

	// The other PR's lock must remain intact through the full polling window —
	// long enough for the gate-block path's owner-scoped Release attempt to
	// land (and silently no-op as ErrLockNotOwned) before t.Cleanup tears
	// down. require.Never both asserts the lock is preserved and synchronises
	// with the async handler so we don't race shutdown.
	require.Never(t, func() bool {
		lock, err := svc.Storage().Locks().Get(t.Context(), dbName, "mysql")
		return err != nil || lock == nil || lock.Owner != otherOwner
	}, 1*time.Second, 50*time.Millisecond, "other PR's lock must remain intact after fresh-HEAD gate block on PR #1")
}

// TestE2EApplyThreeEnvEnforcement verifies that checkPriorEnvironments checks ALL
// prior environments, not just the immediately previous one. Uses 3 environments
// (sandbox → staging → production) and calls checkPriorEnvironments directly
// (bypassing config validation which currently restricts to staging/production).
func TestE2EApplyThreeEnvEnforcement(t *testing.T) {
	dbName := "webhook_three_env"
	svc := setupE2EService(t, dbName)
	svc.Config().EnvironmentOrder = []string{"sandbox", "staging", "production"}

	mux := http.NewServeMux()
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)

	client := gh.NewClient(nil)
	client.BaseURL, _ = url.Parse(server.URL + "/")

	// Capture comments
	comments := make(chan string, 10)
	mux.HandleFunc("POST /repos/octocat/hello-world/issues/1/comments", func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Body string `json:"body"`
		}
		require.NoError(t, json.NewDecoder(r.Body).Decode(&body))
		comments <- body.Body
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(map[string]any{"id": 99})
	})

	h := newE2EHandler(t, svc, client)
	envs := []string{"sandbox", "staging", "production"}

	// Case 1: production blocked when sandbox is action_required
	seedCheck(t, svc, dbName, "sandbox", "action_required")

	blocked := h.checkPriorEnvironments(t.Context(), "octocat/hello-world", 1,
		dbName, "mysql", "production", envs, 1, "testuser")
	assert.True(t, blocked, "production should be blocked when sandbox is action_required")

	select {
	case body := <-comments:
		assert.Contains(t, body, "sandbox")
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for blocked comment (case 1)")
	}

	// Case 2: production blocked when sandbox is success but staging is action_required
	seedCheck(t, svc, dbName, "sandbox", "success")
	seedCheck(t, svc, dbName, "staging", "action_required")

	blocked = h.checkPriorEnvironments(t.Context(), "octocat/hello-world", 1,
		dbName, "mysql", "production", envs, 1, "testuser")
	assert.True(t, blocked, "production should be blocked when staging is action_required")

	select {
	case body := <-comments:
		assert.Contains(t, body, "staging")
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for blocked comment (case 2)")
	}

	// Case 3: production allowed when both sandbox and staging are success
	seedCheck(t, svc, dbName, "staging", "success")

	blocked = h.checkPriorEnvironments(t.Context(), "octocat/hello-world", 1,
		dbName, "mysql", "production", envs, 1, "testuser")
	assert.False(t, blocked, "production should not be blocked when all prior envs are success")

	// Case 4: staging only requires sandbox (not production)
	seedCheck(t, svc, dbName, "sandbox", "action_required")

	blocked = h.checkPriorEnvironments(t.Context(), "octocat/hello-world", 1,
		dbName, "mysql", "staging", envs, 1, "testuser")
	assert.True(t, blocked, "staging should be blocked when sandbox is action_required")

	// Case 5: sandbox (first env) is never blocked
	blocked = h.checkPriorEnvironments(t.Context(), "octocat/hello-world", 1,
		dbName, "mysql", "sandbox", envs, 1, "testuser")
	assert.False(t, blocked, "sandbox (first env) should never be blocked")
}

// TestE2EApplyStoresServerSideTarget verifies that the apply handler stores the
// target resolved from server config on the generated plan.
func TestE2EApplyStoresServerSideTarget(t *testing.T) {
	dbName := "webhook_apply_target"
	svc := setupE2EService(t, dbName)

	mux := http.NewServeMux()
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)

	client := gh.NewClient(nil)
	client.BaseURL, _ = url.Parse(server.URL + "/")

	schemabotConfig := fmt.Sprintf("database: %s\ntype: mysql\nenvironments:\n  - staging\n", dbName)
	schemaFiles := map[string]string{
		"users.sql": "CREATE TABLE `users` (\n  `id` bigint unsigned NOT NULL AUTO_INCREMENT,\n  `name` varchar(255) NOT NULL,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;",
	}

	result := setupFakeGitHubForPlan(t, mux, schemaFiles, schemabotConfig, dbName)

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	installClient := ghclient.NewInstallationClient(client, logger)
	factory := &fakeClientFactory{client: installClient}

	h := NewHandler(svc, factory, nil, logger)

	req := buildWebhookRequest(t, webhookPayloadOpts{
		comment: "schemabot apply -e staging",
		isPR:    true,
	}, nil)

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	require.Equal(t, http.StatusOK, rr.Code)
	assert.Contains(t, rr.Body.String(), "apply started")

	// Wait for the apply plan confirmation comment.
	select {
	case body := <-result.comments:
		assert.Contains(t, body, "Schema Change Plan (Apply)")
		assert.Contains(t, body, "CREATE TABLE")
		assert.Contains(t, body, dbName)
	case <-time.After(30 * time.Second):
		t.Fatal("timed out waiting for apply plan comment")
	}

	// Verify the stored plan has the server-side target.
	plans, err := svc.Storage().Plans().GetByPR(t.Context(), "octocat/hello-world", 1)
	require.NoError(t, err)
	require.NotEmpty(t, plans, "expected at least one stored plan")

	// The plan stored during the apply re-plan should reference our database
	var found bool
	for _, plan := range plans {
		if plan.Database == dbName {
			assert.Equal(t, dbName, plan.Target)
			assert.Equal(t, dbName, plan.Deployment)
			found = true
			break
		}
	}
	assert.True(t, found, "expected a stored plan for database %s", dbName)

	// Clean up lock
	t.Cleanup(func() {
		_ = svc.Storage().Locks().ForceRelease(context.WithoutCancel(t.Context()), dbName, "mysql")
	})
}

// seedTargetTable creates a table in the target database.
func seedTargetTable(t *testing.T, dbName, ddl string) {
	t.Helper()

	appDSN := strings.Replace(e2eTargetDSN, "/target_test", "/"+dbName, 1) + "&multiStatements=true"
	db, err := sql.Open("mysql", appDSN)
	require.NoError(t, err)
	defer utils.CloseAndLog(db)

	_, err = db.ExecContext(t.Context(), ddl)
	require.NoError(t, err)
}
