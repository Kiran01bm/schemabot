package github

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	gh "github.com/google/go-github/v68/github"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPRInfoCache_HitsCacheWithinTTL(t *testing.T) {
	clock := newPRInfoFakeClock()
	c := NewPRInfoCache(time.Minute)
	c.now = clock.Now

	var calls atomic.Int32
	fetch := func() (*PullRequestInfo, error) {
		calls.Add(1)
		return &PullRequestInfo{HeadSHA: "abc123", User: "octocat"}, nil
	}

	first, err := c.Do(t.Context(), "octo/repo", 42, fetch)
	require.NoError(t, err)

	clock.Advance(30 * time.Second) // still inside TTL

	second, err := c.Do(t.Context(), "octo/repo", 42, fetch)
	require.NoError(t, err)

	assert.Equal(t, *first, *second, "second call should return the cached value")
	assert.Equal(t, int32(1), calls.Load(), "fetch should be invoked only once within the TTL")
}

func TestPRInfoCache_RefetchesAfterTTL(t *testing.T) {
	clock := newPRInfoFakeClock()
	c := NewPRInfoCache(time.Minute)
	c.now = clock.Now

	var calls atomic.Int32
	fetch := func() (*PullRequestInfo, error) {
		calls.Add(1)
		return &PullRequestInfo{HeadSHA: "abc123"}, nil
	}

	_, err := c.Do(t.Context(), "octo/repo", 42, fetch)
	require.NoError(t, err)

	clock.Advance(time.Minute + time.Second) // outside TTL

	_, err = c.Do(t.Context(), "octo/repo", 42, fetch)
	require.NoError(t, err)

	assert.Equal(t, int32(2), calls.Load(), "expired entry should trigger a fresh fetch")
}

func TestPRInfoCache_KeysAreIndependent(t *testing.T) {
	c := NewPRInfoCache(time.Minute)
	c.now = newPRInfoFakeClock().Now

	var calls atomic.Int32
	fetch := func() (*PullRequestInfo, error) {
		calls.Add(1)
		return &PullRequestInfo{}, nil
	}

	_, err := c.Do(t.Context(), "octo/repo", 1, fetch)
	require.NoError(t, err)
	_, err = c.Do(t.Context(), "octo/repo", 2, fetch)
	require.NoError(t, err)
	_, err = c.Do(t.Context(), "octo/other", 1, fetch)
	require.NoError(t, err)

	assert.Equal(t, int32(3), calls.Load(), "each unique (repo, pr) should miss the cache")
}

func TestPRInfoCache_ErrorsAreNotCached(t *testing.T) {
	c := NewPRInfoCache(time.Minute)
	c.now = newPRInfoFakeClock().Now

	var calls atomic.Int32
	wantErr := errors.New("boom")
	fetch := func() (*PullRequestInfo, error) {
		n := calls.Add(1)
		if n < 3 {
			return nil, wantErr
		}
		return &PullRequestInfo{HeadSHA: "abc123"}, nil
	}

	_, err := c.Do(t.Context(), "octo/repo", 42, fetch)
	assert.ErrorIs(t, err, wantErr)
	_, err = c.Do(t.Context(), "octo/repo", 42, fetch)
	assert.ErrorIs(t, err, wantErr, "second call should also miss and refetch — errors are not cached")

	got, err := c.Do(t.Context(), "octo/repo", 42, fetch)
	require.NoError(t, err)
	assert.Equal(t, &PullRequestInfo{HeadSHA: "abc123"}, got)
	assert.Equal(t, int32(3), calls.Load())
}

func TestPRInfoCache_SingleFlightCollapsesConcurrentFetches(t *testing.T) {
	c := NewPRInfoCache(time.Minute)
	c.now = newPRInfoFakeClock().Now

	const concurrency = 25
	var calls atomic.Int32
	release := make(chan struct{})
	fetch := func() (*PullRequestInfo, error) {
		calls.Add(1)
		<-release // hold open until all goroutines have joined the flight
		return &PullRequestInfo{HeadSHA: "abc123"}, nil
	}

	var wg sync.WaitGroup
	results := make([]*PullRequestInfo, concurrency)
	errs := make([]error, concurrency)
	for i := range concurrency {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			res, err := c.Do(t.Context(), "octo/repo", 42, fetch)
			results[i] = res
			errs[i] = err
		}(i)
	}

	// Give all goroutines time to enqueue on the singleflight group, then
	// release the in-flight fetch.
	time.Sleep(50 * time.Millisecond)
	close(release)
	wg.Wait()

	assert.Equal(t, int32(1), calls.Load(), "all concurrent callers should collapse to one fetch")
	for i := range concurrency {
		require.NoError(t, errs[i])
		require.NotNil(t, results[i])
		assert.Equal(t, "abc123", results[i].HeadSHA)
	}

	// Each caller must receive its own pointer — mutating one must not
	// affect another.
	results[0].HeadSHA = "mutated"
	assert.Equal(t, "abc123", results[1].HeadSHA, "callers must receive independent copies")
}

// TestPRInfoCache_WaiterRespectsItsOwnContext locks in the invariant
// that a caller waiting on another caller's in-flight singleflight fetch
// returns promptly when its own ctx is cancelled, rather than blocking
// until the shared fetch completes. The shared fetch is not aborted —
// the first caller still receives its result.
func TestPRInfoCache_WaiterRespectsItsOwnContext(t *testing.T) {
	c := NewPRInfoCache(time.Minute)
	c.now = newPRInfoFakeClock().Now

	var calls atomic.Int32
	fetchEntered := make(chan struct{})
	releaseFetch := make(chan struct{})
	fetch := func() (*PullRequestInfo, error) {
		calls.Add(1)
		close(fetchEntered)
		<-releaseFetch
		return &PullRequestInfo{HeadSHA: "abc123"}, nil
	}

	// First caller: long-lived ctx, owns the in-flight fetch.
	firstDone := make(chan struct{})
	var firstResult *PullRequestInfo
	var firstErr error
	go func() {
		defer close(firstDone)
		firstResult, firstErr = c.Do(t.Context(), "octo/repo", 42, fetch)
	}()

	// Wait until the fetch is actually in flight before launching the
	// second caller, so we know it will join the singleflight group as a
	// waiter rather than running the fetch itself.
	<-fetchEntered

	// Second caller: its own cancellable ctx. Cancel before the fetch
	// completes and assert the caller returns promptly with ctx.Err().
	secondCtx, secondCancel := context.WithCancel(t.Context())
	secondDone := make(chan struct{})
	var secondErr error
	go func() {
		defer close(secondDone)
		_, secondErr = c.Do(secondCtx, "octo/repo", 42, fetch)
	}()
	secondCancel()

	select {
	case <-secondDone:
	case <-time.After(2 * time.Second):
		t.Fatal("second caller did not return promptly after its ctx was cancelled — likely still blocked on the shared singleflight fetch")
	}
	assert.ErrorIs(t, secondErr, context.Canceled, "second caller should return its own ctx.Err()")

	// First caller must still get the shared fetch's result — its ctx is
	// alive and the second caller's cancellation must not have aborted
	// the shared fetch.
	close(releaseFetch)
	<-firstDone
	require.NoError(t, firstErr)
	require.NotNil(t, firstResult)
	assert.Equal(t, "abc123", firstResult.HeadSHA)
	assert.Equal(t, int32(1), calls.Load(), "fetch should still be invoked exactly once")
}

// prInfoCacheLen returns the current size of the cache map under the lock.
func prInfoCacheLen(c *PRInfoCache) int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.m)
}

func TestPRInfoCache_StoreEvictsExpiredEntries(t *testing.T) {
	clock := newPRInfoFakeClock()
	c := NewPRInfoCache(time.Minute)
	c.now = clock.Now

	fetch := func() (*PullRequestInfo, error) {
		return &PullRequestInfo{HeadSHA: "abc"}, nil
	}

	// Seed five distinct (repo, pr) entries.
	for i := range 5 {
		_, err := c.Do(t.Context(), "octo/repo", i+1, fetch)
		require.NoError(t, err)
	}
	require.Equal(t, 5, prInfoCacheLen(c), "all five fresh entries should be cached")

	// Advance past the TTL — all five are now expired but still in the map.
	clock.Advance(time.Minute + time.Second)

	// A single store should sweep every expired entry before inserting,
	// keeping the map bounded by the active working set within the TTL.
	_, err := c.Do(t.Context(), "octo/repo", 99, fetch)
	require.NoError(t, err)
	assert.Equal(t, 1, prInfoCacheLen(c), "store should sweep expired entries so the map holds only the live ones")
}

func TestPRInfoCache_LookupEvictsExpiredEntry(t *testing.T) {
	clock := newPRInfoFakeClock()
	c := NewPRInfoCache(time.Minute)
	c.now = clock.Now

	fetch := func() (*PullRequestInfo, error) {
		return &PullRequestInfo{HeadSHA: "abc"}, nil
	}

	// Seed one entry, then expire it without ever writing again.
	_, err := c.Do(t.Context(), "octo/repo", 42, fetch)
	require.NoError(t, err)
	require.Equal(t, 1, prInfoCacheLen(c))

	clock.Advance(time.Minute + time.Second)

	// Lookup it directly (do not go through Do, which would re-fetch and
	// re-store). The expired entry must be evicted so it does not occupy
	// memory forever for a key that is never written again.
	_, ok := c.lookup("octo/repo#42")
	assert.False(t, ok, "expired entry must be reported as a miss")
	assert.Equal(t, 0, prInfoCacheLen(c), "lookup must evict the expired entry")
}

// TestFetchPullRequest_CacheCollapsesDuplicateCalls locks in the
// integration wiring between InstallationClient.FetchPullRequest and the
// Client-shared PRInfoCache: repeated calls for the same (repo, pr)
// within the TTL must hit GitHub exactly once, even when issued through
// different InstallationClients backed by the same Client.
func TestFetchPullRequest_CacheCollapsesDuplicateCalls(t *testing.T) {
	var calls atomic.Int32
	mux := http.NewServeMux()
	mux.HandleFunc("/repos/octo/repo/pulls/42", func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintln(w, `{
			"head": {"ref": "feature", "sha": "abc123"},
			"base": {"ref": "main",    "sha": "def456"},
			"user": {"login": "octocat"}
		}`)
	})
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)

	cache := NewPRInfoCache(time.Minute)
	logger := slog.New(slog.NewTextHandler(httptestDiscardWriter{}, &slog.HandlerOptions{Level: slog.LevelError}))

	newIC := func() *InstallationClient {
		ghc := gh.NewClient(nil)
		ghc.BaseURL, _ = url.Parse(server.URL + "/")
		return &InstallationClient{
			client:      ghc,
			logger:      logger,
			prInfoCache: cache,
		}
	}

	ic1 := newIC()
	ic2 := newIC()

	want := &PullRequestInfo{HeadRef: "feature", HeadSHA: "abc123", BaseRef: "main", BaseSHA: "def456", User: "octocat"}

	got, err := ic1.FetchPullRequest(t.Context(), "octo/repo", 42)
	require.NoError(t, err)
	assert.Equal(t, want, got)

	got, err = ic1.FetchPullRequest(t.Context(), "octo/repo", 42)
	require.NoError(t, err)
	assert.Equal(t, want, got)

	got, err = ic2.FetchPullRequest(t.Context(), "octo/repo", 42)
	require.NoError(t, err)
	assert.Equal(t, want, got, "second InstallationClient sharing the same cache must also see the cached result")

	assert.Equal(t, int32(1), calls.Load(),
		"all three FetchPullRequest calls (two on ic1, one on ic2) must collapse to one upstream GitHub call")
}

// TestFetchPullRequest_NoCacheFallsThrough verifies that an InstallationClient
// with a nil prInfoCache (e.g., constructed via NewInstallationClient in tests)
// still works correctly, hitting GitHub on every call.
func TestFetchPullRequest_NoCacheFallsThrough(t *testing.T) {
	var calls atomic.Int32
	mux := http.NewServeMux()
	mux.HandleFunc("/repos/octo/repo/pulls/42", func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintln(w, `{"head": {"sha": "abc123"}, "base": {}, "user": {}}`)
	})
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)

	ghc := gh.NewClient(nil)
	ghc.BaseURL, _ = url.Parse(server.URL + "/")
	ic := &InstallationClient{
		client: ghc,
		logger: slog.New(slog.NewTextHandler(httptestDiscardWriter{}, &slog.HandlerOptions{Level: slog.LevelError})),
		// prInfoCache deliberately nil
	}

	for range 3 {
		_, err := ic.FetchPullRequest(t.Context(), "octo/repo", 42)
		require.NoError(t, err)
	}
	assert.Equal(t, int32(3), calls.Load(), "nil cache should not memoise — every call must hit GitHub")
}

// httptestDiscardWriter swallows slog output so test logs stay quiet.
type httptestDiscardWriter struct{}

func (httptestDiscardWriter) Write(p []byte) (int, error) { return len(p), nil }

// TestForInstallation_CachesByInstallationID locks in the invariant that
// repeat ForInstallation calls for the same installationID return the
// same InstallationClient instance, so the underlying http.Client,
// ghinstallation transport (and its installation-token cache), and any
// per-InstallationClient state survive across webhook deliveries.
// Different installationIDs must still receive distinct clients.
func TestForInstallation_CachesByInstallationID(t *testing.T) {
	c := &Client{
		appID:         12345,
		logger:        slog.New(slog.NewTextHandler(httptestDiscardWriter{}, &slog.HandlerOptions{Level: slog.LevelError})),
		appSlug:       "schemabot", // bypass the slug-fetch retry path
		prInfoCache:   NewPRInfoCache(time.Minute),
		installations: make(map[int64]*InstallationClient),
		privateKey:    testRSAKeyPEM(t),
	}

	a1, err := c.ForInstallation(100)
	require.NoError(t, err)
	a2, err := c.ForInstallation(100)
	require.NoError(t, err)
	b, err := c.ForInstallation(200)
	require.NoError(t, err)

	assert.Same(t, a1, a2, "same installationID must return the cached InstallationClient")
	assert.NotSame(t, a1, b, "distinct installationIDs must return distinct InstallationClients")
	assert.Same(t, c.prInfoCache, a1.prInfoCache, "cached InstallationClient must carry the Client-shared PRInfoCache")
	assert.Same(t, c.prInfoCache, b.prInfoCache, "every InstallationClient must share the same PRInfoCache instance")
}

// TestForInstallation_RefreshesSlugOnCachedClient covers the slug recovery
// path: an InstallationClient constructed before slug recovery (with an
// empty appSlug) must observe the recovered slug on the next
// ForInstallation call, so it does not stay stranded with an empty slug
// for the lifetime of the process.
func TestForInstallation_RefreshesSlugOnCachedClient(t *testing.T) {
	c := &Client{
		appID:         12345,
		logger:        slog.New(slog.NewTextHandler(httptestDiscardWriter{}, &slog.HandlerOptions{Level: slog.LevelError})),
		appSlug:       "", // slug was unavailable at construction time
		prInfoCache:   NewPRInfoCache(time.Minute),
		installations: make(map[int64]*InstallationClient),
		privateKey:    testRSAKeyPEM(t),
	}
	// Bypass the slug-fetch retry by claiming we just tried.
	c.lastSlugAttempt = time.Now()

	ic1, err := c.ForInstallation(100)
	require.NoError(t, err)
	assert.Equal(t, "", ic1.appSlug, "client constructed before recovery must start with empty slug")

	// Simulate the slug becoming available later.
	c.appSlug = "schemabot"

	ic2, err := c.ForInstallation(100)
	require.NoError(t, err)
	assert.Same(t, ic1, ic2, "same InstallationClient should be returned (no rebuild)")
	assert.Equal(t, "schemabot", ic2.appSlug, "cached InstallationClient must adopt the recovered slug")
}

// testRSAKeyPEM generates a fresh 2048-bit RSA private key and returns it
// PEM-encoded. ghinstallation.New requires a parseable RSA private key
// even though no JWT is actually exercised here.
func testRSAKeyPEM(t *testing.T) []byte {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	return pem.EncodeToMemory(&pem.Block{
		Type:  "RSA PRIVATE KEY",
		Bytes: x509.MarshalPKCS1PrivateKey(key),
	})
}

func TestPRInfoCache_NonPositiveTTLDisablesCaching(t *testing.T) {
	c := NewPRInfoCache(0)

	var calls atomic.Int32
	fetch := func() (*PullRequestInfo, error) {
		calls.Add(1)
		return &PullRequestInfo{}, nil
	}

	for range 5 {
		_, err := c.Do(t.Context(), "octo/repo", 42, fetch)
		require.NoError(t, err)
	}
	assert.Equal(t, int32(5), calls.Load(), "TTL<=0 should bypass the cache entirely")
}

// prInfoFakeClock returns a controllable time source for cache TTL tests.
// Named distinctly so it does not collide with other in-package cache
// test files that may carry their own fake clock.
type prInfoFakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func newPRInfoFakeClock() *prInfoFakeClock {
	return &prInfoFakeClock{now: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
}

func (f *prInfoFakeClock) Now() time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.now
}

func (f *prInfoFakeClock) Advance(d time.Duration) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.now = f.now.Add(d)
}
