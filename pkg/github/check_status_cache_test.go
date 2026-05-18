package github

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeClock returns a controllable time source for cache TTL tests.
type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func newFakeClock() *fakeClock {
	return &fakeClock{now: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
}

func (f *fakeClock) Now() time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.now
}

func (f *fakeClock) Advance(d time.Duration) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.now = f.now.Add(d)
}

func TestCheckStatusCache_HitsCacheWithinTTL(t *testing.T) {
	clock := newFakeClock()
	c := NewCheckStatusCache(time.Minute)
	c.now = clock.Now

	var calls atomic.Int32
	fetch := func(_ context.Context) ([]CachedCheckRow, error) {
		calls.Add(1)
		return []CachedCheckRow{{Name: "ci/lint", Status: "completed", Conclusion: "success"}}, nil
	}

	first, err := c.Do(t.Context(), "octo/repo", "abc123", fetch)
	require.NoError(t, err)

	clock.Advance(30 * time.Second) // still inside TTL

	second, err := c.Do(t.Context(), "octo/repo", "abc123", fetch)
	require.NoError(t, err)

	assert.Equal(t, first, second, "second call should return the cached slice")
	assert.Equal(t, int32(1), calls.Load(), "fetch should be invoked only once within the TTL")
}

func TestCheckStatusCache_RefetchesAfterTTL(t *testing.T) {
	clock := newFakeClock()
	c := NewCheckStatusCache(time.Minute)
	c.now = clock.Now

	var calls atomic.Int32
	fetch := func(_ context.Context) ([]CachedCheckRow, error) {
		calls.Add(1)
		return []CachedCheckRow{{Name: "ci/lint"}}, nil
	}

	_, err := c.Do(t.Context(), "octo/repo", "abc123", fetch)
	require.NoError(t, err)

	clock.Advance(time.Minute + time.Second) // outside TTL

	_, err = c.Do(t.Context(), "octo/repo", "abc123", fetch)
	require.NoError(t, err)

	assert.Equal(t, int32(2), calls.Load(), "expired entry should trigger a fresh fetch")
}

func TestCheckStatusCache_KeysAreIndependent(t *testing.T) {
	c := NewCheckStatusCache(time.Minute)
	c.now = newFakeClock().Now

	var calls atomic.Int32
	fetch := func(_ context.Context) ([]CachedCheckRow, error) {
		calls.Add(1)
		return nil, nil
	}

	_, err := c.Do(t.Context(), "octo/repo", "sha-one", fetch)
	require.NoError(t, err)
	_, err = c.Do(t.Context(), "octo/repo", "sha-two", fetch)
	require.NoError(t, err)
	_, err = c.Do(t.Context(), "octo/other", "sha-one", fetch)
	require.NoError(t, err)

	assert.Equal(t, int32(3), calls.Load(), "each unique (repo, sha) should miss the cache")
}

func TestCheckStatusCache_ErrorsAreNotCached(t *testing.T) {
	c := NewCheckStatusCache(time.Minute)
	c.now = newFakeClock().Now

	var calls atomic.Int32
	wantErr := errors.New("boom")
	fetch := func(_ context.Context) ([]CachedCheckRow, error) {
		n := calls.Add(1)
		if n < 3 {
			return nil, wantErr
		}
		return []CachedCheckRow{{Name: "ci/lint"}}, nil
	}

	_, err := c.Do(t.Context(), "octo/repo", "abc", fetch)
	assert.ErrorIs(t, err, wantErr)
	_, err = c.Do(t.Context(), "octo/repo", "abc", fetch)
	assert.ErrorIs(t, err, wantErr, "second call should also miss and refetch — errors are not cached")

	got, err := c.Do(t.Context(), "octo/repo", "abc", fetch)
	require.NoError(t, err)
	assert.Equal(t, []CachedCheckRow{{Name: "ci/lint"}}, got)
	assert.Equal(t, int32(3), calls.Load())
}

func TestCheckStatusCache_SingleFlightCollapsesConcurrentFetches(t *testing.T) {
	c := NewCheckStatusCache(time.Minute)
	c.now = newFakeClock().Now

	const concurrency = 25
	var calls atomic.Int32
	release := make(chan struct{})
	fetch := func(_ context.Context) ([]CachedCheckRow, error) {
		calls.Add(1)
		<-release // hold open until all goroutines have joined the flight
		return []CachedCheckRow{{Name: "ci/lint"}}, nil
	}

	var wg sync.WaitGroup
	results := make([][]CachedCheckRow, concurrency)
	errs := make([]error, concurrency)
	for i := range concurrency {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			res, err := c.Do(t.Context(), "octo/repo", "abc", fetch)
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
		assert.Equal(t, []CachedCheckRow{{Name: "ci/lint"}}, results[i])
	}
}

// TestCheckStatusCache_WaiterRespectsItsOwnContext locks in the invariant
// that a caller waiting on another caller's in-flight singleflight fetch
// returns promptly when its own ctx is cancelled, rather than blocking
// until the shared fetch completes. The shared fetch is not aborted —
// the first caller still receives its result.
func TestCheckStatusCache_WaiterRespectsItsOwnContext(t *testing.T) {
	c := NewCheckStatusCache(time.Minute)
	c.now = newFakeClock().Now

	var calls atomic.Int32
	fetchEntered := make(chan struct{})
	releaseFetch := make(chan struct{})
	fetch := func(_ context.Context) ([]CachedCheckRow, error) {
		calls.Add(1)
		close(fetchEntered)
		<-releaseFetch
		return []CachedCheckRow{{Name: "ci/lint"}}, nil
	}

	// First caller: long-lived ctx, owns the in-flight fetch.
	firstDone := make(chan struct{})
	var firstResult []CachedCheckRow
	var firstErr error
	go func() {
		defer close(firstDone)
		firstResult, firstErr = c.Do(t.Context(), "octo/repo", "abc", fetch)
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
		_, secondErr = c.Do(secondCtx, "octo/repo", "abc", fetch)
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
	assert.Equal(t, []CachedCheckRow{{Name: "ci/lint"}}, firstResult)
	assert.Equal(t, int32(1), calls.Load(), "fetch should still be invoked exactly once")
}

// cacheLen returns the current size of the cache map under the lock.
func cacheLen(c *CheckStatusCache) int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.m)
}

func TestCheckStatusCache_StoreEvictsExpiredEntries(t *testing.T) {
	clock := newFakeClock()
	c := NewCheckStatusCache(time.Minute)
	c.now = clock.Now

	fetch := func(_ context.Context) ([]CachedCheckRow, error) {
		return []CachedCheckRow{{Name: "ci/lint"}}, nil
	}

	// Seed five distinct (repo, sha) entries.
	for i := range 5 {
		_, err := c.Do(t.Context(), "octo/repo", "sha-"+string(rune('a'+i)), fetch)
		require.NoError(t, err)
	}
	require.Equal(t, 5, cacheLen(c), "all five fresh entries should be cached")

	// Advance past the TTL — all five are now expired but still in the map.
	clock.Advance(time.Minute + time.Second)

	// A single store should sweep every expired entry before inserting,
	// keeping the map bounded by the active working set within the TTL.
	_, err := c.Do(t.Context(), "octo/repo", "sha-new", fetch)
	require.NoError(t, err)
	assert.Equal(t, 1, cacheLen(c), "store should sweep expired entries so the map holds only the live ones")
}

func TestCheckStatusCache_LookupEvictsExpiredEntry(t *testing.T) {
	clock := newFakeClock()
	c := NewCheckStatusCache(time.Minute)
	c.now = clock.Now

	fetch := func(_ context.Context) ([]CachedCheckRow, error) {
		return []CachedCheckRow{{Name: "ci/lint"}}, nil
	}

	// Seed one entry, then expire it without ever writing again.
	_, err := c.Do(t.Context(), "octo/repo", "abc", fetch)
	require.NoError(t, err)
	require.Equal(t, 1, cacheLen(c))

	clock.Advance(time.Minute + time.Second)

	// Lookup it directly (do not go through Do, which would re-fetch and
	// re-store). The expired entry must be evicted so it does not occupy
	// memory forever for a key that is never written again.
	_, ok := c.lookup("octo/repo@abc")
	assert.False(t, ok, "expired entry must be reported as a miss")
	assert.Equal(t, 0, cacheLen(c), "lookup must evict the expired entry")
}

// TestGetPRCheckStatuses_RecomputesIsSchemaBotPerCall locks in the invariant
// that IsSchemaBot is derived from the calling InstallationClient's appSlug
// at read time, never baked into the shared cache at fetch time. Without
// this, an entry populated by an InstallationClient whose appSlug was
// unavailable would keep IsSchemaBot=false for SchemaBot's own checks until
// the TTL expired — even for subsequent InstallationClients constructed
// after slug recovery — causing the checks gate to spuriously block applies
// on the bot's own checks.
func TestGetPRCheckStatuses_RecomputesIsSchemaBotPerCall(t *testing.T) {
	cache := NewCheckStatusCache(time.Minute)
	cache.now = newFakeClock().Now

	// Seed the shared cache with a row whose AppSlug matches the recovered
	// slug. This simulates a previous fetch populating the cache before
	// any client knew the slug.
	const repo, sha = "octo/repo", "abc123"
	cache.store(repo+"@"+sha, []CachedCheckRow{
		{Name: "schemabot/apply staging", Status: "completed", Conclusion: "success", AppSlug: "schemabot"},
		{Name: "ci/lint", Status: "completed", Conclusion: "failure", AppSlug: "other-ci"},
	})

	// Client A was spawned before slug recovery (ic.appSlug == "").
	preRecovery := &InstallationClient{appSlug: "", checkStatusCache: cache}
	// Client B is spawned by the same factory after slug recovery succeeded.
	postRecovery := &InstallationClient{appSlug: "schemabot", checkStatusCache: cache}

	preStatuses, err := preRecovery.GetPRCheckStatuses(t.Context(), repo, sha)
	require.NoError(t, err)
	for _, s := range preStatuses {
		assert.False(t, s.IsSchemaBot, "pre-recovery client must not classify any row as own check (slug not known) — got %+v", s)
	}

	postStatuses, err := postRecovery.GetPRCheckStatuses(t.Context(), repo, sha)
	require.NoError(t, err)
	require.Len(t, postStatuses, 2)
	assert.True(t, postStatuses[0].IsSchemaBot, "post-recovery client must re-derive IsSchemaBot=true for the bot's own check from the cached AppSlug")
	assert.False(t, postStatuses[1].IsSchemaBot, "third-party check must remain IsSchemaBot=false")
}

// TestCheckStatusCache_LeaderCancellationDoesNotFailWaiters locks in the
// invariant that the singleflight shared fetch is decoupled from any
// individual caller's ctx: the caller that wins the singleflight may
// cancel or time out without aborting the shared GitHub request and
// without failing unrelated waiters whose own contexts are still valid.
func TestCheckStatusCache_LeaderCancellationDoesNotFailWaiters(t *testing.T) {
	c := NewCheckStatusCache(time.Minute)
	c.now = newFakeClock().Now

	fetchEntered := make(chan struct{})
	releaseFetch := make(chan struct{})
	var fetchCtxCancelled atomic.Bool
	var calls atomic.Int32
	fetch := func(fetchCtx context.Context) ([]CachedCheckRow, error) {
		calls.Add(1)
		close(fetchEntered)
		// If the cache were still passing the leader's ctx straight to
		// fetch, fetchCtx would be cancelled when the leader cancels
		// below. Observe its state after the cancellation point to
		// confirm the cache is feeding us a decoupled ctx.
		<-releaseFetch
		if fetchCtx.Err() != nil {
			fetchCtxCancelled.Store(true)
		}
		return []CachedCheckRow{{Name: "ci/lint", Status: "completed", Conclusion: "success"}}, nil
	}

	// Leader: cancellable ctx. Wins the singleflight, then cancels
	// before the shared fetch completes.
	leaderCtx, leaderCancel := context.WithCancel(t.Context())
	leaderDone := make(chan struct{})
	var leaderResult []CachedCheckRow
	var leaderErr error
	go func() {
		defer close(leaderDone)
		leaderResult, leaderErr = c.Do(leaderCtx, "octo/repo", "abc", fetch)
	}()

	// Wait until the fetch is actually in flight before starting the
	// waiter (so we are sure the waiter joins the singleflight group as
	// a follower, not as the leader).
	<-fetchEntered

	// Waiter: a separate long-lived ctx. Must observe the shared fetch's
	// result even though the leader is about to cancel.
	waiterDone := make(chan struct{})
	var waiterResult []CachedCheckRow
	var waiterErr error
	go func() {
		defer close(waiterDone)
		waiterResult, waiterErr = c.Do(t.Context(), "octo/repo", "abc", fetch)
	}()

	// Cancel the leader before the shared fetch completes. With a
	// decoupled fetchCtx the shared fetch is unaffected; the leader
	// itself returns ctx.Canceled via its outer select.
	leaderCancel()

	<-leaderDone
	assert.ErrorIs(t, leaderErr, context.Canceled, "leader should observe its own cancellation")
	assert.Nil(t, leaderResult)

	// Release the shared fetch. The waiter must receive the result
	// despite the leader having cancelled.
	close(releaseFetch)

	select {
	case <-waiterDone:
	case <-time.After(2 * time.Second):
		t.Fatal("waiter did not receive a result — likely failed by the leader's cancellation")
	}
	require.NoError(t, waiterErr, "waiter must not be affected by the leader's cancellation")
	assert.Equal(t, []CachedCheckRow{{Name: "ci/lint", Status: "completed", Conclusion: "success"}}, waiterResult)
	assert.False(t, fetchCtxCancelled.Load(),
		"shared fetch's ctx must remain alive after the leader cancels — otherwise the GitHub request would have aborted")
	assert.Equal(t, int32(1), calls.Load(), "fetch should run exactly once across leader + waiter")
}

func TestCheckStatusCache_NonPositiveTTLDisablesCaching(t *testing.T) {
	c := NewCheckStatusCache(0)

	var calls atomic.Int32
	fetch := func(_ context.Context) ([]CachedCheckRow, error) {
		calls.Add(1)
		return nil, nil
	}

	for range 5 {
		_, err := c.Do(t.Context(), "octo/repo", "abc", fetch)
		require.NoError(t, err)
	}
	assert.Equal(t, int32(5), calls.Load(), "TTL<=0 should bypass the cache entirely")
}
