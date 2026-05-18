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
	fetch := func() ([]PRCheckStatus, error) {
		calls.Add(1)
		return []PRCheckStatus{{Name: "ci/lint", Status: "completed", Conclusion: "success"}}, nil
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
	fetch := func() ([]PRCheckStatus, error) {
		calls.Add(1)
		return []PRCheckStatus{{Name: "ci/lint"}}, nil
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
	fetch := func() ([]PRCheckStatus, error) {
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
	fetch := func() ([]PRCheckStatus, error) {
		n := calls.Add(1)
		if n < 3 {
			return nil, wantErr
		}
		return []PRCheckStatus{{Name: "ci/lint"}}, nil
	}

	_, err := c.Do(t.Context(), "octo/repo", "abc", fetch)
	assert.ErrorIs(t, err, wantErr)
	_, err = c.Do(t.Context(), "octo/repo", "abc", fetch)
	assert.ErrorIs(t, err, wantErr, "second call should also miss and refetch — errors are not cached")

	got, err := c.Do(t.Context(), "octo/repo", "abc", fetch)
	require.NoError(t, err)
	assert.Equal(t, []PRCheckStatus{{Name: "ci/lint"}}, got)
	assert.Equal(t, int32(3), calls.Load())
}

func TestCheckStatusCache_SingleFlightCollapsesConcurrentFetches(t *testing.T) {
	c := NewCheckStatusCache(time.Minute)
	c.now = newFakeClock().Now

	const concurrency = 25
	var calls atomic.Int32
	release := make(chan struct{})
	fetch := func() ([]PRCheckStatus, error) {
		calls.Add(1)
		<-release // hold open until all goroutines have joined the flight
		return []PRCheckStatus{{Name: "ci/lint"}}, nil
	}

	var wg sync.WaitGroup
	results := make([][]PRCheckStatus, concurrency)
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
		assert.Equal(t, []PRCheckStatus{{Name: "ci/lint"}}, results[i])
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
	fetch := func() ([]PRCheckStatus, error) {
		calls.Add(1)
		close(fetchEntered)
		<-releaseFetch
		return []PRCheckStatus{{Name: "ci/lint"}}, nil
	}

	// First caller: long-lived ctx, owns the in-flight fetch.
	firstDone := make(chan struct{})
	var firstResult []PRCheckStatus
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
	assert.Equal(t, []PRCheckStatus{{Name: "ci/lint"}}, firstResult)
	assert.Equal(t, int32(1), calls.Load(), "fetch should still be invoked exactly once")
}

func TestCheckStatusCache_NonPositiveTTLDisablesCaching(t *testing.T) {
	c := NewCheckStatusCache(0)

	var calls atomic.Int32
	fetch := func() ([]PRCheckStatus, error) {
		calls.Add(1)
		return nil, nil
	}

	for range 5 {
		_, err := c.Do(t.Context(), "octo/repo", "abc", fetch)
		require.NoError(t, err)
	}
	assert.Equal(t, int32(5), calls.Load(), "TTL<=0 should bypass the cache entirely")
}
