package github

import (
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

	var calls int32
	fetch := func() ([]PRCheckStatus, error) {
		atomic.AddInt32(&calls, 1)
		return []PRCheckStatus{{Name: "ci/lint", Status: "completed", Conclusion: "success"}}, nil
	}

	first, err := c.Do("octo/repo", "abc123", fetch)
	require.NoError(t, err)

	clock.Advance(30 * time.Second) // still inside TTL

	second, err := c.Do("octo/repo", "abc123", fetch)
	require.NoError(t, err)

	assert.Equal(t, first, second, "second call should return the cached slice")
	assert.Equal(t, int32(1), atomic.LoadInt32(&calls), "fetch should be invoked only once within the TTL")
}

func TestCheckStatusCache_RefetchesAfterTTL(t *testing.T) {
	clock := newFakeClock()
	c := NewCheckStatusCache(time.Minute)
	c.now = clock.Now

	var calls int32
	fetch := func() ([]PRCheckStatus, error) {
		atomic.AddInt32(&calls, 1)
		return []PRCheckStatus{{Name: "ci/lint"}}, nil
	}

	_, err := c.Do("octo/repo", "abc123", fetch)
	require.NoError(t, err)

	clock.Advance(time.Minute + time.Second) // outside TTL

	_, err = c.Do("octo/repo", "abc123", fetch)
	require.NoError(t, err)

	assert.Equal(t, int32(2), atomic.LoadInt32(&calls), "expired entry should trigger a fresh fetch")
}

func TestCheckStatusCache_KeysAreIndependent(t *testing.T) {
	c := NewCheckStatusCache(time.Minute)
	c.now = newFakeClock().Now

	var calls int32
	fetch := func() ([]PRCheckStatus, error) {
		atomic.AddInt32(&calls, 1)
		return nil, nil
	}

	_, err := c.Do("octo/repo", "sha-one", fetch)
	require.NoError(t, err)
	_, err = c.Do("octo/repo", "sha-two", fetch)
	require.NoError(t, err)
	_, err = c.Do("octo/other", "sha-one", fetch)
	require.NoError(t, err)

	assert.Equal(t, int32(3), atomic.LoadInt32(&calls), "each unique (repo, sha) should miss the cache")
}

func TestCheckStatusCache_ErrorsAreNotCached(t *testing.T) {
	c := NewCheckStatusCache(time.Minute)
	c.now = newFakeClock().Now

	var calls int32
	wantErr := errors.New("boom")
	fetch := func() ([]PRCheckStatus, error) {
		n := atomic.AddInt32(&calls, 1)
		if n < 3 {
			return nil, wantErr
		}
		return []PRCheckStatus{{Name: "ci/lint"}}, nil
	}

	_, err := c.Do("octo/repo", "abc", fetch)
	assert.ErrorIs(t, err, wantErr)
	_, err = c.Do("octo/repo", "abc", fetch)
	assert.ErrorIs(t, err, wantErr, "second call should also miss and refetch — errors are not cached")

	got, err := c.Do("octo/repo", "abc", fetch)
	require.NoError(t, err)
	assert.Equal(t, []PRCheckStatus{{Name: "ci/lint"}}, got)
	assert.Equal(t, int32(3), atomic.LoadInt32(&calls))
}

func TestCheckStatusCache_SingleFlightCollapsesConcurrentFetches(t *testing.T) {
	c := NewCheckStatusCache(time.Minute)
	c.now = newFakeClock().Now

	const concurrency = 25
	var calls int32
	release := make(chan struct{})
	fetch := func() ([]PRCheckStatus, error) {
		atomic.AddInt32(&calls, 1)
		<-release // hold open until all goroutines have joined the flight
		return []PRCheckStatus{{Name: "ci/lint"}}, nil
	}

	var wg sync.WaitGroup
	results := make([][]PRCheckStatus, concurrency)
	errs := make([]error, concurrency)
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			res, err := c.Do("octo/repo", "abc", fetch)
			results[i] = res
			errs[i] = err
		}(i)
	}

	// Give all goroutines time to enqueue on the singleflight group, then
	// release the in-flight fetch.
	time.Sleep(50 * time.Millisecond)
	close(release)
	wg.Wait()

	assert.Equal(t, int32(1), atomic.LoadInt32(&calls), "all concurrent callers should collapse to one fetch")
	for i := 0; i < concurrency; i++ {
		require.NoError(t, errs[i])
		assert.Equal(t, []PRCheckStatus{{Name: "ci/lint"}}, results[i])
	}
}

func TestCheckStatusCache_NonPositiveTTLDisablesCaching(t *testing.T) {
	c := NewCheckStatusCache(0)

	var calls int32
	fetch := func() ([]PRCheckStatus, error) {
		atomic.AddInt32(&calls, 1)
		return nil, nil
	}

	for i := 0; i < 5; i++ {
		_, err := c.Do("octo/repo", "abc", fetch)
		require.NoError(t, err)
	}
	assert.Equal(t, int32(5), atomic.LoadInt32(&calls), "TTL<=0 should bypass the cache entirely")
}
