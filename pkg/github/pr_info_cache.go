package github

import (
	"context"
	"strconv"
	"sync"
	"time"

	"golang.org/x/sync/singleflight"
)

// DefaultPRInfoCacheTTL is the default TTL used when NewClient constructs
// the shared PRInfoCache. 30s is long enough to absorb webhook retries
// and tight back-to-back command bursts targeting the same (repo, pr),
// but short enough that staleness is irrelevant for human-paced PR
// comment flows.
const DefaultPRInfoCacheTTL = 30 * time.Second

// PRInfoCache memoises FetchPullRequest results keyed by (repo, pr) with
// a fixed TTL. Concurrent fetches for the same key collapse into a single
// upstream request via singleflight. Errors are not cached — each failure
// re-attempts fresh so transient GitHub outages do not pin a bad state
// for the TTL window.
//
// One cache is owned by the Client factory and shared across every
// InstallationClient it produces, so cache hits actually accrue across
// the short-lived InstallationClients spawned per webhook delivery —
// which is the dedup pattern that justifies the cache.
//
// Unlike CheckStatusCache, the cached value is stored directly as
// PullRequestInfo: it has no identity-dependent fields (HeadRef, HeadSHA,
// BaseRef, BaseSHA, User), so cross-instance reuse is structurally safe
// without any per-call re-derivation.
type PRInfoCache struct {
	ttl   time.Duration
	mu    sync.RWMutex
	m     map[string]prInfoCacheEntry
	group singleflight.Group
	now   func() time.Time // overridable for tests
}

type prInfoCacheEntry struct {
	info    PullRequestInfo
	fetched time.Time
}

// NewPRInfoCache constructs a cache with the given TTL. A non-positive
// TTL disables caching: Do always invokes fetch.
func NewPRInfoCache(ttl time.Duration) *PRInfoCache {
	return &PRInfoCache{
		ttl: ttl,
		m:   make(map[string]prInfoCacheEntry),
		now: time.Now,
	}
}

// Do returns the cached PR info for (repo, pr) if a fresh entry exists,
// otherwise invokes fetch and stores its result. Concurrent callers for
// the same (repo, pr) collapse into a single fetch invocation.
//
// Each caller observes its own ctx for cancellation/deadline: a caller
// whose ctx fires while waiting on another caller's in-flight fetch
// returns promptly with ctx.Err(), without aborting the shared fetch
// (other waiters and future callers can still receive its result).
//
// When the cache's TTL is non-positive, fetch is invoked on every call
// and no result is stored.
//
// The returned *PullRequestInfo points to a fresh copy of the cached
// value, so callers can read or mutate it freely without affecting
// other readers.
func (c *PRInfoCache) Do(ctx context.Context, repo string, pr int, fetch func() (*PullRequestInfo, error)) (*PullRequestInfo, error) {
	if c.ttl <= 0 {
		return fetch()
	}

	key := repo + "#" + strconv.Itoa(pr)

	if info, ok := c.lookup(key); ok {
		return info, nil
	}

	ch := c.group.DoChan(key, func() (any, error) {
		// Re-check inside the single-flight critical section: a concurrent
		// caller may have populated the entry between our miss and here.
		if info, ok := c.lookup(key); ok {
			return info, nil
		}
		info, err := fetch()
		if err != nil {
			return nil, err
		}
		if info == nil {
			return nil, nil
		}
		c.store(key, *info)
		// Return a fresh copy so subsequent waiters/callers each get their
		// own pointer.
		stored := *info
		return &stored, nil
	})

	select {
	case res := <-ch:
		if res.Err != nil {
			return nil, res.Err
		}
		if res.Val == nil {
			return nil, nil
		}
		// Single-flight broadcasts the same value to every waiter. Hand
		// each one its own copy so a caller mutating the returned struct
		// cannot affect another caller's view.
		shared := res.Val.(*PullRequestInfo)
		copyOf := *shared
		return &copyOf, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (c *PRInfoCache) lookup(key string) (*PullRequestInfo, bool) {
	c.mu.RLock()
	entry, ok := c.m[key]
	c.mu.RUnlock()
	if !ok {
		return nil, false
	}
	if c.now().Sub(entry.fetched) >= c.ttl {
		// Opportunistically evict on read so a key that is repeatedly
		// looked up after expiry does not occupy memory until something
		// happens to write to it.
		c.mu.Lock()
		if cur, stillThere := c.m[key]; stillThere && c.now().Sub(cur.fetched) >= c.ttl {
			delete(c.m, key)
		}
		c.mu.Unlock()
		return nil, false
	}
	info := entry.info
	return &info, true
}

func (c *PRInfoCache) store(key string, info PullRequestInfo) {
	c.mu.Lock()
	defer c.mu.Unlock()
	// Sweep expired entries so the map stays bounded by the active
	// working set within the TTL window, not the total distinct keys
	// ever seen. Keys that are written once and never looked up again
	// (e.g. an inactive PR) would otherwise occupy memory for the
	// lifetime of the process.
	now := c.now()
	for k, entry := range c.m {
		if now.Sub(entry.fetched) >= c.ttl {
			delete(c.m, k)
		}
	}
	c.m[key] = prInfoCacheEntry{info: info, fetched: now}
}
