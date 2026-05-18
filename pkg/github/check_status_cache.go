package github

import (
	"context"
	"sync"
	"time"

	"golang.org/x/sync/singleflight"
)

// DefaultCheckStatusCacheTTL is the default TTL used when ForInstallation
// constructs a per-InstallationClient CheckStatusCache. 30s is long enough
// to absorb tight command bursts targeting the same (repo, sha) within a
// single InstallationClient's lifetime, but short enough that staleness is
// irrelevant for human-paced PR comment flows.
const DefaultCheckStatusCacheTTL = 30 * time.Second

// CheckStatusCache memoises GetPRCheckStatuses results keyed by (repo, sha)
// with a fixed TTL. Concurrent fetches for the same key collapse into a
// single upstream request via singleflight. Errors are not cached — each
// failure re-attempts fresh so transient GitHub outages do not pin a bad
// state for the TTL window.
//
// One cache is owned per InstallationClient (not shared across the Client
// factory) so cached entries are pinned to the appSlug snapshot the owning
// client was constructed with — IsSchemaBot can be baked in at fetch time
// without later skew from cross-instance slug recovery.
type CheckStatusCache struct {
	ttl   time.Duration
	mu    sync.RWMutex
	m     map[string]checkStatusCacheEntry
	group singleflight.Group
	now   func() time.Time // overridable for tests
}

type checkStatusCacheEntry struct {
	statuses []PRCheckStatus
	fetched  time.Time
}

// NewCheckStatusCache constructs a cache with the given TTL. A non-positive
// TTL disables caching: Do always invokes fetch.
func NewCheckStatusCache(ttl time.Duration) *CheckStatusCache {
	return &CheckStatusCache{
		ttl: ttl,
		m:   make(map[string]checkStatusCacheEntry),
		now: time.Now,
	}
}

// Do returns the cached statuses for (repo, sha) if a fresh entry exists,
// otherwise invokes fetch and stores its result. Concurrent callers for the
// same (repo, sha) collapse into a single fetch invocation.
//
// Each caller observes its own ctx for cancellation/deadline: a caller whose
// ctx fires while waiting on another caller's in-flight fetch returns
// promptly with ctx.Err(), without aborting the shared fetch (other waiters
// and future callers can still receive its result).
//
// When the cache's TTL is non-positive, fetch is invoked on every call and
// no result is stored.
func (c *CheckStatusCache) Do(ctx context.Context, repo, sha string, fetch func() ([]PRCheckStatus, error)) ([]PRCheckStatus, error) {
	if c.ttl <= 0 {
		return fetch()
	}

	key := repo + "@" + sha

	if statuses, ok := c.lookup(key); ok {
		return statuses, nil
	}

	ch := c.group.DoChan(key, func() (any, error) {
		// Re-check inside the single-flight critical section: a concurrent
		// caller may have populated the entry between our miss and here.
		if statuses, ok := c.lookup(key); ok {
			return statuses, nil
		}
		statuses, err := fetch()
		if err != nil {
			return nil, err
		}
		c.store(key, statuses)
		return statuses, nil
	})

	select {
	case res := <-ch:
		if res.Err != nil {
			return nil, res.Err
		}
		return res.Val.([]PRCheckStatus), nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (c *CheckStatusCache) lookup(key string) ([]PRCheckStatus, bool) {
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
	return entry.statuses, true
}

func (c *CheckStatusCache) store(key string, statuses []PRCheckStatus) {
	c.mu.Lock()
	defer c.mu.Unlock()
	// Sweep expired entries so the map stays bounded by the active
	// working set within the TTL window, not the total distinct keys
	// ever seen. Keys that are written once and never looked up again
	// (e.g. a PR's old head SHA after a force-push) would otherwise
	// occupy memory for the lifetime of the InstallationClient.
	now := c.now()
	for k, entry := range c.m {
		if now.Sub(entry.fetched) >= c.ttl {
			delete(c.m, k)
		}
	}
	c.m[key] = checkStatusCacheEntry{statuses: statuses, fetched: now}
}
