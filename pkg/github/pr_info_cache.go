package github

import (
	"context"
	"strconv"
	"sync"
)

// requestPRInfoCache memoises FetchPullRequest results for the lifetime
// of a single webhook delivery (or any other ctx-bounded operation it is
// attached to). A fresh cache is constructed at every entry point via
// WithPRInfoCache, so the cached value cannot outlive the scope it was
// created for — eliminating the staleness risk that a TTL'd cross-delivery
// cache would have for mutable PR head data (HeadSHA can change between
// deliveries when a new commit is pushed).
//
// Within one scope, multiple handlers calling FetchPullRequest for the
// same (repo, pr) collapse to a single upstream GitHub call. Concurrent
// callers from spawned goroutines that share the ctx see the populated
// entry once the first fetch completes.
type requestPRInfoCache struct {
	mu sync.Mutex
	m  map[string]*PullRequestInfo
}

type prInfoCacheCtxKey struct{}

// WithPRInfoCache returns a new context carrying a fresh request-scoped
// PR-info cache. Webhook entry points should wrap their per-operation
// ctx with this so FetchPullRequest calls within that scope dedupe.
//
// Calling WithPRInfoCache twice on the same ctx replaces the previous
// cache with a fresh one; nested scopes thus get their own clean cache.
func WithPRInfoCache(ctx context.Context) context.Context {
	return context.WithValue(ctx, prInfoCacheCtxKey{}, &requestPRInfoCache{
		m: make(map[string]*PullRequestInfo),
	})
}

// prInfoCacheFromContext returns the request-scoped cache attached to ctx,
// or nil if none has been attached. Returning nil lets FetchPullRequest
// fall through to a raw fetch for callers (tests, ad-hoc usage) that did
// not set up a cache scope.
func prInfoCacheFromContext(ctx context.Context) *requestPRInfoCache {
	c, _ := ctx.Value(prInfoCacheCtxKey{}).(*requestPRInfoCache)
	return c
}

// get returns the cached PR info for (repo, pr) if present.
func (c *requestPRInfoCache) get(repo string, pr int) (*PullRequestInfo, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	info, ok := c.m[cacheKey(repo, pr)]
	return info, ok
}

// set stores PR info for (repo, pr). A copy is stored so callers
// mutating the returned struct cannot affect the cached value.
func (c *requestPRInfoCache) set(repo string, pr int, info *PullRequestInfo) {
	if info == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	stored := *info
	c.m[cacheKey(repo, pr)] = &stored
}

func cacheKey(repo string, pr int) string {
	return repo + "#" + strconv.Itoa(pr)
}
