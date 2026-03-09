package executor

import (
	"sync"
	"time"
)

type cacheEntry[T any] struct {
	value     T
	expiresAt time.Time
}

type ttlCache[T any] struct {
	mu         sync.Mutex
	entries    map[string]cacheEntry[T]
	ttl        time.Duration
	maxEntries int
}

func newTTLCache[T any](ttl time.Duration, maxEntries int) *ttlCache[T] {
	if ttl <= 0 {
		ttl = 5 * time.Minute
	}
	if maxEntries <= 0 {
		maxEntries = 1024
	}
	return &ttlCache[T]{
		entries:    make(map[string]cacheEntry[T]),
		ttl:        ttl,
		maxEntries: maxEntries,
	}
}

func (c *ttlCache[T]) Get(key string) (T, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	entry, ok := c.entries[key]
	if !ok {
		var zero T
		return zero, false
	}
	if time.Now().After(entry.expiresAt) {
		delete(c.entries, key)
		var zero T
		return zero, false
	}
	return entry.value, true
}

// evictSampleSize is the max number of entries to inspect during eviction.
// Keeps Set O(1) amortized instead of O(n).
const evictSampleSize = 16

func (c *ttlCache[T]) Set(key string, value T) {
	c.mu.Lock()
	defer c.mu.Unlock()

	now := time.Now()
	c.entries[key] = cacheEntry[T]{value: value, expiresAt: now.Add(c.ttl)}
	if len(c.entries) <= c.maxEntries {
		return
	}

	// Approximate eviction: sample a small batch of entries (Go map range
	// order is randomised), evict any expired ones and — if still over
	// capacity — the entry with the nearest expiry from the sample.
	var oldestKey string
	var oldestExp time.Time
	sampled := 0
	for k, entry := range c.entries {
		if now.After(entry.expiresAt) {
			delete(c.entries, k)
		} else if oldestKey == "" || entry.expiresAt.Before(oldestExp) {
			oldestKey = k
			oldestExp = entry.expiresAt
		}
		sampled++
		if sampled >= evictSampleSize {
			break
		}
	}
	if len(c.entries) > c.maxEntries && oldestKey != "" {
		delete(c.entries, oldestKey)
	}
}
