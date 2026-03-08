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

func (c *ttlCache[T]) Set(key string, value T) {
	c.mu.Lock()
	defer c.mu.Unlock()

	now := time.Now()
	c.entries[key] = cacheEntry[T]{value: value, expiresAt: now.Add(c.ttl)}
	if len(c.entries) <= c.maxEntries {
		return
	}
	for k, entry := range c.entries {
		if now.After(entry.expiresAt) {
			delete(c.entries, k)
		}
	}
	for len(c.entries) > c.maxEntries {
		for k := range c.entries {
			delete(c.entries, k)
			break
		}
	}
}
