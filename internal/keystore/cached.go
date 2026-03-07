package keystore

import "context"

// KeyCache abstracts the cache backend used by CachedKeyStore.
// The Redis implementation in internal/redis satisfies this interface.
type KeyCache interface {
	GetCachedKeys(ctx context.Context, cardID string) (*CardKeyMaterial, error)
	CacheKeys(ctx context.Context, cardID string, keys *CardKeyMaterial) error
}

// CachedKeyStore is a decorator that adds caching to any KeyStore.
// On cache miss it delegates to the inner KeyStore and caches the result.
type CachedKeyStore struct {
	inner KeyStore
	cache KeyCache
}

// NewCachedKeyStore wraps inner with a caching layer.
func NewCachedKeyStore(inner KeyStore, cache KeyCache) *CachedKeyStore {
	return &CachedKeyStore{inner: inner, cache: cache}
}

// GetKeys checks the cache first. On miss, delegates to inner and caches the result.
func (c *CachedKeyStore) GetKeys(ctx context.Context, cardID string) (*CardKeyMaterial, error) {
	keys, err := c.cache.GetCachedKeys(ctx, cardID)
	if err != nil {
		// Cache error — fall through to inner store.
		keys = nil
	}
	if keys != nil {
		return keys, nil
	}

	keys, err = c.inner.GetKeys(ctx, cardID)
	if err != nil {
		return nil, err
	}

	_ = c.cache.CacheKeys(ctx, cardID, keys)
	return keys, nil
}
