package keystore

import (
	"context"
	"sync/atomic"
	"testing"
)

// countingKeyStore wraps a KeyStore and counts calls to GetKeys.
type countingKeyStore struct {
	inner KeyStore
	calls atomic.Int64
}

func (c *countingKeyStore) GetKeys(ctx context.Context, cardID string) (*CardKeyMaterial, error) {
	c.calls.Add(1)
	return c.inner.GetKeys(ctx, cardID)
}

// mockCache implements the KeyCache interface for testing.
type mockCache struct {
	store map[string]*CardKeyMaterial
}

func newMockCache() *mockCache {
	return &mockCache{store: make(map[string]*CardKeyMaterial)}
}

func (m *mockCache) GetCachedKeys(ctx context.Context, cardID string) (*CardKeyMaterial, error) {
	if keys, ok := m.store[cardID]; ok {
		return keys, nil
	}
	return nil, nil
}

func (m *mockCache) CacheKeys(ctx context.Context, cardID string, keys *CardKeyMaterial) error {
	m.store[cardID] = keys
	return nil
}

func TestCachedKeyStore_CacheMiss_DelegatesToInner(t *testing.T) {
	db := setupTestDB(t)
	cardID := seedTestCard(t, db)

	inner := &countingKeyStore{inner: NewSoftwareKeyStore(db)}
	cache := newMockCache()
	cached := NewCachedKeyStore(inner, cache)

	keys, err := cached.GetKeys(context.Background(), cardID)
	if err != nil {
		t.Fatalf("GetKeys failed: %v", err)
	}
	if keys == nil {
		t.Fatal("expected non-nil keys")
	}
	if inner.calls.Load() != 1 {
		t.Errorf("inner calls = %d, want 1", inner.calls.Load())
	}
}

func TestCachedKeyStore_CacheHit_SkipsInner(t *testing.T) {
	db := setupTestDB(t)
	cardID := seedTestCard(t, db)

	inner := &countingKeyStore{inner: NewSoftwareKeyStore(db)}
	cache := newMockCache()
	cached := NewCachedKeyStore(inner, cache)

	// First call — cache miss.
	_, err := cached.GetKeys(context.Background(), cardID)
	if err != nil {
		t.Fatalf("first GetKeys failed: %v", err)
	}

	// Second call — cache hit.
	keys, err := cached.GetKeys(context.Background(), cardID)
	if err != nil {
		t.Fatalf("second GetKeys failed: %v", err)
	}
	if keys == nil {
		t.Fatal("expected non-nil keys on cache hit")
	}
	if inner.calls.Load() != 1 {
		t.Errorf("inner calls = %d, want 1 (should have hit cache)", inner.calls.Load())
	}
}
