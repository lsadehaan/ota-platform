# HSM Keystore Abstraction Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Add a `KeyStore` / `CryptoProvider` abstraction layer so the executor retrieves OTA keys through an interface instead of directly accessing DB/Redis, enabling HSM-backed enterprise implementations.

**Architecture:** Two interfaces (`KeyStore` for key retrieval, `CryptoProvider` for MAC/encrypt operations) live in a new `internal/keystore/` package. A `SoftwareKeyStore` reads from PostgreSQL, a `CachedKeyStore` decorator adds Redis caching. The GSM 03.48 builder gains an optional `CryptoProvider` path. The executor worker is refactored to accept a `KeyStore` dependency instead of doing direct DB/Redis key lookups.

**Tech Stack:** Go 1.26, GORM (PostgreSQL), Redis (go-redis/v9), existing `pkg/gsm0348` builder

---

## Phase 1: KeyStore Interface and Software Implementation

### Task 1: Create KeyStore interface and types

**Files:**
- Create: `internal/keystore/keystore.go`
- Test: `internal/keystore/keystore_test.go`

**Step 1: Write the test**

Create `internal/keystore/keystore_test.go`:

```go
package keystore

import (
	"testing"
)

func TestCardKeyMaterial_HasRequiredFields(t *testing.T) {
	m := &CardKeyMaterial{
		EncKey:    []byte{0x01, 0x02},
		AuthKey:   []byte{0x03, 0x04},
		KEK:       []byte{0x05, 0x06},
		ProfileID: "test-profile",
		MSISDN:    "+1234567890",
	}

	if len(m.EncKey) != 2 {
		t.Errorf("EncKey length = %d, want 2", len(m.EncKey))
	}
	if m.ProfileID != "test-profile" {
		t.Errorf("ProfileID = %q, want %q", m.ProfileID, "test-profile")
	}
	if m.MSISDN != "+1234567890" {
		t.Errorf("MSISDN = %q, want %q", m.MSISDN, "+1234567890")
	}
}
```

**Step 2: Run the test to verify it fails**

Run: `cd /home/ubuntu/ota-platform && go test ./internal/keystore/ -run TestCardKeyMaterial -v`
Expected: FAIL — package does not exist yet

**Step 3: Write the implementation**

Create `internal/keystore/keystore.go`:

```go
package keystore

import "context"

// CardKeyMaterial holds the cryptographic keys and metadata for a SIM card.
type CardKeyMaterial struct {
	EncKey    []byte // KIC — ciphering key
	AuthKey   []byte // KID — signing/MAC key
	KEK       []byte // Key encryption key (for key provisioning to card)
	ProfileID string
	MSISDN    string
}

// KeyStore retrieves key material for a card.
type KeyStore interface {
	GetKeys(ctx context.Context, cardID string) (*CardKeyMaterial, error)
}
```

**Step 4: Run the test to verify it passes**

Run: `cd /home/ubuntu/ota-platform && go test ./internal/keystore/ -run TestCardKeyMaterial -v`
Expected: PASS

**Step 5: Commit**

```bash
git add internal/keystore/keystore.go internal/keystore/keystore_test.go
git commit -m "feat(keystore): add KeyStore interface and CardKeyMaterial type"
```

---

### Task 2: Create CryptoProvider interface

**Files:**
- Create: `internal/keystore/crypto.go`
- Modify: `internal/keystore/keystore_test.go`

**Step 1: Write the test**

Append to `internal/keystore/keystore_test.go`:

```go
func TestCryptoProvider_InterfaceSatisfied(t *testing.T) {
	// Compile-time check that a mock can satisfy CryptoProvider
	var _ CryptoProvider = &mockCryptoProvider{}
}

type mockCryptoProvider struct{}

func (m *mockCryptoProvider) ComputeMAC(ctx context.Context, cardID string, algo, mode string, data []byte) ([]byte, error) {
	return []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08}, nil
}

func (m *mockCryptoProvider) Encrypt(ctx context.Context, cardID string, algo, mode string, data []byte) ([]byte, error) {
	return data, nil
}
```

**Step 2: Run the test to verify it fails**

Run: `cd /home/ubuntu/ota-platform && go test ./internal/keystore/ -run TestCryptoProvider -v`
Expected: FAIL — `CryptoProvider` not defined

**Step 3: Write the implementation**

Create `internal/keystore/crypto.go`:

```go
package keystore

import "context"

// CryptoProvider performs MAC computation and encryption without exposing
// raw key material. When available, the GSM 03.48 builder delegates crypto
// operations to this interface instead of using software crypto with raw keys.
type CryptoProvider interface {
	ComputeMAC(ctx context.Context, cardID string, algo, mode string, data []byte) ([]byte, error)
	Encrypt(ctx context.Context, cardID string, algo, mode string, data []byte) ([]byte, error)
}
```

**Step 4: Run the test to verify it passes**

Run: `cd /home/ubuntu/ota-platform && go test ./internal/keystore/ -run TestCryptoProvider -v`
Expected: PASS

**Step 5: Commit**

```bash
git add internal/keystore/crypto.go internal/keystore/keystore_test.go
git commit -m "feat(keystore): add CryptoProvider interface"
```

---

### Task 3: Create SoftwareKeyStore implementation

**Files:**
- Create: `internal/keystore/software.go`
- Create: `internal/keystore/software_test.go`

**Step 1: Write the test**

Create `internal/keystore/software_test.go`:

```go
package keystore

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

// cardModel mirrors db.Card fields needed for testing — avoids circular import.
type cardModel struct {
	ID        uuid.UUID `gorm:"type:uuid;primaryKey"`
	ICCID     string    `gorm:"uniqueIndex;not null"`
	IMSI      string    `gorm:"uniqueIndex;not null"`
	MSISDN    string    `gorm:"uniqueIndex;not null"`
	ProfileID uuid.UUID `gorm:"type:uuid;not null"`
	EncKey    []byte    `gorm:"not null"`
	AuthKey   []byte    `gorm:"not null"`
	KEK       []byte
	Status    string `gorm:"not null;default:'active'"`
}

func (cardModel) TableName() string { return "cards" }

func setupTestDB(t *testing.T) *gorm.DB {
	t.Helper()
	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{})
	if err != nil {
		t.Fatalf("failed to open sqlite: %v", err)
	}
	if err := db.AutoMigrate(&cardModel{}); err != nil {
		t.Fatalf("failed to migrate: %v", err)
	}
	return db
}

func TestSoftwareKeyStore_GetKeys(t *testing.T) {
	db := setupTestDB(t)

	cardID := uuid.New()
	profileID := uuid.New()
	encKey := []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A, 0x0B, 0x0C, 0x0D, 0x0E, 0x0F, 0x10}
	authKey := []byte{0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0x19, 0x1A, 0x1B, 0x1C, 0x1D, 0x1E, 0x1F, 0x20}
	kek := []byte{0x21, 0x22, 0x23, 0x24, 0x25, 0x26, 0x27, 0x28, 0x29, 0x2A, 0x2B, 0x2C, 0x2D, 0x2E, 0x2F, 0x30}

	db.Create(&cardModel{
		ID:        cardID,
		ICCID:     "8901234567890123456",
		IMSI:      "234567890123456",
		MSISDN:    "+31612345678",
		ProfileID: profileID,
		EncKey:    encKey,
		AuthKey:   authKey,
		KEK:       kek,
		Status:    "active",
	})

	store := NewSoftwareKeyStore(db)
	keys, err := store.GetKeys(context.Background(), cardID.String())
	if err != nil {
		t.Fatalf("GetKeys failed: %v", err)
	}

	if string(keys.EncKey) != string(encKey) {
		t.Errorf("EncKey mismatch")
	}
	if string(keys.AuthKey) != string(authKey) {
		t.Errorf("AuthKey mismatch")
	}
	if string(keys.KEK) != string(kek) {
		t.Errorf("KEK mismatch")
	}
	if keys.ProfileID != profileID.String() {
		t.Errorf("ProfileID = %q, want %q", keys.ProfileID, profileID.String())
	}
	if keys.MSISDN != "+31612345678" {
		t.Errorf("MSISDN = %q, want %q", keys.MSISDN, "+31612345678")
	}
}

func TestSoftwareKeyStore_GetKeys_NotFound(t *testing.T) {
	db := setupTestDB(t)
	store := NewSoftwareKeyStore(db)

	_, err := store.GetKeys(context.Background(), uuid.New().String())
	if err == nil {
		t.Fatal("expected error for nonexistent card, got nil")
	}
}
```

**Step 2: Run the test to verify it fails**

Run: `cd /home/ubuntu/ota-platform && go test ./internal/keystore/ -run TestSoftwareKeyStore -v`
Expected: FAIL — `NewSoftwareKeyStore` not defined

**Step 3: Write the implementation**

Create `internal/keystore/software.go`:

```go
package keystore

import (
	"context"
	"fmt"

	"gorm.io/gorm"
)

// softwareCard is a minimal projection of the cards table, scoped to key fields only.
// This avoids importing internal/db and keeps the keystore package independent.
type softwareCard struct {
	ID        string `gorm:"primaryKey"`
	EncKey    []byte
	AuthKey   []byte
	KEK       []byte
	ProfileID string
	MSISDN    string
}

func (softwareCard) TableName() string { return "cards" }

// SoftwareKeyStore retrieves key material directly from PostgreSQL.
// This is the default (open-core) implementation.
type SoftwareKeyStore struct {
	db *gorm.DB
}

// NewSoftwareKeyStore creates a SoftwareKeyStore backed by the given database.
func NewSoftwareKeyStore(db *gorm.DB) *SoftwareKeyStore {
	return &SoftwareKeyStore{db: db}
}

// GetKeys retrieves the card's key material from PostgreSQL.
func (s *SoftwareKeyStore) GetKeys(ctx context.Context, cardID string) (*CardKeyMaterial, error) {
	var card softwareCard
	if err := s.db.WithContext(ctx).
		Select("id, enc_key, auth_key, kek, profile_id, msisdn").
		First(&card, "id = ?", cardID).Error; err != nil {
		return nil, fmt.Errorf("keystore: load card %s: %w", cardID, err)
	}

	return &CardKeyMaterial{
		EncKey:    card.EncKey,
		AuthKey:   card.AuthKey,
		KEK:       card.KEK,
		ProfileID: card.ProfileID,
		MSISDN:    card.MSISDN,
	}, nil
}
```

**Step 4: Run the test to verify it passes**

Run: `cd /home/ubuntu/ota-platform && go test ./internal/keystore/ -run TestSoftwareKeyStore -v`
Expected: PASS

**Step 5: Commit**

```bash
git add internal/keystore/software.go internal/keystore/software_test.go
git commit -m "feat(keystore): add SoftwareKeyStore (PostgreSQL-backed)"
```

---

### Task 4: Create CachedKeyStore decorator

**Files:**
- Create: `internal/keystore/cached.go`
- Create: `internal/keystore/cached_test.go`

**Step 1: Write the test**

Create `internal/keystore/cached_test.go`:

```go
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
```

Also add a shared test helper to `internal/keystore/software_test.go`:

```go
// seedTestCard inserts a test card and returns its ID as a string.
func seedTestCard(t *testing.T, db *gorm.DB) string {
	t.Helper()
	cardID := uuid.New()
	profileID := uuid.New()
	db.Create(&cardModel{
		ID:        cardID,
		ICCID:     "8901234567890123456",
		IMSI:      "234567890123456",
		MSISDN:    "+31612345678",
		ProfileID: profileID,
		EncKey:    []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A, 0x0B, 0x0C, 0x0D, 0x0E, 0x0F, 0x10},
		AuthKey:   []byte{0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0x19, 0x1A, 0x1B, 0x1C, 0x1D, 0x1E, 0x1F, 0x20},
		KEK:       []byte{0x21, 0x22, 0x23, 0x24, 0x25, 0x26, 0x27, 0x28, 0x29, 0x2A, 0x2B, 0x2C, 0x2D, 0x2E, 0x2F, 0x30},
		Status:    "active",
	})
	return cardID.String()
}
```

**Step 2: Run the test to verify it fails**

Run: `cd /home/ubuntu/ota-platform && go test ./internal/keystore/ -run TestCachedKeyStore -v`
Expected: FAIL — `NewCachedKeyStore` and `KeyCache` not defined

**Step 3: Write the implementation**

Create `internal/keystore/cached.go`:

```go
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
```

**Step 4: Run the test to verify it passes**

Run: `cd /home/ubuntu/ota-platform && go test ./internal/keystore/ -run TestCachedKeyStore -v`
Expected: PASS

**Step 5: Commit**

```bash
git add internal/keystore/cached.go internal/keystore/cached_test.go internal/keystore/software_test.go
git commit -m "feat(keystore): add CachedKeyStore decorator with KeyCache interface"
```

---

### Task 5: Create Redis KeyCache adapter

**Files:**
- Create: `internal/keystore/rediscache.go`

This task creates a thin adapter that makes the existing `redis.Client` satisfy the `KeyCache` interface by delegating to the existing `GetCardKeys` / `CacheCardKeys` methods.

**Step 1: Write the implementation**

Create `internal/keystore/rediscache.go`:

```go
package keystore

import (
	"context"

	redispkg "ota-platform/internal/redis"
)

// RedisKeyCache adapts the existing redis.Client to satisfy the KeyCache interface.
type RedisKeyCache struct {
	client RedisKeyCacheClient
}

// RedisKeyCacheClient is the subset of redis.Client methods needed for key caching.
type RedisKeyCacheClient interface {
	GetCardKeys(ctx context.Context, cardID string) (*redispkg.CardKeys, error)
	CacheCardKeys(ctx context.Context, cardID string, keys *redispkg.CardKeys) error
}

// NewRedisKeyCache creates a RedisKeyCache backed by the given Redis client.
func NewRedisKeyCache(client RedisKeyCacheClient) *RedisKeyCache {
	return &RedisKeyCache{client: client}
}

// GetCachedKeys retrieves card keys from Redis cache.
// Returns (nil, nil) on cache miss.
func (r *RedisKeyCache) GetCachedKeys(ctx context.Context, cardID string) (*CardKeyMaterial, error) {
	cardKeys, err := r.client.GetCardKeys(ctx, cardID)
	if err != nil {
		return nil, err
	}
	if cardKeys == nil {
		return nil, nil
	}

	return &CardKeyMaterial{
		EncKey:    cardKeys.EncKey,
		AuthKey:   cardKeys.AuthKey,
		KEK:       cardKeys.KEK,
		ProfileID: cardKeys.ProfileID,
		MSISDN:    cardKeys.MSISDN,
	}, nil
}

// CacheKeys stores card keys in Redis cache.
func (r *RedisKeyCache) CacheKeys(ctx context.Context, cardID string, keys *CardKeyMaterial) error {
	return r.client.CacheCardKeys(ctx, cardID, &redispkg.CardKeys{
		EncKey:    keys.EncKey,
		AuthKey:   keys.AuthKey,
		KEK:       keys.KEK,
		ProfileID: keys.ProfileID,
		MSISDN:    keys.MSISDN,
	})
}
```

**Step 2: Verify it compiles**

Run: `cd /home/ubuntu/ota-platform && go build ./internal/keystore/`
Expected: Success

**Step 3: Commit**

```bash
git add internal/keystore/rediscache.go
git commit -m "feat(keystore): add Redis KeyCache adapter"
```

---

### Task 6: Create backend registry

**Files:**
- Create: `internal/keystore/registry.go`
- Create: `internal/keystore/registry_test.go`

**Step 1: Write the test**

Create `internal/keystore/registry_test.go`:

```go
package keystore

import (
	"testing"

	"go.uber.org/zap"
	"gorm.io/gorm"
)

func TestRegistry_DefaultSoftware(t *testing.T) {
	db := setupTestDB(t)

	cfg := Config{Backend: "software"}
	ks, cp, err := Build(cfg, db, nil, zap.NewNop())
	if err != nil {
		t.Fatalf("Build failed: %v", err)
	}
	if ks == nil {
		t.Fatal("expected non-nil KeyStore")
	}
	if cp != nil {
		t.Fatal("expected nil CryptoProvider for software backend")
	}
}

func TestRegistry_UnknownBackend(t *testing.T) {
	cfg := Config{Backend: "nonexistent"}
	_, _, err := Build(cfg, nil, nil, zap.NewNop())
	if err == nil {
		t.Fatal("expected error for unknown backend")
	}
}
```

**Step 2: Run the test to verify it fails**

Run: `cd /home/ubuntu/ota-platform && go test ./internal/keystore/ -run TestRegistry -v`
Expected: FAIL — `Config`, `Build` not defined

**Step 3: Write the implementation**

Create `internal/keystore/registry.go`:

```go
package keystore

import (
	"fmt"
	"sync"

	"go.uber.org/zap"
	"gorm.io/gorm"
)

// Config holds keystore configuration.
type Config struct {
	Backend      string `json:"backend" yaml:"backend"`           // "software", or enterprise backend names
	CacheEnabled bool   `json:"cache_enabled" yaml:"cache_enabled"` // wrap with CachedKeyStore
}

// Factory creates a KeyStore and optional CryptoProvider from configuration.
type Factory func(cfg Config, db *gorm.DB, cache KeyCache, logger *zap.Logger) (KeyStore, CryptoProvider, error)

var (
	mu        sync.RWMutex
	factories = map[string]Factory{}
)

// Register registers a named backend factory. Enterprise packages call this
// from init() to register HSM backends.
func Register(name string, factory Factory) {
	mu.Lock()
	defer mu.Unlock()
	factories[name] = factory
}

// Build constructs a KeyStore (and optional CryptoProvider) based on config.
// If CacheEnabled is true and the backend does not provide its own CryptoProvider,
// the KeyStore is wrapped with a CachedKeyStore decorator.
func Build(cfg Config, db *gorm.DB, cache KeyCache, logger *zap.Logger) (KeyStore, CryptoProvider, error) {
	backend := cfg.Backend
	if backend == "" {
		backend = "software"
	}

	if backend == "software" {
		ks := NewSoftwareKeyStore(db)
		if cfg.CacheEnabled && cache != nil {
			return NewCachedKeyStore(ks, cache), nil, nil
		}
		return ks, nil, nil
	}

	mu.RLock()
	factory, ok := factories[backend]
	mu.RUnlock()
	if !ok {
		return nil, nil, fmt.Errorf("keystore: unknown backend %q", backend)
	}

	ks, cp, err := factory(cfg, db, cache, logger)
	if err != nil {
		return nil, nil, fmt.Errorf("keystore: build %q: %w", backend, err)
	}

	// Wrap with cache if backend doesn't provide CryptoProvider
	// (i.e., keys are extracted and can be cached).
	if cp == nil && cfg.CacheEnabled && cache != nil {
		ks = NewCachedKeyStore(ks, cache)
	}

	return ks, cp, nil
}
```

**Step 4: Run the test to verify it passes**

Run: `cd /home/ubuntu/ota-platform && go test ./internal/keystore/ -run TestRegistry -v`
Expected: PASS

**Step 5: Commit**

```bash
git add internal/keystore/registry.go internal/keystore/registry_test.go
git commit -m "feat(keystore): add backend registry with Build() factory"
```

---

## Phase 2: GSM 03.48 Builder CryptoProvider Integration

### Task 7: Add CryptoProvider support to CommandPacketInput

**Files:**
- Modify: `pkg/gsm0348/types.go:82-88`
- Modify: `pkg/gsm0348/builder.go:53-73`
- Test: `pkg/gsm0348/builder_test.go`

**Step 1: Write the test**

Add to `pkg/gsm0348/builder_test.go`:

```go
// TestBuildCommandPacket_WithCryptoProvider tests that the builder delegates
// MAC and encryption to a CryptoProvider when one is provided.
func TestBuildCommandPacket_WithCryptoProvider(t *testing.T) {
	sp := &SecurityProfile{
		CertMode:    CertCC,
		Ciphered:    true,
		CounterMode: CounterReplayCheck,
		PoRMode:     PoRAlways,
		PoRCertMode: CertCC,
		PoRCiphered: false,
		PoRProtocol: 0x01,
		KIcAlgo:     0x01,
		KIcMode:     Cipher3DES_CBC_2Keys,
		KIcKeysetID: 0x01,
		KIDAlgo:     0x01,
		KIDMode:     Cipher3DES_CBC_2Keys,
		KIDKeysetID: 0x01,
	}

	cipherKey := make([]byte, 16)
	signingKey := make([]byte, 16)
	for i := range cipherKey {
		cipherKey[i] = byte(i + 0x10)
		signingKey[i] = byte(i + 0x20)
	}

	// Build with raw keys (baseline).
	inputRaw := &CommandPacketInput{
		TAR:          [3]byte{0xB0, 0x00, 0x10},
		Counter:      [5]byte{0x00, 0x00, 0x00, 0x00, 0x05},
		CipheringKey: cipherKey,
		SigningKey:   signingKey,
		UserData:     []byte{0xA0, 0xA4, 0x00, 0x00, 0x02, 0x3F, 0x00},
	}

	resultRaw, err := sp.BuildCommandPacket(inputRaw)
	if err != nil {
		t.Fatalf("raw BuildCommandPacket failed: %v", err)
	}

	// Build with CryptoProvider that uses the same keys internally.
	provider := &testCryptoProvider{cipherKey: cipherKey, signingKey: signingKey}
	inputCP := &CommandPacketInput{
		TAR:            [3]byte{0xB0, 0x00, 0x10},
		Counter:        [5]byte{0x00, 0x00, 0x00, 0x00, 0x05},
		CryptoProvider: provider,
		CardID:         "test-card-id",
		UserData:       []byte{0xA0, 0xA4, 0x00, 0x00, 0x02, 0x3F, 0x00},
	}

	resultCP, err := sp.BuildCommandPacket(inputCP)
	if err != nil {
		t.Fatalf("CryptoProvider BuildCommandPacket failed: %v", err)
	}

	// Both should produce identical output.
	if hex.EncodeToString(resultRaw) != hex.EncodeToString(resultCP) {
		t.Errorf("CryptoProvider result differs from raw key result\n  raw: %s\n  cp:  %s",
			hex.EncodeToString(resultRaw), hex.EncodeToString(resultCP))
	}

	if !provider.macCalled {
		t.Error("CryptoProvider.ComputeMAC was not called")
	}
	if !provider.encryptCalled {
		t.Error("CryptoProvider.Encrypt was not called")
	}
}

// testCryptoProvider delegates to the existing software crypto using the provided keys.
type testCryptoProvider struct {
	cipherKey     []byte
	signingKey    []byte
	macCalled     bool
	encryptCalled bool
}

func (p *testCryptoProvider) ComputeMAC(ctx context.Context, cardID string, algo, mode string, data []byte) ([]byte, error) {
	p.macCalled = true
	cipherMode := mapCipherMode(mode)
	return computeMAC(cipherMode, p.signingKey, nil, data)
}

func (p *testCryptoProvider) Encrypt(ctx context.Context, cardID string, algo, mode string, data []byte) ([]byte, error) {
	p.encryptCalled = true
	cipherMode := mapCipherMode(mode)
	return encryptData(cipherMode, p.cipherKey, nil, data)
}
```

Also add `"context"` to the import block if not already present.

**Step 2: Run the test to verify it fails**

Run: `cd /home/ubuntu/ota-platform && go test ./pkg/gsm0348/ -run TestBuildCommandPacket_WithCryptoProvider -v`
Expected: FAIL — `CryptoProvider` field not on `CommandPacketInput`

**Step 3: Modify CommandPacketInput and builder**

Modify `pkg/gsm0348/types.go` — add fields to `CommandPacketInput`:

```go
// CommandPacketInput provides all variable data needed to build a single
// GSM 03.48 command packet.
type CommandPacketInput struct {
	TAR          [3]byte
	Counter      [5]byte
	CipheringKey []byte // Used when CryptoProvider is nil
	SigningKey    []byte // Used when CryptoProvider is nil
	UserData     []byte

	// CryptoProvider, when set, is used for MAC and encryption operations
	// instead of the raw CipheringKey/SigningKey fields above.
	CryptoProvider CryptoProvider
	// CardID is required when CryptoProvider is set, to identify which
	// card's keys to use for the crypto operations.
	CardID string
}

// CryptoProvider performs MAC and encryption without exposing raw key material.
type CryptoProvider interface {
	ComputeMAC(ctx context.Context, cardID string, algo, mode string, data []byte) ([]byte, error)
	Encrypt(ctx context.Context, cardID string, algo, mode string, data []byte) ([]byte, error)
}
```

Add `"context"` to the import block in `types.go`.

Modify `pkg/gsm0348/builder.go` — update the MAC and encrypt sections in `BuildCommandPacket`:

In the MAC section (around line 53), replace the `case CertCC:` block:

```go
	case CertCC:
		// Cryptographic Checksum (MAC)
		var mac []byte
		var err error
		if input.CryptoProvider != nil {
			macInput := append(header, paddedData...)
			// Pad to 8-byte boundary if needed.
			if len(macInput)%8 != 0 {
				padLen := 8 - (len(macInput) % 8)
				macInput = append(macInput, make([]byte, padLen)...)
			}
			mac, err = input.CryptoProvider.ComputeMAC(context.Background(), input.CardID, algoName(sp.KIDAlgo), cipherModeName(sp.KIDMode), macInput)
		} else {
			mac, err = computeMAC(sp.KIDMode, input.SigningKey, header, paddedData)
		}
		if err != nil {
			return nil, fmt.Errorf("gsm0348: compute MAC: %w", err)
		}
		copy(signature, mac[:sigLen])
```

In the encryption section (around line 67), replace the `if sp.Ciphered` block:

```go
	if sp.Ciphered {
		var encrypted []byte
		var err error
		if input.CryptoProvider != nil {
			encrypted, err = input.CryptoProvider.Encrypt(context.Background(), input.CardID, algoName(sp.KIcAlgo), cipherModeName(sp.KIcMode), securedData)
		} else {
			encrypted, err = encryptData(sp.KIcMode, input.CipheringKey, input.Counter[:], securedData)
		}
		if err != nil {
			return nil, fmt.Errorf("gsm0348: encrypt data: %w", err)
		}
		securedData = encrypted
	}
```

Add helper functions to `builder.go`:

```go
// algoName returns a string name for the algorithm byte.
func algoName(algo byte) string {
	switch algo {
	case 0x00:
		return "DES"
	case 0x01:
		return "DES"
	case 0x02:
		return "AES"
	default:
		return "DES"
	}
}

// cipherModeName returns a string name for a CipherMode.
func cipherModeName(mode CipherMode) string {
	switch mode {
	case CipherDES_CBC:
		return "DES_CBC"
	case Cipher3DES_CBC_2Keys:
		return "TRIPLE_DES_CBC_2_KEYS"
	case Cipher3DES_CBC_3Keys:
		return "TRIPLE_DES_CBC_3_KEYS"
	case CipherAES_CBC:
		return "AES_CBC"
	default:
		return "DES_CBC"
	}
}
```

Add `"context"` to the import block in `builder.go`.

**Step 4: Run all builder tests to verify they pass**

Run: `cd /home/ubuntu/ota-platform && go test ./pkg/gsm0348/ -v`
Expected: ALL PASS (existing tests still pass, new test passes)

**Step 5: Commit**

```bash
git add pkg/gsm0348/types.go pkg/gsm0348/builder.go pkg/gsm0348/builder_test.go
git commit -m "feat(gsm0348): add optional CryptoProvider path to builder"
```

---

### Task 8: Add CryptoProvider support to ParseResponsePacket

**Files:**
- Modify: `pkg/gsm0348/types.go` (add `ResponseCryptoInput` or extend existing types)
- Modify: `pkg/gsm0348/response.go:16`

The `ParseResponsePacket` method currently takes `cipherKey` and `signKey` as raw bytes. We need to add an alternate path that uses `CryptoProvider`.

**Step 1: Modify ParseResponsePacket signature**

This is a non-breaking change. Add a new method `ParseResponsePacketWithProvider` that accepts a `CryptoProvider` instead of raw keys. The existing method remains unchanged.

Add to `pkg/gsm0348/response.go`:

```go
// ParseResponsePacketWithProvider parses a response packet using a CryptoProvider
// for decryption instead of raw key material.
func (sp *SecurityProfile) ParseResponsePacketWithProvider(raw []byte, cardID string, provider CryptoProvider) (*ResponsePacket, error) {
	if len(raw) < 2 {
		return nil, fmt.Errorf("gsm0348: response packet too short for RPL")
	}

	rpl := int(raw[0])<<8 | int(raw[1])
	if len(raw) < 2+rpl {
		return nil, fmt.Errorf("gsm0348: response packet truncated: RPL=%d but only %d bytes remain", rpl, len(raw)-2)
	}

	pos := 2
	if pos >= len(raw) {
		return nil, fmt.Errorf("gsm0348: response packet too short for RHL")
	}
	rhl := int(raw[pos])
	pos++

	if pos+3 > len(raw) {
		return nil, fmt.Errorf("gsm0348: response packet too short for TAR")
	}
	var tar [3]byte
	copy(tar[:], raw[pos:pos+3])
	pos += 3

	if pos+5 > len(raw) {
		return nil, fmt.Errorf("gsm0348: response packet too short for CNTR")
	}
	var counter [5]byte
	copy(counter[:], raw[pos:pos+5])
	pos += 5

	if pos >= len(raw) {
		return nil, fmt.Errorf("gsm0348: response packet too short for PCNTR")
	}
	pcntr := raw[pos]
	pos++

	if pos >= len(raw) {
		return nil, fmt.Errorf("gsm0348: response packet too short for StatusCode")
	}
	statusCode := raw[pos]
	pos++

	sigLen := rhl - 10
	if sigLen < 0 {
		sigLen = 0
	}

	var signature []byte
	if sigLen > 0 {
		if pos+sigLen > len(raw) {
			return nil, fmt.Errorf("gsm0348: response packet too short for signature")
		}
		signature = make([]byte, sigLen)
		copy(signature, raw[pos:pos+sigLen])
		pos += sigLen
	}

	dataEnd := 2 + rpl
	var data []byte
	if pos < dataEnd {
		data = make([]byte, dataEnd-pos)
		copy(data, raw[pos:dataEnd])
	}

	// Decrypt if PoRCiphered is set — use CryptoProvider.
	if sp.PoRCiphered && len(data) > 0 {
		// CryptoProvider.Encrypt is used for both encrypt and decrypt
		// in CryptoProvider mode. The HSM handles directionality.
		// For now, we use a Decrypt method if available, or fall back.
		// Since CryptoProvider only exposes Encrypt, the HSM implementation
		// must handle decrypt via a separate method or mode parameter.
		// For the MVP, response decryption with HSM is not supported —
		// this path requires raw keys.
		return nil, fmt.Errorf("gsm0348: response decryption with CryptoProvider not yet supported; use ParseResponsePacket with raw keys")
	}

	if int(pcntr) > 0 && len(data) >= int(pcntr) {
		data = data[:len(data)-int(pcntr)]
	}

	return &ResponsePacket{
		TAR:        tar,
		Counter:    counter,
		PCNTR:      pcntr,
		StatusCode: statusCode,
		Signature:  signature,
		Data:       data,
	}, nil
}
```

**Step 2: Verify it compiles and existing tests pass**

Run: `cd /home/ubuntu/ota-platform && go test ./pkg/gsm0348/ -v`
Expected: ALL PASS

**Step 3: Commit**

```bash
git add pkg/gsm0348/response.go
git commit -m "feat(gsm0348): add ParseResponsePacketWithProvider for HSM path"
```

---

## Phase 3: Executor Refactoring

### Task 9: Add KeyStore to CardWorker

**Files:**
- Modify: `internal/executor/worker.go:28-51` (add `keyStore` field, update constructor)
- Modify: `internal/executor/service.go:55-57` (update `NewService`)
- Modify: `internal/executor/runtime.go:31-32` (wire up keystore)

**Step 1: Modify CardWorker struct and constructor**

In `internal/executor/worker.go`, add `keyStore` field and optional `cryptoProvider`:

```go
import (
	// ... existing imports ...
	"ota-platform/internal/keystore"
)

type CardWorker struct {
	db              *gorm.DB
	execution       ExecutionStore
	redis           CoordinationStore
	keyStore        keystore.KeyStore
	cryptoProvider  keystore.CryptoProvider // nil when using software crypto
	smsProducer     Producer
	logProducer     Producer
	eventProducer   Producer
	wsHub           WSHub
	logger          *zap.Logger
}

func NewCardWorker(database *gorm.DB, executionStore ExecutionStore, rdb CoordinationStore, ks keystore.KeyStore, cp keystore.CryptoProvider, smsProducer, logProducer, eventProducer Producer, wsHub WSHub, logger *zap.Logger) *CardWorker {
	return &CardWorker{
		db:             database,
		execution:      executionStore,
		redis:          rdb,
		keyStore:       ks,
		cryptoProvider: cp,
		smsProducer:    smsProducer,
		logProducer:    logProducer,
		eventProducer:  eventProducer,
		wsHub:          wsHub,
		logger:         logger,
	}
}
```

Update `internal/executor/service.go`:

```go
func NewService(database *gorm.DB, executionStore ExecutionStore, store CoordinationStore, ks keystore.KeyStore, cp keystore.CryptoProvider, smsProducer, logProducer, eventProducer Producer, wsHub WSHub, logger *zap.Logger) *Service {
	return NewCardWorker(database, executionStore, store, ks, cp, smsProducer, logProducer, eventProducer, wsHub, logger)
}
```

Add import `"ota-platform/internal/keystore"` to `service.go`.

**Step 2: Update runtime.go to wire the keystore**

In `internal/executor/runtime.go`:

```go
import (
	// ... existing imports ...
	"ota-platform/internal/keystore"
)

func Run(ctx context.Context, logger *zap.Logger) error {
	database := bootstrap.MustGormDB(logger)
	scylla := bootstrap.MustScylla(logger)
	defer scylla.Close()
	rdb := bootstrap.MustCoordinationStore(logger)
	defer rdb.Close()

	// Build keystore.
	keyCfg := keystore.Config{
		Backend:      bootstrap.EnvOr("KEYSTORE_BACKEND", "software"),
		CacheEnabled: true,
	}
	redisCache := keystore.NewRedisKeyCache(rdb)
	ks, cp, err := keystore.Build(keyCfg, database, redisCache, logger.Named("keystore"))
	if err != nil {
		return fmt.Errorf("build keystore: %w", err)
	}

	kafkaBrokers := bootstrap.KafkaBrokers()
	// ... rest unchanged, but pass ks, cp to NewService ...
	cardWorker := NewService(database, executionStore, rdb, ks, cp, smsProducer, logProducer, eventProducer, nil, logger.Named("card-worker"))
	// ... rest unchanged ...
}
```

Add `"fmt"` to imports if not present.

**Step 3: Update all callers of NewService**

Search for all call sites of `executor.NewService` and update them to pass `nil, nil` for keystore/crypto params (or a real keystore where appropriate).

In `internal/planner/pipeline_benchmark_test.go` (line 293 and 329), update:

```go
worker := executor.NewService(nil, pipelineExecutionStore{}, coordination, nil, nil, smsProducer, logProducer, eventProducer, nil, zap.NewNop())
```

**Step 4: Verify compilation**

Run: `cd /home/ubuntu/ota-platform && go build ./...`
Expected: Success

**Step 5: Commit**

```bash
git add internal/executor/worker.go internal/executor/service.go internal/executor/runtime.go internal/planner/pipeline_benchmark_test.go
git commit -m "refactor(executor): inject KeyStore and CryptoProvider dependencies"
```

---

### Task 10: Refactor handleActivate to use KeyStore

**Files:**
- Modify: `internal/executor/worker.go:107-125` (key retrieval in handleActivate)
- Modify: `internal/executor/worker.go:164-172` (packet building)

**Step 1: Replace direct DB/Redis key lookup with KeyStore call**

In `handleActivate`, replace lines 107-125 (the key loading section) with:

```go
	// 2. Load card keys via keystore.
	cardKeys, err := w.keyStore.GetKeys(ctx, event.CardID)
	if err != nil {
		return fmt.Errorf("get card keys: %w", err)
	}
```

Replace the packet building section (lines 164-172) to use CryptoProvider when available:

```go
	// 7. Build GSM 03.48 command packet.
	var tar [3]byte
	copy(tar[:], cmd.TAR)

	var counter [5]byte
	binary.BigEndian.PutUint32(counter[1:], uint32(counterVal))
	counter[0] = byte(counterVal >> 32)

	input := &gsm0348.CommandPacketInput{
		TAR:      tar,
		Counter:  counter,
		UserData: cmd.Script,
	}

	if w.cryptoProvider != nil {
		input.CryptoProvider = w.cryptoProvider
		input.CardID = event.CardID
	} else {
		input.CipheringKey = cardKeys.EncKey
		input.SigningKey = cardKeys.AuthKey
	}

	packet, err := secProfile.BuildCommandPacket(input)
	if err != nil {
		return fmt.Errorf("build command packet: %w", err)
	}
```

Also update the remaining references to `cardKeys` in handleActivate that use `cardKeys.MSISDN` and `cardKeys.ProfileID`. These fields are on `keystore.CardKeyMaterial` which has the same field names, so the code stays the same.

**Step 2: Verify compilation and existing tests**

Run: `cd /home/ubuntu/ota-platform && go build ./... && go test ./internal/executor/ -v`
Expected: Build succeeds, tests pass

**Step 3: Commit**

```bash
git add internal/executor/worker.go
git commit -m "refactor(executor): use KeyStore in handleActivate instead of direct DB/Redis"
```

---

### Task 11: Refactor handleMO to use KeyStore

**Files:**
- Modify: `internal/executor/worker.go:574-592` (key retrieval in handleMO)
- Modify: `internal/executor/worker.go:620` (ParseResponsePacket call)

**Step 1: Replace direct DB/Redis key lookup with KeyStore call**

In `handleMO`, replace lines 574-592 (the key loading section) with:

```go
	// 2. Load card keys via keystore.
	cardKeys, err := w.redis.GetCardKeys(ctx, event.CardID)
	// ... replace entire block with:
	cardKeys, err := w.keyStore.GetKeys(ctx, event.CardID)
	if err != nil {
		return fmt.Errorf("get card keys: %w", err)
	}
```

For the `ParseResponsePacket` call (line 620), keep using raw keys since response decryption with CryptoProvider is not yet supported:

```go
	resp, err := secProfile.ParseResponsePacket(payloadBytes, cardKeys.EncKey, cardKeys.AuthKey)
```

This works because even in CryptoProvider mode, the `KeyStore.GetKeys()` returns the key material. Only in HSM-with-crypto-offload mode would the keys be empty/nil, but response parsing with HSM crypto is explicitly deferred.

**Step 2: Verify compilation and tests**

Run: `cd /home/ubuntu/ota-platform && go build ./... && go test ./internal/executor/ -v`
Expected: Build succeeds, tests pass

**Step 3: Commit**

```bash
git add internal/executor/worker.go
git commit -m "refactor(executor): use KeyStore in handleMO instead of direct DB/Redis"
```

---

### Task 12: Remove card key methods from CoordinationStore interface

**Files:**
- Modify: `internal/executor/service.go:16-17` (remove `GetCardKeys` and `CacheCardKeys`)

**Step 1: Check for remaining usages**

Search the executor package for any remaining calls to `w.redis.GetCardKeys` or `w.redis.CacheCardKeys`. If none remain after Tasks 10-11, remove them from the interface.

**Step 2: Remove from interface**

In `internal/executor/service.go`, remove these two lines from `CoordinationStore`:

```go
	GetCardKeys(ctx context.Context, cardID string) (*redispkg.CardKeys, error)
	CacheCardKeys(ctx context.Context, cardID string, keys *redispkg.CardKeys) error
```

**Step 3: Update any mock implementations**

Check `internal/planner/pipeline_benchmark_test.go` and `internal/executor/worker_benchmark_test.go` for mock implementations of `CoordinationStore` — remove the `GetCardKeys` and `CacheCardKeys` methods from them.

**Step 4: Verify compilation**

Run: `cd /home/ubuntu/ota-platform && go build ./...`
Expected: Success

**Step 5: Commit**

```bash
git add internal/executor/service.go internal/planner/pipeline_benchmark_test.go internal/executor/worker_benchmark_test.go
git commit -m "refactor(executor): remove card key methods from CoordinationStore interface"
```

---

### Task 13: Add bootstrap helper for EnvOr

**Files:**
- Modify or verify: `internal/bootstrap/` package

**Step 1: Check if EnvOr already exists**

Search for `func EnvOr` in the bootstrap package. If it doesn't exist, add it.

```go
// EnvOr returns the value of the environment variable named by key,
// or fallback if the variable is not set or empty.
func EnvOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
```

**Step 2: Verify compilation**

Run: `cd /home/ubuntu/ota-platform && go build ./...`
Expected: Success

**Step 3: Commit (if changes were made)**

```bash
git add internal/bootstrap/
git commit -m "feat(bootstrap): add EnvOr helper"
```

---

## Phase 4: Integration Test

### Task 14: End-to-end keystore integration test

**Files:**
- Create: `internal/keystore/integration_test.go`

**Step 1: Write the integration test**

Create `internal/keystore/integration_test.go`:

```go
package keystore

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

// TestIntegration_SoftwareWithCache tests the full stack:
// CachedKeyStore -> SoftwareKeyStore -> SQLite DB
func TestIntegration_SoftwareWithCache(t *testing.T) {
	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{})
	if err != nil {
		t.Fatalf("failed to open sqlite: %v", err)
	}
	db.AutoMigrate(&cardModel{})

	cardID := uuid.New()
	profileID := uuid.New()
	encKey := []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A, 0x0B, 0x0C, 0x0D, 0x0E, 0x0F, 0x10}
	authKey := []byte{0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0x19, 0x1A, 0x1B, 0x1C, 0x1D, 0x1E, 0x1F, 0x20}

	db.Create(&cardModel{
		ID:        cardID,
		ICCID:     "8901234567890123456",
		IMSI:      "234567890123456",
		MSISDN:    "+31612345678",
		ProfileID: profileID,
		EncKey:    encKey,
		AuthKey:   authKey,
		Status:    "active",
	})

	// Build via registry.
	cache := newMockCache()
	cfg := Config{Backend: "software", CacheEnabled: true}
	ks, cp, err := Build(cfg, db, cache, nil)
	if err != nil {
		t.Fatalf("Build failed: %v", err)
	}
	if cp != nil {
		t.Error("expected nil CryptoProvider for software backend")
	}

	// First call — cache miss, reads from DB.
	keys1, err := ks.GetKeys(context.Background(), cardID.String())
	if err != nil {
		t.Fatalf("first GetKeys failed: %v", err)
	}
	if string(keys1.EncKey) != string(encKey) {
		t.Error("EncKey mismatch on first call")
	}

	// Verify it's in cache now.
	cached, _ := cache.GetCachedKeys(context.Background(), cardID.String())
	if cached == nil {
		t.Fatal("expected keys to be cached after first call")
	}

	// Second call — should hit cache.
	keys2, err := ks.GetKeys(context.Background(), cardID.String())
	if err != nil {
		t.Fatalf("second GetKeys failed: %v", err)
	}
	if string(keys2.EncKey) != string(encKey) {
		t.Error("EncKey mismatch on second call")
	}
}
```

**Step 2: Run the test**

Run: `cd /home/ubuntu/ota-platform && go test ./internal/keystore/ -v`
Expected: ALL PASS

**Step 3: Commit**

```bash
git add internal/keystore/integration_test.go
git commit -m "test(keystore): add end-to-end integration test"
```

---

### Task 15: Run full test suite and verify

**Step 1: Run all tests**

Run: `cd /home/ubuntu/ota-platform && go test ./... -count=1`
Expected: ALL PASS

**Step 2: Run vet**

Run: `cd /home/ubuntu/ota-platform && go vet ./...`
Expected: No issues

**Step 3: Final commit if any cleanup needed**

```bash
git add -A
git commit -m "chore: final cleanup after keystore abstraction"
```
