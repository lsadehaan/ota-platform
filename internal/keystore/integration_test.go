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
