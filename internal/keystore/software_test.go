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
