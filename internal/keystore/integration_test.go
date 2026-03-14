package keystore

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

// TestIntegration_SoftwareKeyStore tests the SoftwareKeyStore reading from SQLite.
func TestIntegration_SoftwareKeyStore(t *testing.T) {
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

	ks := NewSoftwareKeyStore(db)

	keys, err := ks.GetKeys(context.Background(), cardID.String())
	if err != nil {
		t.Fatalf("GetKeys failed: %v", err)
	}
	if string(keys.EncKey) != string(encKey) {
		t.Error("EncKey mismatch")
	}
	if string(keys.AuthKey) != string(authKey) {
		t.Error("AuthKey mismatch")
	}
	if keys.ProfileID != profileID.String() {
		t.Errorf("ProfileID = %q, want %q", keys.ProfileID, profileID.String())
	}
	if keys.MSISDN != "+31612345678" {
		t.Errorf("MSISDN = %q, want %q", keys.MSISDN, "+31612345678")
	}
}

// TestIntegration_ScyllaKeyStore tests the ScyllaKeyStore adapter with a mock reader.
func TestIntegration_ScyllaKeyStore(t *testing.T) {
	encKey := []byte{0x01, 0x02}
	authKey := []byte{0x03, 0x04}
	kek := []byte{0x05, 0x06}

	reader := &mockScyllaReader{
		record: &ScyllaCardKeyRecord{
			CardID:    "card-123",
			EncKey:    encKey,
			AuthKey:   authKey,
			KEK:       kek,
			ProfileID: "profile-456",
			MSISDN:    "+1234567890",
		},
	}

	ks := NewScyllaKeyStore(reader)
	keys, err := ks.GetKeys(context.Background(), "card-123")
	if err != nil {
		t.Fatalf("GetKeys failed: %v", err)
	}
	if string(keys.EncKey) != string(encKey) {
		t.Error("EncKey mismatch")
	}
	if keys.ProfileID != "profile-456" {
		t.Errorf("ProfileID = %q, want %q", keys.ProfileID, "profile-456")
	}
}

type mockScyllaReader struct {
	record *ScyllaCardKeyRecord
}

func (m *mockScyllaReader) GetCardKeys(_ context.Context, _ string) (*ScyllaCardKeyRecord, error) {
	return m.record, nil
}
