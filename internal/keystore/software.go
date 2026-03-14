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
