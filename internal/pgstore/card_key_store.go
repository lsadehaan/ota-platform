package pgstore

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	"go.uber.org/zap"
	"gorm.io/gorm"

	"ota-platform/internal/keystore"
	"ota-platform/internal/store"
)

// CardKeyStore implements controlplane.CardKeyWriter, controlplane.CounterReader,
// and keystore.KeyStore backed by Postgres.
//
// Card keys live on the `cards` table (enc_key, auth_key, kek columns).
// Counters live in the `card_counters` table.
type CardKeyStore struct {
	db     *gorm.DB
	logger *zap.Logger
}

// NewCardKeyStore creates a new Postgres-backed CardKeyStore.
func NewCardKeyStore(database *gorm.DB, logger *zap.Logger) *CardKeyStore {
	return &CardKeyStore{db: database, logger: logger}
}

// ---------- CardKeyWriter ----------

// WriteCardKeys updates the key material on the cards table.
func (s *CardKeyStore) WriteCardKeys(ctx context.Context, cardID string, encKey, authKey, kek []byte, profileID, msisdn string) error {
	id, err := uuid.Parse(cardID)
	if err != nil {
		return fmt.Errorf("parse card_id %s: %w", cardID, err)
	}
	result := s.db.WithContext(ctx).
		Table("cards").
		Where("id = ?", id).
		Updates(map[string]interface{}{
			"enc_key":  encKey,
			"auth_key": authKey,
			"kek":      kek,
		})
	if result.Error != nil {
		return fmt.Errorf("write card keys: %w", result.Error)
	}
	// RowsAffected == 0 is expected when called during card creation:
	// the card row exists only inside an uncommitted transaction that this
	// connection can't see. The keys are already set on the INSERT, so
	// a 0-row UPDATE is harmless.
	return nil
}

// WriteCardKeysBatch batch-updates card keys.
func (s *CardKeyStore) WriteCardKeysBatch(ctx context.Context, records []store.CardKeyRecord) error {
	if len(records) == 0 {
		return nil
	}
	return s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		for _, rec := range records {
			id, err := uuid.Parse(rec.CardID)
			if err != nil {
				return fmt.Errorf("parse card_id %s: %w", rec.CardID, err)
			}
			if err := tx.
				Table("cards").
				Where("id = ?", id).
				Updates(map[string]interface{}{
					"enc_key":  rec.EncKey,
					"auth_key": rec.AuthKey,
					"kek":      rec.KEK,
				}).Error; err != nil {
				return fmt.Errorf("write card keys for %s: %w", rec.CardID, err)
			}
		}
		return nil
	})
}

// DeleteCardKeys nulls out the key material on the cards table.
func (s *CardKeyStore) DeleteCardKeys(ctx context.Context, cardID string) error {
	id, err := uuid.Parse(cardID)
	if err != nil {
		return fmt.Errorf("parse card_id %s: %w", cardID, err)
	}
	if err := s.db.WithContext(ctx).
		Table("cards").
		Where("id = ?", id).
		Updates(map[string]interface{}{
			"enc_key":  nil,
			"auth_key": nil,
			"kek":      nil,
		}).Error; err != nil {
		return fmt.Errorf("delete card keys: %w", err)
	}
	return nil
}

// DeleteMSISDNMapping is a no-op for Postgres since MSISDN lives on the cards table.
func (s *CardKeyStore) DeleteMSISDNMapping(ctx context.Context, msisdn string) error {
	s.logger.Debug("DeleteMSISDNMapping is a no-op in Postgres mode", zap.String("msisdn", msisdn))
	return nil
}

// ---------- CounterReader ----------

// GetCountersByCard returns all counter records for a given card.
func (s *CardKeyStore) GetCountersByCard(ctx context.Context, cardID string) ([]store.CardCounterRecord, error) {
	id, err := uuid.Parse(cardID)
	if err != nil {
		return nil, fmt.Errorf("parse card_id %s: %w", cardID, err)
	}

	type counterRow struct {
		CardID        uuid.UUID `gorm:"column:card_id"`
		ApplicationID uuid.UUID `gorm:"column:application_id"`
		CounterValue  int64     `gorm:"column:counter_value"`
	}

	var rows []counterRow
	if err := s.db.WithContext(ctx).
		Table("card_counters").
		Where("card_id = ?", id).
		Find(&rows).Error; err != nil {
		return nil, fmt.Errorf("get counters by card: %w", err)
	}

	out := make([]store.CardCounterRecord, 0, len(rows))
	for _, r := range rows {
		out = append(out, store.CardCounterRecord{
			CardID:        r.CardID.String(),
			ApplicationID: r.ApplicationID.String(),
			CounterValue:  r.CounterValue,
		})
	}
	return out, nil
}

// ---------- keystore.KeyStore ----------

// GetKeys retrieves the cryptographic key material for a card.
func (s *CardKeyStore) GetKeys(ctx context.Context, cardID string) (*keystore.CardKeyMaterial, error) {
	id, err := uuid.Parse(cardID)
	if err != nil {
		return nil, fmt.Errorf("parse card_id %s: %w", cardID, err)
	}

	type cardKeyRow struct {
		EncKey    []byte `gorm:"column:enc_key"`
		AuthKey   []byte `gorm:"column:auth_key"`
		KEK       []byte `gorm:"column:kek"`
		ProfileID string `gorm:"column:profile_id"`
		MSISDN    string `gorm:"column:msisdn"`
	}

	var row cardKeyRow
	if err := s.db.WithContext(ctx).
		Table("cards").
		Select("enc_key, auth_key, kek, profile_id, msisdn").
		Where("id = ?", id).
		Scan(&row).Error; err != nil {
		if err == gorm.ErrRecordNotFound {
			return nil, fmt.Errorf("card %s not found", cardID)
		}
		return nil, fmt.Errorf("get keys: %w", err)
	}

	if row.EncKey == nil && row.AuthKey == nil {
		return nil, fmt.Errorf("card %s has no key material", cardID)
	}

	return &keystore.CardKeyMaterial{
		EncKey:    row.EncKey,
		AuthKey:   row.AuthKey,
		KEK:       row.KEK,
		ProfileID: row.ProfileID,
		MSISDN:    row.MSISDN,
	}, nil
}

// Compile-time interface checks.
var _ keystore.KeyStore = (*CardKeyStore)(nil)
