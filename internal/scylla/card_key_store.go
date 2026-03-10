package scylla

import (
	"context"
	"fmt"

	"github.com/gocql/gocql"
)

// CardKeyRecord holds the cryptographic keys and metadata for a SIM card.
type CardKeyRecord struct {
	CardID    string
	EncKey    []byte
	AuthKey   []byte
	KEK       []byte
	ProfileID string
	MSISDN    string
}

// CardKeyStore reads and writes card key material in ScyllaDB.
type CardKeyStore struct {
	client *Client
}

// NewCardKeyStore creates a new CardKeyStore.
func NewCardKeyStore(client *Client) *CardKeyStore {
	return &CardKeyStore{client: client}
}

// WriteCardKeys writes a single card's key material to ScyllaDB.
func (s *CardKeyStore) WriteCardKeys(ctx context.Context, cardID string, encKey, authKey, kek []byte, profileID, msisdn string) error {
	cardUUID, err := gocql.ParseUUID(cardID)
	if err != nil {
		return fmt.Errorf("parse card_id %s: %w", cardID, err)
	}
	profileUUID, err := gocql.ParseUUID(profileID)
	if err != nil {
		return fmt.Errorf("parse profile_id %s: %w", profileID, err)
	}

	q := s.client.Session().Query(
		`INSERT INTO card_keys (card_id, enc_key, auth_key, kek, profile_id, msisdn) VALUES (?, ?, ?, ?, ?, ?)`,
		cardUUID, encKey, authKey, kek, profileUUID, msisdn,
	).WithContext(ctx)

	if err := q.Exec(); err != nil {
		return fmt.Errorf("write card keys for %s: %w", cardID, err)
	}
	return nil
}

// WriteCardKeysBatch writes multiple card key records to ScyllaDB in batches.
// Chunks into max 100 records per unlogged batch to avoid ScyllaDB batch size limits.
func (s *CardKeyStore) WriteCardKeysBatch(ctx context.Context, records []CardKeyRecord) error {
	if len(records) == 0 {
		return nil
	}

	const chunkSize = 100
	for start := 0; start < len(records); start += chunkSize {
		end := start + chunkSize
		if end > len(records) {
			end = len(records)
		}
		if err := s.writeCardKeysBatchChunk(ctx, records[start:end]); err != nil {
			return err
		}
	}
	return nil
}

func (s *CardKeyStore) writeCardKeysBatchChunk(ctx context.Context, records []CardKeyRecord) error {
	batch := s.client.Session().NewBatch(gocql.UnloggedBatch).WithContext(ctx)

	for _, rec := range records {
		cardUUID, err := gocql.ParseUUID(rec.CardID)
		if err != nil {
			return fmt.Errorf("parse card_id %s: %w", rec.CardID, err)
		}
		profileUUID, err := gocql.ParseUUID(rec.ProfileID)
		if err != nil {
			return fmt.Errorf("parse profile_id %s: %w", rec.ProfileID, err)
		}

		batch.Entries = append(batch.Entries, gocql.BatchEntry{
			Stmt: `INSERT INTO card_keys (card_id, enc_key, auth_key, kek, profile_id, msisdn) VALUES (?, ?, ?, ?, ?, ?)`,
			Args: []interface{}{cardUUID, rec.EncKey, rec.AuthKey, rec.KEK, profileUUID, rec.MSISDN},
		})
	}

	if err := s.client.Session().ExecuteBatch(batch); err != nil {
		return fmt.Errorf("execute card keys batch: %w", err)
	}
	return nil
}

// DeleteCardKeys removes a card's key material from ScyllaDB.
func (s *CardKeyStore) DeleteCardKeys(ctx context.Context, cardID string) error {
	cardUUID, err := gocql.ParseUUID(cardID)
	if err != nil {
		return fmt.Errorf("parse card_id %s: %w", cardID, err)
	}

	q := s.client.Session().Query(
		`DELETE FROM card_keys WHERE card_id = ?`, cardUUID,
	).WithContext(ctx)

	if err := q.Exec(); err != nil {
		return fmt.Errorf("delete card keys for %s: %w", cardID, err)
	}
	return nil
}

// GetCardKeys reads a single card's key material from ScyllaDB.
// Returns nil, gocql.ErrNotFound if the card is not found.
func (s *CardKeyStore) GetCardKeys(ctx context.Context, cardID string) (*CardKeyRecord, error) {
	cardUUID, err := gocql.ParseUUID(cardID)
	if err != nil {
		return nil, fmt.Errorf("parse card_id %s: %w", cardID, err)
	}

	var (
		encKey    []byte
		authKey   []byte
		kek       []byte
		profileID gocql.UUID
		msisdn    string
	)

	q := s.client.Session().Query(
		`SELECT enc_key, auth_key, kek, profile_id, msisdn FROM card_keys WHERE card_id = ?`,
		cardUUID,
	).WithContext(ctx)

	if err := q.Scan(&encKey, &authKey, &kek, &profileID, &msisdn); err != nil {
		return nil, fmt.Errorf("get card keys for %s: %w", cardID, err)
	}

	return &CardKeyRecord{
		CardID:    cardID,
		EncKey:    encKey,
		AuthKey:   authKey,
		KEK:       kek,
		ProfileID: profileID.String(),
		MSISDN:    msisdn,
	}, nil
}
