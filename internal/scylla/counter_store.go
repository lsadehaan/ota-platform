package scylla

import (
	"context"
	"fmt"

	"github.com/gocql/gocql"
)

// CardCounterRecord holds a counter value for a (card, application) pair.
type CardCounterRecord struct {
	CardID        string
	ApplicationID string
	CounterValue  int64
}

// CounterStore reads and writes card counters in ScyllaDB.
type CounterStore struct {
	client *Client
}

// NewCounterStore creates a new CounterStore.
func NewCounterStore(client *Client) *CounterStore {
	return &CounterStore{client: client}
}

// IncrCounter atomically increments a card's counter for the given application
// and returns the new value. Uses two queries both at QUORUM:
//  1. UPDATE counter_value = counter_value + 1
//  2. SELECT counter_value
//
// Safe because each (card_id, application_id) is only processed by one executor
// goroutine at a time (Kafka partition key = card_id).
func (s *CounterStore) IncrCounter(ctx context.Context, cardID, applicationID string) (int64, error) {
	cardUUID, err := gocql.ParseUUID(cardID)
	if err != nil {
		return 0, fmt.Errorf("parse card_id %s: %w", cardID, err)
	}
	appUUID, err := gocql.ParseUUID(applicationID)
	if err != nil {
		return 0, fmt.Errorf("parse application_id %s: %w", applicationID, err)
	}

	// Step 1: Increment
	q := s.client.Session().Query(
		`UPDATE card_counters SET counter_value = counter_value + 1 WHERE card_id = ? AND application_id = ?`,
		cardUUID, appUUID,
	).WithContext(ctx)
	if err := q.Exec(); err != nil {
		return 0, fmt.Errorf("increment counter for card %s app %s: %w", cardID, applicationID, err)
	}

	// Step 2: Read back
	var value int64
	q = s.client.Session().Query(
		`SELECT counter_value FROM card_counters WHERE card_id = ? AND application_id = ?`,
		cardUUID, appUUID,
	).WithContext(ctx)
	if err := q.Scan(&value); err != nil {
		return 0, fmt.Errorf("read counter for card %s app %s: %w", cardID, applicationID, err)
	}

	return value, nil
}

// GetCounter reads the current counter value for a (card, application) pair.
// Returns 0 if the row does not exist.
func (s *CounterStore) GetCounter(ctx context.Context, cardID, applicationID string) (int64, error) {
	cardUUID, err := gocql.ParseUUID(cardID)
	if err != nil {
		return 0, fmt.Errorf("parse card_id %s: %w", cardID, err)
	}
	appUUID, err := gocql.ParseUUID(applicationID)
	if err != nil {
		return 0, fmt.Errorf("parse application_id %s: %w", applicationID, err)
	}

	var value int64
	q := s.client.Session().Query(
		`SELECT counter_value FROM card_counters WHERE card_id = ? AND application_id = ?`,
		cardUUID, appUUID,
	).WithContext(ctx)
	if err := q.Scan(&value); err != nil {
		if err == gocql.ErrNotFound {
			return 0, nil
		}
		return 0, fmt.Errorf("get counter for card %s app %s: %w", cardID, applicationID, err)
	}

	return value, nil
}

// GetCountersByCard reads all counters for a given card.
func (s *CounterStore) GetCountersByCard(ctx context.Context, cardID string) ([]CardCounterRecord, error) {
	cardUUID, err := gocql.ParseUUID(cardID)
	if err != nil {
		return nil, fmt.Errorf("parse card_id %s: %w", cardID, err)
	}

	q := s.client.Session().Query(
		`SELECT card_id, application_id, counter_value FROM card_counters WHERE card_id = ?`,
		cardUUID,
	).WithContext(ctx)

	iter := q.Iter()
	var records []CardCounterRecord
	var (
		cID    gocql.UUID
		appID  gocql.UUID
		cValue int64
	)
	for iter.Scan(&cID, &appID, &cValue) {
		records = append(records, CardCounterRecord{
			CardID:        cID.String(),
			ApplicationID: appID.String(),
			CounterValue:  cValue,
		})
	}
	if err := iter.Close(); err != nil {
		return nil, fmt.Errorf("get counters for card %s: %w", cardID, err)
	}

	return records, nil
}

// SeedCounter sets a counter to a specific value by incrementing by the given amount.
// Intended for migration only.
func (s *CounterStore) SeedCounter(ctx context.Context, cardID, applicationID string, value int64) error {
	cardUUID, err := gocql.ParseUUID(cardID)
	if err != nil {
		return fmt.Errorf("parse card_id %s: %w", cardID, err)
	}
	appUUID, err := gocql.ParseUUID(applicationID)
	if err != nil {
		return fmt.Errorf("parse application_id %s: %w", applicationID, err)
	}

	q := s.client.Session().Query(
		`UPDATE card_counters SET counter_value = counter_value + ? WHERE card_id = ? AND application_id = ?`,
		value, cardUUID, appUUID,
	).WithContext(ctx)
	if err := q.Exec(); err != nil {
		return fmt.Errorf("seed counter for card %s app %s: %w", cardID, applicationID, err)
	}

	return nil
}
