package scylla

import (
	"context"
	"fmt"

	"github.com/gocql/gocql"

	kafkapkg "ota-platform/internal/kafka"
)

// CardStateStore writes card state change events to ScyllaDB in batches.
type CardStateStore struct {
	client *Client
}

func NewCardStateStore(client *Client) *CardStateStore {
	return &CardStateStore{client: client}
}

// WriteCardState writes a single card state change event to ScyllaDB.
// Same INSERTs as WriteBatch but for one event — no SELECT, no DELETE, no counter update.
func (s *CardStateStore) WriteCardState(ctx context.Context, ev kafkapkg.CardStateChange) error {
	cardUUID, err := gocql.ParseUUID(ev.CardID)
	if err != nil {
		return fmt.Errorf("parse card_id %s: %w", ev.CardID, err)
	}
	campaignUUID, err := gocql.ParseUUID(ev.CampaignID)
	if err != nil {
		return fmt.Errorf("parse campaign_id %s: %w", ev.CampaignID, err)
	}

	var lastMsgID *gocql.UUID
	if ev.LastMsgID != "" {
		parsed, err := gocql.ParseUUID(ev.LastMsgID)
		if err == nil {
			lastMsgID = &parsed
		}
	}

	cardBucket := s.client.CardBucket(ev.CardID)
	campaignBucket := s.client.CampaignBucket(ev.CampaignID, ev.CardID)
	updatedAt := ev.Timestamp.UTC()
	writeTS := updatedAt.UnixMicro()

	batch := s.client.Session().NewBatch(gocql.UnloggedBatch).WithContext(ctx)

	batch.Entries = append(batch.Entries, gocql.BatchEntry{
		Stmt: `INSERT INTO card_state_by_card (
			card_bucket, card_id, campaign_id, status, current_step, retry_count,
			last_msg_id, last_smpp_message_id, last_error_code, last_error_text, updated_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) USING TIMESTAMP ?`,
		Args: []interface{}{
			cardBucket, cardUUID, campaignUUID, ev.Status, ev.CurrentStep, ev.RetryCount,
			lastMsgID, ev.LastSMPPID, ev.LastErrCode, ev.LastError, updatedAt, writeTS,
		},
	})

	batch.Entries = append(batch.Entries, gocql.BatchEntry{
		Stmt: `INSERT INTO campaign_card_status_by_bucket (
			campaign_id, campaign_bucket, status, updated_at, card_id, current_step, retry_count, last_msg_id, last_error_text
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		Args: []interface{}{
			campaignUUID, campaignBucket, ev.Status, updatedAt, cardUUID, ev.CurrentStep, ev.RetryCount, lastMsgID, ev.LastError,
		},
	})

	if ev.Status == "failed" {
		batch.Entries = append(batch.Entries, gocql.BatchEntry{
			Stmt: `INSERT INTO failed_cards_by_campaign_bucket (
				campaign_id, campaign_bucket, card_id, failed_at, current_step, retry_count, last_msg_id, last_error_code, last_error_text
			) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			Args: []interface{}{
				campaignUUID, campaignBucket, cardUUID, updatedAt, ev.CurrentStep, ev.RetryCount, lastMsgID, ev.LastErrCode, ev.LastError,
			},
		})
	}

	if err := s.client.Session().ExecuteBatch(batch); err != nil {
		return fmt.Errorf("write card state: %w", err)
	}
	return nil
}

// WriteBatch writes a batch of card state change events to ScyllaDB.
// Uses unlogged batch for performance. Each event results in:
//   - INSERT into card_state_by_card (with USING TIMESTAMP for LWW)
//   - INSERT into campaign_card_status_by_bucket (append-only, no DELETE)
//   - INSERT into failed_cards_by_campaign_bucket (for failed cards only)
//
// No SELECTs, no DELETEs, no counter updates.
func (s *CardStateStore) WriteBatch(ctx context.Context, events []kafkapkg.CardStateChange) error {
	if len(events) == 0 {
		return nil
	}

	batch := s.client.Session().NewBatch(gocql.UnloggedBatch).WithContext(ctx)

	for _, ev := range events {
		cardUUID, err := gocql.ParseUUID(ev.CardID)
		if err != nil {
			return fmt.Errorf("parse card_id %s: %w", ev.CardID, err)
		}
		campaignUUID, err := gocql.ParseUUID(ev.CampaignID)
		if err != nil {
			return fmt.Errorf("parse campaign_id %s: %w", ev.CampaignID, err)
		}

		var lastMsgID *gocql.UUID
		if ev.LastMsgID != "" {
			parsed, err := gocql.ParseUUID(ev.LastMsgID)
			if err == nil {
				lastMsgID = &parsed
			}
		}

		cardBucket := s.client.CardBucket(ev.CardID)
		campaignBucket := s.client.CampaignBucket(ev.CampaignID, ev.CardID)
		updatedAt := ev.Timestamp.UTC()
		writeTS := updatedAt.UnixMicro()

		// INSERT into card_state_by_card with USING TIMESTAMP (LWW)
		batch.Entries = append(batch.Entries, gocql.BatchEntry{
			Stmt: `INSERT INTO card_state_by_card (
				card_bucket, card_id, campaign_id, status, current_step, retry_count,
				last_msg_id, last_smpp_message_id, last_error_code, last_error_text, updated_at
			) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) USING TIMESTAMP ?`,
			Args: []interface{}{
				cardBucket, cardUUID, campaignUUID, ev.Status, ev.CurrentStep, ev.RetryCount,
				lastMsgID, ev.LastSMPPID, ev.LastErrCode, ev.LastError, updatedAt, writeTS,
			},
		})

		// INSERT into campaign_card_status_by_bucket (append-only)
		batch.Entries = append(batch.Entries, gocql.BatchEntry{
			Stmt: `INSERT INTO campaign_card_status_by_bucket (
				campaign_id, campaign_bucket, status, updated_at, card_id, current_step, retry_count, last_msg_id, last_error_text
			) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			Args: []interface{}{
				campaignUUID, campaignBucket, ev.Status, updatedAt, cardUUID, ev.CurrentStep, ev.RetryCount, lastMsgID, ev.LastError,
			},
		})

		// INSERT into failed_cards_by_campaign_bucket for failed cards
		if ev.Status == "failed" {
			batch.Entries = append(batch.Entries, gocql.BatchEntry{
				Stmt: `INSERT INTO failed_cards_by_campaign_bucket (
					campaign_id, campaign_bucket, card_id, failed_at, current_step, retry_count, last_msg_id, last_error_code, last_error_text
				) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
				Args: []interface{}{
					campaignUUID, campaignBucket, cardUUID, updatedAt, ev.CurrentStep, ev.RetryCount, lastMsgID, ev.LastErrCode, ev.LastError,
				},
			})
		}
	}

	if err := s.client.Session().ExecuteBatch(batch); err != nil {
		return fmt.Errorf("execute card state batch: %w", err)
	}
	return nil
}
