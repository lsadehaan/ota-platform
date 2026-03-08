package scylla

import (
	"context"
	"fmt"
	"time"

	"github.com/gocql/gocql"
	"github.com/google/uuid"
	"gorm.io/gorm"

	"ota-platform/internal/db"
)

type ExecutionStore struct {
	client    *Client
	controlDB *gorm.DB
}

func NewExecutionStore(client *Client, controlDB *gorm.DB) *ExecutionStore {
	return &ExecutionStore{
		client:    client,
		controlDB: controlDB,
	}
}

func (s *ExecutionStore) UpdateCampaignCard(ctx context.Context, campaignID, cardID string, updates map[string]interface{}) error {
	cardUUID, err := gocql.ParseUUID(cardID)
	if err != nil {
		return fmt.Errorf("parse card_id: %w", err)
	}
	campaignUUID, err := gocql.ParseUUID(campaignID)
	if err != nil {
		return fmt.Errorf("parse campaign_id: %w", err)
	}

	var (
		status        string
		currentStep   int
		retryCount    int
		lastErrorText string
		lastErrorCode string
		lastSMPPMsgID string
		updatedAt     time.Time
		lastMsgID     *gocql.UUID
	)

	oldStatus := "pending"
	oldUpdatedAt := time.Time{}
	oldCurrentStep := 0
	oldRetryCount := 0
	oldLastErrorText := ""
	oldLastErrorCode := ""
	oldLastSMPPMsgID := ""
	var oldLastMsgID *gocql.UUID
	if err := s.client.Session().Query(
		`SELECT status, current_step, retry_count, last_msg_id, last_smpp_message_id, last_error_code, last_error_text, updated_at FROM card_state_by_card WHERE card_bucket = ? AND card_id = ?`,
		s.client.CardBucket(cardID), cardUUID,
	).WithContext(ctx).Scan(&oldStatus, &oldCurrentStep, &oldRetryCount, &oldLastMsgID, &oldLastSMPPMsgID, &oldLastErrorCode, &oldLastErrorText, &oldUpdatedAt); err != nil && err != gocql.ErrNotFound {
		return fmt.Errorf("load existing card snapshot: %w", err)
	}

	status = oldStatus
	currentStep = oldCurrentStep
	retryCount = oldRetryCount
	lastErrorText = oldLastErrorText
	lastErrorCode = oldLastErrorCode
	lastSMPPMsgID = oldLastSMPPMsgID
	lastMsgID = oldLastMsgID

	if v, ok := updates["status"].(string); ok {
		status = v
	}
	if v, ok := updates["current_step"].(int); ok {
		currentStep = v
	}
	if v, ok := updates["retry_count"].(int); ok {
		retryCount = v
	}
	if v, ok := updates["last_error"].(string); ok {
		lastErrorText = v
	}
	if v, ok := updates["last_error_code"].(string); ok {
		lastErrorCode = v
	}
	if v, ok := updates["last_smpp_message_id"].(string); ok {
		lastSMPPMsgID = v
	}
	switch v := updates["updated_at"].(type) {
	case time.Time:
		updatedAt = v.UTC()
	default:
		updatedAt = time.Now().UTC()
	}

	if raw, ok := updates["last_msg_id"]; ok {
		parsed, err := toOptionalGocqlUUID(raw)
		if err != nil {
			return fmt.Errorf("parse last_msg_id: %w", err)
		}
		lastMsgID = parsed
	}

	cardBucket := s.client.CardBucket(cardID)
	applied := false
	if oldUpdatedAt.IsZero() {
		query := `INSERT INTO card_state_by_card (
			card_bucket, card_id, campaign_id, status, current_step, retry_count,
			last_msg_id, last_smpp_message_id, last_error_code, last_error_text, updated_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) IF NOT EXISTS`
		var err error
		cas := map[string]interface{}{}
		applied, err = s.client.Session().Query(query,
			cardBucket, cardUUID, campaignUUID, status, currentStep, retryCount,
			lastMsgID, lastSMPPMsgID, lastErrorCode, lastErrorText, updatedAt,
		).WithContext(ctx).MapScanCAS(cas)
		if err != nil {
			return fmt.Errorf("insert card snapshot: %w", err)
		}
	} else {
		query := `UPDATE card_state_by_card
			SET campaign_id = ?, status = ?, current_step = ?, retry_count = ?, last_msg_id = ?,
				last_smpp_message_id = ?, last_error_code = ?, last_error_text = ?, updated_at = ?
			WHERE card_bucket = ? AND card_id = ?
			IF updated_at = ?`
		var err error
		cas := map[string]interface{}{}
		applied, err = s.client.Session().Query(query,
			campaignUUID, status, currentStep, retryCount, lastMsgID, lastSMPPMsgID,
			lastErrorCode, lastErrorText, updatedAt, cardBucket, cardUUID, oldUpdatedAt,
		).WithContext(ctx).MapScanCAS(cas)
		if err != nil {
			return fmt.Errorf("update card snapshot: %w", err)
		}
	}
	if !applied {
		var currentUpdatedAt time.Time
		if err := s.client.Session().Query(
			`SELECT updated_at FROM card_state_by_card WHERE card_bucket = ? AND card_id = ?`,
			cardBucket, cardUUID,
		).WithContext(ctx).Scan(&currentUpdatedAt); err == nil && !currentUpdatedAt.Before(updatedAt) {
			return nil
		}
		return fmt.Errorf("card snapshot compare-and-set failed for card %s", cardID)
	}

	campaignBucket := s.client.CampaignBucket(campaignID, cardID)
	if counterColumn(status) != counterColumn(oldStatus) {
		if err := s.client.Session().Query(
			fmt.Sprintf(`UPDATE campaign_progress_by_bucket SET %s = %s + ?, %s = %s + ? WHERE campaign_id = ? AND campaign_bucket = ?`,
				counterColumn(oldStatus), counterColumn(oldStatus), counterColumn(status), counterColumn(status)),
			int64(-1), int64(1), campaignUUID, campaignBucket,
		).WithContext(ctx).Exec(); err != nil {
			return fmt.Errorf("update campaign progress counters: %w", err)
		}
	}

	if err := s.client.Session().Query(
		`INSERT INTO campaign_card_status_by_bucket (
			campaign_id, campaign_bucket, status, updated_at, card_id, current_step, retry_count, last_msg_id, last_error_text
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		campaignUUID, campaignBucket, status, updatedAt, cardUUID, currentStep, retryCount, lastMsgID, lastErrorText,
	).WithContext(ctx).Exec(); err != nil {
		return fmt.Errorf("upsert campaign status row: %w", err)
	}

	if !oldUpdatedAt.IsZero() && (oldStatus != status || !oldUpdatedAt.Equal(updatedAt)) {
		_ = s.client.Session().Query(
			`DELETE FROM campaign_card_status_by_bucket WHERE campaign_id = ? AND campaign_bucket = ? AND status = ? AND updated_at = ? AND card_id = ?`,
			campaignUUID, campaignBucket, oldStatus, oldUpdatedAt, cardUUID,
		).WithContext(ctx).Exec()
	}

	if status == "failed" {
		if err := s.client.Session().Query(
			`INSERT INTO failed_cards_by_campaign_bucket (
				campaign_id, campaign_bucket, card_id, failed_at, current_step, retry_count, last_msg_id, last_error_code, last_error_text
			) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			campaignUUID, campaignBucket, cardUUID, updatedAt, currentStep, retryCount, lastMsgID, lastErrorCode, lastErrorText,
		).WithContext(ctx).Exec(); err != nil {
			return fmt.Errorf("upsert failed card row: %w", err)
		}
	} else {
		_ = s.client.Session().Query(
			`DELETE FROM failed_cards_by_campaign_bucket WHERE campaign_id = ? AND campaign_bucket = ? AND card_id = ?`,
			campaignUUID, campaignBucket, cardUUID,
		).WithContext(ctx).Exec()
	}

	return nil
}

func toOptionalGocqlUUID(raw interface{}) (*gocql.UUID, error) {
	switch v := raw.(type) {
	case nil:
		return nil, nil
	case gocql.UUID:
		return &v, nil
	case uuid.UUID:
		u := gocql.UUID(v)
		return &u, nil
	case string:
		parsed, err := gocql.ParseUUID(v)
		if err != nil {
			return nil, err
		}
		return &parsed, nil
	case [16]byte:
		u := gocql.UUID(v)
		return &u, nil
	default:
		return nil, nil
	}
}

func (s *ExecutionStore) CompleteCampaignIfRunning(ctx context.Context, campaignID, finalStatus string, completedAt time.Time) (bool, error) {
	result := s.controlDB.WithContext(ctx).
		Model(&db.Campaign{}).
		Where("id = ? AND status = ?", campaignID, "running").
		Updates(map[string]interface{}{"status": finalStatus, "completed_at": completedAt})
	if result.Error != nil {
		return false, result.Error
	}
	return result.RowsAffected > 0, nil
}

func (s *ExecutionStore) CampaignStats(ctx context.Context, campaignID string) (CampaignStats, error) {
	campaignUUID, err := gocql.ParseUUID(campaignID)
	if err != nil {
		return CampaignStats{}, fmt.Errorf("parse campaign_id: %w", err)
	}

	iter := s.client.Session().Query(
		`SELECT pending, in_progress, completed, failed, skipped FROM campaign_progress_by_bucket WHERE campaign_id = ?`,
		campaignUUID,
	).WithContext(ctx).Iter()

	var stats CampaignStats
	var pending, inProgress, completed, failed, skipped int64
	for iter.Scan(&pending, &inProgress, &completed, &failed, &skipped) {
		stats.Pending += pending
		stats.InProgress += inProgress
		stats.Completed += completed
		stats.Failed += failed
		stats.Skipped += skipped
	}
	if err := iter.Close(); err != nil {
		return CampaignStats{}, err
	}
	stats.Total = stats.Pending + stats.InProgress + stats.Completed + stats.Failed + stats.Skipped
	return stats, nil
}

func counterColumn(status string) string {
	switch status {
	case "pending", "activating":
		return "pending"
	case "in_progress", "awaiting_dlr", "awaiting_mo":
		return "in_progress"
	case "completed":
		return "completed"
	case "failed":
		return "failed"
	case "skipped":
		return "skipped"
	default:
		return "pending"
	}
}
