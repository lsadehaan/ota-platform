package pgstore

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"gorm.io/gorm"

	"ota-platform/internal/db"
	"ota-platform/internal/store"
)

// ExecutionStore implements executor.ExecutionStore backed by Postgres.
type ExecutionStore struct {
	db *gorm.DB
}

// NewExecutionStore creates a new Postgres-backed ExecutionStore.
func NewExecutionStore(database *gorm.DB) *ExecutionStore {
	return &ExecutionStore{db: database}
}

// statusColumnOrPending normalizes card statuses to their counter column names.
// Falls back to "pending" for unknown statuses (used when reading existing rows).
func statusColumnOrPending(status string) string {
	if col := statusColumn(status); col != "" {
		return col
	}
	return "pending"
}

// UpdateCampaignCard appends a new card execution state row and adjusts campaign_stats.
// Append-only: reads latest state, then INSERTs a new row with higher transition_seq.
func (s *ExecutionStore) UpdateCampaignCard(ctx context.Context, campaignID, cardID string, updates map[string]interface{}) error {
	campUUID, err := uuid.Parse(campaignID)
	if err != nil {
		return fmt.Errorf("parse campaign_id: %w", err)
	}
	cardUUID, err := uuid.Parse(cardID)
	if err != nil {
		return fmt.Errorf("parse card_id: %w", err)
	}

	return s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		// Read current (latest) state to determine counter delta and base values.
		var current db.CardExecutionState
		if err := tx.Where("card_id = ? AND campaign_id = ?", cardUUID, campUUID).
			Order("transition_seq DESC").First(&current).Error; err != nil {
			if err == gorm.ErrRecordNotFound {
				current.Status = "pending"
			} else {
				return fmt.Errorf("read current card state: %w", err)
			}
		}

		oldCol := statusColumnOrPending(current.Status)

		// Build the new row from current + updates.
		newRow := current
		newRow.TransitionSeq = current.TransitionSeq + 1
		newRow.UpdatedAt = time.Now().UTC()
		if v, ok := updates["status"].(string); ok {
			newRow.Status = v
		}
		if v, ok := updates["current_step"].(int); ok {
			newRow.CurrentStep = v
		}
		if v, ok := updates["retry_count"].(int); ok {
			newRow.RetryCount = v
		}
		if v, ok := updates["last_error"].(string); ok {
			newRow.LastError = v
		}
		if v, ok := updates["last_smpp_id"].(string); ok {
			newRow.LastSMPPID = v
		}
		if v, ok := updates["updated_at"].(time.Time); ok {
			newRow.UpdatedAt = v
		}

		// Append-only INSERT.
		if err := tx.Create(&newRow).Error; err != nil {
			return fmt.Errorf("insert card execution state: %w", err)
		}

		newCol := statusColumnOrPending(newRow.Status)

		// Adjust campaign_stats if the counter column changed.
		if oldCol != newCol {
			sql := fmt.Sprintf(
				`UPDATE campaign_stats SET %s = %s - 1, %s = %s + 1 WHERE campaign_id = ?`,
				oldCol, oldCol, newCol, newCol,
			)
			if err := tx.Exec(sql, campUUID).Error; err != nil {
				return fmt.Errorf("update campaign stats counters: %w", err)
			}
		}

		return nil
	})
}

// CompleteCampaignIfRunning checks campaign_stats to see if all cards are done,
// then updates the campaign status if so.
func (s *ExecutionStore) CompleteCampaignIfRunning(ctx context.Context, campaignID, finalStatus string, completedAt time.Time) (bool, error) {
	campUUID, err := uuid.Parse(campaignID)
	if err != nil {
		return false, fmt.Errorf("parse campaign_id: %w", err)
	}

	// Check if pending + in_progress == 0 in campaign_stats.
	var row db.CampaignStatsRow
	if err := s.db.WithContext(ctx).Where("campaign_id = ?", campUUID).First(&row).Error; err != nil {
		if err == gorm.ErrRecordNotFound {
			return false, nil
		}
		return false, fmt.Errorf("read campaign stats: %w", err)
	}
	if row.Pending > 0 || row.InProgress > 0 {
		return false, nil
	}

	// All cards terminal: update campaign.
	result := s.db.WithContext(ctx).
		Model(&db.Campaign{}).
		Where("id = ? AND status = ?", campUUID, "running").
		Updates(map[string]interface{}{"status": finalStatus, "completed_at": completedAt})
	if result.Error != nil {
		return false, result.Error
	}
	return result.RowsAffected > 0, nil
}

// CampaignStats reads aggregate counters from campaign_stats (O(1)).
func (s *ExecutionStore) CampaignStats(ctx context.Context, campaignID string) (store.CampaignStats, error) {
	campUUID, err := uuid.Parse(campaignID)
	if err != nil {
		return store.CampaignStats{}, fmt.Errorf("parse campaign_id: %w", err)
	}
	var row db.CampaignStatsRow
	if err := s.db.WithContext(ctx).Where("campaign_id = ?", campUUID).First(&row).Error; err != nil {
		if err == gorm.ErrRecordNotFound {
			return store.CampaignStats{}, nil
		}
		return store.CampaignStats{}, fmt.Errorf("read campaign stats: %w", err)
	}
	return store.CampaignStats{
		Total:      row.Total,
		Pending:    row.Pending,
		InProgress: row.InProgress,
		Completed:  row.Completed,
		Failed:     row.Failed,
		Skipped:    row.Skipped,
	}, nil
}
