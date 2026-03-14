package pgstore

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
	"go.uber.org/zap"
	"gorm.io/gorm"

	"ota-platform/internal/db"
	"ota-platform/internal/pipeline"
)

// cardLatestState holds the latest execution state for a card within a campaign,
// read via DISTINCT ON query during crash recovery.
type cardLatestState struct {
	CardID      uuid.UUID `gorm:"column:card_id"`
	Status      string    `gorm:"column:status"`
	CurrentStep int       `gorm:"column:current_step"`
	RetryCount  int       `gorm:"column:retry_count"`
}

// RecoverRunningCampaigns scans for campaigns that were running when the process
// last exited (or crashed) and re-creates planner shards for non-terminal cards
// so that the planner can resume them on startup.
func RecoverRunningCampaigns(ctx context.Context, database *gorm.DB, shardSize int, logger *zap.Logger) error {
	// 1. Reset stuck 'publishing' shards back to 'pending' so they get re-claimed.
	if err := database.WithContext(ctx).Model(&db.CampaignShard{}).
		Where("status = ?", "publishing").
		Updates(map[string]interface{}{
			"status":     "pending",
			"claimed_at": nil,
			"updated_at": time.Now(),
		}).Error; err != nil {
		logger.Warn("failed to reset publishing shards", zap.Error(err))
	}

	// 2. Find all campaigns that were running at the time of crash.
	var campaigns []db.Campaign
	if err := database.WithContext(ctx).
		Preload("CampaignCommands", func(tx *gorm.DB) *gorm.DB { return tx.Order("sequence ASC") }).
		Where("status = ?", "running").
		Find(&campaigns).Error; err != nil {
		return fmt.Errorf("query running campaigns: %w", err)
	}

	if len(campaigns) == 0 {
		logger.Info("no running campaigns to recover")
		return nil
	}

	logger.Info("recovering running campaigns", zap.Int("count", len(campaigns)))

	// 3. Recover each campaign individually; continue on per-campaign errors.
	for _, campaign := range campaigns {
		if err := recoverCampaign(ctx, database, campaign, shardSize, logger); err != nil {
			logger.Error("failed to recover campaign",
				zap.String("campaign_id", campaign.ID.String()),
				zap.String("campaign_name", campaign.Name),
				zap.Error(err),
			)
			// Continue with other campaigns — don't abort all.
		}
	}

	return nil
}

// recoverCampaign rebuilds planner shards for a single campaign by reading
// the latest execution state per card and re-sharding non-terminal cards.
func recoverCampaign(ctx context.Context, database *gorm.DB, campaign db.Campaign, shardSize int, logger *zap.Logger) error {
	campaignLogger := logger.With(
		zap.String("campaign_id", campaign.ID.String()),
		zap.String("campaign_name", campaign.Name),
	)

	// 1. Query latest state per card using DISTINCT ON.
	var cards []cardLatestState
	err := database.WithContext(ctx).Raw(`
		SELECT DISTINCT ON (card_id) card_id, status, current_step, retry_count
		FROM card_execution_states
		WHERE campaign_id = ?
		ORDER BY card_id, transition_seq DESC
	`, campaign.ID).Scan(&cards).Error
	if err != nil {
		return fmt.Errorf("query card execution states: %w", err)
	}

	// 2. Filter to non-terminal cards.
	terminalStatuses := map[string]bool{
		"completed": true,
		"failed":    true,
		"skipped":   true,
	}

	var events []pipeline.CardEvent
	for _, card := range cards {
		if terminalStatuses[card.Status] {
			continue
		}

		step := card.CurrentStep
		if step == 0 && len(campaign.CampaignCommands) > 0 {
			step = campaign.CampaignCommands[0].Sequence
		}

		events = append(events, pipeline.CardEvent{
			Type:       "card.activate",
			EventID:    uuid.New().String(),
			CardID:     card.CardID.String(),
			CampaignID: campaign.ID.String(),
			Step:       step,
			RetryCount: card.RetryCount,
			Timestamp:  time.Now(),
		})
	}

	if len(events) == 0 {
		campaignLogger.Info("no non-terminal cards to recover")
		return nil
	}

	campaignLogger.Info("recovering non-terminal cards",
		zap.Int("total_cards", len(cards)),
		zap.Int("non_terminal", len(events)),
	)

	// 3. Delete existing pending/publishing shards for this campaign (they're stale).
	if err := database.WithContext(ctx).
		Where("campaign_id = ? AND status IN ?", campaign.ID, []string{"pending", "publishing"}).
		Delete(&db.CampaignShard{}).Error; err != nil {
		return fmt.Errorf("delete stale shards: %w", err)
	}

	// 4. Get next shard sequence number.
	var maxSeq int
	if err := database.WithContext(ctx).
		Model(&db.CampaignShard{}).
		Where("campaign_id = ?", campaign.ID).
		Select("COALESCE(MAX(sequence), 0)").
		Scan(&maxSeq).Error; err != nil {
		return fmt.Errorf("query max shard sequence: %w", err)
	}
	seq := maxSeq + 1

	// 5. Create new shards in batches of shardSize.
	for i := 0; i < len(events); i += shardSize {
		end := min(i+shardSize, len(events))
		items, err := json.Marshal(events[i:end])
		if err != nil {
			return fmt.Errorf("marshal shard items: %w", err)
		}
		shard := db.CampaignShard{
			ID:         uuid.New(),
			CampaignID: campaign.ID,
			Sequence:   seq,
			Status:     "pending",
			ItemCount:  end - i,
			Items:      items,
		}
		if err := database.WithContext(ctx).Create(&shard).Error; err != nil {
			return fmt.Errorf("create recovery shard %d: %w", seq, err)
		}
		seq++
	}

	campaignLogger.Info("campaign recovery complete",
		zap.Int("shards_created", seq-maxSeq-1),
		zap.Int("cards_recovered", len(events)),
	)

	return nil
}
