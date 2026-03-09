package reconciler

import (
	"context"
	"time"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
	"go.uber.org/zap"
	"gorm.io/gorm"

	"ota-platform/internal/config"
	"ota-platform/internal/db"
	kafkapkg "ota-platform/internal/kafka"
	scyllastore "ota-platform/internal/scylla"
)

// CardStateWriter writes card state changes directly to ScyllaDB.
type CardStateWriter interface {
	WriteCardState(ctx context.Context, ev kafkapkg.CardStateChange) error
}

type Service struct {
	db                  *gorm.DB
	query               *scyllastore.QueryStore
	cardStateWriter     CardStateWriter
	logger              *zap.Logger
	reclaimInterval     time.Duration
	cleanupInterval     time.Duration
	retention           time.Duration
	staleCardTimeout    time.Duration
	reclaimedTotal      metric.Int64Counter
	cleanedTotal        metric.Int64Counter
	staleCardsRecovered metric.Int64Counter
}

func NewService(database *gorm.DB, query *scyllastore.QueryStore, cardStateWriter CardStateWriter, logger *zap.Logger) *Service {
	meter := otel.Meter("reconciler")
	reclaimedTotal, err := meter.Int64Counter("ota.reconciler.shards_reclaimed", metric.WithDescription("Campaign shards reclaimed by the reconciler"))
	if err != nil {
		logger.Warn("create reconciler shards_reclaimed metric", zap.Error(err))
	}
	cleanedTotal, err := meter.Int64Counter("ota.reconciler.shards_cleaned", metric.WithDescription("Published campaign shards cleaned by the reconciler"))
	if err != nil {
		logger.Warn("create reconciler shards_cleaned metric", zap.Error(err))
	}
	staleCardsRecovered, err := meter.Int64Counter("ota.reconciler.stale_cards_recovered", metric.WithDescription("Stale non-terminal cards recovered to failed"))
	if err != nil {
		logger.Warn("create reconciler stale_cards_recovered metric", zap.Error(err))
	}

	staleTimeout := time.Duration(config.GetEnvInt("RECONCILER_STALE_CARD_TIMEOUT_MINUTES", 30)) * time.Minute

	return &Service{
		db:                  database,
		query:               query,
		cardStateWriter:     cardStateWriter,
		logger:              logger,
		reclaimInterval:     1 * time.Minute,
		cleanupInterval:     10 * time.Minute,
		retention:           24 * time.Hour,
		staleCardTimeout:    staleTimeout,
		reclaimedTotal:      reclaimedTotal,
		cleanedTotal:        cleanedTotal,
		staleCardsRecovered: staleCardsRecovered,
	}
}

func (s *Service) Run(ctx context.Context) error {
	reclaimTicker := time.NewTicker(s.reclaimInterval)
	defer reclaimTicker.Stop()

	cleanupTicker := time.NewTicker(s.cleanupInterval)
	defer cleanupTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			s.logger.Info("reconciler shutdown complete")
			return nil
		case <-reclaimTicker.C:
			s.reclaimStaleCampaignShards(ctx)
			s.completeTerminalCampaigns(ctx)
		case <-cleanupTicker.C:
			s.cleanupPublishedCampaignShards(ctx)
		}
	}
}

func (s *Service) reclaimStaleCampaignShards(ctx context.Context) {
	result := s.db.WithContext(ctx).
		Model(&db.CampaignShard{}).
		Where("status = ? AND claimed_at IS NOT NULL AND claimed_at < ?", "publishing", time.Now().Add(-5*time.Minute)).
		Updates(map[string]interface{}{
			"status":     "pending",
			"claimed_at": nil,
			"updated_at": time.Now(),
		})
	if result.Error != nil {
		s.logger.Error("failed to reclaim stale campaign shards", zap.Error(result.Error))
		return
	}
	if result.RowsAffected > 0 {
		if s.reclaimedTotal != nil {
			s.reclaimedTotal.Add(ctx, result.RowsAffected)
		}
		s.logger.Info("reclaimed stale campaign shards", zap.Int64("count", result.RowsAffected))
	}
}

func (s *Service) cleanupPublishedCampaignShards(ctx context.Context) {
	result := s.db.WithContext(ctx).
		Where("status = ? AND published_at < ?", "published", time.Now().Add(-s.retention)).
		Delete(&db.CampaignShard{})
	if result.Error != nil {
		s.logger.Error("failed to cleanup published campaign shards", zap.Error(result.Error))
		return
	}
	if result.RowsAffected > 0 {
		if s.cleanedTotal != nil {
			s.cleanedTotal.Add(ctx, result.RowsAffected)
		}
		s.logger.Info("cleaned published campaign shards", zap.Int64("count", result.RowsAffected))
	}
}

func (s *Service) completeTerminalCampaigns(ctx context.Context) {
	if s.query == nil {
		return
	}

	var campaigns []db.Campaign
	if err := s.db.WithContext(ctx).
		Where("status = ?", "running").
		Find(&campaigns).Error; err != nil {
		s.logger.Error("failed to list running campaigns for reconciliation", zap.Error(err))
		return
	}

	staleThreshold := time.Now().Add(-s.staleCardTimeout)

	for _, campaign := range campaigns {
		stats, staleCards, err := s.query.CampaignStatsWithStaleCards(ctx, campaign.ID, staleThreshold)
		if err != nil {
			s.logger.Warn("failed to load campaign stats for reconciliation",
				zap.String("campaign_id", campaign.ID.String()),
				zap.Error(err),
			)
			continue
		}

		// Recover stale cards first (moves them to terminal/failed).
		for _, card := range staleCards {
			if s.recoverStaleCard(ctx, campaign.ID, card) {
				stats.InProgress--
				stats.Failed++
			}
		}

		if stats.Total == 0 {
			continue
		}
		terminal := stats.Completed + stats.Failed + stats.Skipped
		if terminal < stats.Total {
			continue
		}

		finalStatus := "completed"
		if stats.Failed > 0 {
			if stats.Completed > 0 || stats.Skipped > 0 {
				finalStatus = "completed_with_errors"
			} else {
				finalStatus = "failed"
			}
		} else if stats.Skipped > 0 {
			finalStatus = "completed_with_errors"
		}

		now := time.Now().UTC()
		result := s.db.WithContext(ctx).
			Model(&db.Campaign{}).
			Where("id = ? AND status = ?", campaign.ID, "running").
			Updates(map[string]any{
				"status":       finalStatus,
				"completed_at": now,
				"updated_at":   now,
			})
		if result.Error != nil {
			s.logger.Warn("failed to reconcile terminal campaign",
				zap.String("campaign_id", campaign.ID.String()),
				zap.Error(result.Error),
			)
			continue
		}
		if result.RowsAffected > 0 {
			s.logger.Info("reconciled terminal campaign",
				zap.String("campaign_id", campaign.ID.String()),
				zap.String("status", finalStatus),
				zap.Int64("total", stats.Total),
				zap.Int64("completed", stats.Completed),
				zap.Int64("failed", stats.Failed),
				zap.Int64("skipped", stats.Skipped),
			)
		}
	}
}

func (s *Service) recoverStaleCard(ctx context.Context, campaignID uuid.UUID, card scyllastore.CampaignCardView) bool {
	if s.cardStateWriter == nil {
		return false
	}

	if err := s.cardStateWriter.WriteCardState(ctx, kafkapkg.CardStateChange{
		CampaignID: campaignID.String(),
		CardID:     card.CardID,
		Status:     "failed",
		LastError:  "stale: no progress for " + s.staleCardTimeout.String(),
		Timestamp:  time.Now(),
	}); err != nil {
		s.logger.Error("failed to write stale card recovery state",
			zap.String("card_id", card.CardID),
			zap.String("campaign_id", campaignID.String()),
			zap.Error(err),
		)
		return false
	}

	if s.staleCardsRecovered != nil {
		s.staleCardsRecovered.Add(ctx, 1)
	}

	s.logger.Warn("recovered stale card",
		zap.String("card_id", card.CardID),
		zap.String("campaign_id", campaignID.String()),
		zap.String("old_status", card.Status),
		zap.Time("last_updated", card.UpdatedAt),
	)
	return true
}
