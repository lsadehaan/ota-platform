package reconciler

import (
	"context"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
	"go.uber.org/zap"
	"gorm.io/gorm"

	"ota-platform/internal/db"
	scyllastore "ota-platform/internal/scylla"
)

type Service struct {
	db              *gorm.DB
	query           *scyllastore.QueryStore
	logger          *zap.Logger
	reclaimInterval time.Duration
	cleanupInterval time.Duration
	retention       time.Duration
	reclaimedTotal  metric.Int64Counter
	cleanedTotal    metric.Int64Counter
}

func NewService(database *gorm.DB, query *scyllastore.QueryStore, logger *zap.Logger) *Service {
	meter := otel.Meter("reconciler")
	reclaimedTotal, err := meter.Int64Counter("ota.reconciler.shards_reclaimed", metric.WithDescription("Campaign shards reclaimed by the reconciler"))
	if err != nil {
		logger.Warn("create reconciler shards_reclaimed metric", zap.Error(err))
	}
	cleanedTotal, err := meter.Int64Counter("ota.reconciler.shards_cleaned", metric.WithDescription("Published campaign shards cleaned by the reconciler"))
	if err != nil {
		logger.Warn("create reconciler shards_cleaned metric", zap.Error(err))
	}
	return &Service{
		db:              database,
		query:           query,
		logger:          logger,
		reclaimInterval: 1 * time.Minute,
		cleanupInterval: 10 * time.Minute,
		retention:       24 * time.Hour,
		reclaimedTotal:  reclaimedTotal,
		cleanedTotal:    cleanedTotal,
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

	for _, campaign := range campaigns {
		stats, err := s.query.CampaignStats(ctx, campaign.ID)
		if err != nil {
			s.logger.Warn("failed to load campaign stats for reconciliation",
				zap.String("campaign_id", campaign.ID.String()),
				zap.Error(err),
			)
			continue
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
