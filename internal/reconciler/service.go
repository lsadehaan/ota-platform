package reconciler

import (
	"context"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
	"go.uber.org/zap"
	"gorm.io/gorm"

	"ota-platform/internal/db"
)

type Service struct {
	db              *gorm.DB
	logger          *zap.Logger
	reclaimInterval time.Duration
	cleanupInterval time.Duration
	retention       time.Duration
	reclaimedTotal  metric.Int64Counter
	cleanedTotal    metric.Int64Counter
}

func NewService(database *gorm.DB, logger *zap.Logger) *Service {
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
