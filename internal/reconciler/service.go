package reconciler

import (
	"context"
	"time"

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
}

func NewService(database *gorm.DB, logger *zap.Logger) *Service {
	return &Service{
		db:              database,
		logger:          logger,
		reclaimInterval: 1 * time.Minute,
		cleanupInterval: 10 * time.Minute,
		retention:       24 * time.Hour,
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
		s.logger.Info("cleaned published campaign shards", zap.Int64("count", result.RowsAffected))
	}
}
