package planner

import (
	"context"
	"encoding/json"
	"time"

	"github.com/google/uuid"
	"go.uber.org/zap"
	"gorm.io/gorm"

	"ota-platform/internal/db"
	kafkapkg "ota-platform/internal/kafka"
)

// Service publishes planner-owned campaign shards to Kafka.
type Service struct {
	db           *gorm.DB
	publisher    Publisher
	logger       *zap.Logger
	pollInterval time.Duration
	batchSize    int
}

type Publisher interface {
	Publish(ctx context.Context, key string, message interface{}) error
}

func NewService(database *gorm.DB, publisher Publisher, logger *zap.Logger) *Service {
	return &Service{
		db:           database,
		publisher:    publisher,
		logger:       logger,
		pollInterval: 100 * time.Millisecond,
		batchSize:    100,
	}
}

func (s *Service) Run(ctx context.Context) {
	ticker := time.NewTicker(s.pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.publishBatch(ctx)
		}
	}
}

func (s *Service) publishBatch(ctx context.Context) {
	var shards []db.CampaignShard
	claimErr := s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		return tx.Raw(`
			UPDATE campaign_shards
			SET status = 'publishing',
			    claimed_at = now(),
			    updated_at = now()
			WHERE id IN (
				SELECT id FROM campaign_shards
				WHERE status = 'pending' AND claimed_at IS NULL
				ORDER BY created_at ASC
				LIMIT ?
				FOR UPDATE SKIP LOCKED
			)
			RETURNING *
		`, s.batchSize).Scan(&shards).Error
	})
	if claimErr != nil {
		s.logger.Error("failed to claim campaign shards", zap.Error(claimErr))
		return
	}
	if len(shards) == 0 {
		return
	}

	s.publishClaimedShards(ctx, shards)
}

func (s *Service) publishClaimedShards(ctx context.Context, shards []db.CampaignShard) {
	if len(shards) == 0 {
		return
	}

	publishedIDs := make([]uuid.UUID, 0, len(shards))
	pendingResetIDs := make([]uuid.UUID, 0)
	for idx, shard := range shards {
		var events []kafkapkg.CardEvent
		if err := json.Unmarshal(shard.Items, &events); err != nil {
			s.logger.Error("failed to unmarshal shard items",
				zap.String("shard_id", shard.ID.String()),
				zap.Error(err),
			)
			pendingResetIDs = append(pendingResetIDs, shard.ID)
			break
		}

		ok := true
		publishedFromShard := 0
		for _, event := range events {
			if err := s.publisher.Publish(ctx, event.CardID, event); err != nil {
				s.logger.Error("failed to publish shard event",
					zap.String("shard_id", shard.ID.String()),
					zap.String("event_id", event.EventID),
					zap.String("card_id", event.CardID),
					zap.Error(err),
				)
				if publishedFromShard == 0 {
					pendingResetIDs = append(pendingResetIDs, shard.ID)
				} else {
					s.logger.Warn("leaving partially published shard for reconciler recovery",
						zap.String("shard_id", shard.ID.String()),
						zap.Int("published_events", publishedFromShard),
						zap.Int("shard_size", len(events)),
					)
				}
				ok = false
				break
			}
			publishedFromShard++
		}
		if !ok {
			for _, remaining := range shards[idx+1:] {
				pendingResetIDs = append(pendingResetIDs, remaining.ID)
			}
			break
		}
		publishedIDs = append(publishedIDs, shard.ID)
	}

	if len(pendingResetIDs) > 0 {
		now := time.Now()
		if err := s.db.WithContext(ctx).
			Model(&db.CampaignShard{}).
			Where("id IN ?", pendingResetIDs).
			Updates(map[string]interface{}{
				"status":     "pending",
				"claimed_at": nil,
				"updated_at": now,
			}).Error; err != nil {
			s.logger.Error("failed to reset unpublished campaign shards", zap.Error(err))
		}
	}

	if len(publishedIDs) == 0 {
		return
	}

	now := time.Now()
	if err := s.db.WithContext(ctx).
		Model(&db.CampaignShard{}).
		Where("id IN ?", publishedIDs).
		Updates(map[string]interface{}{
			"status":       "published",
			"published_at": now,
			"updated_at":   now,
		}).Error; err != nil {
		s.logger.Error("failed to mark campaign shards published", zap.Error(err))
		return
	}

	s.logger.Debug("published campaign shard batch", zap.Int("count", len(publishedIDs)))
}
