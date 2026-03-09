package planner

import (
	"context"
	"encoding/json"
	"os"
	"strconv"
	"time"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
	"gorm.io/gorm"

	"ota-platform/internal/db"
	kafkapkg "ota-platform/internal/kafka"
)

// Service publishes planner-owned campaign shards to Kafka.
type Service struct {
	db              *gorm.DB
	publisher       Publisher
	logger          *zap.Logger
	pollInterval    time.Duration
	batchSize       int
	tracer          trace.Tracer
	shardsClaimed   metric.Int64Counter
	shardsPublished metric.Int64Counter
	publishDuration metric.Float64Histogram
}

type Publisher interface {
	Publish(ctx context.Context, key string, message interface{}) error
}

type BatchPublisher interface {
	PublishBatch(ctx context.Context, items []kafkapkg.BatchItem) error
}

func NewService(database *gorm.DB, publisher Publisher, logger *zap.Logger) *Service {
	meter := otel.Meter("campaign-planner")
	shardsClaimed, err := meter.Int64Counter("ota.planner.shards_claimed", metric.WithDescription("Planner shards claimed"))
	if err != nil {
		logger.Warn("create planner shards_claimed metric", zap.Error(err))
	}
	shardsPublished, err := meter.Int64Counter("ota.planner.shards_published", metric.WithDescription("Planner shards published"))
	if err != nil {
		logger.Warn("create planner shards_published metric", zap.Error(err))
	}
	publishDuration, err := meter.Float64Histogram("ota.planner.publish_duration_ms", metric.WithDescription("Planner batch publish duration in milliseconds"))
	if err != nil {
		logger.Warn("create planner publish_duration metric", zap.Error(err))
	}
	return &Service{
		db:              database,
		publisher:       publisher,
		logger:          logger,
		pollInterval:    plannerPollInterval(),
		batchSize:       plannerClaimBatchSize(),
		tracer:          otel.Tracer("campaign-planner"),
		shardsClaimed:   shardsClaimed,
		shardsPublished: shardsPublished,
		publishDuration: publishDuration,
	}
}

func plannerPollInterval() time.Duration {
	if v := os.Getenv("PLANNER_POLL_INTERVAL_MS"); v != "" {
		if ms, err := strconv.Atoi(v); err == nil && ms > 0 {
			return time.Duration(ms) * time.Millisecond
		}
	}
	return 500 * time.Millisecond
}

func plannerClaimBatchSize() int {
	if v := os.Getenv("PLANNER_CLAIM_BATCH_SIZE"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return n
		}
	}
	return 1000
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
	started := time.Now()
	ctx, span := s.tracer.Start(ctx, "planner.publish-batch")
	defer func() {
		if s.publishDuration != nil {
			s.publishDuration.Record(ctx, float64(time.Since(started).Milliseconds()))
		}
		span.End()
	}()

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
		span.RecordError(claimErr)
		span.SetStatus(codes.Error, claimErr.Error())
		s.logger.Error("failed to claim campaign shards", zap.Error(claimErr))
		return
	}
	if len(shards) == 0 {
		span.SetStatus(codes.Ok, "no shards")
		return
	}
	if s.shardsClaimed != nil {
		s.shardsClaimed.Add(ctx, int64(len(shards)))
	}

	s.publishClaimedShards(ctx, shards)
	span.SetStatus(codes.Ok, "")
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

		if batchPublisher, ok := s.publisher.(BatchPublisher); ok {
			items := make([]kafkapkg.BatchItem, 0, len(events))
			for _, event := range events {
				items = append(items, kafkapkg.BatchItem{
					Key:   event.CardID,
					Value: event,
				})
			}
			if err := batchPublisher.PublishBatch(ctx, items); err != nil {
				s.logger.Error("failed to publish shard batch",
					zap.String("shard_id", shard.ID.String()),
					zap.Int("shard_size", len(events)),
					zap.Error(err),
				)
				pendingResetIDs = append(pendingResetIDs, shard.ID)
				for _, remaining := range shards[idx+1:] {
					pendingResetIDs = append(pendingResetIDs, remaining.ID)
				}
				break
			}
			publishedIDs = append(publishedIDs, shard.ID)
			continue
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
	if s.shardsPublished != nil {
		s.shardsPublished.Add(ctx, int64(len(publishedIDs)))
	}
}
