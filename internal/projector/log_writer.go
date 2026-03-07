package projector

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"

	contractevents "ota-platform/internal/contracts/events"
	"ota-platform/internal/db"
	kafkapkg "ota-platform/internal/kafka"
)

// LogWriter consumes message-log events from Kafka and batch-writes them to the configured durable store.
type LogWriter struct {
	store    MessageLogStore
	consumer *kafkapkg.Consumer
	logger   *zap.Logger

	incoming      chan kafkapkg.MessageLogAction
	batchSize     int
	flushInterval time.Duration
	tracer        trace.Tracer
	actionsTotal  metric.Int64Counter
	flushDuration metric.Float64Histogram
}

// NewLogWriter creates a new LogWriter that consumes from the message-log Kafka topic.
func NewLogWriter(store MessageLogStore, brokers []string, logger *zap.Logger) *LogWriter {
	meter := otel.Meter("read-model-projector")
	actionsTotal, err := meter.Int64Counter("ota.projector.actions_processed", metric.WithDescription("Message log actions processed"))
	if err != nil {
		logger.Warn("create projector actions_processed metric", zap.Error(err))
	}
	flushDuration, err := meter.Float64Histogram("ota.projector.flush_duration_ms", metric.WithDescription("Projector flush duration in milliseconds"))
	if err != nil {
		logger.Warn("create projector flush_duration metric", zap.Error(err))
	}
	lw := &LogWriter{
		store:         store,
		logger:        logger,
		incoming:      make(chan kafkapkg.MessageLogAction, 5000),
		batchSize:     500,
		flushInterval: 100 * time.Millisecond,
		tracer:        otel.Tracer("read-model-projector"),
		actionsTotal:  actionsTotal,
		flushDuration: flushDuration,
	}

	lw.consumer = kafkapkg.NewConsumer(
		brokers,
		contractevents.TopicMessageLog,
		"log-writers",
		lw.handleMessage,
		logger.Named("log-consumer"),
	)

	return lw
}

// Start launches the consumer and batch writer goroutines.
func (lw *LogWriter) Start(ctx context.Context) {
	go lw.batchWriteLoop(ctx)

	if err := lw.consumer.Start(ctx); err != nil {
		lw.logger.Error("log writer consumer stopped", zap.Error(err))
	}
}

func (lw *LogWriter) handleMessage(ctx context.Context, key []byte, value []byte) error {
	ctx, span := lw.tracer.Start(ctx, "projector.handle-message")
	defer span.End()

	var action kafkapkg.MessageLogAction
	if err := json.Unmarshal(value, &action); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		lw.logger.Error("unmarshal message log action", zap.Error(err))
		return nil // don't retry unmarshal errors
	}
	if lw.actionsTotal != nil {
		lw.actionsTotal.Add(ctx, 1)
	}

	select {
	case lw.incoming <- action:
	case <-ctx.Done():
		span.RecordError(ctx.Err())
		span.SetStatus(codes.Error, ctx.Err().Error())
		return ctx.Err()
	}
	span.SetStatus(codes.Ok, "")
	return nil
}

func (lw *LogWriter) batchWriteLoop(ctx context.Context) {
	creates := make([]db.MessageLog, 0, lw.batchSize)
	updates := make([]kafkapkg.MessageLogAction, 0, 100)
	ticker := time.NewTicker(lw.flushInterval)
	defer ticker.Stop()

	flush := func() {
		if len(creates) == 0 && len(updates) == 0 {
			return
		}
		started := time.Now()
		if len(creates) > 0 {
			if err := lw.store.CreateBatch(ctx, creates, lw.batchSize); err != nil {
				lw.logger.Error("batch create message logs failed", zap.Int("count", len(creates)), zap.Error(err))
			}
			creates = creates[:0]
		}
		if len(updates) > 0 {
			if err := lw.store.ApplyUpdates(ctx, updates); err != nil {
				lw.logger.Error("batch update message logs failed", zap.Int("count", len(updates)), zap.Error(err))
			}
			updates = updates[:0]
		}
		if lw.flushDuration != nil {
			lw.flushDuration.Record(ctx, float64(time.Since(started).Milliseconds()))
		}
	}

	for {
		select {
		case action := <-lw.incoming:
			switch action.Action {
			case "create":
				if action.Log != nil {
					ml, err := lw.toMessageLog(action.Log)
					if err != nil {
						lw.logger.Warn("dropping malformed message log create",
							zap.String("id", action.Log.ID),
							zap.Error(err),
						)
						continue
					}
					creates = append(creates, ml)
					if len(creates) >= lw.batchSize {
						flush()
					}
				}
			case "update":
				updates = append(updates, action)
				if len(updates) >= 100 {
					flush()
				}
			}
		case <-ticker.C:
			flush()
		case <-ctx.Done():
			flush() // final flush
			return
		}
	}
}

func (lw *LogWriter) toMessageLog(entry *kafkapkg.MessageLogEntry) (db.MessageLog, error) {
	logID, err := uuid.Parse(entry.ID)
	if err != nil {
		return db.MessageLog{}, fmt.Errorf("parse log id: %w", err)
	}
	cardID, err := uuid.Parse(entry.CardID)
	if err != nil {
		return db.MessageLog{}, fmt.Errorf("parse card id: %w", err)
	}

	ml := db.MessageLog{
		ID:        logID,
		CardID:    cardID,
		Direction: entry.Direction,
		Status:    entry.Status,
	}
	createdAt := time.Now().UTC()
	if entry.CreatedAt != "" {
		parsed, err := time.Parse(time.RFC3339Nano, entry.CreatedAt)
		if err != nil {
			return db.MessageLog{}, fmt.Errorf("parse created_at: %w", err)
		}
		createdAt = parsed.UTC()
	}
	updatedAt := createdAt
	if entry.UpdatedAt != "" {
		parsed, err := time.Parse(time.RFC3339Nano, entry.UpdatedAt)
		if err != nil {
			return db.MessageLog{}, fmt.Errorf("parse updated_at: %w", err)
		}
		updatedAt = parsed.UTC()
	}
	ml.CreatedAt = createdAt
	ml.UpdatedAt = updatedAt

	if entry.CampaignID != "" {
		cid, err := uuid.Parse(entry.CampaignID)
		if err != nil {
			return db.MessageLog{}, fmt.Errorf("parse campaign id: %w", err)
		}
		ml.CampaignID = &cid
	}
	if entry.RawPayload != "" {
		ml.RawPayload, _ = base64.StdEncoding.DecodeString(entry.RawPayload)
	}
	if entry.SecuredPayload != "" {
		ml.SecuredPayload, _ = base64.StdEncoding.DecodeString(entry.SecuredPayload)
	}
	if entry.CounterHex != "" {
		ml.CounterHex = &entry.CounterHex
	}
	if entry.SMPPMessageID != "" {
		ml.SMPPMessageID = &entry.SMPPMessageID
	}

	return ml, nil
}

// Close closes the underlying Kafka consumer.
func (lw *LogWriter) Close() error {
	return lw.consumer.Close()
}
