package projector

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"os"
	"strconv"
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
	consumer consumer
	logger   *zap.Logger

	incoming      []chan kafkapkg.MessageLogAction
	flushWorkers  int
	batchSize     int
	updateBatch   int
	flushInterval time.Duration
	tracer        trace.Tracer
	actionsTotal  metric.Int64Counter
	flushDuration metric.Float64Histogram
	writeRetries  metric.Int64Counter
}

type consumer interface {
	Start(ctx context.Context) error
	Close() error
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
	writeRetries, err := meter.Int64Counter("ota.projector.write_retries", metric.WithDescription("Total projector write retries"))
	if err != nil {
		logger.Warn("create projector write_retries metric", zap.Error(err))
	}
	flushWorkers := envInt("PROJECTOR_FLUSH_WORKERS", 8)
	if flushWorkers <= 0 {
		flushWorkers = 8
	}
	batchSize := envInt("PROJECTOR_BATCH_SIZE", 2000)
	if batchSize <= 0 {
		batchSize = 2000
	}
	updateBatch := envInt("PROJECTOR_UPDATE_BATCH_SIZE", 2000)
	if updateBatch <= 0 {
		updateBatch = 2000
	}
	flushInterval := time.Duration(envInt("PROJECTOR_FLUSH_INTERVAL_MS", 100)) * time.Millisecond
	if flushInterval <= 0 {
		flushInterval = 100 * time.Millisecond
	}

	incoming := make([]chan kafkapkg.MessageLogAction, flushWorkers)
	for i := range incoming {
		incoming[i] = make(chan kafkapkg.MessageLogAction, batchSize*2)
	}

	lw := &LogWriter{
		store:         store,
		logger:        logger,
		incoming:      incoming,
		flushWorkers:  flushWorkers,
		batchSize:     batchSize,
		updateBatch:   updateBatch,
		flushInterval: flushInterval,
		tracer:        otel.Tracer("read-model-projector"),
		actionsTotal:  actionsTotal,
		flushDuration: flushDuration,
		writeRetries:  writeRetries,
	}

	workers := 16
	if v := os.Getenv("PROJECTOR_WORKERS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			workers = n
		}
	}
	lw.consumer = kafkapkg.NewConcurrentConsumer(
		brokers,
		contractevents.TopicMessageLog,
		"log-writers",
		workers,
		lw.handleMessage,
		logger.Named("log-consumer"),
	)

	return lw
}

// Start launches the consumer and batch writer goroutines.
func (lw *LogWriter) Start(ctx context.Context) {
	for i := 0; i < lw.flushWorkers; i++ {
		go lw.batchWriteLoop(ctx, lw.incoming[i])
	}

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

	shard := lw.shardForAction(action)
	select {
	case lw.incoming[shard] <- action:
	case <-ctx.Done():
		span.RecordError(ctx.Err())
		span.SetStatus(codes.Error, ctx.Err().Error())
		return ctx.Err()
	}
	span.SetStatus(codes.Ok, "")
	return nil
}

func (lw *LogWriter) batchWriteLoop(ctx context.Context, incoming <-chan kafkapkg.MessageLogAction) {
	creates := make(map[string]db.MessageLog, lw.batchSize)
	updates := make(map[string]kafkapkg.MessageLogAction, lw.updateBatch)
	ticker := time.NewTicker(lw.flushInterval)
	defer ticker.Stop()

	flush := func() {
		if len(creates) == 0 && len(updates) == 0 {
			return
		}
		started := time.Now()
		if len(creates) > 0 {
			createSlice := make([]db.MessageLog, 0, len(creates))
			for _, create := range creates {
				createSlice = append(createSlice, create)
			}
			for attempt := 1; attempt <= 10; attempt++ {
				err := lw.store.CreateBatch(ctx, createSlice, lw.batchSize)
				if err == nil {
					break
				}
				if lw.writeRetries != nil {
					lw.writeRetries.Add(ctx, 1)
				}
				if attempt == 10 {
					lw.logger.Error("batch create message logs failed after max retries, dropping batch",
						zap.Int("count", len(createSlice)),
						zap.Error(err),
					)
					break
				}
				lw.logger.Error("batch create message logs failed, retrying",
					zap.Int("count", len(createSlice)),
					zap.Int("attempt", attempt),
					zap.Error(err),
				)
				backoff := time.Duration(attempt) * 500 * time.Millisecond
				if backoff > 10*time.Second {
					backoff = 10 * time.Second
				}
				select {
				case <-ctx.Done():
					return
				case <-time.After(backoff):
				}
			}
			clear(creates)
		}
		if len(updates) > 0 {
			updateSlice := make([]kafkapkg.MessageLogAction, 0, len(updates))
			for _, update := range updates {
				updateSlice = append(updateSlice, update)
			}
			for attempt := 1; attempt <= 10; attempt++ {
				err := lw.store.ApplyUpdates(ctx, updateSlice)
				if err == nil {
					break
				}
				if lw.writeRetries != nil {
					lw.writeRetries.Add(ctx, 1)
				}
				if attempt == 10 {
					lw.logger.Error("batch update message logs failed after max retries, dropping batch",
						zap.Int("count", len(updateSlice)),
						zap.Error(err),
					)
					break
				}
				lw.logger.Error("batch update message logs failed, retrying",
					zap.Int("count", len(updateSlice)),
					zap.Int("attempt", attempt),
					zap.Error(err),
				)
				backoff := time.Duration(attempt) * 500 * time.Millisecond
				if backoff > 10*time.Second {
					backoff = 10 * time.Second
				}
				select {
				case <-ctx.Done():
					return
				case <-time.After(backoff):
				}
			}
			clear(updates)
		}
		if lw.flushDuration != nil {
			lw.flushDuration.Record(ctx, float64(time.Since(started).Milliseconds()))
		}
	}

	for {
		select {
		case action := <-incoming:
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
					if pending, ok := updates[action.Log.ID]; ok {
						applyUpdatesToMessageLog(&ml, pending.Updates)
						delete(updates, action.Log.ID)
					}
					creates[action.Log.ID] = ml
					if len(creates) >= lw.batchSize {
						flush()
					}
				}
			case "update":
				if action.ID == "" {
					continue
				}
				if create, ok := creates[action.ID]; ok {
					applyUpdatesToMessageLog(&create, action.Updates)
					creates[action.ID] = create
				} else if existing, ok := updates[action.ID]; ok {
					existing.Updates = mergeUpdateMaps(existing.Updates, action.Updates)
					updates[action.ID] = existing
				} else {
					updates[action.ID] = action
				}
				if len(updates) >= lw.updateBatch {
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

func (lw *LogWriter) shardForAction(action kafkapkg.MessageLogAction) int {
	key := action.ID
	if key == "" && action.Log != nil {
		key = action.Log.ID
	}
	if key == "" || lw.flushWorkers <= 1 {
		return 0
	}
	h := fnv.New32a()
	_, _ = h.Write([]byte(key))
	return int(h.Sum32() % uint32(lw.flushWorkers))
}

func envInt(key string, fallback int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return n
		}
	}
	return fallback
}

func mergeUpdateMaps(dst, src map[string]interface{}) map[string]interface{} {
	if len(src) == 0 {
		return dst
	}
	if dst == nil {
		dst = make(map[string]interface{}, len(src))
	}
	for k, v := range src {
		dst[k] = v
	}
	return dst
}

func applyUpdatesToMessageLog(log *db.MessageLog, updates map[string]interface{}) {
	if log == nil || len(updates) == 0 {
		return
	}
	for key, value := range updates {
		switch key {
		case "status":
			if v, ok := value.(string); ok {
				log.Status = v
			}
		case "smpp_message_id":
			switch v := value.(type) {
			case string:
				log.SMPPMessageID = &v
			case *string:
				log.SMPPMessageID = v
			case nil:
				log.SMPPMessageID = nil
			}
		case "dlr_status":
			switch v := value.(type) {
			case string:
				log.DLRStatus = &v
			case *string:
				log.DLRStatus = v
			case nil:
				log.DLRStatus = nil
			}
		case "counter_hex":
			switch v := value.(type) {
			case string:
				log.CounterHex = &v
			case *string:
				log.CounterHex = v
			case nil:
				log.CounterHex = nil
			}
		case "por_status_code":
			switch v := value.(type) {
			case int16:
				log.PORStatusCode = &v
			case *int16:
				log.PORStatusCode = v
			case nil:
				log.PORStatusCode = nil
			}
		case "por_data":
			switch v := value.(type) {
			case []byte:
				log.PORData = v
			case string:
				log.PORData = []byte(v)
			}
		}
	}
	log.UpdatedAt = time.Now().UTC()
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
