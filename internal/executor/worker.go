package executor

import (
	"context"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"hash/fnv"
	"sync"
	"time"

	"github.com/google/uuid"
	jsoniter "github.com/json-iterator/go"
	goredis "github.com/redis/go-redis/v9"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
	"go.uber.org/zap"
	"golang.org/x/sync/singleflight"
	"gorm.io/gorm"

	"ota-platform/internal/db"
	"ota-platform/internal/pipeline"
	"ota-platform/internal/keystore"
	redispkg "ota-platform/internal/redis"
	"ota-platform/pkg/gsm0348"
	"ota-platform/pkg/hexutil"
	"ota-platform/pkg/smsframe"
)

// shardedCache is a fixed-shard concurrent map. O(1) lookups, no internal
// scanning — unlike sync.Map which periodically copies dirty→read under load.
const cacheShards = 256

type shardedCache[V any] struct {
	shards [cacheShards]cacheShard[V]
}

type cacheShard[V any] struct {
	mu sync.RWMutex
	m  map[string]V
}

func newShardedCache[V any]() *shardedCache[V] {
	c := &shardedCache[V]{}
	for i := range c.shards {
		c.shards[i].m = make(map[string]V)
	}
	return c
}

func (c *shardedCache[V]) shard(key string) *cacheShard[V] {
	h := fnv.New32a()
	h.Write([]byte(key))
	return &c.shards[h.Sum32()%cacheShards]
}

func (c *shardedCache[V]) Load(key string) (V, bool) {
	s := c.shard(key)
	s.mu.RLock()
	v, ok := s.m[key]
	s.mu.RUnlock()
	return v, ok
}

func (c *shardedCache[V]) Store(key string, value V) {
	s := c.shard(key)
	s.mu.Lock()
	s.m[key] = value
	s.mu.Unlock()
}

func (c *shardedCache[V]) Delete(key string) {
	s := c.shard(key)
	s.mu.Lock()
	delete(s.m, key)
	s.mu.Unlock()
}

// CardWorker processes card events using the coordination store for
// hot state and PostgreSQL for durable state.
type CardWorker struct {
	db             *gorm.DB
	execution      ExecutionStore
	redis          CoordinationStore
	keyStore       keystore.KeyStore
	cryptoProvider keystore.CryptoProvider // nil when using software crypto
	smsProducer    Producer                // publishes to send-sms channel
	logProducer    Producer                // publishes to message-log channel
	eventProducer  Producer                // publishes to card-events channel (for self-triggering next steps)
	cardState      CardStateWriter         // writes card state via batched channel writer
	counterStore   CounterStore            // process-local counter cache (zero DB during execution)
	wsHub          WSHub
	logger         *zap.Logger
	telemetry      *executorTelemetry
	retryWg        *sync.WaitGroup
	retrySem       chan struct{} // bounds concurrent retry goroutines
	retryOverflows metric.Int64Counter

	// Process-local campaign context cache. Campaign commands and params are
	// immutable during execution, so caching eliminates repeated Redis/DB
	// lookups (~600k Redis roundtrips saved per 200k-card campaign).
	// Pointer types so txn-mode copies share the same cache.
	campaignCache       *sync.Map           // map[campaignID]*campaignExecutionContext
	campaignSF          *singleflight.Group // coalesces concurrent cache misses
	campaignStatusCache *sync.Map           // map[campaignID]*cachedStatus — 30s TTL
	cacheHits           metric.Int64Counter
	cacheMisses         metric.Int64Counter

	// Process-local card key cache. Card keys (enc_key, auth_key, kek,
	// profile_id) are immutable — cache indefinitely to avoid repeated
	// DB reads (~200k reads eliminated per 200k-card campaign).
	cardKeyCache *shardedCache[*keystore.CardKeyMaterial]
}

type campaignExecutionContext struct {
	commands      []redispkg.CampaignCommandCache
	commandByStep map[int]*redispkg.CampaignCommandCache
	params        *redispkg.CampaignParams
	cachedAt      time.Time // for TTL-based eviction
}

type cachedStatus struct {
	status   string
	cachedAt time.Time
}

const campaignCacheTTL = 1 * time.Hour
const campaignStatusCacheTTL = 5 * time.Second

// NewCardWorker creates a new CardWorker.
func NewCardWorker(database *gorm.DB, executionStore ExecutionStore, rdb CoordinationStore, ks keystore.KeyStore, cp keystore.CryptoProvider, smsProducer, logProducer, eventProducer Producer, cardState CardStateWriter, counterStore CounterStore, wsHub WSHub, logger *zap.Logger) *CardWorker {
	meter := otel.Meter("card-executor")
	retryOverflows, err := meter.Int64Counter("ota.executor.retry_overflows",
		metric.WithDescription("Retry goroutine limit overflows (card failed immediately)"),
	)
	if err != nil {
		logger.Warn("create executor retry_overflows metric", zap.Error(err))
	}
	cacheHits, err := meter.Int64Counter("ota.executor.campaign_cache_hits",
		metric.WithDescription("Process-local campaign context cache hits"),
	)
	if err != nil {
		logger.Warn("create campaign_cache_hits metric", zap.Error(err))
	}
	cacheMisses, err := meter.Int64Counter("ota.executor.campaign_cache_misses",
		metric.WithDescription("Process-local campaign context cache misses"),
	)
	if err != nil {
		logger.Warn("create campaign_cache_misses metric", zap.Error(err))
	}

	return &CardWorker{
		db:                  database,
		execution:           executionStore,
		redis:               rdb,
		keyStore:            ks,
		cryptoProvider:      cp,
		smsProducer:         smsProducer,
		logProducer:         logProducer,
		eventProducer:       eventProducer,
		cardState:           cardState,
		counterStore:        counterStore,
		wsHub:               wsHub,
		logger:              logger,
		telemetry:           newExecutorTelemetry(logger),
		retryWg:             &sync.WaitGroup{},
		retrySem:            make(chan struct{}, 10000),
		retryOverflows:      retryOverflows,
		campaignCache:       &sync.Map{},
		campaignSF:          &singleflight.Group{},
		campaignStatusCache: &sync.Map{},
		cacheHits:           cacheHits,
		cacheMisses:         cacheMisses,
		cardKeyCache:        newShardedCache[*keystore.CardKeyMaterial](),
	}
}

// nextSeq increments and returns the next transition_seq for a card state.
// The state object is mutated in place.
func nextSeq(state *redispkg.CardState) int64 {
	state.TransitionSeq++
	return state.TransitionSeq
}

// HandleEvent is the message handler for card events.
func (w *CardWorker) HandleEvent(ctx context.Context, key []byte, value []byte) error {
	var event pipeline.CardEvent
	if err := jsoniter.ConfigCompatibleWithStandardLibrary.Unmarshal(value, &event); err != nil {
		w.logger.Error("unmarshal card event", zap.Error(err))
		return err // will be retried by consumer
	}
	ctx, span, started := w.telemetry.startEvent(ctx, event.Type, event.CampaignID, event.CardID, event.Step)
	var handleErr error
	defer func() {
		w.telemetry.finishEvent(ctx, span, started, event.Type, handleErr)
	}()

	// Idempotency check
	isNew, err := w.redis.CheckAndSetDedupe(ctx, event.EventID)
	if err != nil {
		w.logger.Warn("dedupe check failed, proceeding anyway", zap.Error(err))
		w.telemetry.recordDedupeError(ctx)
	} else if !isNew {
		w.logger.Debug("duplicate event, skipping", zap.String("event_id", event.EventID))
		return nil
	}

	switch event.Type {
	case "card.activate":
		handleErr = w.handleActivate(ctx, &event)
	case "card.dlr_received":
		handleErr = w.handleDLR(ctx, &event)
	case "card.mo_received":
		handleErr = w.handleMO(ctx, &event)
	default:
		w.logger.Warn("unknown event type", zap.String("type", event.Type))
		handleErr = nil
	}
	return handleErr
}

// HandleCardEvent processes a pre-deserialized card event, avoiding JSON marshal/unmarshal
// overhead when events come from Go channels instead of Kafka.
func (w *CardWorker) HandleCardEvent(ctx context.Context, event *pipeline.CardEvent) error {
	ctx, span, started := w.telemetry.startEvent(ctx, event.Type, event.CampaignID, event.CardID, event.Step)
	var handleErr error
	defer func() {
		w.telemetry.finishEvent(ctx, span, started, event.Type, handleErr)
	}()

	// Idempotency check
	isNew, err := w.redis.CheckAndSetDedupe(ctx, event.EventID)
	if err != nil {
		w.logger.Warn("dedupe check failed, proceeding anyway", zap.Error(err))
		w.telemetry.recordDedupeError(ctx)
	} else if !isNew {
		w.logger.Debug("duplicate event, skipping", zap.String("event_id", event.EventID))
		return nil
	}

	switch event.Type {
	case "card.activate":
		handleErr = w.handleActivate(ctx, event)
	case "card.dlr_received":
		handleErr = w.handleDLR(ctx, event)
	case "card.mo_received":
		handleErr = w.handleMO(ctx, event)
	default:
		w.logger.Warn("unknown event type", zap.String("type", event.Type))
	}
	return handleErr
}

// handleActivate builds and sends the OTA command for the given card step.
func (w *CardWorker) handleActivate(ctx context.Context, event *pipeline.CardEvent) error {
	// 1. Check campaign status from process-local cache, then Redis, then DB.
	opStart := time.Now()
	status, err := w.getCampaignStatusCached(ctx, event.CampaignID)
	w.telemetry.recordOp(ctx, "activate", "campaign_status", time.Since(opStart))
	if err != nil {
		return fmt.Errorf("get campaign status: %w", err)
	}
	if status != "running" {
		w.InvalidateCampaignCache(event.CampaignID)
		w.logger.Debug("skipping activate for non-running campaign",
			zap.String("campaign_id", event.CampaignID),
			zap.String("status", status),
		)
		return nil
	}

	// 2. Load campaign execution context from the process-local cache (cheap, usually cached).
	opStart = time.Now()
	campaignCtx, err := w.loadCampaignContext(ctx, event.CampaignID)
	w.telemetry.recordOp(ctx, "activate", "load_campaign_ctx", time.Since(opStart))
	if err != nil {
		return fmt.Errorf("load campaign context: %w", err)
	}

	// 3. Find the command matching event.Step.
	cmd := campaignCtx.commandByStep[event.Step]
	if cmd == nil {
		return fmt.Errorf("no command found at sequence %d for campaign %s", event.Step, event.CampaignID)
	}

	// 4. Load card keys (cached process-locally; immutable per card).
	var cardKeys *keystore.CardKeyMaterial
	cachedKeys, keysHit := w.cardKeyCache.Load(event.CardID)
	if keysHit {
		cardKeys = cachedKeys
	} else {
		opStart = time.Now()
		var keysErr error
		cardKeys, keysErr = w.loadCardKeys(ctx, event.CardID)
		w.telemetry.recordOp(ctx, "activate", "load_card_keys", time.Since(opStart))
		if keysErr != nil {
			return fmt.Errorf("get card keys: %w", keysErr)
		}
	}

	// 5. Get current counter value then increment — both are process-local
	// map operations (zero DB I/O). Counter values are batch-preloaded by
	// the planner before shard activation.
	counterVal, err := w.counterStore.GetCounter(ctx, event.CardID, cmd.ApplicationID)
	if err != nil {
		return fmt.Errorf("get counter: %w", err)
	}
	if err := w.counterStore.IncrCounterAsync(ctx, event.CardID, cmd.ApplicationID); err != nil {
		return fmt.Errorf("incr counter: %w", err)
	}

	// 6. Build security profile from the cached command.
	secProfile := buildSecProfileFromCache(cmd)

	// 7. Build GSM 03.48 command packet.
	var tar [3]byte
	copy(tar[:], cmd.TAR)

	var counter [5]byte
	binary.BigEndian.PutUint32(counter[1:], uint32(counterVal))
	counter[0] = byte(counterVal >> 32)

	input := &gsm0348.CommandPacketInput{
		TAR:      tar,
		Counter:  counter,
		UserData: cmd.Script,
	}

	if w.cryptoProvider != nil {
		input.CryptoProvider = w.cryptoProvider
		input.CardID = event.CardID
	} else {
		input.CipheringKey = cardKeys.EncKey
		input.SigningKey = cardKeys.AuthKey
	}

	opStart = time.Now()
	packet, err := secProfile.BuildCommandPacket(input)
	w.telemetry.recordOp(ctx, "activate", "build_packet", time.Since(opStart))
	if err != nil {
		return fmt.Errorf("build command packet: %w", err)
	}

	// 8. Load card profile for SMS framing.
	profile, err := w.loadProfileCached(ctx, cardKeys.ProfileID)
	if err != nil {
		return fmt.Errorf("load profile: %w", err)
	}

	// 9. Check for campaign max concat override.
	bufferSize := profile.BufferSize
	maxConcat := profile.MaxConcatSMS

	campaignParams := campaignCtx.params
	if campaignParams.MaxConcatOverride != nil {
		maxConcat = *campaignParams.MaxConcatOverride
	}

	// 10. Frame with SplitForConcat.
	parts, err := smsframe.SplitForConcat(packet, bufferSize, maxConcat)
	if err != nil {
		return fmt.Errorf("split for concat: %w", err)
	}

	// 11. Throttle check.
	if campaignParams.ThrottleSMSPerSec != nil && *campaignParams.ThrottleSMSPerSec > 0 {
		allowed, err := w.redis.AcquireThrottle(ctx, event.CampaignID, *campaignParams.ThrottleSMSPerSec)
		if err != nil {
			w.logger.Warn("throttle check failed, proceeding", zap.Error(err))
		} else if !allowed {
			backoff := time.Duration(event.RetryCount+1) * 100 * time.Millisecond
			if backoff > time.Second {
				backoff = time.Second
			}
			retryEvent := pipeline.CardEvent{
				Type:       "card.activate",
				EventID:    uuid.New().String(),
				CardID:     event.CardID,
				CampaignID: event.CampaignID,
				Step:       event.Step,
				RetryCount: event.RetryCount,
				Timestamp:  time.Now(),
			}
			w.telemetry.recordRetry(ctx, "throttle")
			w.scheduleActivateRetry(ctx, retryEvent, backoff, "throttle")
			return nil
		}
	}

	// 12. Build MessageLog entry payload.
	counterHex := hex.EncodeToString(counter[:])
	msgID := uuid.New()
	createdAt := time.Now().UTC()
	logEntry := &pipeline.MessageLogEntry{
		ID:             msgID.String(),
		CampaignID:     event.CampaignID,
		CardID:         event.CardID,
		Direction:      "MT",
		CreatedAt:      createdAt,
		UpdatedAt:      createdAt,
		RawPayload:     cmd.Script,
		SecuredPayload: packet,
		CounterHex:     counterHex,
	}

	// 13. Build SendSMSMessage and publish to send-sms topic.
	smsParts := make([]pipeline.SMSPart, len(parts))
	for i, p := range parts {
		smsParts[i] = pipeline.SMSPart{
			Sequence: i + 1,
			Total:    len(parts),
			RefNum:   0,
			Payload:  p,
		}
	}

	smsMsg := pipeline.SendSMSMessage{
		MsgID:      msgID.String(),
		CampaignID: event.CampaignID,
		CardID:     event.CardID,
		MSISDN:     cardKeys.MSISDN,
		TON:        1, // International
		NPI:        1, // ISDN/telephone
		DataCoding: byte(profile.DCS),
		ProtocolID: byte(profile.PID),
		ESMClass:   0x40, // UDHI indicator
		Parts:      smsParts,
	}

	// Capture timestamp BEFORE publishing so any DLR/MO completion that uses
	// time.Now() will have a later timestamp, ensuring correct ScyllaDB LWW
	// ordering even if UpdateCampaignCard runs after the DLR/MO handler.
	activateTS := time.Now()

	// Set Redis state BEFORE publishing to send-sms so the DLR/MO consumers
	// can find the card state when the gateway responds.
	opStart = time.Now()
	activateState := &redispkg.CardState{
		CampaignID:    event.CampaignID,
		CurrentStep:   event.Step,
		Status:        "awaiting_dlr",
		RetryCount:    event.RetryCount,
		LastMsgID:     msgID.String(),
		LastEventID:   event.EventID,
		TransitionSeq: 1, // first transition for this card in this campaign
	}
	if err := w.redis.SetCardState(ctx, event.CardID, activateState); err != nil {
		w.logger.Warn("failed to set card state in redis", zap.Error(err))
	}
	w.telemetry.recordOp(ctx, "activate", "redis_set_state", time.Since(opStart))

	opStart = time.Now()
	_, err = w.smsProducer.Publish(ctx, event.CardID, smsMsg)
	if err != nil {
		failCreate := pipeline.MessageLogAction{
			Action: "create",
			Log: func() *pipeline.MessageLogEntry {
				entry := *logEntry
				entry.Status = "send_failed"
				return &entry
			}(),
		}
		if _, logErr := w.logProducer.Publish(ctx, event.CardID, failCreate); logErr != nil {
			w.logger.Warn("failed to publish message log (send_failed)", zap.Error(logErr))
		}
		return fmt.Errorf("publish SMS to kafka: %w", err)
	}
	w.telemetry.recordOp(ctx, "activate", "kafka_send_sms", time.Since(opStart))

	opStart = time.Now()
	successCreate := pipeline.MessageLogAction{
		Action: "create",
		Log: func() *pipeline.MessageLogEntry {
			entry := *logEntry
			entry.Status = "sent"
			return &entry
		}(),
	}
	if _, err := w.logProducer.Publish(ctx, event.CardID, successCreate); err != nil {
		w.logger.Warn("failed to publish message log create", zap.Error(err))
	}
	w.telemetry.recordOp(ctx, "activate", "kafka_log", time.Since(opStart))

	// Write card state via async Kafka (fire-and-forget; reconciler repairs failures).
	if w.cardState != nil {
		opStart = time.Now()
		_, err = w.cardState.WriteCardState(ctx, pipeline.CardStateChange{
			CampaignID:    event.CampaignID,
			CardID:        event.CardID,
			Status:        "in_progress",
			CurrentStep:   event.Step,
			RetryCount:    event.RetryCount,
			LastMsgID:     msgID.String(),
			Timestamp:     activateTS,
			TransitionSeq: activateState.TransitionSeq,
			StatsFrom:     "pending",
			StatsTo:       "in_progress",
		})
		if err != nil {
			return fmt.Errorf("write card state: %w", err)
		}
		w.telemetry.recordOp(ctx, "activate", "write_card_state", time.Since(opStart))
	}

	if w.wsHub != nil {
		w.wsHub.BroadcastToCampaign(event.CampaignID, &WSEvent{
			Type:       "card_status",
			CampaignID: event.CampaignID,
			CardID:     event.CardID,
			Data: map[string]interface{}{
				"status":       "in_progress",
				"current_step": event.Step,
				"msg_id":       msgID.String(),
				"msisdn":       cardKeys.MSISDN,
			},
		})
	}

	w.logger.Info("sent OTA command",
		zap.String("campaign_id", event.CampaignID),
		zap.String("card_id", event.CardID),
		zap.String("msisdn", cardKeys.MSISDN),
		zap.Int("step", event.Step),
		zap.Int("parts", len(parts)),
		zap.String("msg_id", msgID.String()),
	)
	return nil
}

// handleDLR processes a delivery report event.
func (w *CardWorker) handleDLR(ctx context.Context, event *pipeline.CardEvent) error {
	// 1. Load card state from Redis.
	opStart := time.Now()
	state, err := w.redis.GetCardState(ctx, event.CardID)
	w.telemetry.recordOp(ctx, "dlr", "redis_get_state", time.Since(opStart))
	if err != nil {
		return fmt.Errorf("get card state: %w", err)
	}
	if state == nil {
		// Card state may not exist yet if the activate worker is still
		// setting it in Redis.  Return an error so the consumer retries
		// with backoff, giving the activate path time to finish.
		return fmt.Errorf("DLR for card %s: no card state in Redis (activate may still be in progress)", event.CardID)
	}
	// Accept awaiting_dlr, in_progress, and awaiting_mo.  With separate
	// card-dlr / card-mo topics the MO consumer may have already advanced
	// the card to awaiting_mo or beyond; we still want to update the
	// message log with DLR metadata.
	switch state.Status {
	case "awaiting_dlr", "in_progress", "awaiting_mo":
		// OK — proceed.
	default:
		w.logger.Warn("stale DLR event: card state not awaiting_dlr",
			zap.String("card_id", event.CardID),
			zap.String("campaign_id", event.CampaignID),
			zap.String("status", state.Status),
		)
		return nil
	}

	// 2. Update MessageLog via log producer (DLR status + SMPP message ID).
	if event.MsgID != "" {
		opStart = time.Now()
		dlrUpdate := pipeline.MessageLogAction{
			Action: "update",
			ID:     event.MsgID,
			Updates: map[string]interface{}{
				"dlr_status":      event.DLRStatus,
				"smpp_message_id": event.SMPPMessageID,
			},
		}
		if _, logErr := w.logProducer.Publish(ctx, event.CardID, dlrUpdate); logErr != nil {
			w.logger.Warn("failed to publish message log (dlr update)", zap.Error(logErr))
		}
		w.telemetry.recordOp(ctx, "dlr", "kafka_log", time.Since(opStart))
	}

	// 3. Broadcast DLR update via WebSocket.
	if w.wsHub != nil {
		w.wsHub.BroadcastToCampaign(event.CampaignID, &WSEvent{
			Type:       "dlr_update",
			CampaignID: event.CampaignID,
			CardID:     event.CardID,
			Data: map[string]interface{}{
				"msg_id":     event.MsgID,
				"dlr_status": event.DLRStatus,
			},
		})
	}

	// If the MO consumer already advanced the card past awaiting_dlr, skip
	// the state transition — just the log/ws updates above are needed.
	if state.Status != "awaiting_dlr" && state.Status != "in_progress" {
		return nil
	}

	// 4. Handle based on DLR status.
	switch event.DLRStatus {
	case "DELIVRD":
		// Load campaign commands from cache.
		opStart = time.Now()
		campaignCtx, err := w.loadCampaignContext(ctx, event.CampaignID)
		w.telemetry.recordOp(ctx, "dlr", "load_campaign_ctx", time.Since(opStart))
		if err != nil {
			return fmt.Errorf("load campaign context: %w", err)
		}

		currentCmd := campaignCtx.commandByStep[state.CurrentStep]

		if currentCmd != nil && currentCmd.ExpectResponse {
			// Wait for MO — update card state to awaiting_mo.
			opStart = time.Now()
			state.Status = "awaiting_mo"
			nextSeq(state)
			if err := w.redis.SetCardState(ctx, event.CardID, state); err != nil {
				w.logger.Warn("failed to update card state to awaiting_mo", zap.Error(err))
			}
			w.telemetry.recordOp(ctx, "dlr", "redis_set_state", time.Since(opStart))
			w.logger.Debug("DLR delivered, awaiting MO response",
				zap.String("campaign_id", event.CampaignID),
				zap.String("card_id", event.CardID),
			)
		} else {
			// No response expected — advance to next step or complete.
			return w.advanceOrComplete(ctx, event, campaignCtx.commands, state)
		}

	case "FAILED_PARTIAL_SUBMIT":
		errDesc := fmt.Sprintf("submit failed after partial accept: %s", event.ErrorCode)

		seq := nextSeq(state)
		if err := w.redis.SetCardState(ctx, event.CardID, &redispkg.CardState{
			CampaignID:    event.CampaignID,
			CurrentStep:   state.CurrentStep,
			Status:        "failed",
			RetryCount:    state.RetryCount,
			LastMsgID:     state.LastMsgID,
			LastEventID:   event.EventID,
			TransitionSeq: seq,
		}); err != nil {
			w.logger.Warn("failed to set card state to failed after partial submit", zap.Error(err))
		}

		if w.cardState != nil {
			if _, csErr := w.cardState.WriteCardState(ctx, pipeline.CardStateChange{
				CampaignID:    event.CampaignID,
				CardID:        event.CardID,
				Status:        "failed",
				LastError:     errDesc,
				Timestamp:     time.Now(),
				TransitionSeq: seq,
				StatsFrom:     "pending",
				StatsTo:       "failed",
			}); csErr != nil {
				return fmt.Errorf("write card state: %w", csErr)
			}
		}

		if w.wsHub != nil {
			w.wsHub.BroadcastToCampaign(event.CampaignID, &WSEvent{
				Type:       "card_status",
				CampaignID: event.CampaignID,
				CardID:     event.CardID,
				Data: map[string]interface{}{
					"status":     "failed",
					"last_error": errDesc,
				},
			})
		}

		w.logger.Warn("card failed due to partial submit",
			zap.String("campaign_id", event.CampaignID),
			zap.String("card_id", event.CardID),
			zap.String("error_code", event.ErrorCode),
		)
		w.telemetry.recordCardProcessed(ctx, "failed_partial_submit")
		w.evictCardCaches(event.CardID, "")

		// Campaign completion is detected by the reconciler (1-minute poll).
	case "FAILED_SUBMIT", "FAILED", "UNDELIV", "EXPIRED", "DELETED", "REJECTD":
		// Get max retries from campaign.
		campaignCtx, err := w.loadCampaignContext(ctx, event.CampaignID)
		if err != nil {
			return fmt.Errorf("load campaign context: %w", err)
		}
		campaignParams := campaignCtx.params

		if state.RetryCount+1 < campaignParams.MaxRetries {
			// Retry: publish card.activate with same step, incremented retry count.
			retryEvent := pipeline.CardEvent{
				Type:       "card.activate",
				EventID:    uuid.New().String(),
				CardID:     event.CardID,
				CampaignID: event.CampaignID,
				Step:       state.CurrentStep,
				RetryCount: state.RetryCount + 1,
				Timestamp:  time.Now(),
			}
			w.telemetry.recordRetry(ctx, "dlr_failure")

			w.logger.Info("retrying card after DLR failure",
				zap.String("campaign_id", event.CampaignID),
				zap.String("card_id", event.CardID),
				zap.Int("retry_count", state.RetryCount+1),
				zap.String("dlr_status", event.DLRStatus),
			)

			if _, pubErr := w.eventProducer.Publish(ctx, event.CardID, retryEvent); pubErr != nil {
				return pubErr
			}
			return nil
		}

		// Max retries exhausted — mark card as failed.
		errDesc := fmt.Sprintf("DLR status: %s (error_code: %s)", event.DLRStatus, event.ErrorCode)

		seq := nextSeq(state)
		if err := w.redis.SetCardState(ctx, event.CardID, &redispkg.CardState{
			CampaignID:    event.CampaignID,
			CurrentStep:   state.CurrentStep,
			Status:        "failed",
			RetryCount:    state.RetryCount + 1,
			LastMsgID:     state.LastMsgID,
			LastEventID:   event.EventID,
			TransitionSeq: seq,
		}); err != nil {
			w.logger.Warn("failed to set card state to failed", zap.Error(err))
		}

		if w.cardState != nil {
			if _, csErr := w.cardState.WriteCardState(ctx, pipeline.CardStateChange{
				CampaignID:    event.CampaignID,
				CardID:        event.CardID,
				Status:        "failed",
				RetryCount:    state.RetryCount + 1,
				LastError:     errDesc,
				Timestamp:     time.Now(),
				TransitionSeq: seq,
				StatsFrom:     "in_progress",
				StatsTo:       "failed",
			}); csErr != nil {
				return fmt.Errorf("write card state: %w", csErr)
			}
		}

		if w.wsHub != nil {
			w.wsHub.BroadcastToCampaign(event.CampaignID, &WSEvent{
				Type:       "card_status",
				CampaignID: event.CampaignID,
				CardID:     event.CardID,
				Data: map[string]interface{}{
					"status":      "failed",
					"last_error":  errDesc,
					"retry_count": state.RetryCount + 1,
				},
			})
		}

		w.logger.Warn("card failed after max retries",
			zap.String("campaign_id", event.CampaignID),
			zap.String("card_id", event.CardID),
			zap.Int("retries", state.RetryCount+1),
		)
		w.telemetry.recordCardProcessed(ctx, "failed_dlr")
		w.evictCardCaches(event.CardID, "")

		// Check if campaign is now complete.
		// Campaign completion is detected by the reconciler (1-minute poll).
	}

	return nil
}

func (w *CardWorker) scheduleActivateRetry(ctx context.Context, event pipeline.CardEvent, delay time.Duration, reason string) {

	select {
	case w.retrySem <- struct{}{}:
	default:
		// Retry scheduler full. Redis is non-authoritative — only force-fail
		// if we can positively confirm the card is non-terminal for this campaign.
		// Otherwise, let the stale-card reconciler handle it.
		if w.retryOverflows != nil {
			w.retryOverflows.Add(ctx, 1)
		}
		current, _ := w.redis.GetCardState(ctx, event.CardID)
		if current == nil {
			w.logger.Error("retry overflow with no Redis state; skipping force-fail, deferring to reconciler",
				zap.String("card_id", event.CardID),
				zap.String("campaign_id", event.CampaignID),
				zap.String("reason", reason),
			)
			return
		}
		if current.CampaignID != event.CampaignID {
			w.logger.Warn("retry overflow for stale campaign context; skipping force-fail",
				zap.String("card_id", event.CardID),
				zap.String("event_campaign_id", event.CampaignID),
				zap.String("state_campaign_id", current.CampaignID),
			)
			return
		}
		if current.Status == "completed" || current.Status == "failed" || current.Status == "skipped" {
			w.logger.Warn("retry overflow: card already terminal, skipping force-fail",
				zap.String("card_id", event.CardID),
				zap.String("campaign_id", event.CampaignID),
				zap.String("status", current.Status),
			)
			return
		}

		// Confirmed: same campaign, non-terminal — safe to force-fail.
		w.logger.Error("retry goroutine limit reached, failing card",
			zap.String("card_id", event.CardID),
			zap.String("campaign_id", event.CampaignID),
			zap.String("reason", reason),
		)
		overflowSeq := nextSeq(current)
		if w.cardState != nil {
			_, _ = w.cardState.WriteCardState(ctx, pipeline.CardStateChange{
				CampaignID:    event.CampaignID,
				CardID:        event.CardID,
				Status:        "failed",
				LastError:     "retry_scheduler_overflow: " + reason,
				Timestamp:     time.Now(),
				TransitionSeq: overflowSeq,
				StatsFrom:     "in_progress",
				StatsTo:       "failed",
			})
		}
		_ = w.redis.SetCardState(ctx, event.CardID, &redispkg.CardState{
			CampaignID:    event.CampaignID,
			Status:        "failed",
			TransitionSeq: overflowSeq,
		})
		w.telemetry.recordCardProcessed(ctx, "failed")
		w.evictCardCaches(event.CardID, "")
		return
	}
	w.retryWg.Add(1)
	go func() {
		defer w.retryWg.Done()
		defer func() { <-w.retrySem }()
		timer := time.NewTimer(delay)
		defer timer.Stop()

		select {
		case <-ctx.Done():
			return
		case <-timer.C:
		}

		publishCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, err := w.eventProducer.Publish(publishCtx, event.CardID, event)
		if err != nil {
			w.logger.Warn("failed to publish delayed activate retry",
				zap.String("card_id", event.CardID),
				zap.String("campaign_id", event.CampaignID),
				zap.String("reason", reason),
				zap.Error(err),
			)
		}
	}()
}

// DrainRetries waits for all in-flight retry goroutines to complete.
func (w *CardWorker) DrainRetries() {
	w.retryWg.Wait()
}

// handleMO processes a mobile-originated response (PoR from SIM).
func (w *CardWorker) handleMO(ctx context.Context, event *pipeline.CardEvent) error {
	// 1. Load card state from Redis.
	opStart := time.Now()
	state, err := w.redis.GetCardState(ctx, event.CardID)
	w.telemetry.recordOp(ctx, "mo", "redis_get_state", time.Since(opStart))
	if err != nil {
		return fmt.Errorf("get card state: %w", err)
	}
	if state == nil {
		// Card state may not exist yet if the activate worker is still
		// setting it in Redis.  Return an error so the consumer retries
		// with backoff, giving the activate path time to finish.
		return fmt.Errorf("MO for card %s: no card state in Redis (activate may still be in progress)", event.CardID)
	}
	// Accept MO when card is awaiting_dlr — the MO arriving proves SMS was
	// delivered (SIM can only respond after receiving the OTA SMS).  With
	// separate card-dlr / card-mo topics the MO consumer can outpace the DLR
	// consumer, so we must not discard these events.
	if state.Status != "awaiting_mo" && state.Status != "awaiting_dlr" {
		w.logger.Warn("stale MO event: card state not awaiting_mo/awaiting_dlr",
			zap.String("card_id", event.CardID),
			zap.String("campaign_id", event.CampaignID),
			zap.String("status", state.Status),
		)
		return nil
	}
	if event.CampaignID == "" {
		event.CampaignID = state.CampaignID
	}

	// 2. Load card keys via keystore.
	opStart = time.Now()
	cardKeys, err := w.loadCardKeys(ctx, event.CardID)
	w.telemetry.recordOp(ctx, "mo", "load_card_keys", time.Since(opStart))
	if err != nil {
		return fmt.Errorf("get card keys: %w", err)
	}

	// 3. Parse the MO payload.
	payloadBytes, err := hexutil.Decode(event.PayloadHex)
	if err != nil {
		return fmt.Errorf("decode MO payload hex: %w", err)
	}

	// 4. Load campaign command cache to get security profile.
	opStart = time.Now()
	campaignCtx, err := w.loadCampaignContext(ctx, event.CampaignID)
	w.telemetry.recordOp(ctx, "mo", "load_campaign_ctx", time.Since(opStart))
	if err != nil {
		return fmt.Errorf("load campaign context: %w", err)
	}

	currentCmd := campaignCtx.commandByStep[state.CurrentStep]
	if currentCmd == nil {
		return fmt.Errorf("no command found at sequence %d for campaign %s", state.CurrentStep, event.CampaignID)
	}

	secProfile := buildSecProfileFromCache(currentCmd)

	// 5. Parse response packet.
	opStart = time.Now()
	resp, err := secProfile.ParseResponsePacket(payloadBytes, cardKeys.EncKey, cardKeys.AuthKey)
	w.telemetry.recordOp(ctx, "mo", "parse_response", time.Since(opStart))
	if err != nil {
		return fmt.Errorf("parse response packet: %w", err)
	}

	w.logger.Info("received MO response",
		zap.String("campaign_id", event.CampaignID),
		zap.String("card_id", event.CardID),
		zap.String("msisdn", event.SourceMSISDN),
		zap.Uint8("por_status", resp.StatusCode),
		zap.String("por_data", hex.EncodeToString(resp.Data)),
	)

	// 6. Update MessageLog via log producer.
	// Create MO entry.
	opStart = time.Now()
	moLogID := uuid.New()
	moNow := time.Now().UTC()
	moLogAction := pipeline.MessageLogAction{
		Action: "create",
		Log: &pipeline.MessageLogEntry{
			ID:         moLogID.String(),
			CampaignID: event.CampaignID,
			CardID:     event.CardID,
			Direction:  "MO",
			CreatedAt:  moNow,
			UpdatedAt:  moNow,
			RawPayload: payloadBytes,
			Status:     "received",
		},
	}
	if _, logErr := w.logProducer.Publish(ctx, event.CardID, moLogAction); logErr != nil {
		w.logger.Warn("failed to publish message log (MO create)", zap.Error(logErr))
	}
	w.telemetry.recordOp(ctx, "mo", "kafka_log_mo", time.Since(opStart))

	// Update MT entry with PoR data.
	if state.LastMsgID != "" {
		opStart = time.Now()
		porStatus := int16(resp.StatusCode)
		mtUpdate := pipeline.MessageLogAction{
			Action: "update",
			ID:     state.LastMsgID,
			Updates: map[string]interface{}{
				"por_status_code": porStatus,
				"por_data":        resp.Data,
				"status":          "processed",
			},
		}
		if _, logErr := w.logProducer.Publish(ctx, event.CardID, mtUpdate); logErr != nil {
			w.logger.Warn("failed to publish message log (MT update)", zap.Error(logErr))
		}
		w.telemetry.recordOp(ctx, "mo", "kafka_log_mt_update", time.Since(opStart))
	}

	// 7. Evaluate PoR status.
	if resp.StatusCode == gsm0348.RespOK {
		if w.wsHub != nil {
			w.wsHub.BroadcastToCampaign(event.CampaignID, &WSEvent{
				Type:       "card_status",
				CampaignID: event.CampaignID,
				CardID:     event.CardID,
				Data: map[string]interface{}{
					"por_status":   "ok",
					"current_step": state.CurrentStep,
					"por_data":     hex.EncodeToString(resp.Data),
				},
			})
		}

		// Advance to next step or mark completed.
		return w.advanceOrComplete(ctx, &pipeline.CardEvent{
			CardID:     event.CardID,
			CampaignID: event.CampaignID,
			Step:       state.CurrentStep,
			EventID:    event.EventID,
		}, campaignCtx.commands, state)
	}

	// PoR error — mark card as failed.
	errDesc := fmt.Sprintf("PoR error: status_code=0x%02X (%s)", resp.StatusCode, porStatusDescription(resp.StatusCode))

	seq := nextSeq(state)
	if err := w.redis.SetCardState(ctx, event.CardID, &redispkg.CardState{
		CampaignID:    event.CampaignID,
		CurrentStep:   state.CurrentStep,
		Status:        "failed",
		RetryCount:    state.RetryCount,
		LastMsgID:     state.LastMsgID,
		LastEventID:   event.EventID,
		TransitionSeq: seq,
	}); err != nil {
		w.logger.Warn("failed to set card state to failed", zap.Error(err))
	}

	if w.cardState != nil {
		_, csErr := w.cardState.WriteCardState(ctx, pipeline.CardStateChange{
			CampaignID:    event.CampaignID,
			CardID:        event.CardID,
			Status:        "failed",
			LastError:     errDesc,
			Timestamp:     time.Now(),
			TransitionSeq: seq,
			StatsFrom:     "in_progress",
			StatsTo:       "failed",
		})
		if csErr != nil {
			return fmt.Errorf("write card state: %w", csErr)
		}
	}

	if w.wsHub != nil {
		w.wsHub.BroadcastToCampaign(event.CampaignID, &WSEvent{
			Type:       "card_status",
			CampaignID: event.CampaignID,
			CardID:     event.CardID,
			Data: map[string]interface{}{
				"status":     "failed",
				"por_status": fmt.Sprintf("0x%02X", resp.StatusCode),
				"last_error": errDesc,
			},
		})
	}

	w.logger.Warn("card failed due to PoR error",
		zap.String("campaign_id", event.CampaignID),
		zap.String("card_id", event.CardID),
		zap.Uint8("status_code", resp.StatusCode),
	)
	w.telemetry.recordCardProcessed(ctx, "failed_por")
	w.evictCardCaches(event.CardID, "")

	// Campaign completion is detected by the reconciler (1-minute poll).
	return nil
}

// advanceOrComplete publishes the next step activation or marks the card as completed.
func (w *CardWorker) advanceOrComplete(ctx context.Context, event *pipeline.CardEvent, commands []redispkg.CampaignCommandCache, state ...*redispkg.CardState) error {
	// Find next command after current step.
	nextStep := -1
	for _, cmd := range commands {
		if cmd.Sequence > event.Step {
			if nextStep == -1 || cmd.Sequence < nextStep {
				nextStep = cmd.Sequence
			}
		}
	}

	if nextStep == -1 {
		// No more commands — card completed.
		completedAt := time.Now()
		var seq int64
		if len(state) > 0 && state[0] != nil {
			seq = nextSeq(state[0])
		}
		w.redis.SetCardState(ctx, event.CardID, &redispkg.CardState{
			CampaignID:    event.CampaignID,
			Status:        "completed",
			TransitionSeq: seq,
		})
		if w.cardState != nil {
			_, err := w.cardState.WriteCardState(ctx, pipeline.CardStateChange{
				CampaignID:    event.CampaignID,
				CardID:        event.CardID,
				Status:        "completed",
				Timestamp:     completedAt,
				TransitionSeq: seq,
				StatsFrom:     "in_progress",
				StatsTo:       "completed",
			})
			if err != nil {
				return fmt.Errorf("write card state: %w", err)
			}
		}

		// WebSocket broadcast.
		if w.wsHub != nil {
			w.wsHub.BroadcastToCampaign(event.CampaignID, &WSEvent{
				Type:       "card_status",
				CampaignID: event.CampaignID,
				CardID:     event.CardID,
				Data:       map[string]interface{}{"status": "completed"},
			})
		}

		w.logger.Info("card completed all steps",
			zap.String("campaign_id", event.CampaignID),
			zap.String("card_id", event.CardID),
		)
		w.telemetry.recordCardProcessed(ctx, "completed")
		w.evictCardCaches(event.CardID, "")

		// Check if campaign is complete.
		// Campaign completion is detected by the reconciler (1-minute poll).
		return nil
	}

	// Publish next step activation.
	nextEvent := pipeline.CardEvent{
		Type:       "card.activate",
		EventID:    uuid.New().String(),
		CardID:     event.CardID,
		CampaignID: event.CampaignID,
		Step:       nextStep,
		Timestamp:  time.Now(),
	}
	_, err := w.eventProducer.Publish(ctx, event.CardID, nextEvent)
	return err
}

// loadProfileCached loads a card profile using cache-aside: try Redis first, fall back to DB.
func (w *CardWorker) loadProfileCached(ctx context.Context, profileID string) (*db.Profile, error) {
	var profile db.Profile
	err := w.redis.GetCachedProfile(ctx, profileID, &profile)
	if err != nil && !errors.Is(err, goredis.Nil) {
		w.logger.Warn("redis profile cache error", zap.Error(err))
	}
	if err == nil {
		return &profile, nil
	}
	// Cache miss or error — load from DB.
	if err := w.db.WithContext(ctx).First(&profile, "id = ?", profileID).Error; err != nil {
		return nil, fmt.Errorf("load profile %s: %w", profileID, err)
	}
	_ = w.redis.CacheProfile(ctx, profileID, &profile)
	return &profile, nil
}

// loadCampaignParamsCached loads campaign params using cache-aside: try Redis first, fall back to DB.
func (w *CardWorker) loadCampaignParamsCached(ctx context.Context, campaignID string) (*redispkg.CampaignParams, error) {
	params, err := w.redis.GetCachedCampaignParams(ctx, campaignID)
	if err != nil {
		w.logger.Warn("redis campaign params cache error", zap.Error(err))
	}
	if params != nil {
		return params, nil
	}
	// Cache miss — load from DB.
	var campaign db.Campaign
	if err := w.db.WithContext(ctx).Select("max_concat_override", "throttle_sms_per_sec", "max_retries").
		First(&campaign, "id = ?", campaignID).Error; err != nil {
		return nil, fmt.Errorf("load campaign params from db: %w", err)
	}
	params = &redispkg.CampaignParams{
		MaxConcatOverride: campaign.MaxConcatOverride,
		ThrottleSMSPerSec: campaign.ThrottleSMSPerSec,
		MaxRetries:        campaign.MaxRetries,
	}
	_ = w.redis.CacheCampaignParams(ctx, campaignID, params)
	return params, nil
}

// loadCampaignCommands loads campaign commands from Redis cache, falling back to DB.
func (w *CardWorker) loadCampaignCommands(ctx context.Context, campaignID string) ([]redispkg.CampaignCommandCache, error) {
	commands, err := w.redis.GetCampaignCommands(ctx, campaignID)
	if err != nil {
		w.logger.Warn("redis campaign commands cache error", zap.String("campaign_id", campaignID), zap.Error(err))
	}
	if commands != nil {
		return commands, nil
	}

	// Cache miss — load from DB.
	var dbCmds []db.CampaignCommand
	if err := w.db.WithContext(ctx).
		Preload("Application").
		Where("campaign_id = ?", campaignID).
		Order("sequence ASC").
		Find(&dbCmds).Error; err != nil {
		return nil, fmt.Errorf("load campaign commands from db: %w", err)
	}

	commands = make([]redispkg.CampaignCommandCache, len(dbCmds))
	for i, dc := range dbCmds {
		commands[i] = redispkg.CampaignCommandCache{
			Sequence:       dc.Sequence,
			ApplicationID:  dc.ApplicationID.String(),
			Script:         dc.Script,
			ExpectResponse: dc.ExpectResponse,
			TAR:            dc.Application.TAR,
			KIcAlgo:        dc.Application.KIcAlgo,
			KIcMode:        dc.Application.KIcMode,
			KIcKeysetID:    int(dc.Application.KIcKeysetID),
			KIdAlgo:        dc.Application.KIdAlgo,
			KIdMode:        dc.Application.KIdMode,
			KIdKeysetID:    int(dc.Application.KIdKeysetID),
			CertMode:       dc.Application.CertificationMode,
			Ciphered:       dc.Application.Ciphered,
			CounterMode:    dc.Application.CounterMode,
			PORMode:        dc.Application.PORMode,
			PORProtocol:    dc.Application.PORProtocol,
			PORCiphered:    dc.Application.PORCiphered,
			PORCertMode:    dc.Application.PORCertMode,
		}
	}

	// Cache in Redis for future use.
	_ = w.redis.CacheCampaignCommands(ctx, campaignID, commands)

	return commands, nil
}

func (w *CardWorker) loadCardKeys(ctx context.Context, cardID string) (*keystore.CardKeyMaterial, error) {
	if cached, ok := w.cardKeyCache.Load(cardID); ok {
		return cached, nil
	}
	keys, err := w.keyStore.GetKeys(ctx, cardID)
	if err != nil {
		return nil, err
	}
	w.cardKeyCache.Store(cardID, keys)
	return keys, nil
}

// PreloadCardKeys populates the process-local card key cache from a
// batch-loaded map. Called by the planner's BatchPreloader before shard
// events are published, eliminating per-card Postgres reads during execution.
func (w *CardWorker) PreloadCardKeys(keys map[string]*keystore.CardKeyMaterial) {
	for cardID, km := range keys {
		w.cardKeyCache.Store(cardID, km)
	}
}

// evictCardCaches removes a card from all process-local caches.
// Called when a card reaches a terminal state (completed/failed/skipped).
// This bounds memory growth during large campaigns — without eviction,
// caches grow to N (total cards) instead of staying at ~25k (in-flight).
func (w *CardWorker) evictCardCaches(cardID, _ string) {
	w.cardKeyCache.Delete(cardID)
	w.counterStore.EvictCard(cardID)
	w.redis.EvictCardState(cardID)
}

func (w *CardWorker) loadCampaignContext(ctx context.Context, campaignID string) (*campaignExecutionContext, error) {
	// Fast path: check process-local cache (no allocations, no network).
	if cached, ok := w.campaignCache.Load(campaignID); ok {
		cec := cached.(*campaignExecutionContext)
		if time.Since(cec.cachedAt) < campaignCacheTTL {
			if w.cacheHits != nil {
				w.cacheHits.Add(ctx, 1)
			}
			return cec, nil
		}
		// TTL expired — evict and reload.
		w.campaignCache.Delete(campaignID)
	}

	// Cache miss — use singleflight to coalesce concurrent misses for the
	// same campaign (e.g. when many cards activate simultaneously).
	if w.cacheMisses != nil {
		w.cacheMisses.Add(ctx, 1)
	}
	result, err, _ := w.campaignSF.Do(campaignID, func() (interface{}, error) {
		// Double-check: another goroutine in the same singleflight group
		// may have already populated the cache.
		if cached, ok := w.campaignCache.Load(campaignID); ok {
			cec := cached.(*campaignExecutionContext)
			if time.Since(cec.cachedAt) < campaignCacheTTL {
				return cec, nil
			}
			w.campaignCache.Delete(campaignID)
		}

		commands, err := w.loadCampaignCommands(ctx, campaignID)
		if err != nil {
			return nil, err
		}
		params, err := w.loadCampaignParamsCached(ctx, campaignID)
		if err != nil {
			return nil, err
		}

		commandByStep := make(map[int]*redispkg.CampaignCommandCache, len(commands))
		for i := range commands {
			commandByStep[commands[i].Sequence] = &commands[i]
		}

		cec := &campaignExecutionContext{
			commands:      commands,
			commandByStep: commandByStep,
			params:        params,
			cachedAt:      time.Now(),
		}
		w.campaignCache.Store(campaignID, cec)
		return cec, nil
	})
	if err != nil {
		return nil, err
	}
	return result.(*campaignExecutionContext), nil
}

// InvalidateCampaignCache removes a campaign's cached context and status.
func (w *CardWorker) InvalidateCampaignCache(campaignID string) {
	w.campaignCache.Delete(campaignID)
	w.campaignStatusCache.Delete(campaignID)
}

// getCampaignStatusCached returns campaign status using a process-local cache
// with a 30s TTL, falling back to Redis then DB. Eliminates ~200 Redis
// roundtrips/sec for running campaigns.
func (w *CardWorker) getCampaignStatusCached(ctx context.Context, campaignID string) (string, error) {
	if cached, ok := w.campaignStatusCache.Load(campaignID); ok {
		cs := cached.(*cachedStatus)
		if time.Since(cs.cachedAt) < campaignStatusCacheTTL {
			return cs.status, nil
		}
		w.campaignStatusCache.Delete(campaignID)
	}

	status, err := w.redis.GetCampaignStatus(ctx, campaignID)
	if err != nil {
		return "", err
	}
	if status == "" {
		var campaign db.Campaign
		if err := w.db.WithContext(ctx).Select("status").First(&campaign, "id = ?", campaignID).Error; err != nil {
			return "", fmt.Errorf("load campaign status from db: %w", err)
		}
		status = campaign.Status
		_ = w.redis.SetCampaignStatus(ctx, campaignID, status)
	}
	w.campaignStatusCache.Store(campaignID, &cachedStatus{status: status, cachedAt: time.Now()})
	return status, nil
}

// StartCacheSweeper runs a background goroutine that periodically evicts
// expired campaign context entries. Call once at startup; stops when ctx
// is cancelled.
func (w *CardWorker) StartCacheSweeper(ctx context.Context) {
	go func() {
		ticker := time.NewTicker(5 * time.Minute)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				now := time.Now()
				w.campaignCache.Range(func(key, value any) bool {
					cec := value.(*campaignExecutionContext)
					if now.Sub(cec.cachedAt) >= campaignCacheTTL {
						w.campaignCache.Delete(key)
					}
					return true
				})
				w.campaignStatusCache.Range(func(key, value any) bool {
					cs := value.(*cachedStatus)
					if now.Sub(cs.cachedAt) >= campaignStatusCacheTTL {
						w.campaignStatusCache.Delete(key)
					}
					return true
				})
			}
		}
	}()
}

// buildSecProfileFromCache converts a CampaignCommandCache to a gsm0348.SecurityProfile.
// It reuses the existing parse* functions from campaign.go (same package).
func buildSecProfileFromCache(cmd *redispkg.CampaignCommandCache) *gsm0348.SecurityProfile {
	return &gsm0348.SecurityProfile{
		CertMode:    parseCertificationMode(cmd.CertMode),
		Ciphered:    cmd.Ciphered,
		CounterMode: parseCounterMode(cmd.CounterMode),
		PoRMode:     parsePoRMode(cmd.PORMode),
		PoRCertMode: parseCertificationMode(cmd.PORCertMode),
		PoRCiphered: cmd.PORCiphered,
		PoRProtocol: parsePoRProtocol(cmd.PORProtocol),

		KIcAlgo:     parseAlgo(cmd.KIcAlgo),
		KIcMode:     parseCipherMode(cmd.KIcMode),
		KIcKeysetID: byte(cmd.KIcKeysetID),

		KIDAlgo:     parseAlgo(cmd.KIdAlgo),
		KIDMode:     parseCipherMode(cmd.KIdMode),
		KIDKeysetID: byte(cmd.KIdKeysetID),

		SecurityBytesWithLengthsAndUDHL: true,
	}
}
