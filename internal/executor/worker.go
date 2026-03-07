package executor

import (
	"context"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	goredis "github.com/redis/go-redis/v9"
	"go.uber.org/zap"
	"gorm.io/gorm"

	"ota-platform/internal/db"
	kafkapkg "ota-platform/internal/kafka"
	"ota-platform/internal/keystore"
	redispkg "ota-platform/internal/redis"
	"ota-platform/pkg/gsm0348"
	"ota-platform/pkg/hexutil"
	"ota-platform/pkg/smsframe"
)

// CardWorker processes card events from Kafka using the coordination store for
// hot state and PostgreSQL for durable state.
type CardWorker struct {
	db             *gorm.DB
	execution      ExecutionStore
	redis          CoordinationStore
	keyStore       keystore.KeyStore
	cryptoProvider keystore.CryptoProvider // nil when using software crypto
	smsProducer    Producer                // publishes to send-sms topic
	logProducer    Producer                // publishes to message-log topic
	eventProducer  Producer                // publishes to card-events topic (for self-triggering next steps)
	wsHub          WSHub
	logger         *zap.Logger
	telemetry      *executorTelemetry
}

// NewCardWorker creates a new CardWorker.
func NewCardWorker(database *gorm.DB, executionStore ExecutionStore, rdb CoordinationStore, ks keystore.KeyStore, cp keystore.CryptoProvider, smsProducer, logProducer, eventProducer Producer, wsHub WSHub, logger *zap.Logger) *CardWorker {
	return &CardWorker{
		db:             database,
		execution:      executionStore,
		redis:          rdb,
		keyStore:       ks,
		cryptoProvider: cp,
		smsProducer:    smsProducer,
		logProducer:    logProducer,
		eventProducer:  eventProducer,
		wsHub:          wsHub,
		logger:         logger,
		telemetry:      newExecutorTelemetry(logger),
	}
}

// HandleEvent is the Kafka message handler for the card-events topic.
func (w *CardWorker) HandleEvent(ctx context.Context, key []byte, value []byte) error {
	var event kafkapkg.CardEvent
	if err := json.Unmarshal(value, &event); err != nil {
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

// handleActivate builds and sends the OTA command for the given card step.
func (w *CardWorker) handleActivate(ctx context.Context, event *kafkapkg.CardEvent) error {
	// 1. Check campaign status from the coordination store, then load from DB on miss.
	status, err := w.redis.GetCampaignStatus(ctx, event.CampaignID)
	if err != nil {
		return fmt.Errorf("get campaign status: %w", err)
	}
	if status == "" {
		// Cache miss — load from DB.
		var campaign db.Campaign
		if err := w.db.WithContext(ctx).Select("status").First(&campaign, "id = ?", event.CampaignID).Error; err != nil {
			return fmt.Errorf("load campaign status from db: %w", err)
		}
		status = campaign.Status
		_ = w.redis.SetCampaignStatus(ctx, event.CampaignID, status)
	}
	if status != "running" {
		w.logger.Debug("skipping activate for non-running campaign",
			zap.String("campaign_id", event.CampaignID),
			zap.String("status", status),
		)
		return nil
	}

	// 2. Load card keys via keystore.
	cardKeys, err := w.keyStore.GetKeys(ctx, event.CardID)
	if err != nil {
		return fmt.Errorf("get card keys: %w", err)
	}

	// 3. Load campaign commands from Redis (or DB on miss).
	commands, err := w.loadCampaignCommands(ctx, event.CampaignID)
	if err != nil {
		return fmt.Errorf("load campaign commands: %w", err)
	}

	// 4. Find the command matching event.Step.
	var cmd *redispkg.CampaignCommandCache
	for i := range commands {
		if commands[i].Sequence == event.Step {
			cmd = &commands[i]
			break
		}
	}
	if cmd == nil {
		return fmt.Errorf("no command found at sequence %d for campaign %s", event.Step, event.CampaignID)
	}

	// 5. Build security profile from the cached command.
	secProfile := buildSecProfileFromCache(cmd)

	// 6. Get/increment counter from Redis.
	counterVal, err := w.redis.IncrCounter(ctx, event.CardID, cmd.ApplicationID)
	if err != nil {
		return fmt.Errorf("increment counter: %w", err)
	}
	// IncrCounter returns the value after increment; use value-1 as the current counter.
	counterVal--

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

	packet, err := secProfile.BuildCommandPacket(input)
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

	campaignParams, err := w.loadCampaignParamsCached(ctx, event.CampaignID)
	if err != nil {
		return fmt.Errorf("load campaign params: %w", err)
	}
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
			timer := time.NewTimer(backoff)
			select {
			case <-ctx.Done():
				timer.Stop()
				return ctx.Err()
			case <-timer.C:
			}

			// Re-publish the same event after a bounded backoff.
			retryEvent := kafkapkg.CardEvent{
				Type:       "card.activate",
				EventID:    uuid.New().String(),
				CardID:     event.CardID,
				CampaignID: event.CampaignID,
				Step:       event.Step,
				RetryCount: event.RetryCount,
				Timestamp:  time.Now(),
			}
			w.telemetry.recordRetry(ctx, "throttle")
			return w.eventProducer.Publish(ctx, event.CardID, retryEvent)
		}
	}

	// 12. Create MessageLog entry by publishing to message-log topic (async).
	counterHex := hex.EncodeToString(counter[:])
	msgID := uuid.New()
	createdAt := time.Now().UTC()

	logAction := kafkapkg.MessageLogAction{
		Action: "create",
		Log: &kafkapkg.MessageLogEntry{
			ID:             msgID.String(),
			CampaignID:     event.CampaignID,
			CardID:         event.CardID,
			Direction:      "MT",
			CreatedAt:      createdAt.Format(time.RFC3339Nano),
			UpdatedAt:      createdAt.Format(time.RFC3339Nano),
			RawPayload:     base64.StdEncoding.EncodeToString(cmd.Script),
			SecuredPayload: base64.StdEncoding.EncodeToString(packet),
			CounterHex:     counterHex,
			Status:         "sending",
		},
	}
	if err := w.logProducer.Publish(ctx, event.CardID, logAction); err != nil {
		w.logger.Warn("failed to publish message log create", zap.Error(err))
	}

	// 13. Build SendSMSMessage and publish to send-sms topic.
	smsParts := make([]kafkapkg.SMSPart, len(parts))
	for i, p := range parts {
		smsParts[i] = kafkapkg.SMSPart{
			Sequence: i + 1,
			Total:    len(parts),
			RefNum:   0,
			Payload:  hex.EncodeToString(p),
		}
	}

	smsMsg := kafkapkg.SendSMSMessage{
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

	if err := w.smsProducer.Publish(ctx, event.CardID, smsMsg); err != nil {
		// Update message log status to failed via log producer.
		failUpdate := kafkapkg.MessageLogAction{
			Action:  "update",
			ID:      msgID.String(),
			Updates: map[string]interface{}{"status": "send_failed"},
		}
		_ = w.logProducer.Publish(ctx, event.CardID, failUpdate)
		return fmt.Errorf("publish SMS to kafka: %w", err)
	}

	// 14. Update card state in Redis.
	if err := w.redis.SetCardState(ctx, event.CardID, &redispkg.CardState{
		CampaignID:  event.CampaignID,
		CurrentStep: event.Step,
		Status:      "awaiting_dlr",
		RetryCount:  event.RetryCount,
		LastMsgID:   msgID.String(),
		LastEventID: event.EventID,
	}); err != nil {
		w.logger.Warn("failed to set card state in redis", zap.Error(err))
	}

	// 15. Update campaign progress: pending -> in_progress.
	progress, _ := w.redis.UpdateProgress(ctx, event.CampaignID, "pending", "in_progress")

	// Update DB campaign card record.
	if w.execution != nil {
		if err := w.execution.UpdateCampaignCard(ctx, event.CampaignID, event.CardID, map[string]interface{}{
			"status":       "in_progress",
			"current_step": event.Step,
			"last_msg_id":  msgID,
			"retry_count":  event.RetryCount,
			"updated_at":   time.Now(),
		}); err != nil {
			w.logger.Warn("failed to persist in-progress campaign card state", zap.Error(err))
		}
	}

	// Update message log status to sent.
	sentUpdate := kafkapkg.MessageLogAction{
		Action:  "update",
		ID:      msgID.String(),
		Updates: map[string]interface{}{"status": "sent"},
	}
	_ = w.logProducer.Publish(ctx, event.CardID, sentUpdate)

	// 16. Broadcast WebSocket event.
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

	_ = progress // used above for UpdateProgress call
	return nil
}

// handleDLR processes a delivery report event.
func (w *CardWorker) handleDLR(ctx context.Context, event *kafkapkg.CardEvent) error {
	// 1. Load card state from Redis.
	state, err := w.redis.GetCardState(ctx, event.CardID)
	if err != nil {
		return fmt.Errorf("get card state: %w", err)
	}
	if state == nil || (state.Status != "awaiting_dlr" && state.Status != "in_progress") {
		w.logger.Warn("stale DLR event: card state not awaiting_dlr",
			zap.String("card_id", event.CardID),
			zap.String("campaign_id", event.CampaignID),
		)
		return nil
	}

	// 2. Update MessageLog via log producer (DLR status + SMPP message ID).
	if event.MsgID != "" {
		dlrUpdate := kafkapkg.MessageLogAction{
			Action: "update",
			ID:     event.MsgID,
			Updates: map[string]interface{}{
				"dlr_status":      event.DLRStatus,
				"smpp_message_id": event.SMPPMessageID,
			},
		}
		_ = w.logProducer.Publish(ctx, event.CardID, dlrUpdate)
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

	// 4. Handle based on DLR status.
	switch event.DLRStatus {
	case "DELIVRD":
		// Load campaign commands from cache.
		commands, err := w.loadCampaignCommands(ctx, event.CampaignID)
		if err != nil {
			return fmt.Errorf("load campaign commands: %w", err)
		}

		// Find current command by step.
		var currentCmd *redispkg.CampaignCommandCache
		for i := range commands {
			if commands[i].Sequence == state.CurrentStep {
				currentCmd = &commands[i]
				break
			}
		}

		if currentCmd != nil && currentCmd.ExpectResponse {
			// Wait for MO — update card state to awaiting_mo.
			state.Status = "awaiting_mo"
			if err := w.redis.SetCardState(ctx, event.CardID, state); err != nil {
				w.logger.Warn("failed to update card state to awaiting_mo", zap.Error(err))
			}
			w.logger.Debug("DLR delivered, awaiting MO response",
				zap.String("campaign_id", event.CampaignID),
				zap.String("card_id", event.CardID),
			)
		} else {
			// No response expected — advance to next step or complete.
			return w.advanceOrComplete(ctx, event, commands)
		}

	case "FAILED_PARTIAL_SUBMIT":
		errDesc := fmt.Sprintf("submit failed after partial accept: %s", event.ErrorCode)

		if err := w.redis.SetCardState(ctx, event.CardID, &redispkg.CardState{
			CampaignID:  event.CampaignID,
			CurrentStep: state.CurrentStep,
			Status:      "failed",
			RetryCount:  state.RetryCount,
			LastMsgID:   state.LastMsgID,
			LastEventID: event.EventID,
		}); err != nil {
			w.logger.Warn("failed to set card state to failed after partial submit", zap.Error(err))
		}

		w.redis.UpdateProgress(ctx, event.CampaignID, "in_progress", "failed")
		if w.execution != nil {
			if err := w.execution.UpdateCampaignCard(ctx, event.CampaignID, event.CardID, map[string]interface{}{
				"status":     "failed",
				"last_error": errDesc,
				"updated_at": time.Now(),
			}); err != nil {
				w.logger.Warn("failed to persist partial submit failure", zap.Error(err))
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

		w.checkCampaignComplete(ctx, event.CampaignID)
	case "FAILED_SUBMIT", "FAILED", "UNDELIV", "EXPIRED", "DELETED", "REJECTD":
		// Get max retries from campaign.
		campaignParams, err := w.loadCampaignParamsCached(ctx, event.CampaignID)
		if err != nil {
			return fmt.Errorf("load campaign params: %w", err)
		}

		if state.RetryCount+1 < campaignParams.MaxRetries {
			// Retry: publish card.activate with same step, incremented retry count.
			retryEvent := kafkapkg.CardEvent{
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

			return w.eventProducer.Publish(ctx, event.CardID, retryEvent)
		}

		// Max retries exhausted — mark card as failed.
		errDesc := fmt.Sprintf("DLR status: %s (error_code: %s)", event.DLRStatus, event.ErrorCode)

		if err := w.redis.SetCardState(ctx, event.CardID, &redispkg.CardState{
			CampaignID:  event.CampaignID,
			CurrentStep: state.CurrentStep,
			Status:      "failed",
			RetryCount:  state.RetryCount + 1,
			LastMsgID:   state.LastMsgID,
			LastEventID: event.EventID,
		}); err != nil {
			w.logger.Warn("failed to set card state to failed", zap.Error(err))
		}

		w.redis.UpdateProgress(ctx, event.CampaignID, "in_progress", "failed")

		if w.execution != nil {
			if err := w.execution.UpdateCampaignCard(ctx, event.CampaignID, event.CardID, map[string]interface{}{
				"status":      "failed",
				"retry_count": state.RetryCount + 1,
				"last_error":  errDesc,
				"updated_at":  time.Now(),
			}); err != nil {
				w.logger.Warn("failed to persist failed campaign card state", zap.Error(err))
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

		// Check if campaign is now complete.
		w.checkCampaignComplete(ctx, event.CampaignID)
	}

	return nil
}

// handleMO processes a mobile-originated response (PoR from SIM).
func (w *CardWorker) handleMO(ctx context.Context, event *kafkapkg.CardEvent) error {
	// 1. Load card state from Redis.
	state, err := w.redis.GetCardState(ctx, event.CardID)
	if err != nil {
		return fmt.Errorf("get card state: %w", err)
	}
	if state == nil || state.Status != "awaiting_mo" {
		w.logger.Warn("stale MO event: card state not awaiting_mo",
			zap.String("card_id", event.CardID),
			zap.String("campaign_id", event.CampaignID),
		)
		return nil
	}
	if event.CampaignID == "" {
		event.CampaignID = state.CampaignID
	}

	// 2. Load card keys via keystore.
	cardKeys, err := w.keyStore.GetKeys(ctx, event.CardID)
	if err != nil {
		return fmt.Errorf("get card keys: %w", err)
	}

	// 3. Parse the MO payload.
	payloadBytes, err := hexutil.Decode(event.PayloadHex)
	if err != nil {
		return fmt.Errorf("decode MO payload hex: %w", err)
	}

	// 4. Load campaign command cache to get security profile.
	commands, err := w.loadCampaignCommands(ctx, event.CampaignID)
	if err != nil {
		return fmt.Errorf("load campaign commands: %w", err)
	}

	var currentCmd *redispkg.CampaignCommandCache
	for i := range commands {
		if commands[i].Sequence == state.CurrentStep {
			currentCmd = &commands[i]
			break
		}
	}
	if currentCmd == nil {
		return fmt.Errorf("no command found at sequence %d for campaign %s", state.CurrentStep, event.CampaignID)
	}

	secProfile := buildSecProfileFromCache(currentCmd)

	// 5. Parse response packet.
	resp, err := secProfile.ParseResponsePacket(payloadBytes, cardKeys.EncKey, cardKeys.AuthKey)
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
	moLogID := uuid.New()
	moLogAction := kafkapkg.MessageLogAction{
		Action: "create",
		Log: &kafkapkg.MessageLogEntry{
			ID:         moLogID.String(),
			CampaignID: event.CampaignID,
			CardID:     event.CardID,
			Direction:  "MO",
			CreatedAt:  time.Now().UTC().Format(time.RFC3339Nano),
			UpdatedAt:  time.Now().UTC().Format(time.RFC3339Nano),
			RawPayload: base64.StdEncoding.EncodeToString(payloadBytes),
			Status:     "received",
		},
	}
	_ = w.logProducer.Publish(ctx, event.CardID, moLogAction)

	// Update MT entry with PoR data.
	if state.LastMsgID != "" {
		porStatus := int16(resp.StatusCode)
		mtUpdate := kafkapkg.MessageLogAction{
			Action: "update",
			ID:     state.LastMsgID,
			Updates: map[string]interface{}{
				"por_status_code": porStatus,
				"por_data":        resp.Data,
				"status":          "processed",
			},
		}
		_ = w.logProducer.Publish(ctx, event.CardID, mtUpdate)
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
		return w.advanceOrComplete(ctx, &kafkapkg.CardEvent{
			CardID:     event.CardID,
			CampaignID: event.CampaignID,
			Step:       state.CurrentStep,
			EventID:    event.EventID,
		}, commands)
	}

	// PoR error — mark card as failed.
	errDesc := fmt.Sprintf("PoR error: status_code=0x%02X (%s)", resp.StatusCode, porStatusDescription(resp.StatusCode))

	if err := w.redis.SetCardState(ctx, event.CardID, &redispkg.CardState{
		CampaignID:  event.CampaignID,
		CurrentStep: state.CurrentStep,
		Status:      "failed",
		RetryCount:  state.RetryCount,
		LastMsgID:   state.LastMsgID,
		LastEventID: event.EventID,
	}); err != nil {
		w.logger.Warn("failed to set card state to failed", zap.Error(err))
	}

	w.redis.UpdateProgress(ctx, event.CampaignID, "in_progress", "failed")

	if w.execution != nil {
		if err := w.execution.UpdateCampaignCard(ctx, event.CampaignID, event.CardID, map[string]interface{}{
			"status":     "failed",
			"last_error": errDesc,
			"updated_at": time.Now(),
		}); err != nil {
			w.logger.Warn("failed to persist PoR failure campaign card state", zap.Error(err))
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

	w.checkCampaignComplete(ctx, event.CampaignID)
	return nil
}

// advanceOrComplete publishes the next step activation or marks the card as completed.
func (w *CardWorker) advanceOrComplete(ctx context.Context, event *kafkapkg.CardEvent, commands []redispkg.CampaignCommandCache) error {
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
		w.redis.SetCardState(ctx, event.CardID, &redispkg.CardState{
			CampaignID: event.CampaignID,
			Status:     "completed",
		})
		w.redis.UpdateProgress(ctx, event.CampaignID, "in_progress", "completed")

		// Update DB.
		if w.execution != nil {
			if err := w.execution.UpdateCampaignCard(ctx, event.CampaignID, event.CardID, map[string]interface{}{"status": "completed", "updated_at": time.Now()}); err != nil {
				w.logger.Warn("failed to persist completed campaign card state", zap.Error(err))
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

		// Check if campaign is complete.
		w.checkCampaignComplete(ctx, event.CampaignID)
		return nil
	}

	// Publish next step activation.
	nextEvent := kafkapkg.CardEvent{
		Type:       "card.activate",
		EventID:    uuid.New().String(),
		CardID:     event.CardID,
		CampaignID: event.CampaignID,
		Step:       nextStep,
		Timestamp:  time.Now(),
	}
	return w.eventProducer.Publish(ctx, event.CardID, nextEvent)
}

// checkCampaignComplete checks if all cards are done and updates campaign status.
func (w *CardWorker) checkCampaignComplete(ctx context.Context, campaignID string) {
	progress, err := w.redis.GetProgress(ctx, campaignID)
	if err != nil || progress == nil {
		return
	}

	if progress.Pending > 0 || progress.InProgress > 0 {
		return
	}

	w.completeCampaign(ctx, campaignID, progress)
}

// completeCampaign determines the final status and persists campaign completion.
func (w *CardWorker) completeCampaign(ctx context.Context, campaignID string, progress *redispkg.CampaignProgress) {
	// Use a short-lived derived context so completion writes finish
	// even if parent is near cancellation, but don't hang forever.
	completeCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var finalStatus string
	if progress.Failed > 0 && progress.Completed == 0 {
		finalStatus = "failed"
	} else if progress.Failed > 0 {
		finalStatus = "completed_with_errors"
	} else {
		finalStatus = "completed"
	}

	now := time.Now()
	completed, err := w.execution.CompleteCampaignIfRunning(completeCtx, campaignID, finalStatus, now)
	if err != nil {
		w.logger.Warn("failed to update campaign completion",
			zap.String("campaign_id", campaignID),
			zap.Error(err),
		)
		return
	}
	if !completed {
		return
	}

	if err := w.redis.SetCampaignStatus(completeCtx, campaignID, finalStatus); err != nil {
		w.logger.Warn("failed to set campaign status in coordination store",
			zap.String("campaign_id", campaignID),
			zap.Error(err),
		)
	}

	if w.wsHub != nil {
		w.wsHub.Broadcast(&WSEvent{
			Type:       "campaign_progress",
			CampaignID: campaignID,
			Data: map[string]interface{}{
				"status":       finalStatus,
				"completed_at": now,
				"completed":    progress.Completed,
				"failed":       progress.Failed,
			},
		})
	}

	w.logger.Info("campaign completed",
		zap.String("campaign_id", campaignID),
		zap.String("final_status", finalStatus),
		zap.Int64("completed_cards", progress.Completed),
		zap.Int64("failed_cards", progress.Failed),
	)
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
