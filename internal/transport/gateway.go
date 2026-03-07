package transport

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel/attribute"
	"go.uber.org/zap"

	contractevents "ota-platform/internal/contracts/events"
	kafkapkg "ota-platform/internal/kafka"
	redispkg "ota-platform/internal/redis"
	"ota-platform/internal/smpp"
	"ota-platform/pkg/hexutil"
)

const (
	sendSMSTopic    = contractevents.TopicSendSMS
	cardEventsTopic = contractevents.TopicCardEvents
	consumerGroup   = "sms-gateway"
)

// Gateway consumes from the send-sms Kafka topic, submits SMS messages via SMPP pool,
// and produces card events back to Kafka for DLR/MO processing.
type Gateway struct {
	smppPool      SMPPClient
	consumer      *kafkapkg.ConcurrentConsumer
	eventProducer EventProducer // publishes to card-events topic
	redis         CoordinationStore
	logger        *zap.Logger
	telemetry     *gatewayTelemetry
}

func NewGateway(smppConfig smpp.Config, poolConfig smpp.PoolConfig, kafkaBrokers []string, rdb CoordinationStore, logger *zap.Logger) *Gateway {
	s := &Gateway{
		redis:     rdb,
		logger:    logger,
		telemetry: newGatewayTelemetry(logger),
	}

	// Create Kafka producer for card-events topic
	s.eventProducer = kafkapkg.NewProducer(kafkaBrokers, cardEventsTopic, logger.Named("event-producer"))

	// Create SMPP pool with deliver handler
	handler := func(sourceAddr string, destAddr string, esmClass byte, payload []byte) {
		ctx := context.Background()

		if smpp.IsDLR(esmClass) {
			s.handleDLR(ctx, sourceAddr, payload)
		} else {
			s.handleMO(ctx, sourceAddr, destAddr, payload)
		}
	}

	s.smppPool = smpp.NewPool(smppConfig, poolConfig, handler, logger.Named("smpp-pool"))

	// Create Kafka consumer for send-sms topic (concurrent with partition-parallel workers)
	workers := 8
	if v := os.Getenv("SMS_SENDER_WORKERS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			workers = n
		}
	}
	s.consumer = kafkapkg.NewConcurrentConsumer(
		kafkaBrokers, sendSMSTopic, consumerGroup, workers,
		s.handleSendSMS, logger.Named("send-sms-consumer"),
	)

	return s
}

func (s *Gateway) Start(ctx context.Context) error {
	if err := s.smppPool.Connect(ctx); err != nil {
		return fmt.Errorf("connect SMPP pool: %w", err)
	}
	s.logger.Info("SMPP pool connected", zap.Int("active", s.smppPool.ActiveConnections()))

	if err := s.consumer.Start(ctx); err != nil {
		return fmt.Errorf("kafka consumer: %w", err)
	}
	return nil
}

func (s *Gateway) handleSendSMS(ctx context.Context, key []byte, value []byte) error {
	var msg kafkapkg.SendSMSMessage
	if err := json.Unmarshal(value, &msg); err != nil {
		return fmt.Errorf("unmarshal SendSMSMessage: %w", err)
	}
	ctx, span := s.telemetry.startSpan(ctx, "sms-gateway.send-sms",
		attribute.String("ota.msg_id", msg.MsgID),
		attribute.String("ota.campaign_id", msg.CampaignID),
		attribute.String("ota.card_id", msg.CardID),
		attribute.String("ota.msisdn", msg.MSISDN),
		attribute.Int("ota.parts", len(msg.Parts)),
	)
	var handleErr error
	defer func() {
		s.telemetry.finishSpan(span, handleErr)
	}()

	s.logger.Info("processing send-sms",
		zap.String("msg_id", msg.MsgID),
		zap.String("msisdn", msg.MSISDN),
		zap.Int("parts", len(msg.Parts)),
	)

	submittedParts := 0
	for i, part := range msg.Parts {
		payload, err := hexutil.Decode(part.Payload)
		if err != nil {
			handleErr = s.handleSubmitFailure(ctx, &msg, submittedParts, fmt.Errorf("decode hex part %d: %w", part.Sequence, err))
			s.telemetry.recordSubmitFailure(ctx, "decode_error")
			return handleErr
		}

		req := &smpp.SubmitRequest{
			MSISDN:      msg.MSISDN,
			DestTON:     msg.TON,
			DestNPI:     msg.NPI,
			ESMClass:    msg.ESMClass,
			ProtocolID:  msg.ProtocolID,
			DataCoding:  msg.DataCoding,
			Payload:     payload,
			RegisterDLR: true,
		}

		// Submit via pool (blocks if all windows full = back-pressure)
		submitStart := time.Now()
		resp, err := s.smppPool.Submit(req)
		if err != nil {
			handleErr = s.handleSubmitFailure(ctx, &msg, submittedParts, fmt.Errorf("submit part %d: %w", part.Sequence, err))
			s.telemetry.recordSubmitFailure(ctx, "transport_error")
			return handleErr
		}
		if resp.Error != nil {
			handleErr = s.handleSubmitFailure(ctx, &msg, submittedParts, fmt.Errorf("submit part %d error: %w", part.Sequence, resp.Error))
			s.telemetry.recordSubmitFailure(ctx, "submit_error")
			return handleErr
		}
		s.telemetry.recordSubmit(ctx, "ok", time.Since(submitStart))

		// Store SMPP correlation in the coordination store (replaces sync.Map)
		if err := s.redis.StoreSMPPCorrelation(ctx, resp.MessageID, &redispkg.SMPPMapping{
			MsgID:      msg.MsgID,
			CampaignID: msg.CampaignID,
			CardID:     msg.CardID,
			MSISDN:     msg.MSISDN,
		}); err != nil {
			handleErr = fmt.Errorf("store smpp correlation for part %d: %w", part.Sequence, err)
			return handleErr
		}

		s.logger.Info("SMPP submit ok",
			zap.String("msg_id", msg.MsgID),
			zap.String("smpp_id", resp.MessageID),
			zap.Int("part", i+1),
			zap.Int("total", len(msg.Parts)),
		)
		submittedParts++
	}
	return nil
}

func (s *Gateway) handleSubmitFailure(ctx context.Context, msg *kafkapkg.SendSMSMessage, submittedParts int, submitErr error) error {
	status := "FAILED_SUBMIT"
	if submittedParts > 0 {
		status = "FAILED_PARTIAL_SUBMIT"
	}

	event := kafkapkg.CardEvent{
		Type:       "card.dlr_received",
		EventID:    uuid.New().String(),
		CardID:     msg.CardID,
		CampaignID: msg.CampaignID,
		MsgID:      msg.MsgID,
		DLRStatus:  status,
		ErrorCode:  submitErr.Error(),
		Timestamp:  time.Now(),
	}

	if err := s.publishCardEvent(ctx, msg.CardID, event, "submit-failure"); err != nil {
		if submittedParts > 0 {
			s.logger.Error("failed to publish partial submit failure event",
				zap.String("card_id", msg.CardID),
				zap.String("campaign_id", msg.CampaignID),
				zap.Int("submitted_parts", submittedParts),
				zap.Error(err),
			)
			return nil
		}
		return fmt.Errorf("publish submit failure event: %w", err)
	}

	s.logger.Warn("submit-sms failed",
		zap.String("msg_id", msg.MsgID),
		zap.String("card_id", msg.CardID),
		zap.String("campaign_id", msg.CampaignID),
		zap.Int("submitted_parts", submittedParts),
		zap.String("status", status),
		zap.Error(submitErr),
	)
	return nil
}

func (s *Gateway) handleDLR(ctx context.Context, sourceAddr string, payload []byte) {
	ctx, span := s.telemetry.startSpan(ctx, "sms-gateway.handle-dlr")
	defer s.telemetry.finishSpan(span, nil)

	receipt := smpp.ParseDLRReceipt(string(payload))
	if receipt == nil {
		s.logger.Warn("failed to parse DLR receipt", zap.String("payload", string(payload)))
		return
	}
	s.telemetry.recordDLR(ctx, receipt.Status)

	// Look up internal msg_id from Redis
	var msgID, campaignID, cardID string
	mapping, err := s.redis.LoadSMPPCorrelation(ctx, receipt.MessageID)
	if err != nil {
		s.logger.Warn("Redis correlation lookup failed", zap.Error(err))
	}
	if mapping != nil {
		msgID = mapping.MsgID
		campaignID = mapping.CampaignID
		cardID = mapping.CardID
	}

	if cardID == "" {
		s.logger.Warn("no correlation found for DLR",
			zap.String("smpp_id", receipt.MessageID),
			zap.String("status", receipt.Status),
		)
		return // can't route without card_id
	}

	// Publish as CardEvent to card-events topic
	event := kafkapkg.CardEvent{
		Type:          "card.dlr_received",
		EventID:       uuid.New().String(),
		CardID:        cardID,
		CampaignID:    campaignID,
		MsgID:         msgID,
		SMPPMessageID: receipt.MessageID,
		DLRStatus:     receipt.Status,
		ErrorCode:     receipt.ErrorCode,
		Timestamp:     time.Now(),
	}

	if err := s.publishCardEvent(ctx, cardID, event, "DLR"); err != nil {
		s.logger.Error("failed to publish DLR event",
			zap.String("card_id", cardID),
			zap.String("campaign_id", campaignID),
			zap.Error(err),
		)
	}
}

func (s *Gateway) handleMO(ctx context.Context, sourceAddr, destAddr string, payload []byte) {
	ctx, span := s.telemetry.startSpan(ctx, "sms-gateway.handle-mo",
		attribute.String("ota.source_msisdn", sourceAddr),
	)
	defer s.telemetry.finishSpan(span, nil)
	s.telemetry.recordMO(ctx)

	cardID, err := s.redis.LookupCardByMSISDN(ctx, sourceAddr)
	if err != nil {
		s.logger.Warn("failed to resolve MO source msisdn",
			zap.String("source_msisdn", sourceAddr),
			zap.Error(err),
		)
		return
	}
	if cardID == "" {
		s.logger.Warn("no card mapping found for MO source msisdn",
			zap.String("source_msisdn", sourceAddr),
		)
		return
	}

	var campaignID string
	state, err := s.redis.GetCardState(ctx, cardID)
	if err != nil {
		s.logger.Warn("failed to load card state for MO correlation",
			zap.String("card_id", cardID),
			zap.String("source_msisdn", sourceAddr),
			zap.Error(err),
		)
	} else if state != nil {
		campaignID = state.CampaignID
	}

	event := kafkapkg.CardEvent{
		Type:         "card.mo_received",
		EventID:      uuid.New().String(),
		CardID:       cardID,
		CampaignID:   campaignID,
		SourceMSISDN: sourceAddr,
		PayloadHex:   hex.EncodeToString(payload),
		Timestamp:    time.Now(),
	}

	// Key by card_id to preserve per-card ordering.
	if err := s.publishCardEvent(ctx, cardID, event, "MO"); err != nil {
		s.logger.Error("failed to publish MO event",
			zap.String("card_id", cardID),
			zap.String("campaign_id", campaignID),
			zap.Error(err),
		)
	}
}

func (s *Gateway) publishCardEvent(ctx context.Context, key string, event kafkapkg.CardEvent, eventLabel string) error {
	var lastErr error
	for attempt := 0; attempt < 3; attempt++ {
		if attempt > 0 {
			delay := time.Duration(attempt) * 200 * time.Millisecond
			timer := time.NewTimer(delay)
			select {
			case <-ctx.Done():
				timer.Stop()
				return ctx.Err()
			case <-timer.C:
			}
		}

		if err := s.eventProducer.Publish(ctx, key, event); err != nil {
			lastErr = err
			s.logger.Warn("retrying card event publish",
				zap.String("event_type", event.Type),
				zap.String("label", eventLabel),
				zap.Int("attempt", attempt+1),
				zap.Error(err),
			)
			continue
		}
		return nil
	}
	return fmt.Errorf("publish %s event after retries: %w", eventLabel, lastErr)
}

func (s *Gateway) Close() {
	s.logger.Info("shutting down SMS sender")
	if err := s.consumer.Close(); err != nil {
		s.logger.Error("close consumer", zap.Error(err))
	}
	if err := s.smppPool.Close(); err != nil {
		s.logger.Error("close SMPP pool", zap.Error(err))
	}
	if err := s.eventProducer.Close(); err != nil {
		s.logger.Error("close event producer", zap.Error(err))
	}
}
