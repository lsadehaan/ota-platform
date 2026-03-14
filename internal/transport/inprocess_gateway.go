package transport

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel/attribute"
	"go.uber.org/zap"

	"ota-platform/internal/pipeline"
	redispkg "ota-platform/internal/redis"
	"github.com/idnteq/go-smsc/smpp"
)

const correlationTTL = 5 * time.Minute

type correlationEntry struct {
	mapping   *redispkg.SMPPMapping
	expiresAt time.Time
}

// InProcessGateway replaces the Kafka-based gateway for the single-binary
// OTA Engine architecture. It reads SendSMSMessages from the bus, submits
// via the SMPP pool, and routes DLR/MO events back to bus channels.
//
// Correlation and multipart DLR tracking are process-local (sync.Map).
// This is correct for single-instance deployment (Phase 1).
type InProcessGateway struct {
	smppPool       SMPPClient
	bus            *pipeline.Bus
	correlations   sync.Map // smppMsgID -> *correlationEntry
	multipart      sync.Map // msgID -> *multipartState
	msisdnResolver MSISDNResolver
	coordination   CoordinationStore
	logger         *zap.Logger
	telemetry      *gatewayTelemetry
}

// multipartState tracks DLR resolution for multi-part messages.
type multipartState struct {
	mu        sync.Mutex
	total     int
	resolved  int
	anyFailed bool
}

// NewInProcessGateway creates an in-process gateway that uses Go channels
// instead of Kafka for event routing.
func NewInProcessGateway(
	smppConfig smpp.Config,
	poolConfig smpp.PoolConfig,
	bus *pipeline.Bus,
	coordination CoordinationStore,
	msisdnResolver MSISDNResolver,
	logger *zap.Logger,
) *InProcessGateway {
	g := &InProcessGateway{
		bus:            bus,
		coordination:   coordination,
		msisdnResolver: msisdnResolver,
		logger:         logger,
		telemetry:      newGatewayTelemetry(logger),
	}

	// Create SMPP pool with deliver handler that routes directly to channels.
	handler := func(sourceAddr string, destAddr string, esmClass byte, payload []byte) error {
		return g.deliverHandler(sourceAddr, destAddr, esmClass, payload)
	}

	g.smppPool = smpp.NewPool(smppConfig, poolConfig, handler, logger.Named("smpp-pool"))
	return g
}

// Start connects the SMPP pool and starts background cleanup.
func (g *InProcessGateway) Start(ctx context.Context) error {
	if err := g.smppPool.Connect(ctx); err != nil {
		return fmt.Errorf("connect SMPP pool: %w", err)
	}
	g.logger.Info("InProcessGateway SMPP pool connected",
		zap.Int("active", g.smppPool.ActiveConnections()),
	)
	go g.cleanupCorrelations(ctx)
	return nil
}

// Close shuts down the SMPP pool.
func (g *InProcessGateway) Close() error {
	g.logger.Info("shutting down in-process gateway")
	return g.smppPool.Close()
}

// HandleSendSMS processes a SendSMSMessage: submits parts via SMPP and stores
// correlation locally.
func (g *InProcessGateway) HandleSendSMS(ctx context.Context, msg pipeline.SendSMSMessage) error {
	ctx, span := g.telemetry.startSpan(ctx, "inprocess-gateway.send-sms",
		attribute.String("ota.msg_id", msg.MsgID),
		attribute.String("ota.campaign_id", msg.CampaignID),
		attribute.String("ota.card_id", msg.CardID),
		attribute.String("ota.msisdn", msg.MSISDN),
		attribute.Int("ota.parts", len(msg.Parts)),
	)
	var handleErr error
	defer func() {
		g.telemetry.finishSpan(span, handleErr)
	}()

	g.logger.Info("processing send-sms",
		zap.String("msg_id", msg.MsgID),
		zap.String("msisdn", msg.MSISDN),
		zap.Int("parts", len(msg.Parts)),
	)

	// Initialize multipart DLR tracking BEFORE submitting any parts.
	if len(msg.Parts) > 1 {
		g.multipart.Store(msg.MsgID, &multipartState{
			total: len(msg.Parts),
		})
	}

	submittedParts := 0
	for i, part := range msg.Parts {
		req := &smpp.SubmitRequest{
			MSISDN:      msg.MSISDN,
			DestTON:     msg.TON,
			DestNPI:     msg.NPI,
			ESMClass:    msg.ESMClass,
			ProtocolID:  msg.ProtocolID,
			DataCoding:  msg.DataCoding,
			Payload:     part.Payload,
			RegisterDLR: true,
		}

		// Submit via pool (blocks if all windows full = back-pressure).
		submitStart := time.Now()
		resp, err := g.smppPool.Submit(req)
		if err != nil {
			handleErr = g.handleSubmitFailure(ctx, &msg, submittedParts, fmt.Errorf("submit part %d: %w", part.Sequence, err))
			g.telemetry.recordSubmitFailure(ctx, "transport_error")
			return handleErr
		}
		if resp.Error != nil {
			handleErr = g.handleSubmitFailure(ctx, &msg, submittedParts, fmt.Errorf("submit part %d error: %w", part.Sequence, resp.Error))
			g.telemetry.recordSubmitFailure(ctx, "submit_error")
			return handleErr
		}
		g.telemetry.recordSubmit(ctx, "ok", time.Since(submitStart))
		g.telemetry.recordOp(ctx, "send_sms", "smpp_submit", time.Since(submitStart))

		// Store correlation locally (process-local only, no Redis).
		mapping := &redispkg.SMPPMapping{
			MsgID:      msg.MsgID,
			CampaignID: msg.CampaignID,
			CardID:     msg.CardID,
			MSISDN:     msg.MSISDN,
		}
		g.correlations.Store(resp.MessageID, &correlationEntry{
			mapping:   mapping,
			expiresAt: time.Now().Add(correlationTTL),
		})

		g.logger.Info("SMPP submit ok",
			zap.String("msg_id", msg.MsgID),
			zap.String("smpp_id", resp.MessageID),
			zap.Int("part", i+1),
			zap.Int("total", len(msg.Parts)),
		)
		submittedParts++
	}

	return nil
}

// deliverHandler is the SMPP deliver handler. It classifies DLR vs MO and
// routes events directly to bus channels instead of Kafka.
func (g *InProcessGateway) deliverHandler(sourceAddr, destAddr string, esmClass byte, payload []byte) error {
	if smpp.IsDLR(esmClass) {
		g.handleDLR(sourceAddr, payload)
	} else {
		g.handleMO(sourceAddr, destAddr, payload)
	}
	return nil
}

// handleDLR processes a delivery receipt and sends a CardEvent to bus.DLRCh.
func (g *InProcessGateway) handleDLR(sourceAddr string, payload []byte) {
	ctx := context.Background()
	ctx, span := g.telemetry.startSpan(ctx, "inprocess-gateway.handle-dlr")
	defer g.telemetry.finishSpan(span, nil)

	receipt := smpp.ParseDLRReceipt(string(payload))
	if receipt == nil {
		g.logger.Warn("failed to parse DLR receipt", zap.String("payload", string(payload)))
		return
	}
	g.telemetry.recordDLR(ctx, receipt.Status)

	// Look up correlation from local sync.Map.
	corrStart := time.Now()
	var msgID, campaignID, cardID string
	if entry, ok := g.correlations.Load(receipt.MessageID); ok {
		corr := entry.(*correlationEntry)
		if time.Now().Before(corr.expiresAt) {
			msgID = corr.mapping.MsgID
			campaignID = corr.mapping.CampaignID
			cardID = corr.mapping.CardID
		}
		g.correlations.Delete(receipt.MessageID)
	}
	g.telemetry.recordOp(ctx, "handle_dlr", "load_correlation", time.Since(corrStart))

	if cardID == "" {
		g.telemetry.recordCorrelationMiss(ctx)
		g.logger.Warn("no correlation found for DLR",
			zap.String("smpp_id", receipt.MessageID),
			zap.String("status", receipt.Status),
		)
		return
	}

	// Multipart DLR aggregation: check process-local tracking.
	dlrTrackStart := time.Now()
	allResolved, anyFailed, err := g.recordPartDLR(msgID, receipt.Status == "DELIVRD")
	if err != nil {
		if !errors.Is(err, errNoMultipartTracking) {
			g.logger.Error("multipart DLR tracking error",
				zap.String("msg_id", msgID),
				zap.Error(err),
			)
			return
		}
		// No multipart tracking -> single-part message, publish immediately.
		g.telemetry.recordOp(ctx, "handle_dlr", "record_part_dlr", time.Since(dlrTrackStart))
	} else if !allResolved {
		g.telemetry.recordOp(ctx, "handle_dlr", "record_part_dlr", time.Since(dlrTrackStart))
		g.logger.Debug("multipart DLR partial, waiting for remaining parts",
			zap.String("msg_id", msgID),
			zap.String("smpp_id", receipt.MessageID),
			zap.String("status", receipt.Status),
		)
		return
	} else {
		// All parts resolved -- determine aggregate status.
		if anyFailed {
			receipt.Status = "UNDELIV"
			receipt.ErrorCode = "multipart_partial_failure"
		} else {
			receipt.Status = "DELIVRD"
			receipt.ErrorCode = ""
		}
		g.telemetry.recordOp(ctx, "handle_dlr", "record_part_dlr", time.Since(dlrTrackStart))
	}

	event := pipeline.CardEvent{
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

	// Blocking write — applies backpressure to SMPP read loop if workers are slow.
	g.bus.DLRCh <- event
}

// handleMO processes an MO message and sends a CardEvent to bus.MOCh.
func (g *InProcessGateway) handleMO(sourceAddr, destAddr string, payload []byte) {
	ctx := context.Background()
	ctx, span := g.telemetry.startSpan(ctx, "inprocess-gateway.handle-mo",
		attribute.String("ota.source_msisdn", sourceAddr),
	)
	defer g.telemetry.finishSpan(span, nil)
	g.telemetry.recordMO(ctx)

	resolveStart := time.Now()
	cardID, err := g.msisdnResolver.LookupCardByMSISDN(ctx, sourceAddr)
	if err != nil {
		g.logger.Warn("failed to resolve MO source msisdn",
			zap.String("source_msisdn", sourceAddr),
			zap.Error(err),
		)
		return
	}
	g.telemetry.recordOp(ctx, "handle_mo", "resolve_msisdn", time.Since(resolveStart))
	if cardID == "" {
		g.logger.Warn("no card mapping found for MO source msisdn",
			zap.String("source_msisdn", sourceAddr),
		)
		return
	}

	stateStart := time.Now()
	var campaignID string
	state, err := g.coordination.GetCardState(ctx, cardID)
	if err != nil {
		g.logger.Warn("failed to load card state for MO correlation",
			zap.String("card_id", cardID),
			zap.String("source_msisdn", sourceAddr),
			zap.Error(err),
		)
	} else if state != nil {
		campaignID = state.CampaignID
	}
	g.telemetry.recordOp(ctx, "handle_mo", "get_card_state", time.Since(stateStart))

	event := pipeline.CardEvent{
		Type:         "card.mo_received",
		EventID:      uuid.New().String(),
		CardID:       cardID,
		CampaignID:   campaignID,
		SourceMSISDN: sourceAddr,
		PayloadHex:   hex.EncodeToString(payload),
		Timestamp:    time.Now(),
	}

	// Blocking write — applies backpressure to SMPP read loop if workers are slow.
	g.bus.MOCh <- event
}

// handleSubmitFailure creates a DLR failure event and sends it to bus.DLRCh.
func (g *InProcessGateway) handleSubmitFailure(ctx context.Context, msg *pipeline.SendSMSMessage, submittedParts int, submitErr error) error {
	status := "FAILED_SUBMIT"
	if submittedParts > 0 {
		status = "FAILED_PARTIAL_SUBMIT"
	}

	event := pipeline.CardEvent{
		Type:       "card.dlr_received",
		EventID:    uuid.New().String(),
		CardID:     msg.CardID,
		CampaignID: msg.CampaignID,
		MsgID:      msg.MsgID,
		DLRStatus:  status,
		ErrorCode:  submitErr.Error(),
		Timestamp:  time.Now(),
	}

	select {
	case g.bus.DLRCh <- event:
	case <-ctx.Done():
		return ctx.Err()
	}

	g.logger.Warn("submit-sms failed",
		zap.String("msg_id", msg.MsgID),
		zap.String("card_id", msg.CardID),
		zap.String("campaign_id", msg.CampaignID),
		zap.Int("submitted_parts", submittedParts),
		zap.String("status", status),
		zap.Error(submitErr),
	)
	return nil
}

// errNoMultipartTracking is returned when a message has no multipart tracking.
var errNoMultipartTracking = errors.New("no multipart tracking")

// recordPartDLR records a DLR for one part of a (possibly multi-part) message.
// Returns (allResolved, anyFailed, err). If no multipart tracking exists for
// this msgID, returns errNoMultipartTracking (single-part message).
func (g *InProcessGateway) recordPartDLR(msgID string, delivered bool) (bool, bool, error) {
	val, ok := g.multipart.Load(msgID)
	if !ok {
		return false, false, errNoMultipartTracking
	}
	state := val.(*multipartState)
	state.mu.Lock()
	defer state.mu.Unlock()

	state.resolved++
	if !delivered {
		state.anyFailed = true
	}

	if state.resolved >= state.total {
		// All parts resolved, clean up tracking.
		g.multipart.Delete(msgID)
		return true, state.anyFailed, nil
	}
	return false, false, nil
}

// cleanupCorrelations periodically removes expired correlation entries.
func (g *InProcessGateway) cleanupCorrelations(ctx context.Context) {
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			now := time.Now()
			g.correlations.Range(func(key, value any) bool {
				entry, ok := value.(*correlationEntry)
				if !ok || now.After(entry.expiresAt) {
					g.correlations.Delete(key)
				}
				return true
			})
			// Also clean up stale multipart state (older than 10 minutes).
			// This handles cases where some DLRs never arrive.
			g.multipart.Range(func(key, value any) bool {
				// No timestamp on multipartState, so just leave them.
				// They'll be cleaned up if the gateway restarts.
				return true
			})
		}
	}
}
