package controlplane

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
	"go.uber.org/zap"
	"gorm.io/gorm"

	"ota-platform/internal/config"
	"ota-platform/internal/db"
	"ota-platform/internal/pipeline"
	redispkg "ota-platform/internal/redis"
	"ota-platform/pkg/gsm0348"
)

const defaultCampaignShardSize = 1000

func campaignShardSize() int {
	size := config.GetEnvInt("CAMPAIGN_SHARD_SIZE", defaultCampaignShardSize)
	if size <= 0 {
		return defaultCampaignShardSize
	}
	return size
}

// CampaignService orchestrates campaign lifecycle: starting, pausing, resuming,
// and aborting campaigns. Card-level processing (OTA command building, DLR/MO
// handling) is delegated to the planner/executor pipeline via campaign shards.
type CampaignService struct {
	db                  *gorm.DB
	redis               CoordinationStore
	query               QueryStore
	wsHub               *WSHub
	logger              *zap.Logger
	startRejectOverload metric.Int64Counter
}

// NewCampaignService creates a new CampaignService.
func NewCampaignService(database *gorm.DB, rdb CoordinationStore, queryStore QueryStore, wsHub *WSHub, logger *zap.Logger) *CampaignService {
	meter := otel.Meter("controlplane")
	startRejectOverload, err := meter.Int64Counter("ota.controlplane.campaign_start_rejected_overload_total",
		metric.WithDescription("Campaign starts rejected due to shard backlog overload"),
	)
	if err != nil {
		logger.Warn("create campaign_start_rejected_overload metric", zap.Error(err))
	}
	return &CampaignService{
		db:                  database,
		redis:               rdb,
		query:               queryStore,
		wsHub:               wsHub,
		logger:              logger,
		startRejectOverload: startRejectOverload,
	}
}

// StartCampaign transitions a campaign from pending to running and persists
// planner-owned shard manifests in the same transaction as the campaign state.
func (cs *CampaignService) StartCampaign(ctx context.Context, campaignID uuid.UUID) error {
	// Admission control: reject if system is already overloaded with pending shards.
	var pendingShards int64
	cs.db.WithContext(ctx).Model(&db.CampaignShard{}).
		Where("status IN ?", []string{"pending", "publishing"}).
		Count(&pendingShards)
	maxBacklog := int64(config.GetEnvInt("CAMPAIGN_START_MAX_SHARD_BACKLOG", 1000))
	if pendingShards > maxBacklog {
		if cs.startRejectOverload != nil {
			cs.startRejectOverload.Add(ctx, 1)
		}
		return fmt.Errorf("shard backlog high: %d shards pending/publishing across campaigns (max %d) — try again later", pendingShards, maxBacklog)
	}

	// 1. Load campaign with commands and cards (for validation + shard writes)
	var campaign db.Campaign
	err := cs.db.WithContext(ctx).
		Preload("CampaignCommands", func(tx *gorm.DB) *gorm.DB { return tx.Order("sequence ASC") }).
		Preload("CampaignCommands.Application").
		First(&campaign, "id = ?", campaignID).Error
	if err != nil {
		return fmt.Errorf("load campaign %s: %w", campaignID, err)
	}
	if campaign.Status != "pending" {
		return fmt.Errorf("campaign %s is in status %q, expected 'pending'", campaignID, campaign.Status)
	}
	if len(campaign.CampaignCommands) == 0 {
		return fmt.Errorf("campaign %s has no commands", campaignID)
	}
	now := time.Now()
	firstStep := campaign.CampaignCommands[0].Sequence

	// 2. Cache campaign commands in Redis for workers
	cacheCommands := make([]redispkg.CampaignCommandCache, len(campaign.CampaignCommands))
	for i, cmd := range campaign.CampaignCommands {
		app := cmd.Application
		cacheCommands[i] = redispkg.CampaignCommandCache{
			Sequence:       cmd.Sequence,
			ApplicationID:  cmd.ApplicationID.String(),
			Script:         cmd.Script,
			ExpectResponse: cmd.ExpectResponse,
			TAR:            app.TAR,
			KIcAlgo:        app.KIcAlgo,
			KIcMode:        app.KIcMode,
			KIcKeysetID:    int(app.KIcKeysetID),
			KIdAlgo:        app.KIdAlgo,
			KIdMode:        app.KIdMode,
			KIdKeysetID:    int(app.KIdKeysetID),
			CertMode:       app.CertificationMode,
			Ciphered:       app.Ciphered,
			CounterMode:    app.CounterMode,
			PORMode:        app.PORMode,
			PORProtocol:    app.PORProtocol,
			PORCiphered:    app.PORCiphered,
			PORCertMode:    app.PORCertMode,
		}
	}
	if cs.redis != nil {
		if err := cs.redis.CacheCampaignCommands(ctx, campaignID.String(), cacheCommands); err != nil {
			cs.logger.Warn("failed to cache campaign commands in Redis", zap.Error(err))
		}
	}

	// 3. Initialize card execution states FIRST (before shards exist).
	// This ensures campaign_stats counters are correct before any card processing begins.
	if cs.query != nil {
		if err := cs.forEachCampaignTargetBatch(ctx, cs.db.WithContext(ctx), campaignID, campaignShardSize(), func(batch []uuid.UUID) error {
			if err := cs.query.InitializeCampaign(ctx, campaignID, batch, firstStep, now); err != nil {
				return fmt.Errorf("initialize campaign query state: %w", err)
			}
			return nil
		}); err != nil {
			return err
		}
	}

	// 4. Persist campaign state and planner shards atomically.
	// Shards become visible to the planner only after this transaction commits.
	var pendingCards int
	err = cs.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		// Update campaign status
		if err := tx.Model(&campaign).Updates(map[string]interface{}{
			"status": "running", "started_at": now,
		}).Error; err != nil {
			return err
		}

		shardSequence := 1
		shardSize := campaignShardSize()
		if err := cs.forEachCampaignTargetBatch(ctx, tx, campaignID, shardSize, func(batch []uuid.UUID) error {
			pendingCards += len(batch)

			events := make([]pipeline.CardEvent, 0, len(batch))
			for _, cardID := range batch {
				events = append(events, pipeline.CardEvent{
					Type:       "card.activate",
					EventID:    uuid.New().String(),
					CardID:     cardID.String(),
					CampaignID: campaignID.String(),
					Step:       firstStep,
					Timestamp:  now,
				})
			}

			items, err := json.Marshal(events)
			if err != nil {
				return fmt.Errorf("marshal activation shard %d: %w", shardSequence, err)
			}
			shard := db.CampaignShard{
				ID:         uuid.New(),
				CampaignID: campaignID,
				Sequence:   shardSequence,
				Status:     "pending",
				ItemCount:  len(events),
				Items:      items,
			}
			shardSequence++
			if err := tx.Create(&shard).Error; err != nil {
				return fmt.Errorf("create campaign shard: %w", err)
			}
			return nil
		}); err != nil {
			return err
		}
		if pendingCards == 0 {
			return fmt.Errorf("campaign %s has no target cards", campaignID)
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("start campaign transaction: %w", err)
	}

	// 4. Cache campaign status in the coordination store
	if cs.redis != nil {
		if err := cs.redis.SetCampaignStatus(ctx, campaignID.String(), "running"); err != nil {
			cs.logger.Warn("failed to set campaign status in Redis", zap.Error(err))
		}
	}

	// 5. WebSocket broadcast
	if cs.wsHub != nil {
		cs.wsHub.Broadcast(&WSEvent{
			Type:       "campaign_progress",
			CampaignID: campaignID.String(),
			Data: map[string]interface{}{
				"status": "running", "started_at": now, "total_cards": pendingCards,
			},
		})
	}

	cs.logger.Info("campaign started (async)",
		zap.String("campaign_id", campaignID.String()),
		zap.Int("pending_cards", pendingCards),
	)
	return nil
}

// PauseCampaign pauses a running campaign.
func (cs *CampaignService) PauseCampaign(ctx context.Context, campaignID uuid.UUID) error {
	result := cs.db.WithContext(ctx).Model(&db.Campaign{}).
		Where("id = ? AND status = ?", campaignID, "running").
		Update("status", "paused")
	if result.Error != nil {
		return fmt.Errorf("pause campaign: %w", result.Error)
	}
	if result.RowsAffected == 0 {
		return fmt.Errorf("campaign %s is not in 'running' status", campaignID)
	}
	if cs.redis != nil {
		if err := cs.redis.SetCampaignStatus(ctx, campaignID.String(), "paused"); err != nil {
			cs.logger.Warn("failed to set campaign status in coordination store", zap.Error(err))
		}
	}
	if cs.wsHub != nil {
		cs.wsHub.Broadcast(&WSEvent{
			Type: "campaign_progress", CampaignID: campaignID.String(),
			Data: map[string]interface{}{"status": "paused"},
		})
	}
	return nil
}

// ResumeCampaign resumes a paused campaign by creating new planner shards for
// cards that still need activation.
func (cs *CampaignService) ResumeCampaign(ctx context.Context, campaignID uuid.UUID) error {
	var campaign db.Campaign
	err := cs.db.WithContext(ctx).
		Preload("CampaignCommands", func(tx *gorm.DB) *gorm.DB { return tx.Order("sequence ASC") }).
		Preload("CampaignCommands.Application").
		First(&campaign, "id = ? AND status = ?", campaignID, "paused").Error
	if err != nil {
		return fmt.Errorf("load paused campaign %s: %w", campaignID, err)
	}

	if len(campaign.CampaignCommands) == 0 {
		return fmt.Errorf("campaign %s has no commands", campaignID)
	}
	if cs.query == nil {
		return fmt.Errorf("query store is required to resume campaign %s", campaignID)
	}

	now := time.Now()
	cardStates, err := cs.query.ListCampaignCards(ctx, campaignID, "")
	if err != nil {
		return fmt.Errorf("load campaign state for resume: %w", err)
	}

	// Cache campaign commands in Redis for workers
	cacheCommands := make([]redispkg.CampaignCommandCache, len(campaign.CampaignCommands))
	for i, cmd := range campaign.CampaignCommands {
		app := cmd.Application
		cacheCommands[i] = redispkg.CampaignCommandCache{
			Sequence:       cmd.Sequence,
			ApplicationID:  cmd.ApplicationID.String(),
			Script:         cmd.Script,
			ExpectResponse: cmd.ExpectResponse,
			TAR:            app.TAR,
			KIcAlgo:        app.KIcAlgo,
			KIcMode:        app.KIcMode,
			KIcKeysetID:    int(app.KIcKeysetID),
			KIdAlgo:        app.KIdAlgo,
			KIdMode:        app.KIdMode,
			KIdKeysetID:    int(app.KIdKeysetID),
			CertMode:       app.CertificationMode,
			Ciphered:       app.Ciphered,
			CounterMode:    app.CounterMode,
			PORMode:        app.PORMode,
			PORProtocol:    app.PORProtocol,
			PORCiphered:    app.PORCiphered,
			PORCertMode:    app.PORCertMode,
		}
	}
	if cs.redis != nil {
		if err := cs.redis.CacheCampaignCommands(ctx, campaignID.String(), cacheCommands); err != nil {
			cs.logger.Warn("failed to cache campaign commands in Redis on resume", zap.Error(err))
		}
	}

	// Persist campaign state and planner shards atomically.
	var pendingCards int
	err = cs.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		// Update campaign status
		if err := tx.Model(&campaign).Update("status", "running").Error; err != nil {
			return err
		}

		var shardEvents []pipeline.CardEvent
		for _, cc := range cardStates {
			if cc.Status != "pending" && cc.Status != "activating" {
				continue
			}
			pendingCards++
			step := cc.CurrentStep
			if step == 0 {
				step = campaign.CampaignCommands[0].Sequence
			}
			shardEvents = append(shardEvents, pipeline.CardEvent{
				Type:       "card.activate",
				EventID:    uuid.New().String(),
				CardID:     cc.CardID,
				CampaignID: campaignID.String(),
				Step:       step,
				RetryCount: cc.RetryCount,
				Timestamp:  now,
			})
		}

		if len(shardEvents) > 0 {
			var lastSequence int
			if err := tx.Model(&db.CampaignShard{}).
				Where("campaign_id = ?", campaignID).
				Select("COALESCE(MAX(sequence), 0)").
				Scan(&lastSequence).Error; err != nil {
				return fmt.Errorf("load last campaign shard sequence: %w", err)
			}

			shardSize := campaignShardSize()
			shards := make([]db.CampaignShard, 0, (len(shardEvents)+shardSize-1)/shardSize)
			for i, seq := 0, lastSequence+1; i < len(shardEvents); i, seq = i+shardSize, seq+1 {
				end := i + shardSize
				if end > len(shardEvents) {
					end = len(shardEvents)
				}
				items, err := json.Marshal(shardEvents[i:end])
				if err != nil {
					return fmt.Errorf("marshal resume shard %d: %w", seq, err)
				}
				shards = append(shards, db.CampaignShard{
					ID:         uuid.New(),
					CampaignID: campaignID,
					Sequence:   seq,
					Status:     "pending",
					ItemCount:  end - i,
					Items:      items,
				})
			}
			if err := tx.CreateInBatches(&shards, 100).Error; err != nil {
				return fmt.Errorf("create resume campaign shards: %w", err)
			}
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("resume campaign transaction: %w", err)
	}

	// Update Redis
	if cs.redis != nil {
		if err := cs.redis.SetCampaignStatus(ctx, campaignID.String(), "running"); err != nil {
			cs.logger.Warn("failed to set campaign status in Redis", zap.Error(err))
		}
	}

	if cs.wsHub != nil {
		cs.wsHub.Broadcast(&WSEvent{
			Type:       "campaign_progress",
			CampaignID: campaignID.String(),
			Data: map[string]interface{}{
				"status":        "running",
				"resumed_cards": pendingCards,
			},
		})
	}

	cs.logger.Info("campaign resumed (async)",
		zap.String("campaign_id", campaignID.String()),
		zap.Int("pending_cards", pendingCards),
	)
	return nil
}

// AbortCampaign stops a campaign and marks remaining cards as skipped.
func (cs *CampaignService) AbortCampaign(ctx context.Context, campaignID uuid.UUID) error {
	var campaign db.Campaign
	err := cs.db.WithContext(ctx).First(&campaign, "id = ?", campaignID).Error
	if err != nil {
		return fmt.Errorf("load campaign %s: %w", campaignID, err)
	}

	if campaign.Status != "running" && campaign.Status != "paused" {
		return fmt.Errorf("campaign %s is in status %q, cannot abort", campaignID, campaign.Status)
	}

	now := time.Now()
	if err := cs.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := tx.Model(&campaign).Updates(map[string]interface{}{
			"status":       "aborted",
			"completed_at": now,
		}).Error; err != nil {
			return fmt.Errorf("update campaign status: %w", err)
		}
		return nil
	}); err != nil {
		return err
	}

	// Update Redis immediately after Postgres — executor checks Redis for
	// campaign status and must stop processing cards as soon as possible.
	if cs.redis != nil {
		if err := cs.redis.SetCampaignStatus(ctx, campaignID.String(), "aborted"); err != nil {
			cs.logger.Warn("failed to set campaign status in coordination store", zap.Error(err))
		}
	}

	if cs.query != nil {
		if _, err := cs.query.AbortCampaign(ctx, campaignID, now); err != nil {
			cs.logger.Error("failed to abort campaign in query store", zap.Error(err))
		}
	}

	if cs.wsHub != nil {
		cs.wsHub.Broadcast(&WSEvent{
			Type:       "campaign_progress",
			CampaignID: campaignID.String(),
			Data: map[string]interface{}{
				"status":       "aborted",
				"completed_at": now,
			},
		})
	}

	return nil
}

func (cs *CampaignService) forEachCampaignTargetBatch(ctx context.Context, dbtx *gorm.DB, campaignID uuid.UUID, batchSize int, fn func([]uuid.UUID) error) error {
	if batchSize <= 0 {
		batchSize = campaignShardSize()
	}

	type targetRow struct {
		CardID uuid.UUID `gorm:"column:card_id"`
	}

	var lastCardID *uuid.UUID
	for {
		query := dbtx.WithContext(ctx).
			Model(&db.CampaignTarget{}).
			Select("card_id").
			Where("campaign_id = ?", campaignID)
		if lastCardID != nil {
			query = query.Where("card_id > ?", *lastCardID)
		}

		var batchRows []targetRow
		if err := query.
			Order("card_id ASC").
			Limit(batchSize).
			Find(&batchRows).Error; err != nil {
			return err
		}
		if len(batchRows) == 0 {
			return nil
		}

		cardIDs := make([]uuid.UUID, 0, len(batchRows))
		for _, row := range batchRows {
			cardIDs = append(cardIDs, row.CardID)
		}
		if err := fn(cardIDs); err != nil {
			return err
		}

		last := batchRows[len(batchRows)-1].CardID
		lastCardID = &last
		if len(batchRows) < batchSize {
			return nil
		}
	}
}

// ---------------------------------------------------------------------------
// Helper functions (used by card_worker.go)
// ---------------------------------------------------------------------------

// buildSecurityProfile converts a db.Application to a gsm0348.SecurityProfile.
func buildSecurityProfile(app *db.Application) *gsm0348.SecurityProfile {
	return &gsm0348.SecurityProfile{
		CertMode:    parseCertificationMode(app.CertificationMode),
		Ciphered:    app.Ciphered,
		CounterMode: parseCounterMode(app.CounterMode),
		PoRMode:     parsePoRMode(app.PORMode),
		PoRCertMode: parseCertificationMode(app.PORCertMode),
		PoRCiphered: app.PORCiphered,
		PoRProtocol: parsePoRProtocol(app.PORProtocol),

		KIcAlgo:     parseAlgo(app.KIcAlgo),
		KIcMode:     parseCipherMode(app.KIcMode),
		KIcKeysetID: byte(app.KIcKeysetID),

		KIDAlgo:     parseAlgo(app.KIdAlgo),
		KIDMode:     parseCipherMode(app.KIdMode),
		KIDKeysetID: byte(app.KIdKeysetID),

		SecurityBytesWithLengthsAndUDHL: true,
	}
}

// parseCertificationMode converts a string certification mode to the gsm0348 constant.
func parseCertificationMode(mode string) gsm0348.CertificationMode {
	switch mode {
	case "NO_SECURITY", "NONE":
		return gsm0348.CertNone
	case "RC":
		return gsm0348.CertRC
	case "CC":
		return gsm0348.CertCC
	case "DS":
		return gsm0348.CertDS
	default:
		return gsm0348.CertNone
	}
}

// parseCounterMode converts a string counter mode to the gsm0348 constant.
func parseCounterMode(mode string) gsm0348.CounterMode {
	switch mode {
	case "NO_COUNTER", "NONE":
		return gsm0348.CounterNone
	case "COUNTER_NO_REPLAY":
		return gsm0348.CounterNoReplay
	case "COUNTER_REPLAY_OR_CHECK":
		return gsm0348.CounterReplayCheck
	default:
		return gsm0348.CounterNone
	}
}

// parsePoRMode converts a string PoR mode to the gsm0348 constant.
func parsePoRMode(mode string) gsm0348.PoRMode {
	switch mode {
	case "NO_REPLY", "NONE":
		return gsm0348.PoRNone
	case "REPLY_ALWAYS":
		return gsm0348.PoRAlways
	case "REPLY_ON_ERROR":
		return gsm0348.PoROnError
	default:
		return gsm0348.PoRNone
	}
}

// parseCipherMode converts a string cipher mode to the gsm0348 constant.
func parseCipherMode(mode string) gsm0348.CipherMode {
	switch mode {
	case "DES_CBC":
		return gsm0348.CipherDES_CBC
	case "TRIPLE_DES_CBC_2_KEYS":
		return gsm0348.Cipher3DES_CBC_2Keys
	case "TRIPLE_DES_CBC_3_KEYS":
		return gsm0348.Cipher3DES_CBC_3Keys
	case "AES_CBC":
		return gsm0348.CipherAES_CBC
	default:
		return gsm0348.CipherDES_CBC
	}
}

// parseAlgo converts a string algorithm identifier to a byte value.
func parseAlgo(algo string) byte {
	switch algo {
	case "DES":
		return 0x01
	case "AES":
		return 0x02
	default:
		return 0x01
	}
}

// parsePoRProtocol converts a string PoR protocol to a byte value.
func parsePoRProtocol(protocol string) byte {
	switch protocol {
	case "SMS_DELIVER_REPORT":
		return 0x01
	case "SMS_SUBMIT":
		return 0x02
	default:
		return 0x02
	}
}

// porStatusDescription returns a human-readable description of a GSM 03.48 PoR status code.
func porStatusDescription(code byte) string {
	switch code {
	case gsm0348.RespOK:
		return "command executed successfully"
	case gsm0348.RespRCCCDSFailed:
		return "RC/CC/DS verification failed"
	case gsm0348.RespCNTRLow:
		return "counter too low"
	case gsm0348.RespCNTRHighNoUpdate:
		return "counter too high, not updated"
	case gsm0348.RespCNTRHighUpdated:
		return "counter too high, updated"
	case gsm0348.RespCNTRBlocked:
		return "counter blocked"
	case gsm0348.RespCryptError:
		return "cryptographic error"
	case gsm0348.RespInsNotSupported:
		return "instruction not supported"
	case gsm0348.RespCNTRIncreaseNotAllowed:
		return "counter increase not allowed"
	case gsm0348.RespTARUnknown:
		return "TAR unknown"
	case gsm0348.RespInsufficientMemory:
		return "insufficient memory"
	default:
		return "unknown status"
	}
}
