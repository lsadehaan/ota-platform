package pgstore

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/google/uuid"
	"go.uber.org/zap"
	"gorm.io/gorm"

	"ota-platform/internal/keystore"
)

// CardKeyCacheWriter allows the batch preloader to populate the executor's
// process-local card key cache before shard events are published.
type CardKeyCacheWriter interface {
	PreloadCardKeys(keys map[string]*keystore.CardKeyMaterial)
}

// MSISDNCacheWriter allows the batch preloader to populate the MSISDN
// resolver's process-local cache from card key data already being loaded.
type MSISDNCacheWriter interface {
	PreloadMSISDNs(mappings map[string]string)
}

// BatchPreloader batch-loads card data from Postgres and populates
// process-local caches before shard events are published to the executor.
// This eliminates per-card Postgres queries during campaign execution.
type BatchPreloader struct {
	db           *gorm.DB
	counterStore *CounterStore
	cardKeyCache CardKeyCacheWriter
	msisdnCache  MSISDNCacheWriter
	logger       *zap.Logger

	// campaignMeta caches campaign command metadata (immutable during execution).
	campaignMetaMu sync.RWMutex
	campaignMeta   map[string]*campaignCommandMeta
}

type campaignCommandMeta struct {
	applicationID string
	stepCount     int
}

// NewBatchPreloader creates a BatchPreloader that populates the given caches.
func NewBatchPreloader(db *gorm.DB, counterStore *CounterStore, cardKeyCache CardKeyCacheWriter, msisdnCache MSISDNCacheWriter, logger *zap.Logger) *BatchPreloader {
	return &BatchPreloader{
		db:           db,
		counterStore: counterStore,
		cardKeyCache: cardKeyCache,
		msisdnCache:  msisdnCache,
		logger:       logger,
		campaignMeta: make(map[string]*campaignCommandMeta),
	}
}

// ShardPreloader is the interface consumed by the planner.
type ShardPreloader interface {
	PreloadShard(ctx context.Context, campaignID string, cardIDs []string) error
}

// PreloadShard batch-loads card keys, counters, and card execution states
// for all cards in a shard, populating process-local caches before events
// are published. Errors are non-fatal — the executor falls back to per-card
// lookups on cache miss.
func (p *BatchPreloader) PreloadShard(ctx context.Context, campaignID string, cardIDs []string) error {
	if len(cardIDs) == 0 {
		return nil
	}

	// 1. Batch load card keys
	if err := p.preloadCardKeys(ctx, cardIDs); err != nil {
		return fmt.Errorf("preload card keys: %w", err)
	}

	// 2. Batch pre-allocate and load counters
	meta, err := p.getCampaignMeta(ctx, campaignID)
	if err != nil {
		p.logger.Warn("failed to get campaign meta for counter preload", zap.Error(err))
	} else if meta != nil && meta.applicationID != "" && meta.stepCount > 0 {
		if err := p.preloadCounters(ctx, cardIDs, meta.applicationID, meta.stepCount); err != nil {
			return fmt.Errorf("preload counters: %w", err)
		}
	}

	// 3. Card state preload skipped: cards in pending shards have known state
	// (pending, step=0). The executor's GetCardState falls back to a fast
	// per-card index scan (0.1ms) on cache miss. The batch DISTINCT ON query
	// degrades as the append-only table grows during campaign execution.

	return nil
}

// preloadCardKeys batch-SELECTs card key material and populates the executor cache.
func (p *BatchPreloader) preloadCardKeys(ctx context.Context, cardIDs []string) error {
	uuids := make([]uuid.UUID, 0, len(cardIDs))
	for _, id := range cardIDs {
		u, err := uuid.Parse(id)
		if err != nil {
			continue
		}
		uuids = append(uuids, u)
	}
	if len(uuids) == 0 {
		return nil
	}

	type cardKeyRow struct {
		ID        uuid.UUID `gorm:"column:id"`
		EncKey    []byte    `gorm:"column:enc_key"`
		AuthKey   []byte    `gorm:"column:auth_key"`
		KEK       []byte    `gorm:"column:kek"`
		ProfileID string    `gorm:"column:profile_id"`
		MSISDN    string    `gorm:"column:msisdn"`
	}

	// Use array param instead of IN (1000 UUIDs) for fewer bind params.
	uuidStrs := make([]string, len(uuids))
	for i, u := range uuids {
		uuidStrs[i] = u.String()
	}
	arrayLit := "{" + strings.Join(uuidStrs, ",") + "}"

	var rows []cardKeyRow
	if err := p.db.WithContext(ctx).
		Table("cards").
		Select("id, enc_key, auth_key, kek, profile_id, msisdn").
		Where("id = ANY(?::uuid[])", arrayLit).
		Find(&rows).Error; err != nil {
		return fmt.Errorf("batch select card keys: %w", err)
	}

	keys := make(map[string]*keystore.CardKeyMaterial, len(rows))
	for _, row := range rows {
		if row.EncKey == nil && row.AuthKey == nil {
			continue // skip cards without key material
		}
		keys[row.ID.String()] = &keystore.CardKeyMaterial{
			EncKey:    row.EncKey,
			AuthKey:   row.AuthKey,
			KEK:       row.KEK,
			ProfileID: row.ProfileID,
			MSISDN:    row.MSISDN,
		}
	}

	if len(keys) > 0 {
		p.cardKeyCache.PreloadCardKeys(keys)
	}

	// Populate MSISDN resolver cache from the same data (avoids per-card
	// Postgres queries when MO messages arrive for these cards).
	if p.msisdnCache != nil {
		msisdns := make(map[string]string, len(rows))
		for _, row := range rows {
			if row.MSISDN != "" {
				msisdns[row.MSISDN] = row.ID.String()
			}
		}
		if len(msisdns) > 0 {
			p.msisdnCache.PreloadMSISDNs(msisdns)
		}
	}

	p.logger.Debug("preloaded card keys",
		zap.Int("requested", len(cardIDs)),
		zap.Int("loaded", len(keys)),
	)
	return nil
}

// preloadCounters ensures counter rows exist in Postgres and pre-allocates
// counter values for the shard, populating the process-local CounterStore.
func (p *BatchPreloader) preloadCounters(ctx context.Context, cardIDs []string, applicationID string, stepsPerCard int) error {
	appUUID, err := uuid.Parse(applicationID)
	if err != nil {
		return fmt.Errorf("parse application_id %s: %w", applicationID, err)
	}

	uuids := make([]uuid.UUID, 0, len(cardIDs))
	for _, id := range cardIDs {
		u, err := uuid.Parse(id)
		if err != nil {
			continue
		}
		uuids = append(uuids, u)
	}
	if len(uuids) == 0 {
		return nil
	}

	// 1. Ensure rows exist: batch INSERT using unnest with array literal.
	// Sends 2 params (array + app UUID) instead of 3000 individual params.
	cardUUIDStrs := make([]string, len(uuids))
	for i, u := range uuids {
		cardUUIDStrs[i] = u.String()
	}
	cardArrayLit := "{" + strings.Join(cardUUIDStrs, ",") + "}"
	insertSQL := `INSERT INTO card_counters (card_id, application_id, counter_value)
		SELECT unnest($1::uuid[]), $2::uuid, 0
		ON CONFLICT DO NOTHING`
	if err := p.db.WithContext(ctx).Exec(insertSQL, cardArrayLit, appUUID).Error; err != nil {
		return fmt.Errorf("bulk insert counters: %w", err)
	}

	// 2. Atomically pre-allocate counter range:
	// UPDATE ... SET counter_value = counter_value + stepsPerCard
	// RETURNING card_id, counter_value - stepsPerCard AS start_value
	type counterResult struct {
		CardID     uuid.UUID `gorm:"column:card_id"`
		StartValue int64     `gorm:"column:start_value"`
	}
	var results []counterResult
	if err := p.db.WithContext(ctx).Raw(
		`UPDATE card_counters
		 SET counter_value = counter_value + $1
		 WHERE card_id = ANY($2::uuid[]) AND application_id = $3
		 RETURNING card_id, counter_value - $1 AS start_value`,
		stepsPerCard, cardArrayLit, appUUID,
	).Scan(&results).Error; err != nil {
		return fmt.Errorf("pre-allocate counters: %w", err)
	}

	// 3. Populate process-local CounterStore
	values := make(map[string]int64, len(results))
	for _, r := range results {
		key := counterKey(r.CardID.String(), applicationID)
		values[key] = r.StartValue
	}
	if len(values) > 0 {
		p.counterStore.PreloadCounters(values)
	}

	p.logger.Debug("preloaded counters",
		zap.Int("cards", len(cardIDs)),
		zap.Int("allocated", len(results)),
		zap.Int("steps_per_card", stepsPerCard),
	)
	return nil
}

// getCampaignMeta returns cached campaign command metadata (application_id, step_count).
// The result is cached because campaign commands are immutable during execution.
func (p *BatchPreloader) getCampaignMeta(ctx context.Context, campaignID string) (*campaignCommandMeta, error) {
	if campaignID == "" {
		return nil, nil
	}

	p.campaignMetaMu.RLock()
	if meta, ok := p.campaignMeta[campaignID]; ok {
		p.campaignMetaMu.RUnlock()
		return meta, nil
	}
	p.campaignMetaMu.RUnlock()

	type metaRow struct {
		ApplicationID uuid.UUID `gorm:"column:application_id"`
		StepCount     int       `gorm:"column:step_count"`
	}

	var rows []metaRow
	if err := p.db.WithContext(ctx).Raw(
		`SELECT application_id, COUNT(*) AS step_count
		 FROM campaign_commands
		 WHERE campaign_id = ?
		 GROUP BY application_id`,
		campaignID,
	).Scan(&rows).Error; err != nil {
		return nil, fmt.Errorf("query campaign commands meta: %w", err)
	}

	if len(rows) == 0 {
		return nil, nil
	}

	// Use the first application_id with the total step count across all applications.
	totalSteps := 0
	for _, r := range rows {
		totalSteps += r.StepCount
	}

	meta := &campaignCommandMeta{
		applicationID: rows[0].ApplicationID.String(),
		stepCount:     totalSteps,
	}

	p.campaignMetaMu.Lock()
	p.campaignMeta[campaignID] = meta
	p.campaignMetaMu.Unlock()

	return meta, nil
}
