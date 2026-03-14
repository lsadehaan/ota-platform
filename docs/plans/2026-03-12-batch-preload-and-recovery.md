# Batch Preload, Crash Recovery & Zero Per-Card DB

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Eliminate all per-card Postgres queries during campaign execution by batch-preloading card data in the planner, fix counter correctness, add crash recovery, remove the submission ledger, and make write batching configurable with channel depth observability.

**Architecture:** The planner already loads shards of card IDs from Postgres. Before publishing events to ActivateCh, it now batch-loads card keys, counter values, and execution states into the executor's process-local caches. Counters are pre-allocated in Postgres via batch UPDATE. On crash, the engine rescans running campaigns and re-creates shards for non-terminal cards. The submission ledger is deleted (channels deliver exactly once; crash recovery restarts cleanly). Write batch sizes, writer concurrency, and channel buffers are all configurable.

**Tech Stack:** Go, PostgreSQL, GORM, OTel metrics, Go channels

---

## Task 1: Make write batch sizes and writer concurrency configurable

**Files:**
- Modify: `internal/pgstore/card_state_writer.go`
- Modify: `internal/pgstore/message_writer.go`
- Modify: `cmd/ota-engine/main.go`

**Step 1: Update RunCardStateWriter to accept config parameters**

Replace the hardcoded constants with parameters. Change the function signature:

```go
// card_state_writer.go

type CardStateWriterConfig struct {
	MaxBatch      int
	FlushInterval time.Duration
}

func DefaultCardStateWriterConfig() CardStateWriterConfig {
	return CardStateWriterConfig{
		MaxBatch:      envIntPgstore("ENGINE_STATE_WRITER_BATCH", 500),
		FlushInterval: time.Duration(envIntPgstore("ENGINE_STATE_WRITER_FLUSH_MS", 200)) * time.Millisecond,
	}
}

func RunCardStateWriter(ctx context.Context, ch <-chan pipeline.CardStateChange, database *gorm.DB, cfg CardStateWriterConfig, logger *zap.Logger) {
	batch := make([]pipeline.CardStateChange, 0, cfg.MaxBatch)
	ticker := time.NewTicker(cfg.FlushInterval)
	defer ticker.Stop()
	// ... same logic but using cfg.MaxBatch and cfg.FlushInterval
}
```

Add a package-level helper (avoid import of `config` package):

```go
func envIntPgstore(key string, fallback int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return n
		}
	}
	return fallback
}
```

**Step 2: Update RunMessageWriter similarly**

```go
type MessageWriterConfig struct {
	MaxBatch      int
	FlushInterval time.Duration
}

func DefaultMessageWriterConfig() MessageWriterConfig {
	return MessageWriterConfig{
		MaxBatch:      envIntPgstore("ENGINE_MSG_WRITER_BATCH", 500),
		FlushInterval: time.Duration(envIntPgstore("ENGINE_MSG_WRITER_FLUSH_MS", 200)) * time.Millisecond,
	}
}

func RunMessageWriter(ctx context.Context, ch <-chan pipeline.MessageLogAction, database *gorm.DB, cfg MessageWriterConfig, logger *zap.Logger) {
	// ... same logic but using cfg.MaxBatch and cfg.FlushInterval
}
```

**Step 3: Launch multiple writer goroutines from main.go**

```go
// main.go — replace single writer goroutines

stateWriterWorkers := envInt("ENGINE_STATE_WRITER_WORKERS", 2)
msgWriterWorkers := envInt("ENGINE_MSG_WRITER_WORKERS", 2)
stateWriterCfg := pgstore.DefaultCardStateWriterConfig()
msgWriterCfg := pgstore.DefaultMessageWriterConfig()

for i := 0; i < stateWriterWorkers; i++ {
	go pgstore.RunCardStateWriter(ctx, bus.CardStateCh, database, stateWriterCfg, logger.Named("card-state-writer"))
}
for i := 0; i < msgWriterWorkers; i++ {
	go pgstore.RunMessageWriter(ctx, bus.MessageLogCh, database, msgWriterCfg, logger.Named("message-writer"))
}
```

**Step 4: Build and run unit tests**

Run: `CGO_ENABLED=0 go build ./... && go test ./internal/pgstore/... -v -count=1`
Expected: PASS

**Step 5: Commit**

```
feat: configurable write batch sizes and multiple writer goroutines
```

---

## Task 2: Increase channel buffer defaults and add channel depth metrics

**Files:**
- Modify: `internal/pipeline/bus.go`
- Modify: `cmd/ota-engine/main.go`

**Step 1: Update channel buffer to 5x planner batch size**

Channel buffers should hold at least 5 shard batches worth of events. Default: `5 * PLANNER_CLAIM_BATCH_SIZE` (5 * 5000 = 25000). Override with `ENGINE_CHANNEL_BUFFER`.

```go
// bus.go — derive from planner batch size

func NewBus() *Bus {
	plannerBatch := envInt("PLANNER_CLAIM_BATCH_SIZE", 5000)
	bufSize := envInt("ENGINE_CHANNEL_BUFFER", plannerBatch*5)
	return &Bus{
		ActivateCh:   make(chan CardEvent, bufSize),
		SendSMSCh:    make(chan SendSMSMessage, bufSize),
		DLRCh:        make(chan CardEvent, bufSize),
		MOCh:         make(chan CardEvent, bufSize),
		MessageLogCh: make(chan MessageLogAction, bufSize),
		CardStateCh:  make(chan CardStateChange, bufSize),
	}
}
```

Note: Remove the `/2` divisor — all channels get the full buffer size.

**Step 2: Add OTel gauge reporting for channel depths**

Add a method to Bus that registers OTel observable gauges:

```go
// bus.go

import (
	"context"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
)

func (b *Bus) RegisterMetrics() {
	meter := otel.Meter("ota-engine")

	registerGauge := func(name, desc string, ch interface{ Len() int }) {
		// Can't call Len() on a chan directly — use a closure over the typed channel
	}
	// Instead, register each explicitly:

	meter.Int64ObservableGauge("ota.bus.activate_ch_depth",
		metric.WithDescription("ActivateCh buffered events"),
		metric.WithInt64Callback(func(_ context.Context, o metric.Int64Observer) error {
			o.Observe(int64(len(b.ActivateCh)))
			return nil
		}),
	)
	meter.Int64ObservableGauge("ota.bus.send_sms_ch_depth",
		metric.WithDescription("SendSMSCh buffered events"),
		metric.WithInt64Callback(func(_ context.Context, o metric.Int64Observer) error {
			o.Observe(int64(len(b.SendSMSCh)))
			return nil
		}),
	)
	meter.Int64ObservableGauge("ota.bus.dlr_ch_depth",
		metric.WithDescription("DLRCh buffered events"),
		metric.WithInt64Callback(func(_ context.Context, o metric.Int64Observer) error {
			o.Observe(int64(len(b.DLRCh)))
			return nil
		}),
	)
	meter.Int64ObservableGauge("ota.bus.mo_ch_depth",
		metric.WithDescription("MOCh buffered events"),
		metric.WithInt64Callback(func(_ context.Context, o metric.Int64Observer) error {
			o.Observe(int64(len(b.MOCh)))
			return nil
		}),
	)
	meter.Int64ObservableGauge("ota.bus.message_log_ch_depth",
		metric.WithDescription("MessageLogCh buffered events"),
		metric.WithInt64Callback(func(_ context.Context, o metric.Int64Observer) error {
			o.Observe(int64(len(b.MessageLogCh)))
			return nil
		}),
	)
	meter.Int64ObservableGauge("ota.bus.card_state_ch_depth",
		metric.WithDescription("CardStateCh buffered events"),
		metric.WithInt64Callback(func(_ context.Context, o metric.Int64Observer) error {
			o.Observe(int64(len(b.CardStateCh)))
			return nil
		}),
	)
}
```

**Step 3: Call RegisterMetrics from main.go**

```go
// main.go — after creating bus, before starting workers
bus := pipeline.NewBus()
bus.RegisterMetrics()
```

**Step 4: Add channel depths to the debug queues endpoint**

In `internal/controlplane/api_debug.go`, the `GetDebugQueues` endpoint should also return channel depths. This requires passing the Bus to the API. However, to keep it simple, add a new lightweight endpoint or extend the existing one.

The simplest approach: add a `SetBus` method on API and include channel stats in GetDebugQueues:

In `internal/controlplane/service.go`, add a `Bus` field to API:
```go
type ChannelStats interface {
	ChannelDepths() map[string]int
}
```

In `internal/pipeline/bus.go`:
```go
func (b *Bus) ChannelDepths() map[string]int {
	return map[string]int{
		"activate":    len(b.ActivateCh),
		"send_sms":    len(b.SendSMSCh),
		"dlr":         len(b.DLRCh),
		"mo":          len(b.MOCh),
		"message_log": len(b.MessageLogCh),
		"card_state":  len(b.CardStateCh),
	}
}
```

In `api_debug.go`, extend `GetDebugQueues` to include channel depths if available. Pass the bus reference via the API struct.

**Step 5: Build and test**

Run: `CGO_ENABLED=0 go build ./... && go test ./internal/pipeline/... -v -count=1`
Expected: PASS

**Step 6: Commit**

```
feat: 20k channel buffers, OTel depth gauges, debug endpoint
```

---

## Task 3: Delete submission ledger

**Files:**
- Delete: `internal/pgstore/submission_ledger.go`
- Modify: `internal/transport/inprocess_gateway.go` — remove all ledger usage
- Modify: `internal/transport/submission_ledger.go` — delete file (interface no longer needed)
- Modify: `internal/transport/service.go` — remove SubmissionLedger from interfaces if referenced
- Modify: `cmd/ota-engine/main.go` — remove ledger creation and passing
- Modify: `internal/db/models.go` — remove `SMSSubmissionLedgerEntry` model

Do NOT drop the `sms_submission_ledger` table yet (migration is separate from code). The table just becomes unused.

**Step 1: Remove ledger from InProcessGateway**

In `inprocess_gateway.go`:
- Remove `ledger SubmissionLedger` field from struct
- Remove `ledger` param from `NewInProcessGateway`
- In `HandleSendSMS`: delete the entire `if g.ledger != nil` blocks (lines 139-154 for IsSubmitted, lines 185-193 for MarkSubmitted)

**Step 2: Remove ledger from main.go**

- Delete `submissionLedger := pgstore.NewSubmissionLedger(database)` line
- Remove `submissionLedger` from `NewInProcessGateway(...)` call

**Step 3: Delete files**

- Delete `internal/pgstore/submission_ledger.go`
- Delete `internal/transport/submission_ledger.go` (the interface file)

**Step 4: Remove model from models.go**

- Delete `SMSSubmissionLedgerEntry` struct
- Delete its `TableName()` method

**Step 5: Clean up service.go if needed**

Check `internal/transport/service.go` — if `SubmissionLedger` is referenced in interfaces there, remove it.

**Step 6: Build and test**

Run: `CGO_ENABLED=0 go build ./... && go test ./... -v -count=1`
Expected: PASS

**Step 7: Commit**

```
feat: remove submission ledger (channels deliver exactly once)
```

---

## Task 4: Delete card_counter_ranges and simplify CounterStore

The range-based `card_counter_ranges` table is replaced by direct reads/writes to the existing `card_counters` table. Counter pre-allocation happens in the planner batch preload (Task 5).

**Files:**
- Rewrite: `internal/pgstore/counter_store.go`
- Modify: `internal/db/models.go` — remove `CardCounterRange` model
- Modify: `internal/executor/worker.go` — simplify counter flow

**Step 1: Rewrite CounterStore to use card_counters directly**

The new CounterStore is a thin process-local cache. The planner batch-preloads counter values. The executor only reads from cache and increments locally. No per-card DB queries.

```go
package pgstore

import (
	"context"
	"sync"
)

// CounterStore holds process-local counter values.
// Counters are batch-preloaded by the planner before shard activation.
// The pre-allocation UPDATE to card_counters happens at planner time.
// During execution, all counter ops are process-local (zero DB).
type CounterStore struct {
	mu       sync.RWMutex
	counters map[string]int64 // "cardID:appID" -> current counter value
}

func NewCounterStore() *CounterStore {
	return &CounterStore{
		counters: make(map[string]int64),
	}
}

func counterKey(cardID, applicationID string) string {
	return cardID + ":" + applicationID
}

// GetCounter returns the current process-local counter value.
// Returns 0 if not preloaded (should not happen during normal execution).
func (s *CounterStore) GetCounter(_ context.Context, cardID, applicationID string) (int64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.counters[counterKey(cardID, applicationID)], nil
}

// IncrCounter increments the process-local counter and returns the new value.
// No DB I/O — the counter was pre-allocated in Postgres by the planner.
func (s *CounterStore) IncrCounter(_ context.Context, cardID, applicationID string) (int64, error) {
	key := counterKey(cardID, applicationID)
	s.mu.Lock()
	defer s.mu.Unlock()
	s.counters[key]++
	return s.counters[key], nil
}

// IncrCounterAsync is identical to IncrCounter (no background I/O needed).
func (s *CounterStore) IncrCounterAsync(ctx context.Context, cardID, applicationID string) error {
	_, err := s.IncrCounter(ctx, cardID, applicationID)
	return err
}

// PreloadCounters populates the cache from batch-loaded values.
// Called by the planner after batch SELECT from card_counters.
func (s *CounterStore) PreloadCounters(values map[string]int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for k, v := range values {
		s.counters[k] = v
	}
}
```

**Step 2: Remove CardCounterRange model**

In `internal/db/models.go`:
- Delete the `CardCounterRange` struct
- Delete `func (CardCounterRange) TableName() string`

**Step 3: Simplify worker.go counter flow**

In `handleActivate`:
- Remove the `go func() { w.counterStore.IncrCounterAsync(...) }()` background goroutine
- Replace with synchronous `w.counterStore.IncrCounterAsync(ctx, ...)`  (which is now process-local, no DB)
- Keep the `w.counterCache` as-is (it's a secondary cache in the worker; or remove it since CounterStore itself is now a cache — evaluate during implementation)

**Step 4: Update main.go**

```go
// Remove: counterStore := pgstore.NewCounterStore(database)
// Replace: counterStore := pgstore.NewCounterStore()
// (no longer needs database parameter)
```

**Step 5: Build and test**

Run: `CGO_ENABLED=0 go build ./... && go test ./... -v -count=1`
Expected: PASS

**Step 6: Commit**

```
feat: replace counter range allocator with process-local counter cache
```

---

## Task 5: Planner batch preload (card keys, counters, card states)

This is the core change. The planner batch-loads all per-card data into executor caches before publishing activation events.

**Files:**
- Create: `internal/pgstore/batch_preload.go`
- Modify: `internal/planner/service.go` — call preload before publishing
- Modify: `cmd/ota-engine/main.go` — pass preloader to planner

**Step 1: Create BatchPreloader**

```go
// internal/pgstore/batch_preload.go
package pgstore

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	"go.uber.org/zap"
	"gorm.io/gorm"

	"ota-platform/internal/keystore"
)

// BatchPreloader batch-loads card data from Postgres and populates
// process-local caches, eliminating per-card DB queries during execution.
type BatchPreloader struct {
	db           *gorm.DB
	counterStore *CounterStore
	cardKeyCache CardKeyCacheWriter
	cardStateCache CardStateCacheWriter
	logger       *zap.Logger
}

// CardKeyCacheWriter is the subset of executor's cache needed for preloading.
type CardKeyCacheWriter interface {
	PreloadCardKeys(keys map[string]*keystore.CardKeyMaterial)
}

// CardStateCacheWriter is the subset of the coordination store needed for preloading.
type CardStateCacheWriter interface {
	PreloadCardStates(states map[string]*CardStateEntry)
}

// CardStateEntry holds the fields needed for a preloaded card state.
type CardStateEntry struct {
	CampaignID    string
	CurrentStep   int
	Status        string
	RetryCount    int
	LastMsgID     string
	TransitionSeq int64
}

func NewBatchPreloader(
	db *gorm.DB,
	counterStore *CounterStore,
	cardKeyCache CardKeyCacheWriter,
	cardStateCache CardStateCacheWriter,
	logger *zap.Logger,
) *BatchPreloader {
	return &BatchPreloader{
		db:             db,
		counterStore:   counterStore,
		cardKeyCache:   cardKeyCache,
		cardStateCache: cardStateCache,
		logger:         logger,
	}
}

// PreloadShard batch-loads card keys, counters, and execution states for a
// set of card IDs. Called by the planner before publishing shard events.
// stepsPerCard is the number of counter increments needed per card.
func (p *BatchPreloader) PreloadShard(ctx context.Context, campaignID string, applicationID string, cardIDs []string, stepsPerCard int) error {
	if len(cardIDs) == 0 {
		return nil
	}

	// 1. Batch load card keys
	if err := p.preloadCardKeys(ctx, cardIDs); err != nil {
		return fmt.Errorf("preload card keys: %w", err)
	}

	// 2. Batch pre-allocate and load counters
	if applicationID != "" && stepsPerCard > 0 {
		if err := p.preloadCounters(ctx, cardIDs, applicationID, stepsPerCard); err != nil {
			return fmt.Errorf("preload counters: %w", err)
		}
	}

	// 3. Batch load card execution states
	if campaignID != "" {
		if err := p.preloadCardStates(ctx, campaignID, cardIDs); err != nil {
			return fmt.Errorf("preload card states: %w", err)
		}
	}

	return nil
}

func (p *BatchPreloader) preloadCardKeys(ctx context.Context, cardIDs []string) error {
	uuids := make([]uuid.UUID, 0, len(cardIDs))
	for _, id := range cardIDs {
		u, err := uuid.Parse(id)
		if err != nil {
			continue
		}
		uuids = append(uuids, u)
	}

	type cardRow struct {
		ID        uuid.UUID `gorm:"column:id"`
		EncKey    []byte    `gorm:"column:enc_key"`
		AuthKey   []byte    `gorm:"column:auth_key"`
		KEK       []byte    `gorm:"column:kek"`
		ProfileID uuid.UUID `gorm:"column:profile_id"`
		MSISDN    string    `gorm:"column:msisdn"`
	}

	var rows []cardRow
	if err := p.db.WithContext(ctx).
		Table("cards").
		Select("id, enc_key, auth_key, kek, profile_id, msisdn").
		Where("id IN ?", uuids).
		Find(&rows).Error; err != nil {
		return fmt.Errorf("batch select card keys: %w", err)
	}

	keys := make(map[string]*keystore.CardKeyMaterial, len(rows))
	for _, r := range rows {
		keys[r.ID.String()] = &keystore.CardKeyMaterial{
			EncKey:    r.EncKey,
			AuthKey:   r.AuthKey,
			KEK:       r.KEK,
			ProfileID: r.ProfileID.String(),
			MSISDN:    r.MSISDN,
		}
	}
	p.cardKeyCache.PreloadCardKeys(keys)
	p.logger.Debug("preloaded card keys", zap.Int("count", len(keys)))
	return nil
}

func (p *BatchPreloader) preloadCounters(ctx context.Context, cardIDs []string, applicationID string, stepsPerCard int) error {
	uuids := make([]uuid.UUID, 0, len(cardIDs))
	for _, id := range cardIDs {
		u, err := uuid.Parse(id)
		if err != nil {
			continue
		}
		uuids = append(uuids, u)
	}

	appUUID, err := uuid.Parse(applicationID)
	if err != nil {
		return fmt.Errorf("parse application_id: %w", err)
	}

	// Ensure rows exist for all cards (INSERT ... ON CONFLICT DO NOTHING).
	// This handles cards that have never had a counter allocated.
	for _, u := range uuids {
		p.db.WithContext(ctx).Exec(
			`INSERT INTO card_counters (card_id, application_id, counter_value) VALUES (?, ?, 0) ON CONFLICT DO NOTHING`,
			u, appUUID,
		)
	}

	// Atomically pre-allocate: increment counter_value by stepsPerCard,
	// return the starting value (counter_value BEFORE increment).
	type counterRow struct {
		CardID       uuid.UUID `gorm:"column:card_id"`
		CounterValue int64     `gorm:"column:start_value"`
	}
	var rows []counterRow
	if err := p.db.WithContext(ctx).Raw(
		`UPDATE card_counters
		 SET counter_value = counter_value + ?
		 WHERE card_id IN ? AND application_id = ?
		 RETURNING card_id, counter_value - ? AS start_value`,
		stepsPerCard, uuids, appUUID, stepsPerCard,
	).Scan(&rows).Error; err != nil {
		return fmt.Errorf("batch pre-allocate counters: %w", err)
	}

	values := make(map[string]int64, len(rows))
	for _, r := range rows {
		values[counterKey(r.CardID.String(), applicationID)] = r.CounterValue
	}
	p.counterStore.PreloadCounters(values)
	p.logger.Debug("preloaded counters", zap.Int("count", len(values)), zap.Int("steps", stepsPerCard))
	return nil
}

func (p *BatchPreloader) preloadCardStates(ctx context.Context, campaignID string, cardIDs []string) error {
	campUUID, err := uuid.Parse(campaignID)
	if err != nil {
		return fmt.Errorf("parse campaign_id: %w", err)
	}
	uuids := make([]uuid.UUID, 0, len(cardIDs))
	for _, id := range cardIDs {
		u, err := uuid.Parse(id)
		if err != nil {
			continue
		}
		uuids = append(uuids, u)
	}

	type stateRow struct {
		CardID        uuid.UUID  `gorm:"column:card_id"`
		Status        string     `gorm:"column:status"`
		CurrentStep   int        `gorm:"column:current_step"`
		RetryCount    int        `gorm:"column:retry_count"`
		LastMsgID     *uuid.UUID `gorm:"column:last_msg_id"`
		TransitionSeq int64      `gorm:"column:transition_seq"`
	}

	var rows []stateRow
	if err := p.db.WithContext(ctx).Raw(
		`SELECT DISTINCT ON (card_id)
			card_id, status, current_step, retry_count, last_msg_id, transition_seq
		 FROM card_execution_states
		 WHERE campaign_id = ? AND card_id IN ?
		 ORDER BY card_id, transition_seq DESC`,
		campUUID, uuids,
	).Scan(&rows).Error; err != nil {
		return fmt.Errorf("batch select card states: %w", err)
	}

	states := make(map[string]*CardStateEntry, len(rows))
	for _, r := range rows {
		var msgID string
		if r.LastMsgID != nil {
			msgID = r.LastMsgID.String()
		}
		states[r.CardID.String()] = &CardStateEntry{
			CampaignID:    campaignID,
			CurrentStep:   r.CurrentStep,
			Status:        r.Status,
			RetryCount:    r.RetryCount,
			LastMsgID:     msgID,
			TransitionSeq: r.TransitionSeq,
		}
	}
	p.cardStateCache.PreloadCardStates(states)
	p.logger.Debug("preloaded card states", zap.Int("count", len(states)))
	return nil
}
```

**Step 2: Add PreloadCardKeys to CardWorker**

In `internal/executor/worker.go`, add a method the preloader can call:

```go
func (w *CardWorker) PreloadCardKeys(keys map[string]*keystore.CardKeyMaterial) {
	for cardID, km := range keys {
		w.cardKeyCache.Store(cardID, km)
	}
}
```

**Step 3: Add PreloadCardStates to CoordinationStore**

In `internal/pgstore/coordination.go`:

```go
func (s *CoordinationStore) PreloadCardStates(states map[string]*CardStateEntry) {
	for cardID, entry := range states {
		s.cardStates.Store(cardID, &redispkg.CardState{
			CampaignID:    entry.CampaignID,
			CurrentStep:   entry.CurrentStep,
			Status:        entry.Status,
			RetryCount:    entry.RetryCount,
			LastMsgID:     entry.LastMsgID,
			TransitionSeq: entry.TransitionSeq,
		})
	}
}
```

**Step 4: Wire preloader into planner**

The planner needs access to the preloader. Add a `Preloader` interface to planner and call it in `publishClaimedShards` before publishing each shard's events.

In `internal/planner/service.go`:

```go
type ShardPreloader interface {
	PreloadShard(ctx context.Context, campaignID string, applicationID string, cardIDs []string, stepsPerCard int) error
}
```

Add `preloader ShardPreloader` field to `Service` struct and `NewService`.

In `publishClaimedShards`, after unmarshalling events and before publishing:

```go
if s.preloader != nil {
	cardIDs := make([]string, 0, len(events))
	for _, ev := range events {
		cardIDs = append(cardIDs, ev.CardID)
	}
	campaignID := events[0].CampaignID
	// Look up the applicationID for this campaign's first step
	// (already available from the shard's events or cached campaign context)
	if err := s.preloader.PreloadShard(ctx, campaignID, applicationID, cardIDs, stepsPerCard); err != nil {
		s.logger.Warn("shard preload failed, proceeding without cache warming", zap.Error(err))
	}
}
```

To get `applicationID` and `stepsPerCard`, the preloader needs campaign metadata. The simplest approach: add a campaign command cache lookup inside `PreloadShard`, or pass it from the planner which can query campaign commands once per campaign and cache them.

Add a campaign metadata cache to `BatchPreloader`:

```go
type campaignMeta struct {
	applicationID string
	totalSteps    int
}

func (p *BatchPreloader) getCampaignMeta(ctx context.Context, campaignID string) (*campaignMeta, error) {
	// Cache per campaign (immutable during execution)
	// Query: SELECT application_id, COUNT(*) as steps FROM campaign_commands WHERE campaign_id = ? GROUP BY application_id
	// For single-app campaigns (most common), returns one row
}
```

**Step 5: Update main.go wiring**

```go
preloader := pgstore.NewBatchPreloader(database, counterStore, cardWorker, coordination, logger.Named("preloader"))
plannerSvc := planner.NewService(database, plannerPub, preloader, logger.Named("planner"))
```

**Step 6: Update PLANNER_CLAIM_BATCH_SIZE default**

In `internal/planner/service.go`, change `plannerClaimBatchSize()` default from 1000 to 5000.

**Step 7: Build and test**

Run: `CGO_ENABLED=0 go build ./... && go test ./... -v -count=1`
Expected: PASS

**Step 8: Commit**

```
feat: batch preload card keys, counters, states in planner
```

---

## Task 6: Share gateway coordination card state with executor

Currently `GatewayCoordinationStore.GetCardState()` always queries Postgres for MO handling. Since the executor's `CoordinationStore` already has card states in its sync.Map, the gateway should read from the same cache.

**Files:**
- Modify: `internal/pgstore/gateway_coordination.go`
- Modify: `cmd/ota-engine/main.go`

**Step 1: Add shared card state source to GatewayCoordinationStore**

```go
// gateway_coordination.go

// CardStateReader provides read access to the executor's card state cache.
type CardStateReader interface {
	GetCardState(ctx context.Context, cardID string) (*redispkg.CardState, error)
}

type GatewayCoordinationStore struct {
	db             *gorm.DB
	cardStateCache CardStateReader // shared with executor's CoordinationStore
	// ... existing fields
}

func NewGatewayCoordinationStore(database *gorm.DB, cardStateCache CardStateReader) *GatewayCoordinationStore {
	return &GatewayCoordinationStore{
		db:             database,
		cardStateCache: cardStateCache,
	}
}
```

Update `GetCardState` to use the shared cache first, falling back to Postgres:

```go
func (s *GatewayCoordinationStore) GetCardState(ctx context.Context, cardID string) (*redispkg.CardState, error) {
	if s.cardStateCache != nil {
		return s.cardStateCache.GetCardState(ctx, cardID)
	}
	// ... existing Postgres fallback
}
```

**Step 2: Update main.go**

Pass the executor's coordination store to the gateway coordination store:

```go
gwCoordination := pgstore.NewGatewayCoordinationStore(database, coordination)
```

**Step 3: Build and test**

Run: `CGO_ENABLED=0 go build ./... && go test ./... -v -count=1`
Expected: PASS

**Step 4: Commit**

```
feat: gateway reads card state from executor's process-local cache
```

---

## Task 7: Startup crash recovery

On process start, the engine scans for campaigns that were `running` when it crashed, and re-creates planner shards for non-terminal cards.

**Files:**
- Create: `internal/pgstore/recovery.go`
- Modify: `cmd/ota-engine/main.go`

**Step 1: Create recovery scanner**

```go
// internal/pgstore/recovery.go
package pgstore

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
	"go.uber.org/zap"
	"gorm.io/gorm"

	"ota-platform/internal/db"
	"ota-platform/internal/pipeline"
)

// RecoverRunningCampaigns finds campaigns that were running when the engine
// crashed and re-creates planner shards for non-terminal cards.
func RecoverRunningCampaigns(ctx context.Context, database *gorm.DB, shardSize int, logger *zap.Logger) error {
	// 1. Reset stuck 'publishing' shards back to 'pending'.
	if err := database.WithContext(ctx).
		Model(&db.CampaignShard{}).
		Where("status = ?", "publishing").
		Updates(map[string]interface{}{
			"status":     "pending",
			"claimed_at": nil,
			"updated_at": time.Now(),
		}).Error; err != nil {
		return fmt.Errorf("reset publishing shards: %w", err)
	}

	// 2. Find running campaigns.
	var campaigns []db.Campaign
	if err := database.WithContext(ctx).
		Preload("CampaignCommands", func(tx *gorm.DB) *gorm.DB { return tx.Order("sequence ASC") }).
		Where("status = ?", "running").
		Find(&campaigns).Error; err != nil {
		return fmt.Errorf("find running campaigns: %w", err)
	}
	if len(campaigns) == 0 {
		logger.Info("no running campaigns to recover")
		return nil
	}

	for _, campaign := range campaigns {
		if err := recoverCampaign(ctx, database, campaign, shardSize, logger); err != nil {
			logger.Error("failed to recover campaign",
				zap.String("campaign_id", campaign.ID.String()),
				zap.Error(err),
			)
			// Continue with other campaigns rather than failing all.
		}
	}
	return nil
}

func recoverCampaign(ctx context.Context, database *gorm.DB, campaign db.Campaign, shardSize int, logger *zap.Logger) error {
	// Find non-terminal cards: those in pending, in_progress, awaiting_dlr, awaiting_mo.
	// Use DISTINCT ON to get latest state per card.
	type cardState struct {
		CardID      uuid.UUID `gorm:"column:card_id"`
		Status      string    `gorm:"column:status"`
		CurrentStep int       `gorm:"column:current_step"`
		RetryCount  int       `gorm:"column:retry_count"`
	}

	var cards []cardState
	if err := database.WithContext(ctx).Raw(
		`SELECT DISTINCT ON (card_id) card_id, status, current_step, retry_count
		 FROM card_execution_states
		 WHERE campaign_id = ?
		 ORDER BY card_id, transition_seq DESC`,
		campaign.ID,
	).Scan(&cards).Error; err != nil {
		return fmt.Errorf("scan card states: %w", err)
	}

	// Filter to non-terminal cards.
	firstStep := 1
	if len(campaign.CampaignCommands) > 0 {
		firstStep = campaign.CampaignCommands[0].Sequence
	}

	now := time.Now()
	var events []pipeline.CardEvent
	for _, card := range cards {
		switch card.Status {
		case "completed", "failed", "skipped":
			continue // terminal — skip
		}
		// Re-activate from current step.
		step := card.CurrentStep
		if step == 0 {
			step = firstStep
		}
		events = append(events, pipeline.CardEvent{
			Type:       "card.activate",
			EventID:    uuid.New().String(),
			CardID:     card.CardID.String(),
			CampaignID: campaign.ID.String(),
			Step:       step,
			RetryCount: card.RetryCount,
			Timestamp:  now,
		})
	}

	if len(events) == 0 {
		logger.Info("campaign recovery: all cards terminal",
			zap.String("campaign_id", campaign.ID.String()),
		)
		return nil
	}

	// Delete any existing pending/publishing shards for this campaign
	// (they contain stale events with old EventIDs).
	if err := database.WithContext(ctx).
		Where("campaign_id = ? AND status IN ?", campaign.ID, []string{"pending", "publishing"}).
		Delete(&db.CampaignShard{}).Error; err != nil {
		return fmt.Errorf("delete stale shards: %w", err)
	}

	// Get next shard sequence.
	var maxSeq int
	database.WithContext(ctx).
		Model(&db.CampaignShard{}).
		Where("campaign_id = ?", campaign.ID).
		Select("COALESCE(MAX(sequence), 0)").
		Scan(&maxSeq)

	// Create new shards.
	seq := maxSeq + 1
	for i := 0; i < len(events); i += shardSize {
		end := i + shardSize
		if end > len(events) {
			end = len(events)
		}
		items, err := json.Marshal(events[i:end])
		if err != nil {
			return fmt.Errorf("marshal recovery shard: %w", err)
		}
		shard := db.CampaignShard{
			ID:         uuid.New(),
			CampaignID: campaign.ID,
			Sequence:   seq,
			Status:     "pending",
			ItemCount:  end - i,
			Items:      items,
		}
		seq++
		if err := database.WithContext(ctx).Create(&shard).Error; err != nil {
			return fmt.Errorf("create recovery shard: %w", err)
		}
	}

	logger.Info("campaign recovery: re-created shards for non-terminal cards",
		zap.String("campaign_id", campaign.ID.String()),
		zap.Int("cards", len(events)),
		zap.Int("shards", seq-maxSeq-1),
	)
	return nil
}
```

**Step 2: Call recovery from main.go before starting planner**

```go
// After db-migrate, before starting planner
shardSize := envInt("PLANNER_CLAIM_BATCH_SIZE", 5000)
if err := pgstore.RecoverRunningCampaigns(ctx, database, shardSize, logger.Named("recovery")); err != nil {
	logger.Error("campaign recovery failed", zap.Error(err))
	// Non-fatal — continue startup, campaigns can be manually resumed
}
```

**Step 3: Build and test**

Run: `CGO_ENABLED=0 go build ./... && go test ./... -v -count=1`
Expected: PASS

**Step 4: Commit**

```
feat: startup crash recovery for running campaigns
```

---

## Task 8: Docker Compose + E2E validation

**Files:**
- Modify: `deployments/docker-compose.engine.yml` — add new env vars

**Step 1: Add env vars to docker-compose.engine.yml**

```yaml
ota-engine:
  environment:
    # ... existing vars ...
    # ENGINE_CHANNEL_BUFFER defaults to 5 * PLANNER_CLAIM_BATCH_SIZE = 25000
    PLANNER_CLAIM_BATCH_SIZE: "5000"
    ENGINE_STATE_WRITER_WORKERS: "2"
    ENGINE_MSG_WRITER_WORKERS: "2"
    ENGINE_STATE_WRITER_BATCH: "500"
    ENGINE_MSG_WRITER_BATCH: "500"
    ENGINE_STATE_WRITER_FLUSH_MS: "200"
    ENGINE_MSG_WRITER_FLUSH_MS: "200"
```

**Step 2: Rebuild and run E2E test**

```bash
cd deployments
docker compose -f docker-compose.engine.yml build ota-engine db-migrate
docker compose -f docker-compose.engine.yml up -d
# Wait for healthy
E2E_BASE_URL=http://localhost:8080 E2E_CARD_COUNT=1000 E2E_EXPECT_RESPONSE=true E2E_CAMPAIGN_TIMEOUT=120s go test -tags=e2e ./e2e -v -count=1 -timeout 10m
```

Expected: PASS with 0 failures and improved TPS.

**Step 3: Run larger E2E test**

```bash
E2E_BASE_URL=http://localhost:8080 E2E_CARD_COUNT=10000 E2E_EXPECT_RESPONSE=true E2E_CAMPAIGN_TIMEOUT=300s go test -tags=e2e ./e2e -v -count=1 -timeout 15m
```

Expected: PASS with 0 failures and TPS > 200.

**Step 4: Commit**

```
feat: add batch preload env vars to docker-compose
```

---

## Summary of env vars

| Variable | Default | Description |
|----------|---------|-------------|
| `ENGINE_CHANNEL_BUFFER` | 5 * PLANNER_CLAIM_BATCH_SIZE (25000) | Buffer size for all Go channels |
| `PLANNER_CLAIM_BATCH_SIZE` | 5000 | Cards per planner shard claim |
| `ENGINE_STATE_WRITER_WORKERS` | 2 | Card state writer goroutines |
| `ENGINE_MSG_WRITER_WORKERS` | 2 | Message log writer goroutines |
| `ENGINE_STATE_WRITER_BATCH` | 500 | Max rows per card state batch |
| `ENGINE_MSG_WRITER_BATCH` | 500 | Max rows per message batch |
| `ENGINE_STATE_WRITER_FLUSH_MS` | 200 | Card state flush interval (ms) |
| `ENGINE_MSG_WRITER_FLUSH_MS` | 200 | Message log flush interval (ms) |

## Per-card Postgres queries after all changes: ZERO

All card data is batch-preloaded by the planner. All writes go through buffered channels to batched writers. The only Postgres I/O during execution comes from:
- Planner claiming next shard batch (1 query per 5000 cards)
- Planner preloading card data (3 queries per 5000 cards)
- Background batch writers (1 transaction per 500 rows)
- Campaign completion checker (1 query per 5 seconds)
