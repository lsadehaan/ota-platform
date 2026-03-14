# OTA Platform — Performance & Horizontal Scalability Review

**Date:** 2026-03-10
**Baseline:** ~260 TPS sustained on 200k-card campaign (4 ARM CPUs, 24GB RAM, single host)
**Goal:** Identify concrete changes to reach true horizontal scalability

---

## Part 1: Design Principles Scorecard

| Principle | Grade | Key Strength | Key Gap |
|-----------|-------|-------------|---------|
| Backpressure & Flow Control | B+ | SMPP windowing, planner pacing, blocking deliver queue | No signal when commitCh backs up; SMPP pool busy-waits |
| Partitioning & Sharding | A- | FNV bucketing, Kafka key routing, per-key worker dispatch | No token-aware ScyllaDB routing (single-node OK, multi-node penalty) |
| Batching & Pipelining | B | Kafka producer batching, projector chunking | Redis calls unbatched in hot path; JSON serialization per message |
| Connection Pooling | B+ | Configurable pools for all stores | QUORUM hardcoded (harmless at RF=1, wrong at RF=3) |
| Idempotency | B+ | Redis dedupe, ScyllaDB LWW timestamps, append-only tables | RecordPartDLR not atomic (3 Redis ops, race possible) |
| Caching | C+ | Redis cache-aside for campaign data | No process-local cache — 600k+ Redis roundtrips per 200k campaign |
| Memory Efficiency | B- | Pre-sized slices, no timer leaks | No sync.Pool, no GOMEMLIMIT, json.Marshal on every message |
| Graceful Degradation | B | SMPP reconnect, ScyllaDB retry policy, poison pills | No circuit breakers on Redis or ScyllaDB |
| Observability | A- | Full OTel metrics, tracing, structured logging | Missing metric for projector unmarshal errors |

---

## Part 2: Issues Found

### P17. Write amplification across executor + projector (ARCHITECTURAL)

**Location:** `internal/executor/worker.go`, `internal/scylla/card_state_store.go`, `internal/scylla/counter_store.go`, projector message store

**The Problem:**

This is the single most important architectural finding. The total ScyllaDB write volume per card event is massive:

**Executor per card activation (7-9 ops):**
- `GetCardKeys` → 1 read
- `IncrCounter` → 1 UPDATE + 1 SELECT (`counter_store.go:45-61`)
- `WriteCardState("in_progress")` → 2-3 INSERTs (card_state_by_card, campaign_card_status_by_bucket, optionally failed_cards)
- `WriteCardState("completed")` → 2-3 INSERTs

**Projector per message event (8-12 ops):**
- 6-8 table inserts (message_by_id, message_by_card_time, message_by_campaign_bucket_time, message_by_time_bucket, message_by_direction_time_bucket, message_by_status_time_bucket, message_by_dlr_status_time_bucket)
- 2-4 counter updates (metrics tables)

**Total: ~6400 ScyllaDB writes/sec at 222 TPS — all on a single `--smp 1` reactor.**

The `message_by_status_time_bucket` and `message_by_dlr_status_time_bucket` tables require DELETE + INSERT on every status change (status is in the partition key). These are the most expensive projector writes.

**Why it matters:** This is the real throughput ceiling. Even with more ScyllaDB nodes, the write amplification dominates I/O budget. Every logical card event fans out into 15-21 physical writes.

---

### P1. No process-local campaign context cache (CRITICAL)

**Location:** `internal/executor/worker.go:1022` (`loadCampaignContext`)

Every card event (activate, DLR, MO) calls:
- `loadCampaignCommands()` (line 969) → `w.redis.GetCampaignCommands()` (line 970)
- `loadCampaignParamsCached()` (line 945) → `w.redis.GetCachedCampaignParams()` (line 946)

Campaign commands and params are **immutable during execution**. For 200k cards × 3 events/card = **600k Redis roundtrips for data that doesn't change**.

On first access, all workers for the same campaign miss the Redis cache simultaneously, triggering parallel DB queries for the same data.

---

### P14. Hot partition in `message_metrics_by_minute` (CRITICAL)

**Location:** `internal/scylla/client.go:311`

```sql
CREATE TABLE IF NOT EXISTS message_metrics_by_minute (
    minute_bucket timestamp PRIMARY KEY,  -- ALL writes converge here
    total_messages counter,
    ...
)
```

`PRIMARY KEY (minute_bucket)` means **all counter writes for a given minute converge on a single partition**. At 222 TPS, that's 222+ counter writes/sec to one shard. Unlike the message tables (which use `global_bucket` to spread writes), these metrics tables have no bucketing.

`message_metrics_by_hour` has the same problem (`hour_bucket timestamp PRIMARY KEY`).

---

### P13. Gateway DLR/MO producers inherit 50ms batch timeout (HIGH)

**Location:** `internal/transport/gateway.go:60-67`

DLR and MO Kafka producers are created with `NewProducerWithOptions()` which inherits the default 50ms `BatchTimeout` (`producer.go:37`). Since DLR/MO events are published one-at-a-time (not batched), each event waits **up to 50ms** before being sent unless the batch fills.

This adds up to 50ms latency to every DLR and MO delivery through the pipeline.

---

### P7. Redis calls unbatched in handleActivate (HIGH)

**Location:** `internal/executor/worker.go:87-373`

Per card activation, ~5 sequential Redis roundtrips:
1. `CheckAndSetDedupe`
2. `GetCampaignStatus`
3. `GetCampaignCommands`
4. `GetCachedCampaignParams`
5. `SetCardState`

At ~0.1ms per roundtrip = 0.5ms Redis latency per card. At 260 TPS = 130ms/sec of pure Redis wait time. These are sequential — each waits for the previous to complete before starting.

---

### P4. `context.Background()` in SMPP deliver handler (HIGH)

**Location:** `internal/transport/gateway.go:71`

```go
handler := func(sourceAddr string, destAddr string, esmClass byte, payload []byte) {
    ctx := context.Background()  // no timeout, no cancellation
```

DLR and MO processing runs with no timeout. If Redis or Kafka hangs, the deliver worker goroutine is blocked indefinitely, consuming one of the limited deliver worker slots (32 per connection in load config = 384 total slots).

---

### P11. No compaction strategy specified for ScyllaDB tables (HIGH)

**Location:** `internal/scylla/client.go:167-398`

All tables use default compaction (Size-Tiered Compaction Strategy — STCS). This is suboptimal for several table access patterns:

| Table Pattern | Current | Should Be | Why |
|---------------|---------|-----------|-----|
| `message_by_*_time_bucket` (append-heavy, time-bucketed) | STCS | TWCS | Avoids compacting old time windows; better for TTL/deletion |
| `card_state_by_card` (frequently updated) | STCS | LCS | Reduces read amplification; predictable latency |
| `card_keys`, `card_by_msisdn` (lookup, rarely updated) | STCS | LCS | Fewer SSTables to scan on reads |
| `*_metrics_*` (counter tables) | STCS | STCS | Counters require STCS |

At single-node scale this is less visible. At multi-node with real data volumes, wrong compaction causes read latency spikes, excessive disk usage, and compaction storms.

---

### P5. Redis `RecordPartDLR` is not atomic (MEDIUM)

**Location:** `internal/redis/state.go:314`

Three separate Redis commands:
1. `EXISTS` (line 318) — check if multipart tracking exists
2. `HINCRBY` (line 330) — increment delivered/failed count
3. `HGETALL` (line 333) — fetch all fields for aggregation

Two concurrent DLRs for the same multipart message could both increment and both see `allResolved=true`, publishing duplicate card events. Low probability in practice (DLRs for same message rarely arrive simultaneously), but the fix is simple.

---

### P15. Token-aware host selection policy not configured (MEDIUM)

**Location:** `internal/scylla/client.go:52-60`

gocql defaults to `RoundRobinHostPolicy`. On single-node this is harmless, but on multi-node every query goes to a random coordinator that must proxy to the correct replica — **doubling network hops** for every query.

---

### P10. ScyllaDB consistency QUORUM hardcoded (MEDIUM)

**Location:** `internal/scylla/client.go:55`

```go
cluster.Consistency = gocql.Quorum
```

`QUORUM` on RF=1 is functionally identical to `ONE` (quorum of 1 = 1). Harmless now, but when scaling to RF=3 in a single DC, reads will require 2 node responses instead of 1. For single-DC deployment, `LocalOne` is appropriate for most reads; `LocalQuorum` for writes that need durability.

---

### P9. No GOMEMLIMIT configured (MEDIUM)

**Location:** `deployments/docker-compose.yml`, `deployments/docker-compose.load.yml`

Go's GC runs at `GOGC=100` by default (GC triggers when heap doubles). On constrained containers, this can cause either OOM kills (heap grows too fast) or excessive GC (heap too small). `GOMEMLIMIT` (Go 1.19+) sets a soft memory target — the runtime tunes GC frequency to stay under the limit.

---

### P2. 7-9 ScyllaDB operations per card activation (MEDIUM)

**Location:** `internal/executor/worker.go:124-373`, `internal/scylla/counter_store.go:34`

Per card activation breakdown:
- `GetCardKeys` → 1 read
- `IncrCounter` → 1 UPDATE + 1 SELECT (`counter_store.go:45-61`)
- `WriteCardState("in_progress")` → 2-3 INSERTs
- `WriteCardState("completed")` → 2-3 INSERTs

This is a correct observation but the fix is **not** to cache counters in-process. Counters are per-card (not campaign-global), cards are distributed across executor replicas, and a local counter map introduces correctness and coordination complexity. The better approach is to reduce the *other* write amplification first (P17), then re-evaluate whether counter coalescing is needed.

---

### P6. SMPP pool submit busy-waits (MEDIUM)

**Location:** `internal/smpp/pool.go:180`

When all connections have full SMPP windows:
```go
time.Sleep(5 * time.Millisecond)  // spin loop
```

At high load with full windows, this wastes CPU. A cleanup improvement, not a current scaling ceiling.

---

### P12. DLR correlation misses silently dropped (MEDIUM)

**Location:** `internal/transport/gateway.go:305-310`

When no correlation is found for a DLR, it's logged as a warning and dropped. No retry, no DLQ, no metric counter. Need at minimum: counter, alert, and clear visibility.

---

### P16. Consumer commit interval too aggressive (LOW)

**Location:** `internal/kafka/consumer.go:373`

The commit loop runs every 100ms via `time.NewTicker(100 * time.Millisecond)`. With 12+ partitions per topic, this generates up to 120 commits/sec to the broker. Consider increasing to 500ms-1s.

---

### P3. `json.Marshal`/`json.Unmarshal` on every Kafka message (LOW — for now)

**Location:** `internal/kafka/producer.go:123`, `producer.go:169`, all consumer handlers

`encoding/json` uses reflection. At 260+ TPS with 5+ publishes per card ≈ 1300+ marshals/sec plus 1300+ unmarshals/sec on consumers. At current scale this is not the first limiter — ScyllaDB writes, Redis roundtrips, and projector amplification dominate. Becomes important above ~1000 TPS.

---

### P8. kafka-go uses eager rebalancing (LOW — for now)

When a consumer joins/leaves the group, **all** partitions are revoked and reassigned. At scale with 2+ instances per topic, a single instance crash causes ~30s processing pause. kafka-go doesn't support cooperative rebalancing natively. Acceptable for single-host; needs evaluation before multi-instance deployment.

---

### P18. `time.After` used in error-retry paths (LOW)

**Location:** `internal/kafka/consumer.go:112,163,310,358`

Each `time.After` call allocates ~200 bytes that can't be GC'd until the timer fires (pre-Go 1.23). These are error paths (not hot), so low priority. The main commit loop correctly uses `time.NewTicker`.

---

### P19. kafka-go stale messages after rebalance (LOW)

After a consumer group rebalance, the Reader's internal queue may contain messages from partitions no longer assigned. The ConcurrentConsumer's shutdown logic (skip commits during context cancellation) partially mitigates this. Low risk at current single-instance scale.

---

## Part 3: Horizontal Scalability Assessment

### What Scales Linearly

| Component | How | Constraint |
|-----------|-----|-----------|
| Executor | More instances → more Kafka partition consumers | Max parallelism = partition count (64) |
| Gateway | More instances → more SMPP connections + Kafka consumers | Same partition constraint |
| Projector | More instances → more message-log consumers | ScyllaDB write throughput |
| ScyllaDB | More nodes, increase `--smp` | Linear with nodes × smp |
| Kafka | More brokers | Linear with brokers |

### Scaling Bottlenecks

| Component | Bottleneck | When It Hits | Mitigation |
|-----------|-----------|-------------|-----------|
| Kafka partitions | 64 partitions = max 64 concurrent consumers per topic | >8 executor instances (8 partitions each) | Increase partition count (requires volume wipe) |
| Planner | Single instance, `FOR UPDATE SKIP LOCKED` | Cannot parallelize beyond 1 planner | Lease-based shard assignment |
| Postgres | Campaign CRUD, shard management | >100 concurrent campaigns | Move shard management to ScyllaDB/etcd |
| Redis/Dragonfly | Single instance for all services | ~2000 TPS (memory + CPU) | Shard by card_id prefix, or eliminate for card state |
| Reconciler | Serial bucket scans | 1000+ campaigns × 128 buckets | Parallelize bucket scans |
| Write amplification | ~30 physical writes per logical card event | Already the ceiling at ~260 TPS | Prune status-denorm tables, reduce projector fan-out |

### Realistic Throughput Projections

| Scale | Infrastructure | Approximate TPS | Main Constraint |
|-------|---------------|----------------|----------------|
| Current (single host, 4 ARM CPUs) | 1 broker, 1 ScyllaDB `--smp 1`, 1 Redis | ~260 | ScyllaDB single reactor |
| +ScyllaDB smp | Same host, `--smp 2` | ~400-500 | CPU (ScyllaDB + Kafka share 4 cores) |
| 2-node | Separate ScyllaDB host, `--smp 4` | High hundreds to ~1000 | Kafka single broker |
| 3-node cluster | 3 Kafka brokers, 3 ScyllaDB nodes | Low thousands | Redis, planner, write amplification |
| 6+ nodes | Full distributed + Redis sharding | Multiple thousands | JSON serialization, write amplification pruning needed |

**Note on 10k TPS:** The current architecture is not a straightforward 10k TPS design without another round of write-amplification and read-model simplification. The main blocker is not Go or Kafka — it's too many writes per logical card/message event and too much derivative read-model work on the hot path.

### What Would Break First at 10x Scale

1. **Write amplification** — ~30 ScyllaDB writes per card event × 2600 TPS = 78k writes/sec. Even with 3 ScyllaDB nodes at `--smp 4`, this saturates I/O.
2. **Redis** — Single point for dedupe, card state, campaign cache, throttle, SMPP correlation. At 2600 TPS with 5+ ops per card = 13k Redis ops/sec.
3. **Kafka rebalancing** — With 10 executor instances doing eager rebalancing, any instance crash triggers ~30s processing pause across all partitions.
4. **Planner throughput** — Single planner claiming shards with `FOR UPDATE SKIP LOCKED`. At 10M cards, shard generation and claiming becomes the bottleneck.
5. **JSON serialization CPU** — At 13k messages/sec with 5 marshal+unmarshal cycles each, JSON processing alone would consume a full CPU core.

---

## Part 4: Implementation Plan

### Phase 1 — High Impact, Low Effort (immediate)

These changes provide the best ROI and can be done independently.

#### 1.1 Process-local campaign context cache (P1)

**File:** `internal/executor/worker.go`

**Change:**
- Add a `sync.Map` (or bounded LRU) keyed by `campaign_id` to the worker/service struct
- Cache value: `{ commands []Command, commandByStep map[int]Command, params CampaignParams }`
- On `loadCampaignContext()`, check local cache first → Redis second → DB fallback
- Wrap the Redis→DB fallback path in `sync/singleflight` keyed by `campaign_id` to coalesce concurrent cache misses (all workers for same campaign hit at once on first card)
- Invalidate on campaign status change (listen to campaign-status topic or use TTL)
- Bounded to ~1000 entries (campaigns are few, entries are small)

**Expected impact:** +10-15% TPS, significant Redis CPU reduction

#### 1.2 Gateway DLR/MO producer batch timeout (P13)

**File:** `internal/transport/gateway.go:60-67`

**Change:**
- Set `BatchSize: 1` on the DLR and MO producers, or reduce `BatchTimeout` to 1ms
- These producers emit single events, not batches — the 50ms default adds pure latency

```go
dlrOpts := kafkapkg.ProducerOptions{
    RequiredAcks: kafka.RequireOne,
    BatchSize:    1,
}
dlrProducer = kafkapkg.NewProducerWithOptions(dlrTopic, dlrOpts)
```

**Expected impact:** -50ms latency on every DLR/MO delivery

#### 1.3 Metrics table hot partition fix (P14)

**File:** `internal/scylla/client.go` (schema), projector message store (writes), query store (reads)

**Change:**
- Add `global_bucket int` to partition key of `message_metrics_by_minute` and `message_metrics_by_hour`

```sql
CREATE TABLE IF NOT EXISTS message_metrics_by_minute (
    minute_bucket timestamp,
    global_bucket int,
    total_messages counter,
    ...
    PRIMARY KEY ((minute_bucket, global_bucket))
)
```

- Projector writes: hash to `global_bucket` (0..127) when incrementing counters
- Query reads: scatter-gather across all buckets for a given time range, sum client-side
- Same pattern already used by `message_by_time_bucket`

**Expected impact:** Eliminates single-partition counter bottleneck; essential for multi-node

#### 1.4 Deliver handler timeout (P4)

**File:** `internal/transport/gateway.go:71`

**Change:**
```go
ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
defer cancel()
```

**Expected impact:** Prevents goroutine leaks when Redis/Kafka hangs

#### 1.5 GOMEMLIMIT (P9)

**File:** `deployments/docker-compose.yml`

**Change:**
- Add `GOMEMLIMIT` environment variable to each service, set to ~80% of container memory limit
- Example: for a container with 512MB limit, set `GOMEMLIMIT=400MiB`

```yaml
card-executor:
  environment:
    GOMEMLIMIT: "400MiB"
```

**Expected impact:** Reduces GC pause frequency, prevents OOM under load

#### 1.6 DLR correlation miss counter (P12)

**File:** `internal/transport/gateway.go:305-310`

**Change:**
- Add OTel counter `ota.transport.dlr_correlation_misses`
- Increment on every correlation miss
- Consider publishing to a DLQ topic for later investigation

---

### Phase 2 — High Impact, Medium Effort (next sprint)

#### 2.1 Pipeline Redis calls in handleActivate (P7)

**File:** `internal/redis/state.go`, `internal/executor/worker.go`

**Change:**
- Add a `PipelineActivateContext(ctx, cardID, campaignID)` method to the Redis client that executes independent reads in a single pipeline roundtrip:
  1. `CheckAndSetDedupe` (can stay separate — it's a write, must be first)
  2. Pipeline: `GetCampaignStatus` + `GetCampaignCommands` + `GetCachedCampaignParams` in one roundtrip
  3. `SetCardState` (can stay separate — depends on above results)
- Net effect: reduce 5 sequential roundtrips to 3 (dedupe, pipelined reads, state write)
- If combined with P1 (local campaign cache), the pipelined reads further reduce to just `GetCampaignStatus`

**Expected impact:** +5-10% TPS

#### 2.2 Atomic RecordPartDLR via Lua (P5)

**File:** `internal/redis/state.go:314`

**Change:**
- Replace 3 separate Redis commands with a single Lua script:
```lua
-- KEYS[1] = multipart key
-- ARGV[1] = field to increment (delivered/failed)
-- ARGV[2] = total_parts
redis.call('HINCRBY', KEYS[1], ARGV[1], 1)
local vals = redis.call('HGETALL', KEYS[1])
-- check if all parts resolved
-- return result atomically
```
- This eliminates the race where two concurrent DLRs both see `allResolved=true`

**Expected impact:** Eliminates duplicate card event possibility

#### 2.3 ScyllaDB compaction strategies (P11)

**File:** `internal/scylla/client.go:167-398`

**Change:**
- Append `WITH compaction = {...}` to table creation statements based on access pattern:

```sql
-- Time-series append-heavy tables
CREATE TABLE IF NOT EXISTS message_by_time_bucket (...)
WITH CLUSTERING ORDER BY (created_at DESC, msg_id ASC)
AND compaction = {'class': 'TimeWindowCompactionStrategy',
                  'compaction_window_unit': 'HOURS',
                  'compaction_window_size': '4'}

-- Lookup/latest-state tables
CREATE TABLE IF NOT EXISTS card_state_by_card (...)
WITH compaction = {'class': 'LeveledCompactionStrategy'}

CREATE TABLE IF NOT EXISTS card_keys (...)
WITH compaction = {'class': 'LeveledCompactionStrategy'}
```

- Counter tables (`*_metrics_*`, `campaign_progress_*`, `card_counters`) must stay STCS (Scylla requirement for counter tables)

**Note:** Schema changes on existing tables require `ALTER TABLE ... WITH compaction = {...}`. New deployments get it right from `CREATE TABLE`.

**Expected impact:** Better read latency and disk usage at scale; essential for multi-node

#### 2.4 Consumer commit interval increase (P16)

**File:** `internal/kafka/consumer.go:373`

**Change:**
- Increase commit interval from 100ms to 500ms (or make configurable via `KAFKA_CONSUMER_COMMIT_INTERVAL_MS`)
- Reduces broker commit load from ~120 commits/sec to ~24 commits/sec

```go
commitTicker := time.NewTicker(time.Duration(config.GetEnvInt("KAFKA_CONSUMER_COMMIT_INTERVAL_MS", 500)) * time.Millisecond)
```

---

### Phase 3 — Write Amplification Reduction (P17, P2)

This is the most impactful phase for horizontal scalability but requires careful analysis before cutting tables.

#### 3.1 Evaluate status-denormalization tables

**Files:** Projector message store, `internal/scylla/client.go` schema

**Analysis needed:**
- `message_by_status_time_bucket` — is this queried from the UI? How often?
- `message_by_dlr_status_time_bucket` — same question
- `message_by_direction_time_bucket` — same question

Each of these tables requires DELETE + INSERT on every status change because status/direction is in the partition key. If they're rarely queried, they can be:
1. **Removed entirely** — query from `message_by_time_bucket` with client-side filtering
2. **Made lazy** — populated by a background job instead of the hot-path projector
3. **Replaced with materialized views** — Scylla handles the denormalization (trade-off: MV has its own write amplification)

**Potential impact:** Removing 2-3 projector tables saves 4-6 writes per message event. At 222 TPS, that's ~900-1300 fewer writes/sec.

#### 3.2 Batch executor ScyllaDB writes

**File:** `internal/scylla/card_state_store.go`

**Change:**
- Combine `WriteCardState` INSERTs into UNLOGGED batches when they share the same partition key
- `card_state_by_card` and `campaign_card_status_by_bucket` have different partition keys, so they can't be in the same batch — but the campaign-status write can be made conditional (only on terminal states)
- Skip `campaign_card_status_by_bucket` INSERT for intermediate states (`in_progress`) — only write on terminal states (`completed`, `failed`, `skipped`)

**Expected impact:** -2 writes per card for in-progress state transitions

#### 3.3 Counter read optimization (P2)

**File:** `internal/scylla/counter_store.go:34-64`

**Current:** UPDATE + SELECT = 2 roundtrips per card
**Change:** Use `USING TIMESTAMP` trick is not applicable to counter tables. However:
- If the counter value is only needed for the GSM 03.48 packet (monotonic sequence), consider returning the pre-increment value from a single lightweight transaction
- Alternatively: keep the 2-query pattern but make the SELECT eventually consistent (`ONE` instead of `QUORUM`) since the UPDATE already committed

```go
// Read at ONE — the preceding UPDATE at QUORUM guarantees our write is durable.
// Reading at ONE may return a slightly stale value from another replica,
// but since each card_id is processed by exactly one worker (Kafka key routing),
// there are no concurrent writers. The ONE read will always see our own write.
q := c.session.Query(selectStmt, cardID, applicationID).Consistency(gocql.One)
```

**Expected impact:** Reduced read latency for counter queries

---

### Phase 4 — Multi-Node Readiness

#### 4.1 Token-aware host selection policy (P15)

**File:** `internal/scylla/client.go:52`

**Change:**
```go
cluster.PoolConfig.HostSelectionPolicy = gocql.TokenAwareHostPolicy(
    gocql.DCAwareRoundRobinPolicy(""),
)
```

Must be done before adding ScyllaDB nodes. Without it, every query goes to a random coordinator.

#### 4.2 Configurable ScyllaDB consistency (P10)

**File:** `internal/scylla/client.go:55`, `internal/bootstrap/scylla.go`

**Change:**
- Add `SCYLLA_CONSISTENCY` env var (default `local_quorum` for multi-node, `one` for single-node)
- Add `SCYLLA_SERIAL_CONSISTENCY` env var for LWT operations
- Allow per-query override where needed (e.g., counter reads at `ONE`)

```go
consistency := parseConsistency(config.GetEnv("SCYLLA_CONSISTENCY", "local_quorum"))
cluster.Consistency = consistency
```

#### 4.3 SMPP pool: replace busy-wait with sync.Cond (P6)

**File:** `internal/smpp/pool.go:155-183`

**Change:**
```go
type Pool struct {
    // ...
    windowCond *sync.Cond  // signaled when a window slot frees up
}

func (p *Pool) Submit(ctx context.Context, msg Message) error {
    p.windowCond.L.Lock()
    for {
        conn := p.findAvailableConnection()
        if conn != nil {
            p.windowCond.L.Unlock()
            return conn.Submit(msg)
        }
        // Wait for signal or context cancellation
        done := make(chan struct{})
        go func() {
            select {
            case <-ctx.Done():
                p.windowCond.Broadcast()
            case <-done:
            }
        }()
        p.windowCond.Wait()
        close(done)
        if ctx.Err() != nil {
            p.windowCond.L.Unlock()
            return ctx.Err()
        }
    }
}
```

Signal `windowCond.Signal()` when an SMPP response is received and a window slot frees up.

#### 4.4 Kafka rebalancing strategy (P8)

**Impact:** Only relevant when running multiple executor/gateway instances.

**Options:**
1. **Accept eager rebalancing** — OK for ≤3 instances per topic. The ~30s pause is infrequent (only on instance crash/restart).
2. **Switch to confluent-kafka-go** — supports cooperative incremental rebalancing. Higher effort: different API, CGO dependency.
3. **Static partition assignment** — assign partitions manually per instance. Eliminates rebalancing entirely but requires orchestration.

**Recommendation:** Accept eager rebalancing until running 4+ instances. Then evaluate confluent-kafka-go.

---

### Phase 5 — High-Scale Optimizations (>1000 TPS)

#### 5.1 Faster JSON serialization (P3)

**File:** `internal/kafka/producer.go:123,169`, all consumer handlers

**Options (in order of effort):**
1. **jsoniter** — drop-in replacement, 3-6x faster, no code changes beyond import
2. **easyjson** — code generation, 5-10x faster, requires `go generate` for each message type
3. **protobuf** — schema-first, most efficient, but requires broader refactoring

**Recommendation:** Start with jsoniter (1 import change), benchmark, then decide if code-gen is needed.

#### 5.2 Redis call elimination

**Long-term:** Move remaining Redis hot-path operations to ScyllaDB or in-process:
- Campaign context → in-process cache (P1, Phase 1)
- Card state coordination → already in ScyllaDB
- SMPP correlation → consider in-process only (already using sync.Map locally)
- Dedupe → ScyllaDB LWT or in-process bloom filter
- Throttle → consider token bucket in-process with periodic ScyllaDB sync

This reduces Redis to a non-critical-path component.

#### 5.3 `time.After` cleanup (P18)

**File:** `internal/kafka/consumer.go:112,163,310,358`

**Change:** Replace `time.After` in retry loops with reusable `time.Timer`:
```go
timer := time.NewTimer(duration)
defer timer.Stop()
select {
case <-timer.C:
    // ...
}
```

Low priority since these are error paths.

---

## Summary: Priority Order

| # | Issue | Phase | Impact | Effort |
|---|-------|-------|--------|--------|
| 1 | P17: Evaluate/prune status-denorm tables | 3 | Removes ~1000+ writes/sec | Medium |
| 2 | P1: Process-local campaign context cache | 1 | +10-15% TPS | Low |
| 3 | P14: Add global_bucket to metrics tables | 1 | Eliminates hot partition | Low |
| 4 | P13: Gateway DLR/MO producer batch timeout | 1 | -50ms DLR latency | Trivial |
| 5 | P7: Pipeline Redis calls in handleActivate | 2 | +5-10% TPS | Medium |
| 6 | P4: Deliver handler timeout | 1 | Prevents goroutine leaks | Trivial |
| 7 | P11: Compaction strategies | 2 | Better read latency at scale | Low |
| 8 | P15: Token-aware ScyllaDB host policy | 4 | Essential for multi-node | Trivial |
| 9 | P9: GOMEMLIMIT | 1 | Prevents OOM, reduces GC | Trivial |
| 10 | P10: Configurable consistency | 4 | Needed for RF=3 | Low |
| 11 | P5: Atomic RecordPartDLR | 2 | Prevents duplicates | Low |
| 12 | P2/P17: Batch executor writes | 3 | -2 writes per card transition | Medium |
| 13 | P16: Increase commit interval | 2 | Reduces broker load | Trivial |
| 14 | P6: Replace SMPP busy-wait | 4 | Reduces idle CPU | Low |
| 15 | P3: Faster JSON serialization | 5 | CPU savings >1k TPS | Medium |
| 16 | P8: Cooperative Kafka rebalancing | 4 | Needed for multi-instance | High |
| 17 | P12: DLR correlation miss counter | 1 | Observability | Trivial |
| 18 | P18: time.After cleanup | 5 | Minor GC improvement | Trivial |
| 19 | P19: Stale message handling | 4 | Edge case safety | Low |

---

## Verification Checklist

After each phase:
1. `go build ./...` — compiles
2. `go test ./... -count=1` — all tests pass
3. `make lint` — passes
4. `docker compose down -v && docker compose up -d` — clean deploy
5. Run 200k campaign, verify 0 failures
6. Compare TPS before/after
7. Check ScyllaDB `nodetool compactionstats` and `nodetool tablestats` for write reduction
8. Verify OTel metrics for new counters/gauges
