# Async Batched Card State Writer

## Problem

288 executor workers call `WriteCardState` synchronously, each doing:
- 1 CQL UNLOGGED BATCH (3-4 statements)
- 1 counter UPDATE on `campaign_progress_by_bucket`

This creates contention on ScyllaDB counter partitions (288 workers vs 128 buckets)
and puts ScyllaDB write latency directly in the executor hot path.

## Solution

Replace synchronous `WriteCardState` calls with a Kafka-backed async projection:

1. Workers publish `CardStateChange` events to a `card-state-log` Kafka topic (keyed by card_id)
2. A `ConcurrentConsumer` reads the topic with a batching handler
3. Each partition's batch writer flushes every **1 second** or **1000 events** (whichever first)
4. Batch writer deduplicates per card_id, aggregates counter deltas per bucket, writes CQL batches

### Architecture

```
288 workers → Kafka producer (card-state-log, key=card_id) → return immediately
                                    ↓
              ConcurrentConsumer (card-state-log)
              ├── partition 0 → batch writer 0 (flush every 1s or 1000)
              ├── partition 1 → batch writer 1
              └── partition N → batch writer N
```

### Why Kafka (not in-process channel)

- **Multi-node HA**: consumer group rebalancing handles failover
- **Survives restarts**: unprocessed events stay in Kafka
- **No single goroutine SPOF**: each partition has its own writer
- **Ordering per card**: Kafka partition keying guarantees FIFO per card_id

### Batch Writer Logic

Each flush cycle:
1. **Deduplicate per card_id**: if multiple state changes for the same card, keep latest by timestamp
2. **Aggregate counter deltas per (campaign_id, bucket)**: e.g., 3 events across bucket 7
   (pending→in_progress ×2, in_progress→completed ×1) becomes single UPDATE:
   `pending -= 2, in_progress += 1, completed += 1`
3. **CQL UNLOGGED BATCH**: chunked to ≤100 rows per batch (existing pattern)
4. **Counter UPDATEs**: one per touched bucket (aggregated, not per-event)

### What Changes

- `WriteCardState` in executor publishes to Kafka instead of writing ScyllaDB directly
- New `card-state-projector` consumer runs alongside executor (or as its own service)
- Producer: `BatchSize=1` (no batching delay, single events)

### What Doesn't Change

- Worker call sites (all 6 `WriteCardState` calls stay identical)
- Redis card state writes (authoritative during execution)
- Reconciler card state writes (low volume, synchronous)
- ScyllaDB schema (no new tables)
- `CardStateWriter` interface

### Safety

- Card state in ScyllaDB is a **read model** — Redis is authoritative during execution
- Counter updates already best-effort — reconciler repairs drift
- LWW timestamps on `card_state_by_card` handle out-of-order writes
- `campaign_card_latest` uses CQL INSERT (upsert) — idempotent

### Configuration

- `CARD_STATE_BATCH_SIZE`: max events per flush (default 1000)
- `CARD_STATE_FLUSH_INTERVAL_MS`: max time between flushes (default 1000)
- `CARD_STATE_WORKERS`: consumer workers (default 16)
- Topic: `card-state-log`, partitions match other topics (64 in load config)
