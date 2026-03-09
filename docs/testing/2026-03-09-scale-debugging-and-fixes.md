# Scale Debugging: 50k Card Campaign Stall

Date: 2026-03-09

## Context

After implementing the append-only card state projection (replacing 6 synchronous ScyllaDB RPCs per card state change with a single async Kafka publish), the system was tested at 50,000 cards. The campaign stalled at ~96% completion with ~2,000 cards permanently stuck. This document captures the root cause investigation, fixes applied, and lessons learned for running the platform at scale.

Environment: single-machine Docker Compose with all services, load overlay (`docker-compose.load.yml`).

## Symptoms

- Campaign reached 96.1% (48,050/50,000) and stopped progressing
- ~2,512 cards received successful MO response (por_status=0) but never logged "card completed"
- ~3,378 cards were submitted via SMPP but never received DLR/MO back from mock-SMSC
- ~5,289 cards were activated but never produced a "sent OTA" log
- No OOM, no disk exhaustion, no explicit error logs

## Root Causes

### 1. SMPP Deliver Queue Head-of-Line Blocking (Critical)

**File**: `internal/smpp/client.go` (`enqueueDeliver` method)

**Problem**: The SMPP client's `ReadLoop` goroutine reads ALL incoming PDUs from the TCP socket: both `submit_sm_resp` (responses to our outgoing messages) and `deliver_sm` (incoming DLR/MO from the SMSC). When a `deliver_sm` arrives, `ReadLoop` calls `enqueueDeliver()` to place it on a buffered channel for async processing by deliver workers.

The original `enqueueDeliver` implementation **blocked** when the deliver queue was full:

```go
// BEFORE (blocking — causes cascade failure)
func (c *Client) enqueueDeliver(msg deliverMessage) {
    select {
    case c.deliverQ <- msg:
    default:
        // Queue full — BLOCK until space frees
        select {
        case c.deliverQ <- msg:  // blocks ReadLoop!
        }
    }
}
```

When ReadLoop blocks on `enqueueDeliver`:
1. No `submit_sm_resp` PDUs can be read from the socket
2. All pending `Submit()` calls time out after 30 seconds
3. The SMPP pool marks connections as exhausted
4. New submissions fail with "no bound connections with available window capacity"
5. TCP receive buffer fills, triggering TCP flow control back to the SMSC
6. SMSC writes block, goroutines pile up, DLR/MO deliveries stall

**Fix**: Made `enqueueDeliver` non-blocking. If the queue is full, drop the message with a warning log rather than blocking the entire read loop:

```go
// AFTER (non-blocking — protects ReadLoop)
func (c *Client) enqueueDeliver(msg deliverMessage) {
    select {
    case c.deliverQ <- msg:
    default:
        c.logger.Warn("deliver queue full, dropping deliver_sm to protect ReadLoop")
    }
}
```

Additionally increased the deliver queue capacity from 1,024 to 8,192 to make drops extremely rare under normal load.

**Key insight**: In any protocol multiplexing design (where one socket carries both request responses and unsolicited events), the read loop must NEVER block on event processing. Blocking the read loop creates a deadlock: responses can't be delivered, so requests time out, which creates more backpressure.

### 2. Mock-SMSC Thundering Herd (Secondary)

**File**: `internal/mocksmsc/server.go`

**Problem**: For each `submit_sm`, the mock-SMSC spawns a goroutine that sleeps for a fixed `DLR_DELAY_MS` (50ms) then writes the DLR to the TCP socket. Under burst load (50,000 submissions in ~2 seconds), all 50,000 goroutines wake up within a narrow window and contend on the per-connection write mutex.

With 12 SMPP connections, each connection has ~4,167 goroutines all trying to write simultaneously. The write mutex serializes them, but the contention causes long tail latencies and interacts badly with the deliver queue blocking issue.

**Fix**: Added random jitter (0-100% of base delay) to both DLR and MO scheduling:

```go
jitter := time.Duration(rand.Int63n(int64(s.config.DLRDelayMs)+1)) * time.Millisecond
time.After(time.Duration(s.config.DLRDelayMs)*time.Millisecond + jitter)
```

This spreads the DLR/MO deliveries over a wider time window, reducing peak contention.

### 3. Database Connection Pool Starvation (Performance)

**File**: `deployments/docker-compose.load.yml`, `internal/executor/worker.go`

**Problem**: The card-executor was configured with `CARD_WORKER_CONCURRENCY=32` (32 parallel workers) but `DB_MAX_OPEN_CONNS=8` (only 8 database connections). Each new card requires a PostgreSQL query to load encryption keys (cache miss on first activation). With 32 workers competing for 8 connections, most workers spent 200ms+ waiting for a connection — even though the actual query takes 0.08ms.

At 8 connections with ~200ms round-trip (dominated by connection pool wait), throughput was limited to ~40 cards/sec. For 50,000 cards, initial key loading alone would take ~20 minutes.

**Fix**:
- Increased `DB_MAX_OPEN_CONNS` from 8 to 32 (match concurrency)
- Increased `DB_MAX_IDLE_CONNS` from 4 to 16
- Made the in-memory card key cache size configurable via `CARD_KEY_CACHE_SIZE` env var (default 50,000; load config 100,000)

The previous hardcoded cache of 10,000 entries caused eviction churn when processing 50k+ unique cards.

## Performance After Fixes

| Metric | Before | After |
|--------|--------|-------|
| 50k completion | Stalled at 96% | 100% (50,000/50,000) |
| Failed cards | ~2,000 stuck | 0 |
| Throughput | ~28 cards/sec | ~59 cards/sec |
| Deliver queue drops | N/A (blocked instead) | 0 |
| Poison pill messages | Unknown | 0 |
| SLOW SQL queries | Continuous | 681 (cold cache only) |

## Configuration Reference

### card-executor (load profile)

| Setting | Default | Load Value | Notes |
|---------|---------|------------|-------|
| `CARD_WORKER_CONCURRENCY` | 16 | 32 | Workers per consumer topic |
| `DB_MAX_OPEN_CONNS` | 25 | 32 | Must be >= concurrency |
| `DB_MAX_IDLE_CONNS` | 10 | 16 | Half of max open |
| `CARD_KEY_CACHE_SIZE` | 50,000 | 100,000 | In-memory TTL cache for card keys |
| `KAFKA_PRODUCER_BATCH_SIZE` | 1,000 | 10,000 | Larger batches for throughput |
| `KAFKA_PRODUCER_BATCH_TIMEOUT_MS` | 50 | 10 | Lower latency flush |

### sms-gateway (load profile)

| Setting | Default | Load Value | Notes |
|---------|---------|------------|-------|
| `SMS_SENDER_WORKERS` | 8 | 64 | Kafka consumer workers |
| `SMPP_CONNECTIONS` | 5 | 12 | SMPP pool connections |
| `SMPP_WINDOW_SIZE` | 10 | 20 | Max concurrent submits per connection |
| `SMPP_DELIVER_WORKERS` | 8 | 16 | DLR/MO handler goroutines per connection |

### SMPP client (hardcoded)

| Setting | Value | Notes |
|---------|-------|-------|
| Deliver queue capacity | 8,192 | Per connection; was 1,024 |
| Deliver queue overflow | Drop + warn | Was blocking; never block ReadLoop |
| Submit timeout | 30s | Per-request timeout waiting for submit_sm_resp |
| Pool submit deadline | 5s | Deadline to find a connection with window capacity |

## Scaling Rules of Thumb

### Connection Pool Sizing

```
DB_MAX_OPEN_CONNS >= CARD_WORKER_CONCURRENCY
```

If workers exceed connections, the excess workers block waiting for a connection. GORM reports this as "SLOW SQL" even though the actual query is sub-millisecond. The wait time is pure connection acquisition latency.

### Cache Sizing

```
CARD_KEY_CACHE_SIZE >= expected unique cards per campaign
```

The in-memory cache prevents repeated Redis lookups. Redis in turn prevents repeated Postgres lookups. The chain is:

```
In-memory cache (process-local, 15min TTL)
  -> Redis cache (shared, 1hr TTL)
    -> PostgreSQL (source of truth)
```

For a 50k-card campaign, set cache to at least 50k. Cache entries are small (~200 bytes each), so 100k entries uses ~20MB.

### SMPP Throughput

```
Max concurrent submits = SMPP_CONNECTIONS * SMPP_WINDOW_SIZE
```

With 12 connections and window 20, max 240 concurrent submits. If SMPP round-trip is 5ms (local mock), theoretical max is 48,000 submits/sec. Real throughput is lower due to serialization, DLR/MO processing, and Kafka latency.

### Deliver Queue Sizing

```
DELIVER_QUEUE_SIZE >= DELIVER_WORKERS * (DLR_processing_time / DLR_arrival_interval)
```

For burst scenarios where all DLRs arrive simultaneously (e.g., mock-SMSC with fixed delay), the queue must absorb the full burst. With 16 workers each taking ~15ms per DLR, drain rate is ~1,067/sec per connection. A 50k-card burst across 12 connections delivers ~4,167 DLRs per connection in <1 second. Queue of 8,192 absorbs this comfortably.

### Single-Machine Limits

On a single Docker Compose host, the following resources are shared and become bottlenecks:

| Resource | Contention Source |
|----------|------------------|
| CPU | Kafka broker, 7+ Go services, ScyllaDB, Postgres |
| Disk I/O | Kafka log segments (64 partitions), Postgres WAL, ScyllaDB compaction + card state writes |
| Network | Inter-container TCP (loopback), SMPP connections |
| Memory | Kafka page cache, Postgres shared buffers, ScyllaDB memtables |

Realistic single-machine throughput: **50-100 cards/sec end-to-end** (activate -> send -> DLR -> MO -> complete). For higher throughput, use separate hosts for Kafka, Postgres, and ScyllaDB.

## Architecture: Card State Durability

Card state projection has gone through two iterations:

**V1** (6 synchronous RPCs per state change — original):
```
SELECT old state -> INSERT card_state_by_card -> UPDATE counters
  -> INSERT status row -> DELETE old status row -> optional failed_cards
```

**V2** (async Kafka — intermediate, removed):
```
Publish CardStateChange to card-state-log topic (async, RequireOne)
-> CardStateWriter projector batch-writes to ScyllaDB
```

**V3** (direct synchronous ScyllaDB writes — current):
```
Executor -> WriteCardState() -> ScyllaDB unlogged batch:
  1. INSERT card_state_by_card (USING TIMESTAMP for LWW)
  2. INSERT campaign_card_status_by_bucket (append-only)
  3. INSERT failed_cards_by_campaign_bucket (when status=failed)
```

V2 was removed because card state is a **correctness requirement** — the async Kafka path could lose events before reaching Kafka, and the projector acked before ScyllaDB flush. V3 uses direct synchronous writes with no SELECT, no DELETE, and no counter updates. The `card-state-log` topic and `CardStateWriter` projector were deleted.

The executor now requires a ScyllaDB connection (`SCYLLA_HOSTS`, `SCYLLA_KEYSPACE` env vars in `docker-compose.yml`). This increases disk I/O but guarantees card state durability.

## Debugging Methodology

The investigation followed a systematic approach:

1. **Reproduce** - Ran 50k test, confirmed stall at 96%
2. **Gather evidence** - Checked Kafka lag, container logs, resource utilization, DB query plans
3. **Trace data flow** - Followed card lifecycle from activate through DLR/MO to completion
4. **Identify failure modes** - Categorized stuck cards into three groups (no send, no DLR/MO, no completion)
5. **Root cause per failure mode** - Traced each to a specific code path
6. **Fix and verify** - Applied minimal fixes, reran 50k test, confirmed 100% completion

Key diagnostic commands used:

```bash
# Kafka consumer lag
docker exec deployments-kafka-1 /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 --group card-activate-workers --describe

# Campaign progress
curl -s "http://localhost:8080/api/v1/campaigns/$CAMPAIGN_ID" | python3 -m json.tool

# Service error counts
docker logs deployments-card-executor-1 2>&1 | grep '"level":"error"' | wc -l

# Postgres query plan
docker exec deployments-postgres-1 psql -U ota -d ota_platform \
  -c "EXPLAIN ANALYZE SELECT ... FROM cards WHERE id = '...';"

# Redis latency
docker exec deployments-dragonfly-1 redis-cli --latency
```
