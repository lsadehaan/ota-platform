# Local Throughput Findings

Date: 2026-03-08

## Scope

This document captures the current local Docker Compose performance posture for the split-service OTA platform and the main configuration/settings that materially affected throughput.

Environment:
- real PostgreSQL
- real Kafka
- real Dragonfly
- real ScyllaDB
- real `ota-api`, `campaign-planner`, `card-executor`, `read-model-projector`, `sms-gateway`, `reconciler`
- only the SMSC is mocked

Initial workload used for the first relevant run:
- `5000` cards
- `1` OTA command per card
- `1` SMS part per card
- DLR + MO/PoR enabled
- `2` planner replicas
- `10` executor replicas
- `4` gateway replicas

## Results

First strong end-to-end result:
- elapsed: `1m3.404478351s`
- cards/sec: `78.86`
- SMS parts/sec: `78.86`
- sent: `5000`
- delivered: `5000`
- failed: `0`

Updated large-batch result after additional executor/gateway fixes:
- `10000` cards
- `2` planners
- `24` executors
- `8` gateways
- `1` projector
- campaign `started_at`: `2026-03-08T08:40:39.145634Z`
- campaign `completed_at`: `2026-03-08T08:41:55.127392Z`
- elapsed: `75.98s`
- cards/sec: `131.62`
- SMS parts/sec: `131.62`
- sent: `10000`
- delivered by campaign state: `10000`
- failed: `0`

This is a real end-to-end result on the local deployed stack with only SMSC mocked.

## Before / After

Earlier best local result before the latest batching/parallelism changes:
- about `16.92 TPS`

Intermediate result after batching/projector parallelism:
- `78.86 TPS`

Current result after the additional runtime fixes below:
- `131.62 TPS`

Approximate improvement:
- vs `16.92 TPS` baseline: `7.78x`
- vs `78.86 TPS` intermediate result: `1.67x`

## Main Configuration That Moved Throughput

These settings were the main throughput levers in the local load profile.

### Kafka

Configured in `deployments/docker-compose.load.yml`:
- `KAFKA_TOPIC_PARTITIONS=96`
- `KAFKA_PRODUCER_BATCH_SIZE=10000`
- `KAFKA_PRODUCER_BATCH_TIMEOUT_MS=100`
- `KAFKA_CONSUMER_MAX_WAIT_MS=100`
- `KAFKA_CONSUMER_QUEUE_CAPACITY=10000`

Meaning:
- larger producer-side accumulation window
- larger producer flush batches
- larger consumer-side fetch/queue buffers
- higher partition count so the replica counts can stay busy

### Planner

- `CAMPAIGN_SHARD_SIZE=5000`
- `PLANNER_CLAIM_BATCH_SIZE=5000`
- `PLANNER_POLL_INTERVAL_MS=25`

Meaning:
- fewer planner-owned shard rows
- larger publish units
- lower planner overhead for the same 5k-card campaign

### Executor

- `CARD_WORKER_CONCURRENCY=8`
- `10` executor replicas in the load run

Meaning:
- wide cross-card concurrency for card state-machine advancement

Later high-throughput run:
- `CARD_WORKER_CONCURRENCY=16`
- `24` executor replicas

### Gateway

- `SMS_SENDER_WORKERS=32`
- `SMPP_CONNECTIONS=8`
- `SMPP_WINDOW_SIZE=20`
- `4` gateway replicas in the load run

Meaning:
- high potential submit concurrency at the SMPP edge

Later high-throughput run:
- `SMS_SENDER_WORKERS=64`
- `SMPP_CONNECTIONS=12`
- `SMPP_WINDOW_SIZE=20`
- `8` gateway replicas

### Projector

- `PROJECTOR_WORKERS=16`
- `PROJECTOR_FLUSH_WORKERS=8`
- `PROJECTOR_BATCH_SIZE=5000`
- `PROJECTOR_UPDATE_BATCH_SIZE=5000`
- `PROJECTOR_FLUSH_INTERVAL_MS=100`

Meaning:
- the read-model projector is no longer effectively single-lane
- multiple flush lanes can process message-log traffic in parallel

## What the Projector Does

The `read-model-projector` consumes the `message-log` Kafka topic and persists/query-materializes message activity into Scylla.

Concretely, it:
- consumes MT/MO message create/update actions from Kafka
- writes the canonical message rows to Scylla
- maintains the Scylla query/index tables used by:
  - dashboard KPIs
  - message monitoring views
  - recent activity views
  - message throughput and error summaries

Why it matters:
- every MT create/update and MO create/update flows through it
- if it falls behind, `message-log` lag grows
- if it becomes serialized, it can cap overall system throughput even when executors and gateways still have headroom

## Bottleneck Progression Observed

### Earlier state

Before the recent changes:
- overall throughput was low
- CPU usage was surprisingly low
- the stack looked under-parallelized
- projector/message-log traffic was one of the dominant limiting factors

The projector bottleneck was caused by:
- one global incoming queue
- one flush loop
- high create/update chatter
- immediate MT `create(status=sending)` followed by immediate `update(status=sent)` in the executor success path

### After the recent changes

Key fixes:
- Kafka batching made configurable and increased materially
- projector flush path sharded by message ID
- projector updates coalesced in memory per message ID
- executor success path now creates MT rows directly as `sent`

Observed active-run CPU sample:
- `kafka`: `102.86%`
- `read-model-projector`: `23.01%`
- `scylla`: `32.63%`
- `dragonfly`: `20.81%`
- executors combined: roughly `108%`
- gateways combined: roughly `52%`

Interpretation:
- the stack is now using concurrency much better
- the bottleneck shifted away from projector serialization
- Kafka is now the hottest component in the local run

### After the next runtime fixes

The changes that moved the stack from `~79 TPS` to `~132 TPS` were:
- executor completion/progress now derives from durable Scylla query state instead of drifting counters
- Scylla card state updates no longer use per-update CAS/LWT on the hot path
- executor now uses process-local caches for:
  - campaign command context
  - campaign params
  - profiles
  - card keys
- SMPP pool no longer blocks on the first saturated connection window; it skips full connections and uses the next available slot
- OTel metrics now export per replica with `service.instance.id`

Observed hot-path timings from OTel after these fixes:
- executor `card.activate`: about `292ms` average across replicas
- executor `card.mo_received`: about `286ms` average across replicas
- gateway SMPP submit: about `589ms` average across replicas
- projector flush: about `91ms` average
- planner publish: still negligible compared with the rest of the pipeline

Interpretation:
- the major gains came from removing hot-path coordination/state overhead, not from adding more projector replicas
- projector is no longer the first bottleneck
- gateway submit latency is still expensive even with the mock SMSC, which means connection-window utilization and transport overhead are still worth revisiting

## What This Means

Current local performance is no longer primarily blocked by:
- too few executor replicas
- too few gateway replicas
- projector single-lane flushing
- tiny Kafka batches

Current likely next bottlenecks:
1. gateway submit latency and SMPP pool behavior
2. remaining executor hot-path remote round trips
3. single-node Kafka capacity in local Compose
4. remaining message-log/projector overhead

## Important Notes

### 1. This is still a local single-host result

The local Compose result is useful for correctness and relative bottleneck analysis, but it is not an internet-scale capacity result.

### 2. Setup/import time is separate from campaign runtime

The E2E runner first:
- imports cards
- creates campaign targets
- creates the campaign
- starts it

The runtime TPS number above is still the correct final runner result for the scenario, but setup should not be confused with the steady-state campaign engine throughput.

### 3. Telemetry worked during the run

Confirmed useful live metrics included:
- `ota_transport_smpp_submits_total`
- `ota_executor_events_processed_total`
- `ota_projector_actions_processed_total`

Important observability fix:
- Prometheus now retains per-replica labels through the collector
- `service.instance.id` is exported and visible on metrics
- fleet-wide totals can now be summed correctly instead of collapsing across replicas

## Recommended Next Steps

1. Run isolated component performance tests for:
   - planner
   - executor
   - gateway
   - projector

2. Focus next on the transport path:
   - measure SMPP submit queueing/window utilization explicitly
   - determine why gateway submit latency is still high against the mock SMSC

3. Continue reducing executor hot-path round trips where possible.

4. Re-run the same large-batch scenario after transport/executor changes and compare:
   - final TPS
   - gateway submit latency
   - executor `card.activate` latency
   - Kafka CPU
   - consumer lag
