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

Workload used for the most relevant run:
- `5000` cards
- `1` OTA command per card
- `1` SMS part per card
- DLR + MO/PoR enabled
- `2` planner replicas
- `10` executor replicas
- `4` gateway replicas

## Final Observed Result

Final completed run:
- elapsed: `1m3.404478351s`
- cards/sec: `78.86`
- SMS parts/sec: `78.86`
- sent: `5000`
- delivered: `5000`
- failed: `0`

This is a real end-to-end result on the local deployed stack.

## Before / After

Earlier best local result before the latest batching/parallelism changes:
- about `16.92 TPS`

Current result after the changes below:
- `78.86 TPS`

Approximate improvement:
- `4.66x`

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

### Gateway

- `SMS_SENDER_WORKERS=32`
- `SMPP_CONNECTIONS=8`
- `SMPP_WINDOW_SIZE=20`
- `4` gateway replicas in the load run

Meaning:
- high potential submit concurrency at the SMPP edge

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

## What This Means

Current local performance is no longer primarily blocked by:
- too few executor replicas
- too few gateway replicas
- projector single-lane flushing
- tiny Kafka batches

Current likely next bottlenecks:
1. single-node Kafka capacity in local Compose
2. remaining executor hot-path remote round trips
3. remaining message-log/projector overhead
4. local single-host container scheduling effects

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

## Recommended Next Steps

1. Run isolated component performance tests for:
   - planner
   - executor
   - gateway
   - projector

2. Increase Kafka capacity in the test topology:
   - more brokers, not just more partitions on one broker

3. Continue reducing `message-log` event volume where possible.

4. Re-run the same 5k scenario after improving Kafka topology and compare:
   - final TPS
   - Kafka CPU
   - consumer lag
   - projector CPU
