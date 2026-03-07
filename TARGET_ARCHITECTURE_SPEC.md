# OTA Platform Target Architecture Specification

## Purpose

This document defines the target architecture for an OTA SIM management platform designed for sustained internet-scale throughput, including a long-term target of 100,000 outbound SMS parts per second while preserving strict per-card sequencing.

This specification supersedes the current small-scale assumptions in `OTA_PLATFORM_PLAN.md` and `SCALING_ARCHITECTURE.md` where they conflict with the architecture described here.

## Design Goals

1. Preserve strict per-card ordering for all OTA command execution.
2. Maximize cross-card parallelism across arbitrarily large campaigns.
3. Remove PostgreSQL from the hot execution path.
4. Keep a clear separation between control-plane and data-plane responsibilities.
5. Make each major component independently load-testable.
6. Support replay, reconciliation, and recovery without manual data repair.
7. Use durable storage systems that match the dominant access patterns instead of forcing one database to serve all workloads.
8. Keep the deployment operationally simple relative to the throughput target.

## Non-Goals

1. Supporting single-node deployment as the primary production model.
2. Optimizing for minimum infrastructure cost over throughput headroom.
3. Relying on SIM-side message reordering to recover from out-of-order OTA steps.
4. Using PostgreSQL as the durable store for high-rate per-message execution writes.

## Hard Invariants

### Per-card sequencing

For a given card, OTA execution is a strict state machine.

- Step `N+1` must not be sent until step `N` has completed its required transport and SIM acknowledgements.
- If a command expects PoR, the next step must wait for both DLR success and a valid PoR/MO response.
- Kafka partitioning and service design must preserve per-card ordering end-to-end.

### Cross-card parallelism

Different cards are independent units of work.

- The platform must allow any number of cards to progress in parallel.
- Throughput bottlenecks must come from explicit transport or storage limits, not accidental single-threading.

### Replay safety

All data-plane processing is at-least-once.

- Every event must be safe to replay.
- Durable state must be reconstructible from Kafka streams plus durable snapshots.
- Reconciliation must detect and repair stuck work without manual DB edits.

## High-Level Architecture

```text
                           +----------------------+
                           |       Web UI         |
                           |   Dashboards/Admin   |
                           +----------+-----------+
                                      |
                                      v
                           +----------------------+
                           |       ota-api        |
                           |  Control Plane API   |
                           +----------+-----------+
                                      |
                         PostgreSQL   |   Campaign runs / shard metadata
                                      v
                           +----------------------+
                           |   campaign-planner   |
                           | Expand + shard work  |
                           +----------+-----------+
                                      |
                                      v
                                 Kafka topics
                                      |
          +---------------------------+---------------------------+
          |                           |                           |
          v                           v                           v
+-------------------+      +-------------------+      +----------------------+
|   card-executor   |      |    sms-gateway    |      | read-model-projector |
| per-card state    |      | SMPP transport    |      | query/read models    |
| machine           |      | DLR + MO ingress  |      | websocket feeds      |
+-----+--------+----+      +--------+----------+      +-----------+----------+
      |        |                    |                             |
      |        |                    |                             |
      v        v                    v                             v
 Redis/Valkey  ScyllaDB          Redis/Valkey                 ScyllaDB
 hot state     durable           transport                    read/query
 + caches      execution         correlation                  tables
               state + events
```

## Control Plane and Data Plane

### Control Plane

The control plane is low-throughput, relational, and operator-facing.

Primary responsibilities:

1. Card inventory management.
2. Profiles and applications.
3. Security configuration metadata.
4. Campaign definitions.
5. Campaign run creation.
6. Campaign shard metadata.
7. Administrative and audit actions.

Primary store:

- PostgreSQL

### Data Plane

The data plane is high-throughput, append-heavy, and event-driven.

Primary responsibilities:

1. Campaign expansion and work sharding.
2. Per-card OTA execution state machine.
3. SMS transport submission.
4. DLR and MO/PoR ingestion.
5. Durable execution snapshots and event history.
6. Query/read model projection.
7. Recovery and reconciliation.

Primary stores:

- Kafka
- DragonflyDB by default, with Valkey Cluster as the fallback option if measured coordination throughput requires multi-primary sharding
- ScyllaDB

## Service Specifications

### `ota-api`

Responsibilities:

1. Expose REST and websocket-friendly control endpoints.
2. Persist card inventory, profiles, applications, campaign definitions, and campaign runs.
3. Validate campaign requests.
4. Start campaign runs by creating durable planner work, not by directly publishing per-card events.
5. Read from Postgres for control-plane data and from Scylla read models for large execution views.

Constraints:

- No per-card execution loops.
- No SMPP interaction.
- No per-message hot-path writes.

### `campaign-planner`

Responsibilities:

1. Claim pending campaign runs from Postgres.
2. Resolve card targets deterministically.
3. Split runs into shards.
4. Persist shard manifests/checkpoints in Postgres.
5. Publish `card.activate` events in batches to Kafka keyed by `card_id`.
6. Advance shard checkpoints durably.
7. Resume from checkpoints after crash or restart.

Constraints:

- Planner must be idempotent.
- Planner must never rely on “API publish succeeded” as the only source of truth.

### `card-executor`

Responsibilities:

1. Consume `card-events` keyed by `card_id`.
2. Own the per-card state machine.
3. Load hot card state from DragonflyDB or another Redis-compatible coordination store, with durable fallback from Scylla.
4. Build GSM 03.48 packets.
5. Publish `send-sms` transport jobs.
6. Process DLR and MO events.
7. Update durable execution snapshots in Scylla.
8. Emit execution events for projectors.

Constraints:

- All state transitions must be replay-safe.
- No distributed locking per card.
- Ordering comes from Kafka partitioning by `card_id`.

### `sms-gateway`

Responsibilities:

1. Consume `send-sms` jobs keyed by `card_id`.
2. Submit via SMPP using many connections and configured windows.
3. Track transport correlation in DragonflyDB or another Redis-compatible coordination store.
4. Aggregate multipart DLRs.
5. Publish transport ingress events back to Kafka.

Constraints:

- Must preserve ordering for messages for the same card.
- Must exploit concurrency across cards.
- Must support multiple SMPP pools and routes.

### `read-model-projector`

Responsibilities:

1. Consume execution and transport events.
2. Build query tables in ScyllaDB.
3. Maintain campaign summaries, failed-card views, recent errors, and message timelines.
4. Emit aggregate websocket events suitable for operator consumption.

Constraints:

- Projectors must be replayable.
- Query tables must be explicit and purpose-built.
- Do not depend on Scylla materialized views for critical read paths.

### `reconciler`

Responsibilities:

1. Detect stale campaign shards.
2. Detect stuck cards or transport orphan states.
3. Repair or re-enqueue work from durable checkpoints.
4. Produce operator-facing diagnostics.

Constraints:

- Reconciler logic must be deterministic and idempotent.

## Storage Architecture

## PostgreSQL Responsibilities

PostgreSQL remains the system of record for relational metadata and operator-managed entities.

Tables retained in PostgreSQL:

1. `cards`
2. `profiles`
3. `applications`
4. `campaign_definitions`
5. `campaign_runs`
6. `campaign_shards`
7. `scripts`
8. `cap_files`
9. operator audit tables

Tables removed from PostgreSQL hot path:

1. per-card execution state updates
2. per-message execution log updates
3. DLR and MO hot-path persistence
4. large campaign progress scans

### Why PostgreSQL remains

PostgreSQL is the best fit for:

- relational integrity
- configuration and inventory queries
- operator workflows
- transactional campaign run creation

It is not the right fit for sustained hot-path execution writes at 100k TPS.

## ScyllaDB Responsibilities

ScyllaDB is the durable high-write execution store.

Primary uses:

1. durable card execution snapshots
2. durable transport/execution event history
3. campaign progress query tables
4. campaign/card drill-down tables
5. replay and reconciliation backing data

### ScyllaDB design rules

1. Never partition solely by `campaign_id` for high-cardinality campaigns.
2. Use bucketed or sharded partition keys.
3. Prefer explicit query tables over materialized views.
4. Avoid LWT on hot paths.
5. Avoid distributed counters for per-card OTA counters.

### Recommended ScyllaDB tables

#### `card_state_by_card`

Purpose:

- durable latest execution snapshot per card

Primary key:

- partition key: `(card_bucket, card_id)`

Columns:

- `campaign_run_id`
- `status`
- `current_step`
- `retry_count`
- `last_msg_id`
- `counter_value`
- `updated_at`
- `last_error_code`
- `last_error_text`

#### `campaign_card_status_by_bucket`

Purpose:

- query cards by campaign and status without hot campaign-wide partitions

Primary key:

- partition key: `(campaign_run_id, shard_bucket, status)`
- clustering key: `card_id`

Columns:

- `current_step`
- `retry_count`
- `last_msg_id`
- `updated_at`
- `last_error_text`

#### `message_by_card_time`

Purpose:

- efficient card-level message history

Primary key:

- partition key: `(card_bucket, card_id, time_bucket)`
- clustering key: `(created_at, msg_id)`

Columns:

- `campaign_run_id`
- `direction`
- `event_type`
- `status`
- `smpp_message_id`
- `counter_hex`
- `payload_ref`
- `por_status_code`
- `error_code`

#### `message_by_campaign_bucket_time`

Purpose:

- campaign timeline and recent activity by campaign bucket

Primary key:

- partition key: `(campaign_run_id, shard_bucket, time_bucket)`
- clustering key: `(created_at, card_id, msg_id)`

Columns mirror the card timeline table.

#### `campaign_progress_by_bucket`

Purpose:

- durable aggregated counters by campaign shard bucket

Primary key:

- partition key: `(campaign_run_id, shard_bucket)`

Columns:

- `pending`
- `in_progress`
- `completed`
- `failed`
- `skipped`
- `updated_at`

### Counter handling

Per-card OTA counters must not rely on Scylla distributed counters or LWT.

Use this model instead:

1. execution for a card is serialized by Kafka key
2. executor reads latest counter from Redis hot state or Scylla snapshot
3. executor increments in its owned execution stream
4. executor writes updated snapshot back to Scylla

This avoids turning every OTA step into a distributed compare-and-set operation.

## DragonflyDB Coordination Responsibilities

DragonflyDB is the default low-latency ephemeral coordination layer.

Fallback:

1. if benchmarks show one Dragonfly primary cannot sustain the required coordination workload with failover headroom, move this layer to Valkey Cluster
2. application code must therefore depend only on a narrow Redis-compatible coordination interface, not on provider-specific features

Primary uses:

1. card hot state cache
2. campaign command cache
3. card keys cache
4. profile cache
5. dedupe keys
6. transport correlation
7. multipart aggregation
8. throttle buckets
9. temporary progress buckets

### Coordination-store design rules

1. Default production posture is DragonflyDB with 3 nodes: 1 primary and 2 replicas.
2. Avoid single global hot keys.
3. Use bucketed progress and throttle keys.
4. Treat the coordination store as recoverable, not as sole durable truth.
5. Do not rely on provider-specific extensions in the application hot path.

### Required coordination key patterns

1. `card:{card_id}:state`
2. `card:{card_id}:keys`
3. `campaign:{campaign_run_id}:commands`
4. `profile:{profile_id}`
5. `smpp:{smpp_message_id}`
6. `multipart:{msg_id}`
7. `dedupe:{event_id}`
8. `progress:{campaign_run_id}:{bucket}`
9. `throttle:{campaign_run_id}:{bucket}:{second}`

## Kafka Architecture

## Topic Design

All execution and transport topics must use `card_id` as the partition key.

Primary topics:

1. `card-events`
   - `card.activate`
   - `card.dlr_received`
   - `card.mo_received`
   - `card.retry_requested`
   - `card.completed`
   - `card.failed`

2. `send-sms`
   - outbound transport jobs

3. `transport-events`
   - DLR, MO, multipart outcomes, submit failures

4. `execution-events`
   - durable execution log stream for projectors and replay

5. `message-events`
   - optional split-out message audit stream if execution-events becomes too broad

## Topic partitioning

Start with a topology that is serious enough to benchmark honestly but small enough to operate simply.

Recommended starting partition posture:

1. `card-events`: 64 to 128 partitions
2. `send-sms`: 64 to 128 partitions
3. `transport-events`: 64 to 128 partitions
4. `execution-events`: 64 to 128 partitions

Scale-out posture:

1. grow hot topics to 256 to 512 partitions as measured throughput, consumer ownership, and broker capacity require
2. do not start at 3 partitions or other local-dev counts if 100k TPS remains the goal

## Kafka client strategy

For the 100k target, the platform must use a high-throughput Kafka client with:

1. producer batching
2. compression
3. partition-aware consumer control
4. low-allocation message handling
5. partition-safe commit management

Preferred client choices:

1. `franz-go`
2. `confluent-kafka-go`

The current minimal synchronous `kafka-go` producer wrapper is not the target-state data-plane client.

## Campaign Start and Sharding Model

## Campaign runs and shards

Campaign start must be durable and replayable without relying on a per-card SQL outbox as the permanent scaling model.

### Required control-plane tables

#### `campaign_runs`

Fields:

1. `id`
2. `campaign_definition_id`
3. `status`
4. `selection_spec`
5. `created_at`
6. `started_at`
7. `completed_at`
8. `planner_checkpoint`

#### `campaign_shards`

Fields:

1. `id`
2. `campaign_run_id`
3. `shard_key`
4. `selection_chunk`
5. `status`
6. `planned_count`
7. `published_count`
8. `last_card_cursor`
9. `updated_at`

### Planner flow

1. `ota-api` writes a new `campaign_run`.
2. `campaign-planner` claims the run.
3. planner resolves target cards.
4. planner emits deterministic shard manifests.
5. planner persists `campaign_shards`.
6. planner publishes `card.activate` events in batches.
7. planner updates shard checkpoints as it publishes.
8. planner resumes safely from checkpoints after crash.

This model gives the same business safety as the outbox, but with a scaling primitive that is more appropriate for million-card fanout.

## Execution State Machine

Card execution states:

1. `pending`
2. `activating`
3. `awaiting_dlr`
4. `awaiting_mo`
5. `completed`
6. `failed`
7. `skipped`

State transitions are driven only by events keyed by `card_id`.

### Execution flow

1. planner publishes `card.activate`
2. executor loads card state and commands
3. executor allocates next OTA counter and builds GSM 03.48 packet
4. executor publishes `send-sms`
5. transport submits via SMPP
6. transport publishes DLR/MO-derived events
7. executor advances, retries, completes, or fails
8. projector updates read models

## Transport Layer Requirements

## SMS gateway

The transport layer must maximize cross-card concurrency while preserving per-card ordering.

### Required characteristics

1. partition-parallel consumption from `send-sms`
2. many SMPP connections per replica
3. configurable submit windows
4. multiple pools and routes supported
5. multipart tracking in Redis
6. transport ingress published back to Kafka

### Required scaling posture

The target system must support:

1. dozens of `sms-gateway` replicas
2. many SMPP binds per replica
3. enough Kafka partitions to scale linearly
4. transport backpressure via bounded in-flight submit windows

## Read Models and Operator UX

The UI must not depend on raw execution logs or full-table scans.

### Required read models

1. `campaign_summary`
2. `campaign_progress_rollup`
3. `campaign_failed_cards`
4. `campaign_recent_errors`
5. `card_execution_summary`
6. `card_message_timeline`

### Websocket policy

Websocket output must be aggregate-oriented.

Send live:

1. campaign progress rollups
2. sampled errors
3. important state transitions

Do not push every per-card event to browsers at line rate.

## Security Boundaries

1. raw SIM keys remain outside the UI.
2. transport services do not need direct access to Postgres control-plane metadata.
3. secrets must be distributed by a secret manager, not static compose values.
4. Kafka topics carrying execution data must be authenticated and encrypted.
5. internal service APIs must be authenticated.

## Deployment Topology

## Production target shape for 100k TPS

### Infrastructure

1. PostgreSQL HA cluster for control plane
2. Kafka cluster starting at 3 brokers, with scale-out to 5 to 7 brokers as sustained throughput demands
3. DragonflyDB deployment starting at 3 nodes: 1 primary and 2 replicas
4. ScyllaDB cluster starting at 3 nodes, with scale-out to 6 to 12 nodes as write amplification and retention require
5. separate autoscaled deployments for each Go service

### Service deployment posture

1. `ota-api`: 3 to 6 replicas
2. `campaign-planner`: 2 to 4 replicas
3. `card-executor`: dozens of replicas
4. `sms-gateway`: dozens to hundreds of replicas, sized by SMPP route capacity
5. `read-model-projector`: several replicas
6. `reconciler`: 2 or more replicas

## Observability Requirements

The target system must emit:

1. Kafka lag by topic and partition
2. planner publish rate and shard backlog
3. executor event latency and error rate
4. SMPP submit RTT and error distribution
5. DLR latency and PoR latency distributions
6. coordination-store latency and hot-key metrics
7. Scylla write/read latency and partition skew
8. Postgres control-plane query latency
9. campaign completion percentiles
10. card retry and failure reasons

## Capacity Principles

The platform must be sized and validated using measured:

1. average SMS parts per OTA step
2. average steps per card
3. SMPP submit RTT
4. DLR latency
5. PoR latency
6. execution event amplification
7. read-model write amplification

The 100k target is defined as sustained outbound SMS parts per second, not full-card completions per second.

## Migration Principles

1. preserve current per-card sequencing semantics
2. remove PostgreSQL from the hot path before attempting 100k benchmarks
3. split mixed-role binaries before scale testing
4. land projector-built read models before moving operator views off current APIs
5. validate every service independently before end-to-end target tests

## Summary

The target architecture is:

1. PostgreSQL for control-plane metadata
2. Kafka as the event backbone
3. DragonflyDB for hot coordination and cache by default, with Valkey Cluster as the fallback when multi-primary sharding is proven necessary
4. ScyllaDB for durable high-write execution data
5. separate planner, executor, transport, projector, and reconciler services
6. strict per-card ordering via `card_id` partitioning
7. massive cross-card parallelism via high partition counts and horizontal scale

This is the reference architecture for all future implementation and benchmark planning.
