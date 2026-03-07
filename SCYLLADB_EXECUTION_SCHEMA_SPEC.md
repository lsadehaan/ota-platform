# ScyllaDB Execution Schema Specification

## Purpose

This document defines the target ScyllaDB schema for the durable execution plane.

ScyllaDB stores the durable high-write data that the platform generates while processing OTA campaigns at scale.

## Scope

ScyllaDB is responsible for:

1. durable card execution snapshots
2. durable message and execution histories
3. campaign progress rollups by bucket
4. query tables for card and campaign detail views
5. replay and reconciliation support

ScyllaDB is not responsible for:

1. relational configuration metadata
2. operator authoring workflows
3. campaign-definition integrity constraints

## Design Rules

1. Do not partition solely by `campaign_id`.
2. Do not build giant wide partitions for large campaigns.
3. Use bucketed partition keys.
4. Use explicit query tables instead of relying on materialized views for critical paths.
5. Avoid LWT on the execution hot path.
6. Avoid Scylla counter columns for per-card OTA counters.

## Bucketing Strategy

Two bucket concepts are required.

### Card bucket

Derived from `card_id` hash.

Use cases:

1. distribute `card_state_by_card`
2. distribute card timelines
3. spread execution writes uniformly

### Campaign shard bucket

Derived from campaign shard or deterministic shard assignment.

Use cases:

1. campaign status tables
2. campaign progress tables
3. campaign activity views

### Time bucket

Used for message history tables.

Typical values:

1. hourly
2. daily

Choose based on retention volume and query requirements.

## Required Tables

## `card_state_by_card`

Purpose:

- latest durable execution snapshot for a card

Primary key:

```text
PRIMARY KEY ((card_bucket, card_id))
```

Columns:

1. `card_bucket INT`
2. `card_id UUID`
3. `campaign_run_id UUID`
4. `campaign_definition_id UUID`
5. `status TEXT`
6. `current_step INT`
7. `retry_count INT`
8. `last_msg_id UUID`
9. `last_smpp_message_id TEXT`
10. `counter_value BIGINT`
11. `last_error_code TEXT`
12. `last_error_text TEXT`
13. `updated_at TIMESTAMP`
14. `version BIGINT`

Notes:

- `version` is useful for stale snapshot detection and projector ordering.
- one row per card

## `campaign_card_status_by_bucket`

Purpose:

- query cards in a campaign by status

Primary key:

```text
PRIMARY KEY ((campaign_run_id, shard_bucket, status), card_id)
```

Columns:

1. `campaign_run_id UUID`
2. `shard_bucket INT`
3. `status TEXT`
4. `card_id UUID`
5. `current_step INT`
6. `retry_count INT`
7. `last_msg_id UUID`
8. `last_error_text TEXT`
9. `updated_at TIMESTAMP`

Notes:

- this is a query table maintained by projector or executor events
- do not use a materialized view on top of `card_state_by_card` for this
- a status change requires an insert into the new status partition and a delete from the old one
- apply insert-first-then-delete ordering so a projector crash causes temporary duplication rather than temporary invisibility
- projector repair or replay must remove stale old-status rows idempotently

## `campaign_progress_by_bucket`

Purpose:

- durable aggregated progress counts by campaign shard bucket

Primary key:

```text
PRIMARY KEY ((campaign_run_id, shard_bucket))
```

Columns:

1. `campaign_run_id UUID`
2. `shard_bucket INT`
3. `pending BIGINT`
4. `in_progress BIGINT`
5. `completed BIGINT`
6. `failed BIGINT`
7. `skipped BIGINT`
8. `updated_at TIMESTAMP`

Notes:

- updates should be idempotent and derived from execution events
- avoid a single campaign-global row

## `message_by_card_time`

Purpose:

- efficient message and event timeline for a card

Primary key:

```text
PRIMARY KEY ((card_bucket, card_id, time_bucket), created_at, msg_id)
```

Columns:

1. `card_bucket INT`
2. `card_id UUID`
3. `time_bucket TEXT`
4. `created_at TIMESTAMP`
5. `msg_id UUID`
6. `campaign_run_id UUID`
7. `direction TEXT`
8. `event_type TEXT`
9. `status TEXT`
10. `smpp_message_id TEXT`
11. `counter_hex TEXT`
12. `por_status_code SMALLINT`
13. `error_code TEXT`
14. `raw_payload BLOB`
15. `secured_payload BLOB`
16. `por_data BLOB`

Notes:

- cluster by `created_at, msg_id` for timeline order
- this table supports card detail views

## `message_by_campaign_bucket_time`

Purpose:

- recent activity and campaign timeline by bucket

Primary key:

```text
PRIMARY KEY ((campaign_run_id, shard_bucket, time_bucket), created_at, card_id, msg_id)
```

Columns:

1. `campaign_run_id UUID`
2. `shard_bucket INT`
3. `time_bucket TEXT`
4. `created_at TIMESTAMP`
5. `card_id UUID`
6. `msg_id UUID`
7. `direction TEXT`
8. `event_type TEXT`
9. `status TEXT`
10. `smpp_message_id TEXT`
11. `por_status_code SMALLINT`
12. `error_code TEXT`

Notes:

- this table supports campaign detail recent activity and operator inspection

## `failed_cards_by_campaign_bucket`

Purpose:

- quick access to failed cards and their error reasons

Primary key:

```text
PRIMARY KEY ((campaign_run_id, shard_bucket), card_id)
```

Columns:

1. `campaign_run_id UUID`
2. `shard_bucket INT`
3. `card_id UUID`
4. `failed_at TIMESTAMP`
5. `current_step INT`
6. `retry_count INT`
7. `last_msg_id UUID`
8. `last_error_code TEXT`
9. `last_error_text TEXT`

Notes:

- explicit query table for retry-failed workflows

## `campaign_summary`

Purpose:

- compact read model for campaign list and summary pages

Primary key:

```text
PRIMARY KEY ((campaign_run_id))
```

Columns:

1. `campaign_run_id UUID`
2. `status TEXT`
3. `total_cards BIGINT`
4. `pending BIGINT`
5. `in_progress BIGINT`
6. `completed BIGINT`
7. `failed BIGINT`
8. `skipped BIGINT`
9. `started_at TIMESTAMP`
10. `completed_at TIMESTAMP`
11. `updated_at TIMESTAMP`

Notes:

- projector-maintained aggregate for operator list pages

## Counter Model

Per-card OTA counters must be handled through serialized execution, not Scylla counter columns.

Recommended behavior:

1. consume card events keyed by `card_id`
2. executor loads latest snapshot
3. executor increments counter in its owned processing path
4. executor writes the updated snapshot row
5. executor emits an execution event with the allocated counter value

This approach avoids heavy distributed synchronization.

## Data Retention

Retention policies should be table-specific.

Recommended defaults:

1. `card_state_by_card`: keep latest indefinitely
2. `campaign_progress_by_bucket`: keep for the life of the campaign plus audit retention window
3. `campaign_summary`: keep for operator retention window
4. `message_by_card_time`: retain according to business audit need, possibly 30 to 180 days or more
5. `message_by_campaign_bucket_time`: shorter retention than card timelines if needed
6. `failed_cards_by_campaign_bucket`: retain until campaign archival policy expires

## Projection Ownership

The following ownership model is recommended.

### Executor-owned writes

1. `card_state_by_card`
2. execution events to Kafka

### Projector-owned writes

1. `campaign_card_status_by_bucket`
2. `campaign_progress_by_bucket`
3. `message_by_card_time`
4. `message_by_campaign_bucket_time`
5. `failed_cards_by_campaign_bucket`
6. `campaign_summary`

This keeps the executor focused on correctness and lets projectors build query-optimized tables.

## Query Patterns Supported

### Card detail

1. get latest snapshot from `card_state_by_card`
2. get recent message history from `message_by_card_time`

### Campaign summary

1. get aggregate from `campaign_summary`
2. get bucket progress from `campaign_progress_by_bucket` if deeper diagnostics are needed

### Campaign failed cards

1. query `failed_cards_by_campaign_bucket`
2. merge paginated buckets at API layer if necessary

### Campaign recent activity

1. query recent entries from `message_by_campaign_bucket_time`

### Reconciliation

1. scan bucketed card snapshots for stale statuses
2. compare against Kafka/projector checkpoints

## Anti-Patterns To Avoid

1. partitioning the main execution tables only by `campaign_run_id`
2. using Scylla materialized views for core high-rate operational queries
3. using LWT for every state transition
4. using counter columns for per-card OTA counters
5. forcing ad hoc filtering queries that require cluster-wide scans

## Example Ownership Matrix

| Concern | PostgreSQL | ScyllaDB | Redis |
|---|---|---|---|
| campaign definitions | yes | no | cache only |
| planner shards | yes | no | no |
| latest card execution state | no | yes | hot cache |
| card timeline | no | yes | no |
| campaign recent activity | no | yes | no |
| transport correlation | no | no | yes |
| multipart tracking | no | no | yes |
| dedupe | no | no | yes |

## Summary

ScyllaDB is the durable execution store for the platform.

Its schema must be designed around bucketed partitions, explicit query tables, and replay/projector ownership, not around relational-style campaign-centric tables or heavyweight distributed synchronization primitives.
