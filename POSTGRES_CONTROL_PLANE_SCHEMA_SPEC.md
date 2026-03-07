# PostgreSQL Control-Plane Schema Specification

## Purpose

This document defines the target PostgreSQL schema for the control plane.

PostgreSQL remains the system of record for relational metadata, operator-managed entities, campaign definitions, and campaign run orchestration metadata.

It is explicitly not the durable store for high-rate per-message execution traffic.

## Scope

The following domains belong in PostgreSQL:

1. card inventory
2. profiles and applications
3. scripts and CAP metadata
4. campaign definitions
5. campaign runs
6. campaign shards
7. operator and audit metadata

The following do not belong in PostgreSQL hot paths:

1. per-card state transitions during execution
2. per-message execution logs during transport processing
3. DLR/MO event storage at line rate
4. campaign progress counters updated on every transition

## Existing Tables To Retain and Evolve

### `profiles`

> **Status: Exists.** No schema changes required.

Purpose:

- transport and security-profile defaults for groups of cards

Keep from current model:

1. `id`
2. `name`
3. `max_concat_sms`
4. `buffer_size`
5. `pid`
6. `dcs`
7. `security_bytes_type`
8. timestamps

### `applications`

> **Status: Exists.** No schema changes required.

Purpose:

- TAR-addressed applications and their security configuration

Keep from current model:

1. `id`
2. `profile_id`
3. `name`
4. `tar`
5. KIC/KID algorithms and modes
6. keyset IDs
7. certification and PoR settings

### `cards`

> **Status: Exists.** Recommended additions (`tenant_id`, `card_hash_bucket`) have not been applied yet.

Purpose:

- card inventory and card-to-profile association

Keep from current model:

1. `id`
2. `iccid`
3. `imsi`
4. `msisdn`
5. `profile_id`
6. key material columns if keys remain PostgreSQL-resident
7. lifecycle `status`
8. timestamps

Recommended additions:

1. `tenant_id` if multi-tenant support is expected
2. `card_hash_bucket` computed or persisted for selection and sharding support
3. `updated_at`

### `scripts`

> **Status: Exists.** No schema changes required.

Purpose:

- reusable operator-defined command templates

Keep from current model, but treat only as control-plane authoring objects.

### `cap_files`

> **Status: Exists.** No schema changes required.

Purpose:

- uploaded CAP assets and metadata

Keep from current model, but treat only as control-plane assets.

## New Control-Plane Tables

## `campaign_definitions`

> **Status: NOT YET CREATED.** The codebase currently uses a unified `campaigns` table
> that combines definition and run concerns. This table is part of the planned
> definition/run split.

Purpose:

- immutable or versioned campaign templates defined by operators

Recommended columns:

1. `id UUID PRIMARY KEY`
2. `name TEXT NOT NULL`
3. `campaign_type TEXT NOT NULL`
4. `definition_version INT NOT NULL`
5. `description TEXT NULL`
6. `created_by TEXT NULL`
7. `created_at TIMESTAMPTZ NOT NULL`
8. `updated_at TIMESTAMPTZ NOT NULL`
9. `is_active BOOLEAN NOT NULL DEFAULT TRUE`

Notes:

- This separates reusable campaign intent from a specific execution run.

## `campaign_definition_steps`

> **Status: NOT YET CREATED.** Campaign steps are currently stored in the
> `campaign_commands` table. This table is part of the planned definition/run split.

Purpose:

- ordered steps for a campaign definition

Recommended columns:

1. `id UUID PRIMARY KEY`
2. `campaign_definition_id UUID NOT NULL REFERENCES campaign_definitions(id)`
3. `sequence INT NOT NULL`
4. `application_id UUID NOT NULL REFERENCES applications(id)`
5. `step_kind TEXT NOT NULL`
6. `script_template JSONB NOT NULL`
7. `compiled_command_bytes BYTEA NULL`
8. `expect_response BOOLEAN NOT NULL`
9. `metadata JSONB NOT NULL DEFAULT '{}'::jsonb`
10. `created_at TIMESTAMPTZ NOT NULL`

Indexes:

1. unique `(campaign_definition_id, sequence)`

Notes:

1. `script_template` is the canonical control-plane representation for operator-authored script intent and parameterization
2. `compiled_command_bytes` is optional and should only be populated when a step is stored as pre-built literal bytes
3. use JSONB rather than raw `BYTEA` for authoring workflows that require variable substitution or structured step metadata

## `campaign_runs`

> **Status: NOT YET CREATED.** The codebase currently uses the unified `campaigns` table
> for both definition and execution state. This table is part of the planned
> definition/run split.

Purpose:

- a specific execution instance of a campaign definition

Recommended columns:

1. `id UUID PRIMARY KEY`
2. `campaign_definition_id UUID NOT NULL REFERENCES campaign_definitions(id)`
3. `status TEXT NOT NULL`
4. `selection_spec JSONB NOT NULL`
5. `target_count BIGINT NULL`
6. `scheduled_at TIMESTAMPTZ NULL`
7. `started_at TIMESTAMPTZ NULL`
8. `completed_at TIMESTAMPTZ NULL`
9. `created_by TEXT NULL`
10. `created_at TIMESTAMPTZ NOT NULL`
11. `updated_at TIMESTAMPTZ NOT NULL`
12. `max_retries INT NOT NULL DEFAULT 3`
13. `throttle_sms_per_sec INT NULL`
14. `max_concat_override INT NULL`
15. `planner_state JSONB NOT NULL DEFAULT '{}'::jsonb`

Recommended statuses:

1. `pending`
2. `planning`
3. `publishing`
4. `running`
5. `paused`
6. `completed`
7. `completed_with_errors`
8. `failed`
9. `aborted`

## `campaign_shards`

> **Status: EXISTS (partial alignment).** The `campaign_shards` table exists in the
> current schema with 500-card shards and status tracking. However, its foreign key
> currently references the unified `campaigns` table rather than a `campaign_runs` table.
> Column set may differ from the recommended spec below.

Purpose:

- durable planner work units and publication checkpoints

Recommended columns:

1. `id UUID PRIMARY KEY`
2. `campaign_run_id UUID NOT NULL REFERENCES campaign_runs(id) ON DELETE CASCADE`
3. `shard_key TEXT NOT NULL`
4. `shard_index INT NOT NULL`
5. `selection_chunk JSONB NOT NULL`
6. `status TEXT NOT NULL`
7. `planned_count BIGINT NOT NULL DEFAULT 0`
8. `published_count BIGINT NOT NULL DEFAULT 0`
9. `last_card_cursor TEXT NULL`
10. `planner_owner TEXT NULL`
11. `last_published_at TIMESTAMPTZ NULL`
12. `created_at TIMESTAMPTZ NOT NULL`
13. `updated_at TIMESTAMPTZ NOT NULL`

Recommended statuses:

1. `pending`
2. `claimed`
3. `publishing`
4. `published`
5. `reconciling`
6. `failed`

Indexes:

1. `(campaign_run_id, status)`
2. unique `(campaign_run_id, shard_index)`

## `campaign_run_audit`

> **Status: NOT YET CREATED.**

Purpose:

- operator and system audit trail for campaign run lifecycle

Recommended columns:

1. `id UUID PRIMARY KEY`
2. `campaign_run_id UUID NOT NULL REFERENCES campaign_runs(id) ON DELETE CASCADE`
3. `actor_type TEXT NOT NULL`
4. `actor_id TEXT NULL`
5. `action TEXT NOT NULL`
6. `details JSONB NOT NULL DEFAULT '{}'::jsonb`
7. `created_at TIMESTAMPTZ NOT NULL`

## Optional `tenants`

> **Status: NOT YET CREATED.** Multi-tenant support has not been implemented.

If multi-tenant support is needed, add:

1. `tenants`
2. `tenant_id` foreign keys on cards, profiles, applications, scripts, cap files, campaign definitions, and campaign runs

## Existing Tables To Deprecate From Hot Path

### `campaign_cards`

> **Status: DEPRECATION NOT YET DONE.** `campaign_cards` is still actively written to
> on the execution hot path. It remains part of the current runtime flow.

Current purpose:

- per-card execution tracking in PostgreSQL

Target disposition:

- remove from hot execution path
- optional transitional compatibility table only

### `message_log`

> **Status: DEPRECATION NOT YET DONE.** The projector service still writes to
> `message_log` in PostgreSQL on every message event.

Current purpose:

- per-message execution logging in PostgreSQL

Target disposition:

- retire from hot-path ownership
- Scylla-backed query tables replace it for execution history

### `outbox`

Current purpose:

- transactional outbox for per-card activation

Target disposition:

- not the long-term primitive for campaign fanout at 100k TPS
- either retire or limit to low-rate control-plane integration events only

## Indexing Guidance

PostgreSQL indexes should optimize control-plane queries, not data-plane write rates.

Keep or add indexes for:

1. card inventory lookup by `iccid`, `imsi`, `msisdn`
2. cards by `profile_id` and lifecycle `status`
3. campaign runs by `status`, `created_at`, `scheduled_at`
4. campaign shards by `(campaign_run_id, status)`
5. campaign definition steps by `(campaign_definition_id, sequence)`

Avoid indexes designed only for execution-hot timelines or per-message tracking once those move to Scylla.

## Constraints and Rules

1. The control plane must reject invalid campaign definitions before the planner sees them.
2. Campaign definitions must reference real applications; do not use synthetic placeholder applications as the long-term model.
3. Campaign runs must store deterministic target selection specifications.
4. Shard checkpoints must allow replay after planner crash.
5. PostgreSQL schema must remain normalized enough for reliable operator workflows.

## Example Ownership Matrix

| Entity | PostgreSQL | ScyllaDB | Redis |
|---|---|---|---|
| cards | yes | no | cache only |
| profiles | yes | no | cache only |
| applications | yes | no | cache only |
| campaign definitions | yes | no | cache only |
| campaign runs | yes | summary mirrors optional | cache optional |
| campaign shards | yes | no | no |
| per-card execution snapshot | no | yes | hot cache |
| message history | no | yes | no |
| progress buckets | no durable | yes | temporary hot counters |

## Migration Notes

1. This should be an additive migration, not an in-place destructive rewrite.
2. Add `campaign_definitions`, `campaign_definition_steps`, `campaign_runs`, and `campaign_shards` first.
3. Map existing `campaigns` into `campaign_definitions` plus `campaign_runs`.
4. Map existing `campaign_commands` into `campaign_definition_steps`.
5. Keep `campaign_cards` and `message_log` as legacy transitional structures until executor and projector ownership has fully moved off PostgreSQL.
6. Existing PostgreSQL card inventory and application data should remain authoritative throughout migration.

## Summary

PostgreSQL remains the control-plane source of truth for relational metadata and campaign orchestration state.

It must not remain the line-rate execution persistence layer if the platform is to support the 100k TPS target with operational headroom.
