# OTA Platform Service API Specification

## Purpose

This document defines the service API boundaries for the target OTA platform architecture.

It covers:

1. external operator-facing APIs
2. internal service responsibilities
3. internal service interfaces and contracts
4. ownership boundaries
5. recommended endpoint groups

This is not a complete OpenAPI document. It is the architectural API contract that implementation-specific HTTP, gRPC, or async interfaces must follow.

## Service Inventory

The target architecture contains these services:

1. `ota-api`
2. `campaign-planner`
3. `card-executor`
4. `sms-gateway`
5. `read-model-projector`
6. `reconciler`

Only `ota-api` is intended to be directly exposed to operators or the browser.

## Service Boundary Rules

1. `ota-api` owns all external synchronous APIs.
2. `campaign-planner`, `card-executor`, `sms-gateway`, and `read-model-projector` communicate primarily via Kafka.
3. Internal synchronous APIs should be minimal and operational, not business-flow critical where async events are more appropriate.
4. The browser must not call execution or transport services directly.
5. Service-to-service access to Postgres, Scylla, and Redis must follow clear ownership boundaries.

## 1. `ota-api`

### Responsibilities

1. expose control-plane CRUD APIs
2. create campaign definitions and campaign runs
3. expose read-model-backed campaign and card views
4. authenticate operators
5. provide websocket feeds for aggregate live updates

### Data ownership

Direct data stores:

1. PostgreSQL for control-plane entities
2. Scylla read models for execution detail pages **(target -- not yet available; all reads currently from PostgreSQL)**

Indirect data via Kafka:

- planner and execution are triggered asynchronously, not by direct internal API chaining

### External API groups

## Profiles

**Status: Implemented**

Endpoints:

1. `GET /api/v1/profiles`
2. `POST /api/v1/profiles`
3. `GET /api/v1/profiles/:id`
4. `PUT /api/v1/profiles/:id`
5. `DELETE /api/v1/profiles/:id`
6. `POST /api/v1/profiles/:id/applications`
7. `PUT /api/v1/profiles/:id/applications/:appId`
8. `DELETE /api/v1/profiles/:id/applications/:appId`

## Cards

**Status: Implemented**

Endpoints:

1. `GET /api/v1/cards`
2. `POST /api/v1/cards`
3. `GET /api/v1/cards/:id`
4. `PUT /api/v1/cards/:id`
5. `DELETE /api/v1/cards/:id`
6. `GET /api/v1/cards/:id/counters`
7. `POST /api/v1/cards/import`
8. `GET /api/v1/cards/export`

Notes:

- card detail should combine control-plane metadata with projector-built execution summary read models
- `GET /api/v1/cards/:id/counters` returns per-application counter state for a card

## Card groups

**Status: Implemented**

Endpoints:

1. `GET /api/v1/card-groups`
2. `POST /api/v1/card-groups`
3. `GET /api/v1/card-groups/:id`
4. `PUT /api/v1/card-groups/:id`
5. `DELETE /api/v1/card-groups/:id`
6. `POST /api/v1/card-groups/:id/members`
7. `DELETE /api/v1/card-groups/:id/members`

## Scripts and CAP assets

**Status: Implemented**

Endpoints:

1. `GET /api/v1/scripts`
2. `POST /api/v1/scripts`
3. `GET /api/v1/scripts/:id`
4. `PUT /api/v1/scripts/:id`
5. `DELETE /api/v1/scripts/:id`
6. `GET /api/v1/caps`
7. `POST /api/v1/caps/upload`
8. `GET /api/v1/caps/:id`
9. `DELETE /api/v1/caps/:id`
10. `GET /api/v1/caps/:id/apdu-preview`

## Campaigns

**Status: Partially implemented (unified model)**

> **Implementation note:** The current implementation uses a unified `/api/v1/campaigns` resource instead of the target `campaign-definitions` / `campaign-runs` split. The definition/run separation is a future target. The currently implemented endpoints are listed first, followed by the target endpoints.

### Current implemented endpoints

1. `GET /api/v1/campaigns`
2. `POST /api/v1/campaigns`
3. `GET /api/v1/campaigns/:id`
4. `POST /api/v1/campaigns/:id/start`
5. `POST /api/v1/campaigns/:id/pause`
6. `POST /api/v1/campaigns/:id/resume`
7. `POST /api/v1/campaigns/:id/abort`
8. `POST /api/v1/campaigns/:id/retry-failed`

### Target: Campaign definitions

**Status: Not yet implemented**

Recommended endpoints:

1. `GET /api/v1/campaign-definitions`
2. `POST /api/v1/campaign-definitions`
3. `GET /api/v1/campaign-definitions/{definition_id}`
4. `PUT /api/v1/campaign-definitions/{definition_id}`
5. `DELETE /api/v1/campaign-definitions/{definition_id}`

### Target: Campaign runs

**Status: Not yet implemented**

Recommended endpoints:

1. `GET /api/v1/campaign-runs`
2. `POST /api/v1/campaign-runs`
3. `GET /api/v1/campaign-runs/{run_id}`
4. `POST /api/v1/campaign-runs/{run_id}/resume`
5. `POST /api/v1/campaign-runs/{run_id}/abort`
6. `POST /api/v1/campaign-runs/{run_id}/retry-failed`

`retry-failed` semantics:

1. it should create a new campaign run targeting only the failed cards from the source run
2. it should not mutate historical execution state in the existing run
3. the new run should record `source_run_id` or equivalent metadata for traceability

### Target: Campaign-run detail endpoints

**Status: Not yet implemented**

Recommended read-model-backed endpoints:

1. `GET /api/v1/campaign-runs/{run_id}/summary`
2. `GET /api/v1/campaign-runs/{run_id}/cards`
3. `GET /api/v1/campaign-runs/{run_id}/failed-cards`
4. `GET /api/v1/campaign-runs/{run_id}/activity`
5. `GET /api/v1/campaign-runs/{run_id}/errors`

## Dashboard and monitoring

**Status: Implemented**

Endpoints:

1. `GET /api/v1/dashboard/kpis`
2. `GET /api/v1/dashboard/activity`
3. `GET /api/v1/dashboard/sms-throughput`
4. `GET /api/v1/monitoring/health`
5. `GET /api/v1/monitoring/messages`
6. `GET /api/v1/monitoring/messages/:id`
7. `GET /api/v1/monitoring/errors`

Notes:

- `GET /api/v1/monitoring/messages` lists message log entries with pagination
- `GET /api/v1/monitoring/messages/:id` retrieves a single message log entry by ID

## Settings

**Status: Implemented**

Endpoints:

1. `GET /api/v1/settings`
2. `PUT /api/v1/settings`

### Websocket

**Status: Implemented**

Websocket endpoint:

1. `GET /ws`

Websocket payload policy:

1. aggregate campaign progress events
2. sampled critical errors
3. no raw per-card firehose delivery at scale
4. push aggregate updates at most once per second per campaign run unless an operator explicitly requests a narrower debug stream

Reference:

1. the data ownership table in this document is the canonical ownership matrix for service-to-store responsibilities and should be referenced by the target architecture and schema documents

## 2. `campaign-planner`

### Responsibilities

1. claim campaign runs
2. expand target sets
3. create and manage shards
4. publish `card.activate` events
5. maintain publication checkpoints

### External API surface

No browser-facing API.

### Recommended internal interfaces

**Status: Not yet implemented**

#### Operational HTTP endpoints

1. `GET /healthz`
2. `GET /readyz`
3. `GET /metrics`
4. `GET /debug/shards`
5. `POST /internal/reconcile/run/{run_id}`

These are operational only.

### Data ownership

1. reads and writes `campaign_runs` and `campaign_shards` in PostgreSQL
2. publishes to Kafka

## 3. `card-executor`

### Responsibilities

1. consume `card-events`
2. manage card execution state machine
3. allocate counters
4. build GSM 03.48 packets
5. publish `send-sms`
6. publish execution and message events
7. write durable snapshots to Scylla

### External API surface

No browser-facing API.

### Recommended internal interfaces

**Status: Not yet implemented**

#### Operational HTTP endpoints

1. `GET /healthz`
2. `GET /readyz`
3. `GET /metrics`
4. `GET /debug/partitions`
5. `GET /debug/card/{card_id}`

#### Optional internal admin endpoint

1. `POST /internal/reconcile/card/{card_id}`

Use only for operational tooling, not normal business flow.

### Data ownership

1. reads Redis caches and Scylla snapshots
2. writes Scylla snapshots
3. publishes Kafka execution and transport-job events

## 4. `sms-gateway`

### Responsibilities

1. consume `send-sms`
2. submit SMS over SMPP
3. track correlation and multipart outcomes
4. publish transport ingress and submit-result events

### External API surface

No browser-facing API.

### Recommended internal interfaces

**Status: Not yet implemented**

#### Operational HTTP endpoints

1. `GET /healthz`
2. `GET /readyz`
3. `GET /metrics`
4. `GET /debug/routes`
5. `GET /debug/pool`

### Data ownership

1. Redis for transport coordination
2. Kafka for transport job consumption and event production
3. no direct browser-facing read model ownership

## 5. `read-model-projector`

### Responsibilities

1. consume execution and transport events
2. build Scylla query tables
3. emit aggregate live-update events if required
4. maintain read-model freshness for dashboards and detail pages

### External API surface

No browser-facing API by default.

Optional internal query API is acceptable if you prefer a dedicated read API service, but the simpler default is for `ota-api` to read projector-built query tables directly.

### Recommended internal interfaces

**Status: Not yet implemented**

1. `GET /healthz`
2. `GET /readyz`
3. `GET /metrics`
4. `GET /debug/lag`
5. `POST /internal/rebuild/{read_model}`

### Data ownership

1. Scylla query tables
2. optional Kafka live-update topic if websocket fanout is decoupled further later

## 6. `reconciler`

**Status: Not yet implemented**

### Responsibilities

1. detect stale runs, shards, cards, and transport states
2. publish repair events
3. coordinate recovery after failures
4. provide operator-visible diagnostics

### External API surface

No browser-facing API.

### Recommended internal interfaces

1. `GET /healthz`
2. `GET /readyz`
3. `GET /metrics`
4. `GET /debug/stuck-runs`
5. `GET /debug/stuck-cards`
6. `POST /internal/reconcile/run/{run_id}`
7. `POST /internal/reconcile/card/{card_id}`

## Cross-Service Interaction Model

## Synchronous interactions

Keep synchronous inter-service calls minimal.

Recommended synchronous paths:

1. browser -> `ota-api`
2. operator websocket -> `ota-api`
3. operational tooling -> service `/healthz`, `/metrics`, and debug endpoints

Avoid using synchronous calls for core execution flow between planner, executor, transport, and projector.

## Asynchronous interactions

Core business flow is asynchronous through Kafka.

Primary flows:

1. `ota-api` creates `campaign_run`
2. planner reacts and publishes `card.activate`
3. executor publishes `send-sms`
4. transport publishes `transport-events`
5. executor publishes `execution-events`
6. projector updates read models
7. `ota-api` serves read models to UI

## API Contract Principles

1. External API responses must be typed and stable.
2. Read-model endpoints must return flattened shapes the UI actually needs.
3. Pagination must exist for all large result sets.
4. No lossy generic envelope-unwrapping assumptions in clients.
5. Internal service APIs must be authenticated even if only cluster-internal.

## Recommended External Response Shapes

## Campaign run summary

```json
{
  "id": "uuid",
  "campaign_definition_id": "uuid",
  "status": "running",
  "total_cards": 1000000,
  "pending": 100,
  "in_progress": 50000,
  "completed": 940000,
  "failed": 9900,
  "skipped": 0,
  "started_at": "timestamp",
  "completed_at": null,
  "updated_at": "timestamp"
}
```

## Campaign run cards page

```json
{
  "data": [
    {
      "card_id": "uuid",
      "status": "failed",
      "current_step": 2,
      "retry_count": 3,
      "last_msg_id": "uuid",
      "last_error": "DLR UNDELIV"
    }
  ],
  "page": 1,
  "page_size": 50,
  "total": 1000,
  "total_pages": 20
}
```

## Card detail response

```json
{
  "card": {
    "id": "uuid",
    "iccid": "...",
    "imsi": "...",
    "msisdn": "...",
    "profile_id": "uuid"
  },
  "execution": {
    "campaign_run_id": "uuid",
    "status": "awaiting_mo",
    "current_step": 2,
    "retry_count": 1,
    "last_msg_id": "uuid",
    "counter_value": 12345,
    "updated_at": "timestamp"
  },
  "recent_messages": {
    "data": []
  }
}
```

## Authentication and Authorization

### External operator auth

`ota-api` must support:

1. API key auth for simple deployments
2. future-compatible session or token auth for richer operator environments

### Internal service auth

All internal service endpoints should require one of:

1. mTLS
2. signed service tokens
3. cluster-internal auth proxy

## Health and Readiness Contracts

**Status: Not yet implemented on any service**

Every service must expose:

1. `/healthz`
2. `/readyz`
3. `/metrics`

### Health semantics

`/healthz`

- process is alive

`/readyz`

- service is able to process its primary workload safely

Examples:

- planner ready only if PostgreSQL and Kafka are available
- executor ready only if Kafka, Redis, and Scylla are reachable
- transport ready only if Kafka and at least one SMPP route is usable

## Error Contract Principles

External API errors should follow a standard shape:

```json
{
  "error": {
    "code": "string",
    "message": "string",
    "details": {}
  }
}
```

Recommended error categories:

1. `validation_error`
2. `not_found`
3. `conflict`
4. `unauthorized`
5. `forbidden`
6. `dependency_unavailable`
7. `internal_error`

## Data Ownership Summary

> **Note:** ScyllaDB is not yet available. All reads and writes currently use PostgreSQL. The table below reflects the target architecture.

| Service | PostgreSQL | ScyllaDB (target) | Redis | Kafka |
|---|---|---|---|---|
| ota-api | read/write | read query tables | optional cache | publish planner events optional |
| campaign-planner | read/write | no | optional cache | publish card events |
| card-executor | no control-plane writes in hot path | read/write snapshots | read/write hot state | consume and publish |
| sms-gateway | no | no | read/write coordination | consume and publish |
| read-model-projector | no | write query tables | optional cache | consume |
| reconciler | read runs/shards | read snapshots/query tables | read/write repair helpers | publish repair events |

## Recommended Future OpenAPI Split

When formal API specs are generated, split them into:

1. `ota-api-control-openapi.yaml`
2. `ota-api-readmodels-openapi.yaml`
3. internal operational endpoint specs per service if needed

Do not merge all internal and external APIs into one giant contract.

## Summary

The target service API model is:

1. one external operator-facing API service
2. async business flow between planner, executor, transport, projector, and reconciler
3. small operational HTTP surfaces on internal services
4. clear data ownership boundaries
5. read-model-backed external query endpoints

This is the API contract model that should guide future implementation.
