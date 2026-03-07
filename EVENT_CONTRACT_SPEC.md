# OTA Platform Event Contract Specification

## Purpose

This document defines the canonical event contracts for the target OTA platform architecture.

It specifies:

1. Kafka topics
2. partition keys
3. ordering rules
4. message schemas
5. replay and idempotency semantics
6. producer and consumer responsibilities

This document is the source of truth for event design across `ota-api`, `campaign-planner`, `card-executor`, `sms-gateway`, `read-model-projector`, and `reconciler`.

## Design Rules

1. All hot-path execution and transport topics must be partitioned by `card_id` unless explicitly stated otherwise.
2. Per-card ordering is preserved by Kafka partitioning, not by distributed locks.
3. Every event must be replay-safe.
4. Every event must have a stable event ID.
5. Every event must include enough context to support tracing and reconciliation.
6. Events are immutable.
7. Schema evolution must be additive and versioned.

## Wire Format

Target wire format is protobuf.

> **Current implementation:** Wire format is JSON. The `.proto` files in `schemas/proto` exist as schema definitions but are not compiled to Go code. All Kafka messages are currently serialized and deserialized as JSON.

Notes:

1. the `.proto` files in `schemas/proto` are the canonical machine-readable contracts (target)
2. JSON examples in this document match the current wire format
3. IDs remain strings in the canonical v1 schema for debuggability and easier cross-system inspection

## Canonical Envelope

All events must use a common envelope.

```json
{
  "event_id": "uuid",
  "event_type": "string",
  "schema_version": 1,
  "occurred_at": "2026-03-07T12:00:00Z",
  "trace_id": "uuid-or-string",
  "producer": "service-name",
  "tenant_id": "optional-uuid",
  "campaign_run_id": "optional-uuid",
  "campaign_definition_id": "optional-uuid",
  "shard_id": "optional-uuid",
  "card_id": "optional-uuid",
  "payload": {}
}
```

### Required envelope fields

1. `event_id`
2. `event_type`
3. `schema_version`
4. `occurred_at`
5. `producer`
6. `payload`

### Conditionally required fields

1. `card_id` for all per-card execution or transport events
2. `campaign_run_id` for all campaign execution events
3. `shard_id` for planner and shard-scoped events
4. `trace_id` for cross-service tracing

## Topic Inventory

## 1. `planner-events`

**Status: Implemented**

Purpose:

- control-plane to planner orchestration events

Partition key:

- `campaign_run_id`

Primary producers:

1. `ota-api`
2. `reconciler` for replay or repair triggers **(target -- reconciler not yet implemented)**

Primary consumers:

1. `campaign-planner`

Typical volume:

- low relative to card-execution topics

Event types:

1. `campaign.run.created` -- **implemented**
2. `campaign.run.resume_requested` -- **implemented**
3. `campaign.run.abort_requested` -- **implemented**
4. `campaign.run.reconcile_requested` -- **target only, not yet implemented** (requires reconciler service)

## 2. `card-events`

**Status: Implemented**

Purpose:

- canonical per-card execution state machine topic

Partition key:

- `card_id`

Primary producers:

1. `campaign-planner`
2. `card-executor`
3. `sms-gateway`
4. `reconciler` **(target -- reconciler not yet implemented)**

Primary consumers:

1. `card-executor`
2. `read-model-projector`
3. optional observability or audit consumers

This is the most important topic for preserving ordering.

Intentional feedback-loop rule:

1. `card-executor` intentionally consumes `card-events` that it also produces, for example `card.activate` for the next step or retry
2. this is correct by design because the topic is the serialized per-card state-machine input stream
3. executor logic must never advance state solely because it published an event; it must still validate snapshot version, expected status, and retry/step semantics on consume
4. dedupe plus state validation is mandatory to avoid self-triggered duplicate transitions

## 3. `send-sms`

**Status: Implemented**

Purpose:

- outbound transport jobs for SMS submission

Partition key:

- `card_id`

Primary producers:

1. `card-executor`

Primary consumers:

1. `sms-gateway`

## 4. `transport-events`

**Status: Implemented**

Purpose:

- transport ingress and submit-result events

Partition key:

- `card_id`

Primary producers:

1. `sms-gateway`

Primary consumers:

1. `card-executor`
2. `read-model-projector`

## 5. `execution-events`

**Status: Implemented**

Purpose:

- immutable execution log stream for projectors and audit consumers

Partition key:

- `card_id`

Primary producers:

1. `card-executor`
2. `campaign-planner` for planner-side execution-relevant events
3. `reconciler` for repair actions **(target -- reconciler not yet implemented)**

Primary consumers:

1. `read-model-projector`
2. optional audit/export consumers

## 6. `message-log`

**Status: Implemented**

> **Note:** This topic was originally named `message-events` in earlier drafts. The implementation uses `message-log` as the canonical topic name (see `TopicMessageLog` in `internal/contracts/events/topics.go`).

Purpose:

- high-volume message-specific audit stream for per-message tracking separate from execution-events

Partition key:

- `card_id`

Primary producers:

1. `card-executor`
2. `sms-gateway`

Primary consumers:

1. `read-model-projector`

## Ordering Rules

## Per-card ordering

The following topics must preserve strict order for a given `card_id`:

1. `card-events`
2. `send-sms`
3. `transport-events`
4. `execution-events`
5. `message-log`

Rule:

- the partition key must be the exact canonical `card_id` string

## Per-campaign ordering

Planner orchestration events are not hot-path per-card events and may be keyed by `campaign_run_id`.

Do not key per-card execution by `campaign_run_id`.

## Idempotency Rules

Every consumer must be able to process duplicate deliveries safely.

Required mechanisms:

1. `event_id`-based dedupe keys in Redis or equivalent
2. state validation against card snapshot state
3. stale event discard rules
4. projector replay tolerance

### Dedupe TTL guidance

1. hot execution dedupe keys: short-lived but longer than expected redelivery windows
2. projector dedupe: optional if projectors are naturally idempotent by primary key overwrite semantics

## Event Types

## Planner Events

### `campaign.run.created`

**Status: Implemented**

Produced when a new campaign run is created and made available for planning.

Envelope:

```json
{
  "event_type": "campaign.run.created",
  "campaign_run_id": "uuid",
  "campaign_definition_id": "uuid",
  "payload": {
    "selection_spec": {},
    "max_retries": 3,
    "throttle_sms_per_sec": 1000,
    "max_concat_override": 3,
    "scheduled_at": null
  }
}
```

### `campaign.run.resume_requested`

**Status: Implemented**

Produced when a paused or partially published run should resume.

### `campaign.run.abort_requested`

**Status: Implemented**

Produced when a run should stop publishing new work and mark remaining work as skipped or aborted according to policy.

### `campaign.run.reconcile_requested`

**Status: Target only -- not yet implemented (requires reconciler service)**

Produced when a reconciler detects a stale run that needs repair.

## Card Events

## `card.activate`

**Status: Implemented**

Purpose:

- initiate execution of the next step for a card

Required payload fields:

1. `card_id`
2. `campaign_run_id`
3. `campaign_definition_id`
4. `shard_id`
5. `step`
6. `retry_count`
7. `activation_reason`

Example payload:

```json
{
  "step": 1,
  "retry_count": 0,
  "activation_reason": "initial"
}
```

Valid reasons:

1. `initial`
2. `retry`
3. `resume`
4. `reconcile`
5. `next_step`

## `card.dlr_received`

**Status: Implemented**

Purpose:

- notify executor that a DLR outcome has been finalized for a logical message

Required payload fields:

1. `msg_id`
2. `smpp_message_id`
3. `dlr_status`
4. `error_code`
5. `is_final`

Notes:

- multipart aggregation must happen before this event is published if the event represents a logical OTA message outcome
- `error_code` is a string field in the canonical schema to preserve SMPP-style textual and hex code forms without lossy conversion

## `card.mo_received`

**Status: Implemented**

Purpose:

- notify executor that an MO/PoR payload has been received for a card

Required payload fields:

1. `source_msisdn`
2. `payload_hex`
3. `correlation_source`

Recommended `correlation_source` values:

1. `msisdn_cache`
2. `transport_mapping`
3. `reconciler`

## `card.retry_requested`

**Status: Implemented**

Purpose:

- explicit retry request when retry policy decides to retry after transport or PoR failure

Required payload fields:

1. `step`
2. `retry_count`
3. `reason`

## `card.completed`

**Status: Implemented**

Purpose:

- terminal completion signal for a card in a campaign run

Required payload fields:

1. `final_step`
2. `completed_at`

## `card.failed`

**Status: Implemented**

Purpose:

- terminal failure signal for a card in a campaign run

Required payload fields:

1. `step`
2. `retry_count`
3. `failure_code`
4. `failure_reason`

## Transport Job Events

## `sms.submit_requested`

**Status: Implemented**

Topic:

- `send-sms`

Purpose:

- request SMS submission for one logical OTA message

Required payload fields:

1. `msg_id`
2. `card_id`
3. `campaign_run_id`
4. `msisdn`
5. `ton`
6. `npi`
7. `data_coding`
8. `protocol_id`
9. `esm_class`
10. `parts`

Each part object must include:

1. `sequence`
2. `total`
3. `ref_num`
4. `payload_hex`

## Transport Events

## `transport.submit_accepted`

**Status: Implemented**

Purpose:

- transport accepted a logical message for submission and obtained an SMPP correlation for one part or more

Required payload fields:

1. `msg_id`
2. `smpp_message_id`
3. `part_sequence`
4. `part_total`

## `transport.submit_failed`

**Status: Implemented**

Purpose:

- transport could not submit the logical message or a part

Required payload fields:

1. `msg_id`
2. `failure_code`
3. `failure_reason`
4. `part_sequence`
5. `part_total`

## `transport.dlr_finalized`

**Status: Implemented**

Purpose:

- final transport outcome for the logical message after multipart aggregation if applicable

Required payload fields:

1. `msg_id`
2. `smpp_message_id`
3. `dlr_status`
4. `error_code`
5. `logical_status`

Notes:

1. `error_code` is a string field in the canonical schema

`logical_status` values:

1. `delivered`
2. `failed`

## `transport.mo_received`

**Status: Implemented**

Purpose:

- raw MO ingress event from the SMSC

Required payload fields:

1. `source_msisdn`
2. `dest_msisdn`
3. `payload_hex`
4. `resolved_card_id`
5. `correlation_confidence`

## Execution Events

Execution events are immutable facts emitted for projection, audit, and replay.

## `execution.card_state_changed`

**Status: Implemented**

Required payload fields:

1. `previous_status`
2. `new_status`
3. `step`
4. `retry_count`
5. `snapshot_version`

## `execution.counter_allocated`

**Status: Implemented**

Required payload fields:

1. `application_id`
2. `counter_value`
3. `counter_hex`

## `execution.command_built`

**Status: Implemented**

Required payload fields:

1. `application_id`
2. `tar_hex`
3. `expect_response`
4. `parts_count`
5. `msg_id`

## `execution.card_completed`

**Status: Implemented**

Required payload fields:

1. `final_step`
2. `completed_at`

## `execution.card_failed`

**Status: Implemented**

Required payload fields:

1. `step`
2. `failure_code`
3. `failure_reason`
4. `retry_count`

## Message Events

> **Note:** Message events are published to the `message-log` topic (not `message-events`).

## `message.mt_created`

**Status: Implemented**

Required payload fields:

1. `msg_id`
2. `direction`
3. `status`
4. `raw_payload_b64`
5. `secured_payload_b64`
6. `counter_hex`

## `message.mt_updated`

**Status: Implemented**

Required payload fields:

1. `msg_id`
2. `status`
3. optional `smpp_message_id`
4. optional `dlr_status`
5. optional `por_status_code`
6. optional `por_data_b64`

## `message.mo_created`

**Status: Implemented**

Required payload fields:

1. `msg_id`
2. `direction`
3. `status`
4. `raw_payload_b64`
5. optional `por_status_code`

## Replay and Recovery Semantics

## Planner replay

1. planner events may be replayed
2. shard checkpoint state in PostgreSQL must ensure replay-safe publication
3. duplicate `card.activate` events must be tolerated by executor dedupe and state validation

## Executor replay

1. `card-events` and `transport-events` may be replayed
2. executor must validate against durable card snapshot state
3. stale DLR or MO events must be ignored if the card is no longer awaiting them

## Projector replay

1. projectors must be able to rebuild query tables from execution and message events
2. projectors should prefer idempotent upserts keyed by event facts or derived entity keys

## Schema Evolution Rules

1. every event type must include `schema_version`
2. additive fields are preferred
3. field removal or meaning changes require a new version
4. consumers must reject unknown mandatory semantics rather than silently mis-handle them

## Validation Rules

Every service producing an event must validate:

1. required envelope fields present
2. key fields match topic partitioning expectations
3. card-scoped events include `card_id`
4. campaign execution events include `campaign_run_id`
5. timestamps are set at creation time, not mutated later

## Traceability Requirements

Every event should carry enough identifiers for end-to-end tracing:

1. `event_id`
2. `trace_id`
3. `card_id`
4. `campaign_run_id`
5. `msg_id` where applicable
6. `shard_id` where applicable

## Summary

This contract set establishes:

1. `card_id` as the canonical ordering key
2. immutable, replay-safe event design
3. clear topic responsibilities
4. separation between execution control, transport jobs, transport ingress, and read-model projection

All future event implementations should conform to this document.
