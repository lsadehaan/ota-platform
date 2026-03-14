# 100k TPS Readiness Assessment

## Scope

This assessment reviews the current implementation on branch `feat/phase1-perf-scalability` and answers a specific question:

- Can the current architecture reach `100k TPS` by horizontal scaling alone?

For this document, `TPS` means end-to-end card-step throughput through the real pipeline, not just planner fanout or raw Kafka publish rate.

## Executive Summary

The current architecture is not a credible `100k TPS` design as implemented.

The system can scale further horizontally than the pre-transactional versions, but not by enough to make `100k TPS` realistic without additional architectural work.

The strongest current conclusion is:

1. The implementation should be expected to scale into the low thousands TPS with substantial infrastructure and tuning.
2. The implementation should not be expected to scale to `100k TPS` end-to-end by adding replicas alone.
3. The main blockers are:
   - transactional runner serialization
   - Scylla write amplification
   - card-state projection read-before-write cost
   - message-log projection overhead
   - lack of multi-SMSC routing / distribution

## What Already Scales Well

The following parts of the current architecture are directionally correct for horizontal scaling:

1. Kafka partitioning by `card_id`
   - preserves single-writer-per-card semantics
   - allows parallelism across cards

2. Planner fanout
   - campaign planning and shard fanout are not the main throughput limiter

3. Gateway send path
   - multiple gateway instances
   - multiple Kafka consumers
   - multiple SMPP connections per gateway
   - large SMPP windowing

4. Inbound SMPP spool
   - raw inbound `deliver_sm` is durably captured before `deliver_sm_resp`
   - this is the correct ingress durability model

5. Idempotent projections
   - `transition_seq` is the right mechanism for deduplicating card-state projection

These are the right building blocks. The problem is not that the architecture is fundamentally single-threaded or single-node. The problem is that the hot-path costs per card-step are still too high.

## Main Bottlenecks

### 1. Transactional runner model

The current `TransactionalRunner` is explicitly a single-threaded stream processor:

- [txn_runner.go](/home/ubuntu/ota-platform/internal/kafka/txn_runner.go)

That means throughput scales only by:

1. adding more partitions
2. adding more runners
3. adding more service instances

This can scale, but it is not a cheap scaling model. Transactional runners are correct, but they are not a high-density compute model.

This is the first likely scaling wall for the current implementation.

### 2. Card-state projection read amplification

`CardStateProjector.writeBatch()` currently:

1. deduplicates by card
2. reads stored `transition_seq` from Scylla for each distinct card
3. only then applies updates

Files:

- [card_state_projector.go](/home/ubuntu/ota-platform/internal/scylla/card_state_projector.go)

This is functionally correct, but expensive at scale. At very high TPS, a read-before-write per distinct active card becomes a major bottleneck.

This is one of the clearest architectural blockers to `100k TPS`.

### 3. Message-log projection amplification

The `read-model-projector` still performs high-volume batched create/update work for `message-log`:

- [log_writer.go](/home/ubuntu/ota-platform/internal/projector/log_writer.go)

This path is no longer the first bottleneck, but it remains expensive. At `100k TPS`, it would create a very large volume of Kafka actions and Scylla writes.

If `message-log` remains in the critical path at full fidelity, it will materially limit throughput.

### 4. Scylla write amplification

Even after the move to counters and idempotent projection, the system still writes multiple derived/query-state rows per card transition.

This is acceptable at current scale. It is not acceptable at `100k TPS` unless:

1. the write model is simplified further
2. the cluster is scaled substantially
3. some projections become optional or delayed

### 5. Multi-SMSC routing does not exist yet

The code currently targets one configured SMSC endpoint per gateway deployment:

- [runtime.go](/home/ubuntu/ota-platform/internal/transport/runtime.go)

Even if the external telco side could support `100k TPS`, the implementation does not currently provide:

1. multi-SMSC routing
2. carrier affinity
3. carrier-aware throttling
4. failover between SMSC pools

That is a real scalability gap in the outbound transport layer.

## What I Do Not Consider a Hot-Path Blocker

### PostgreSQL

Postgres is control-plane only in the current runtime model. It is not the main execution bottleneck.

### Redis / Dragonfly

Redis / Dragonfly is a real subsystem and must be sized properly, but compared to Kafka transactions and Scylla projection costs, it is not the first architectural wall.

It remains one of the easier pieces to scale.

### Crypto

GSM 03.48 packet build/parse is a real CPU cost, but it scales linearly with cores and instances. It is not the first reason the current implementation misses `100k TPS`.

## Recommended Scaling Judgment

### Current architecture, current implementation

Expected scaling range with enough infrastructure and tuning:

- low hundreds TPS: already demonstrated
- low thousands TPS: plausible
- `100k TPS`: not realistic

### Why `100k TPS` is not realistic yet

Because the current pipeline still pays too much per card-step for:

1. transactional Kafka sequencing
2. card-state dedupe reads
3. message-log projection
4. Scylla write amplification
5. outbound transport topology limitations

Horizontal scaling alone does not remove those costs.

## Recommended Priority Order

Before considering a removal of Kafka transactions, the next design work should be:

1. reduce event and write amplification
2. reduce card-state projection read-before-write cost
3. reduce or demote message-log projection cost
4. widen Kafka partitions / runners / service replicas and measure the real transactional ceiling
5. add multi-SMSC routing support

Only after that should the team consider replacing transactions with a fully idempotent at-least-once design.

## Position on Kafka Transactions

Kafka transactions are a real scaling limiter, but I do not recommend dropping them immediately.

Reason:

1. they currently provide the cleanest correctness boundary for internal topic-to-topic transitions
2. removing them moves complexity into application-level offset / replay / duplicate handling everywhere
3. the current code still has other large bottlenecks that should be reduced first

So the right conclusion is:

- Kafka transactions are a likely major scaling wall
- but they should not be the first thing removed without first reducing amplification elsewhere

## Recommended Next Milestones

### Milestone 1
Establish a realistic multi-node ceiling for the current design:

1. multi-broker Kafka
2. multi-node Scylla
3. wider partitions
4. more transactional runners
5. more gateway instances

This determines the true ceiling of the current architecture.

### Milestone 2
Reduce hot-path projection cost:

1. simplify `card-state` projector read path
2. reduce `message-log` write volume
3. make expensive read models optional or delayed

### Milestone 3
Add transport routing scale:

1. multi-SMSC support
2. routing policy
3. carrier-aware throttling and failover

### Milestone 4
Only if still necessary, re-evaluate Kafka transaction removal:

1. move internal flows to idempotent at-least-once
2. keep ingress spool and submission ledger
3. retain sequence-based projection safety

## Final Conclusion

The current branch is a substantial improvement over earlier versions and has the correct major building blocks:

1. ingress durability
2. transactional internal processing
3. outbound dedupe
4. idempotent card-state projection

But the current implementation is not a `100k TPS` design yet.

The correct expectation is:

- further horizontal scaling is possible
- horizontal scaling alone is not enough for `100k TPS`
- the next work should focus on reducing write/event amplification and projection cost before making larger correctness tradeoffs
