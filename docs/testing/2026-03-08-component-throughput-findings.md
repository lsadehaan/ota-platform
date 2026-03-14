# 2026-03-08 Component Throughput Findings

## Scope

These measurements isolate each major runtime component against the real local deployed stack:
- real PostgreSQL
- real Kafka
- real Dragonfly
- real Scylla
- real deployed service containers
- only the SMSC remains mocked

Each test scaled non-target consumer services to `0` so the measured service owned the hot path being exercised.

## Method

Runner:
- `cmd/component-load-runner`

Planner test:
- ingress: `POST /api/v1/campaigns/{id}/start`
- egress: `card-events` `card.activate`
- workload: `5000` cards
- stack: `2` planners, `0` executors, `0` gateways, `0` projectors

Executor test:
- ingress: `card-events` `card.activate`
- egress: `send-sms`
- workload: `5000` activations
- stack: `10` executors, `0` planners, `0` gateways, `0` projectors

Gateway test:
- ingress: `send-sms`
- egress: `card-events` `card.dlr_received`
- workload: `10000` SMS requests
- stack: `4` gateways, `0` planners, `0` executors, `0` projectors

Projector test:
- ingress: `message-log`
- egress: projector processed-actions metric delta
- workload: `10000` message rows represented as `20000` actions (`create` + `update`)
- stack: `1` projector, `0` planners, `0` executors, `0` gateways

## Results

| Component | Input | Output | Elapsed | Throughput |
| --- | ---: | ---: | --- | ---: |
| Planner | 5000 cards | 5000 `card.activate` events | `1.228s` | `4071.37 card events/sec` |
| Executor | 5000 `card.activate` | 5000 `send-sms` messages | `33.142s` | `150.87 card steps/sec` |
| Gateway | 10000 `send-sms` | 10000 `card.dlr_received` events | `33.546s` | `298.10 SMS submits/sec` |
| Projector | 20000 actions | 20000 processed actions | `22.550s` | `886.90 actions/sec` |

## Interpretation

### 1. Planner is not the current bottleneck

The planner can publish over `4k` card activation events per second on this local stack. That is far above the full E2E result and far above the executor rate.

### 2. Executor is the first clear component bottleneck

The isolated executor ceiling on this stack is about `151` card steps/sec.

That makes it the first hard cap in the current end-to-end chain.

### 3. Gateway is faster than executor

The gateway handled about `298` send/DLR cycles per second in isolation, roughly `2x` the executor card-step rate.

That means the transport edge is not the first cap right now.

### 4. Projector is no longer the first limiter after the batching changes

The projector processed about `887` actions/sec in isolation.

That is still significant load, but it is no longer below the executor ceiling. The earlier projector serialization problem has been materially reduced.

## Important caveats

1. The gateway test measures `send-sms -> DLR` throughput. It does not model full executor return-path completion.
2. The projector result is in `actions/sec`, not cards/sec. One card can generate multiple actions.
3. These are local single-node infrastructure numbers, not cluster ceilings.
4. The component runner excludes fixture setup time from the measurement window.

## Current bottleneck order

Based on the isolated tests and the previous full E2E run:

1. Executor
2. Kafka / event-path overhead around executor
3. Gateway
4. Projector
5. Planner

## Immediate implication

If the target is to move the current local stack from roughly `79 TPS` E2E toward `100+ TPS`, the next optimization effort should focus on:

1. executor hot-path round trips
2. executor output/event volume
3. Kafka broker pressure under executor load

Planner work is not the first place to spend effort.
