# Observability Expansion Plan

## Goals

1. Make the local stack self-observable from one Compose project.
2. Expose enough metrics to identify bottlenecks without guessing.
3. Make stored state easy to inspect across Kafka, Dragonfly, PostgreSQL, and Scylla.
4. Separate business KPIs from component and infrastructure metrics.

## Observability layers

### 1. Business and pipeline KPIs
- active campaigns
- campaigns started/completed/failed
- cards targeted / completed / failed / in progress / pending
- SMS submit TPS
- DLR TPS
- MO / PoR TPS
- card.activate TPS
- card.dlr_received TPS
- card.mo_received TPS
- peak TPS, 30s instant, 1m short average, 5m trend
- per-stage latency p50/p95/p99
- time to first send, time to first completion, total campaign duration
- retry rate, timeout rate, stuck campaign count

### 2. Application metrics
- `ota-api`: HTTP rates, latencies, failures, import throughput, campaign start latency
- `campaign-planner`: shard claims/sec, publish latency, cards published/sec, shard backlog
- `card-executor`: event rates, latency by event type, retry counters, cache hit/miss, state write latency
- `sms-gateway`: submit rate, submit latency, DLR/MO rate, correlation misses, multipart failures, window utilization
- `read-model-projector`: action rate, flush latency, batch size, lag, write failures
- `reconciler`: stale shard reclaim count, completion promotions, loop duration

### 3. Infrastructure and store metrics
- Kafka broker/topic/group metrics
- Dragonfly command mix, latency, memory, key counts
- PostgreSQL connection, lock, WAL, query, vacuum, and statement metrics
- Scylla read/write latency, ops/sec, shard CPU, compaction, cache, tombstones, hot partitions
- container CPU and memory by service group

### 4. Traces and logs
- end-to-end trace continuity across HTTP, Kafka, Scylla, Dragonfly, and SMPP
- structured logs keyed by `campaign_id`, `card_id`, `msg_id`, `smpp_message_id`, `trace_id`

### 5. Data inspection and debug surfaces
- Kafka topic/group/message browser
- Dragonfly key browser
- PostgreSQL browser
- Scylla table/query inspection
- platform debug endpoints for card, campaign, message, lag, and stuck state

## Immediate implementation slice

### Add to main compose
- Grafana
- Prometheus
- OTel Collector
- cAdvisor
- Kafka UI
- RedisInsight
- pgAdmin
- Kafka exporter
- Postgres exporter

### Enable in PostgreSQL
- `pg_stat_statements`
- `shared_preload_libraries=pg_stat_statements`

### Prometheus scraping
- OTel Collector metrics
- cAdvisor container metrics
- Kafka exporter
- Postgres exporter
- Scylla Prometheus endpoint

### First dashboards
- pipeline TPS and latency
- CPU and memory by service
- Kafka lag / broker throughput
- PostgreSQL connections, locks, slow SQL, top statements
- Scylla read/write latency and ops/sec

### First inspection UIs
- Kafka UI
- RedisInsight
- pgAdmin

## Next implementation slice
- Dragonfly metrics integration if the server export path is stable on this host
- Scylla-focused dashboard panels by table
- platform debug endpoints and pages:
  - `/debug/card/{card_id}`
  - `/debug/campaign/{campaign_id}`
  - `/debug/message/{msg_id}`
  - `/debug/queues`
  - `/debug/stuck`

## What we already learned from local testing
- best single-host local full E2E result so far: about `131.62 TPS`
- best local performance used:
  - `2` planners
  - `24` executors
  - `8` gateways
  - `1` projector
  - `192` partitions on the hot topics
  - producer batch size `10000`
  - producer batch timeout `100ms`
  - consumer max wait `100ms`
  - projector workers `32`
  - projector flush workers `16`
- adding projector replicas above `1` on one host hurt throughput
- moving from `1` Kafka broker to `3` brokers on the same host hurt throughput
- executor hot-path caching, larger Kafka batches, and projector parallelism gave the biggest local gains

## Acceptance criteria
- one `docker compose up` starts the full app and observability stack
- Grafana dashboards populate without ad-hoc `/tmp` config hacks
- Kafka UI, RedisInsight, and pgAdmin are reachable from the host
- Postgres and Kafka exporter metrics appear in Prometheus
- local test instructions are documented and reproducible
