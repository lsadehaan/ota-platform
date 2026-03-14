# Local Stack

This stack brings up the full local OTA platform in one Compose project:
- control plane: `ota-api`
- planner: `campaign-planner`
- execution: `card-executor`
- projector: `read-model-projector`
- transport: `sms-gateway`
- reconciler: `reconciler`
- data stores: `postgres`, `dragonfly`, `scylla`, `kafka`
- test SMSC: `mock-smsc`
- UI: `web`
- observability: `otel-collector`, `prometheus`, `grafana`, `cadvisor`

## Start

Base stack:

```bash
docker compose -f deployments/docker-compose.yml up -d --build
```

High-throughput local profile:

```bash
docker compose -f deployments/docker-compose.yml -f deployments/docker-compose.load.yml up -d --build
```

## URLs

- UI: `http://localhost:3001`
- API: `http://localhost:8080`
- Prometheus: `http://localhost:9090`
- Grafana: `http://localhost:3002` (`admin / admin`)
- Kafka UI: `http://localhost:8085`
- RedisInsight: `http://localhost:5540`
- pgAdmin: `http://localhost:5050` (`admin@example.com / admin`)

## Tooling containers

Use the built-in toolbox runners against the live stack.

Seed cards:

```bash
docker compose -f deployments/docker-compose.yml -f deployments/docker-compose.load.yml run --rm seed-card-dataset-runner \
  "go run ./cmd/seed-card-dataset -base-url http://ota-api:8080 -profiles 10 -cards-per-profile 10000 -prefix perf100k"
```

Full E2E load test:

```bash
docker compose -f deployments/docker-compose.yml -f deployments/docker-compose.load.yml run --rm e2e-load-runner \
  "go run ./cmd/e2e-load-runner -cards 10000 -planners 2 -executors 24 -gateways 8 -projectors 1 -expect-response=true -build=false"
```

Component black-box tests:

```bash
docker compose -f deployments/docker-compose.yml -f deployments/docker-compose.load.yml run --rm component-load-runner \
  "go run ./cmd/component-load-runner -component executor -cards 5000 -expect-response=true"
```

## Observability notes

- Grafana dashboards:
  - `OTA Local Performance` — service-level throughput, latency, errors
  - `OTA Platform - Backpressure & Overload` — consumer lag, SMPP queue depth, publish errors, projector retries
  - `OTA Platform - Node Metrics` — host CPU, memory, disk I/O, load averages, network (requires node-exporter)
  - `OTA Infrastructure Overview`
  - `OTA Kafka`
  - `OTA PostgreSQL`
  - `OTA Scylla`
  - `OTA Dragonfly`
- CPU and memory panels are driven by cAdvisor plus a generated Prometheus relabel config.
- The relabel config is generated on stack startup by `prometheus-config` from the current container IDs.
- Kafka broker and consumer-group metrics come from `kafka-exporter`.
- PostgreSQL metrics come from `postgres-exporter` and `pg_stat_statements` is enabled in the main Postgres container.
- Scylla is scraped directly on its Prometheus metrics endpoint.
- Use Kafka UI, RedisInsight, and pgAdmin to inspect stored state directly during debugging.
- Dragonfly is scraped directly on `http://dragonfly:6379/metrics` through Prometheus.
- Internal debug endpoints are available under `/api/v1/debug/*` and pprof is available under `/debug/pprof/*`.

## Debug endpoints

- `GET /api/v1/debug/card/:id`
- `GET /api/v1/debug/campaign/:id`
- `GET /api/v1/debug/message/:id`
- `GET /api/v1/debug/queues`
- `GET /api/v1/debug/stuck`

Examples:

```bash
curl -H "Authorization: Bearer $API_KEY" http://localhost:8080/api/v1/debug/queues
curl -H "Authorization: Bearer $API_KEY" http://localhost:8080/api/v1/debug/stuck
```
- If you recreate services independently and container IDs change, refresh Prometheus with:

```bash
docker compose -f deployments/docker-compose.yml -f deployments/docker-compose.load.yml up -d --force-recreate prometheus-config prometheus
```

## Load profile configuration

The load overlay (`docker-compose.load.yml`) tunes settings for high-throughput testing on a single host. Key settings:

### Kafka

| Setting | Default | Load Value | Notes |
|---------|---------|------------|-------|
| `KAFKA_TOPIC_PARTITIONS` | 12 | 64 | Per-topic; 64 provides ample parallelism for 4 CPUs |
| `KAFKA_CONSUMER_MAX_WAIT_MS` | 500 | 500 | Matches Apache Kafka default; reduces idle polling |
| `KAFKA_CONSUMER_MIN_BYTES` | 10240 | 10240 | 10KB; prevents empty fetch responses |
| `KAFKA_CONSUMER_QUEUE_CAPACITY` | 10000 | 10000 | Internal prefetch buffer |
| `KAFKA_PRODUCER_BATCH_SIZE` | 1000 | 10000 | Larger batches for throughput |
| `KAFKA_PRODUCER_BATCH_TIMEOUT_MS` | 50 | 10-100 | Lower for latency-sensitive producers |
| `KAFKA_RETENTION_MS_TRANSIENT` | 86400000 | 21600000 | 6h for transient topics (load tests) |
| `KAFKA_RETENTION_MS_STATE` | 259200000 | 86400000 | 24h for state topics (load tests) |

### card-executor

The executor uses Kafka transactions (TransactionalRunner) for exactly-once consume-transform-produce semantics. Each runner is a single-threaded transactional loop; scale by adding runners per topic or more executor instances.

| Setting | Default | Load Value | Notes |
|---------|---------|------------|-------|
| `EXECUTOR_ACTIVATE_RUNNERS` | 1 | 2 | Transactional runners for card-activate topic |
| `EXECUTOR_DLR_RUNNERS` | 1 | 2 | Transactional runners for card-dlr topic |
| `EXECUTOR_MO_RUNNERS` | 1 | 2 | Transactional runners for card-mo topic |
| `KAFKA_TXN_BATCH_SIZE` | 100 | 100 | Messages per transaction batch |
| `KAFKA_TXN_FLUSH_INTERVAL_MS` | 100 | 100 | Max ms before transaction flush |
| `DB_MAX_OPEN_CONNS` | 25 | 64 | PostgreSQL connection pool |
| `DB_MAX_IDLE_CONNS` | 10 | 32 | Half of max open |

### read-model-projector

| Setting | Default | Load Value | Notes |
|---------|---------|------------|-------|
| `PROJECTOR_WORKERS` | 16 | 64 | Kafka consumer workers |
| `PROJECTOR_FLUSH_WORKERS` | 8 | 32 | Parallel ScyllaDB batch writers |
| `PROJECTOR_BATCH_SIZE` | 2000 | 500 | Rows per flush; chunked to max 100 per CQL batch |
| `PROJECTOR_UPDATE_BATCH_SIZE` | 2000 | 500 | Update rows per flush |
| `PROJECTOR_FLUSH_INTERVAL_MS` | 100 | 100 | Max time between flushes |

### sms-gateway

| Setting | Default | Load Value | Notes |
|---------|---------|------------|-------|
| `SMS_SENDER_WORKERS` | 8 | 128 | Kafka consumer workers |
| `SMPP_CONNECTIONS` | 5 | 12 | SMPP pool connections |
| `SMPP_WINDOW_SIZE` | 10 | 200 | Max concurrent submits per connection |
| `SMPP_DELIVER_WORKERS` | 8 | 32 | DLR/MO handler goroutines per connection |

### Connection pools

| Setting | Default | Load Value | Service | Notes |
|---------|---------|------------|---------|-------|
| `SCYLLA_NUM_CONNS` | 2 | 4 | all | gocql TCP connections per ScyllaDB host |
| `COORDINATION_POOL_SIZE` | 40 | 100 | executor, gateway | Redis/Dragonfly connection pool size |

## What we learned

Best measured single-host results: **~260 TPS** sustained on 200k-card campaigns (4 ARM CPUs, 24GB RAM).

### Effective optimizations (in order of impact)

1. **Kafka consumer tuning** — Changing `MaxWait` from 50ms to 500ms and `MinBytes` from 1 to 10KB reduced idle CPU by ~96% (241% total to 9.3%). The default 50ms caused 6 consumers to poll the broker 20x/sec each when idle.

2. **Card state durability** — Card state changes are published to the `card-state-log` Kafka topic by the executor (via transactional intent producers) and materialized to ScyllaDB by the `CardStateProjector`. The projector uses `transition_seq` for idempotent dedup — events with a sequence number already applied are discarded, preventing counter drift on redelivery. Counters in `campaign_progress_by_bucket` are updated only from non-discarded events.

3. **Per-topic transactional runners** — Each topic gets configurable `EXECUTOR_{ACTIVATE,DLR,MO}_RUNNERS` (default 1). Each runner is a single-threaded Kafka transactional loop consuming from the same consumer group. Adding runners increases parallelism without sacrificing transactional guarantees, since Kafka distributes partitions across consumers in the same group.

4. **Projector batch chunking** — ScyllaDB rejects CQL batches over `batch_size_fail_threshold_in_kb` (50KB default). Each message log row generates ~7 INSERT statements, so batches of 2000+ rows always fail. `CreateBatch` now chunks to max 100 rows per CQL batch regardless of config. This eliminated all "Batch too large" errors and prevented silent data loss.

5. **SMPP deliver backpressure** — Lossless blocking enqueue: when the deliver queue is full, the SMPP read loop stalls until downstream drains, applying natural TCP backpressure to the SMSC. DLR/MO messages are never dropped. OTel metrics for queue depth and backpressure events (`ota.smpp.deliver_backpressure`).

6. **Partition reduction** — 192 to 64 partitions. 64 is ample for single-broker with 4 CPUs; 192 caused excessive metadata overhead per fetch.

7. **Connection pool tuning** — `SCYLLA_NUM_CONNS` and `COORDINATION_POOL_SIZE` control the number of TCP connections to ScyllaDB and Redis respectively. On single-node ScyllaDB with `--smp 1`, increasing gocql connections beyond 4 degrades throughput because the single reactor thread spends more time on connection management. Similarly, the Redis pool should match worker concurrency but oversizing adds overhead. Tune these based on cluster topology.

8. **SMPP window sizing** — `SMPP_WINDOW_SIZE` controls max concurrent in-flight submits per SMPP connection. Default 10 is too conservative for load testing. At 200 per connection × 12 connections = 2400 concurrent submits, ensuring the SMPP path is never the bottleneck.

### Things that did NOT help

- **Async log producer** — Flooding the single Kafka broker with unbuffered async writes degraded throughput on all critical topics. Keep log producer synchronous with `RequireOne` acks.
- **Multiple readers per topic** — Adding N kafka-go Readers to the same consumer group didn't help because the bottleneck was worker I/O (Kafka publishes, ScyllaDB writes), not fetch throughput. Adding more readers actually degraded performance by adding fetch pressure on the single Kafka broker. Infrastructure is in place but defaults to 1.
- **Increasing worker count beyond saturation** — Once workers are I/O-bound (waiting on Kafka ACKs or ScyllaDB), more workers just add goroutine overhead. 6x workers (192/480) actually produced lower TPS than 2x (64/160) due to increased contention.
- **Reducing Kafka consumer MaxWait** — Lowering `MaxWait` from 500ms to 100ms didn't improve throughput because data was already flowing continuously during active processing. The lower setting just increased fetch request rate on the broker.
- **Reducing mock-SMSC delays** — Lowering DLR/MO delays from 50ms+25ms to 10ms+10ms had negligible impact on TPS, confirming the SMSC roundtrip is not the bottleneck at current throughput.
- **Oversizing ScyllaDB connections** — Increasing `SCYLLA_NUM_CONNS` from 2 to 8 degraded performance because ScyllaDB's single reactor (`--smp 1`) spent more time managing connections. 4 is the sweet spot for single-node.

### Single-host bottleneck analysis

At ~260 TPS on 4 ARM CPUs, the system uses ~85% total CPU. The remaining ~15% is unreachable because:

- **ScyllaDB single reactor** (`--smp 1`): All CQL query processing goes through one reactor thread. Even when the container shows 70%+ CPU (reactor + compaction), the reactor itself is the serialization point. Every card activation requires at least 3 ScyllaDB operations (key read, state write, campaign-status write).
- **Kafka single broker**: Handles all produce/consume for all topics. At ~1250 messages/sec (5+ per card), the broker's request handler threads are moderately loaded.
- **Pipeline latency**: Each card traverses 3 Kafka hops (activate → DLR → MO) plus SMPP roundtrip. The pipeline is throughput-limited by the slowest hop, not CPU.

To exceed ~300 TPS: add CPU cores (ScyllaDB `--smp 2+`), or distribute infrastructure across hosts.

### Scaling guidance

- **Single host**: 1 Kafka broker, 1 instance of each service. Scale workers before adding service replicas. Connection pool sizes should match worker concurrency — oversizing hurts on single-node databases.
- **CPU bottleneck signs**: >90% utilization, high `iowait` in node-exporter dashboard. Check `docker stats` to identify which container is hottest (usually Kafka broker or executor).
- **MO bottleneck signs**: `card-mo` consumer lag growing while other topics stay flat. Increase `EXECUTOR_MO_RUNNERS`.
- **Projector lag**: `message-log` consumer lag growing. Check for "Batch too large" errors. Reduce `PROJECTOR_BATCH_SIZE` or increase `PROJECTOR_FLUSH_WORKERS`.
- **Disk I/O**: Direct ScyllaDB card state writes + Kafka log segments are disk-intensive. On VMs with shared storage, iowait can dominate. Monitor via node-exporter dashboard.
- **ScyllaDB reactor saturation**: If ScyllaDB CPU is high but disk I/O and network are low, the single reactor is the bottleneck. Increase `--smp` (requires more CPU cores) or distribute across nodes.
- **Clean deploys**: Use `docker compose down -v` to wipe all volumes when changing partition counts (Kafka topics cannot reduce partitions in-place).
- **Test timeout**: Set `E2E_TEST_TIMEOUT` for large runs (default 30m). The Go test binary has a 10m default timeout that kills long tests.

## Stop

```bash
docker compose -f deployments/docker-compose.yml -f deployments/docker-compose.load.yml down --remove-orphans
```
