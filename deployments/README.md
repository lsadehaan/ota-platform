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

| Setting | Default | Load Value | Notes |
|---------|---------|------------|-------|
| `CARD_WORKER_CONCURRENCY` | 16 | 32 | Base worker count per topic |
| `CARD_ACTIVATE_WORKERS` | (base) | (base) | Override for activate topic |
| `CARD_DLR_WORKERS` | (base) | (base) | Override for DLR topic |
| `CARD_MO_WORKERS` | (base) | 80 | MO is ~10x more expensive than DLR |
| `CARD_*_READERS` | 1 | 1 | Multiple kafka-go Readers per topic (same consumer group) |
| `DB_MAX_OPEN_CONNS` | 25 | 32 | Must be >= CARD_WORKER_CONCURRENCY |
| `DB_MAX_IDLE_CONNS` | 10 | 16 | Half of max open |

### read-model-projector

| Setting | Default | Load Value | Notes |
|---------|---------|------------|-------|
| `PROJECTOR_WORKERS` | 16 | 32 | Kafka consumer workers |
| `PROJECTOR_FLUSH_WORKERS` | 8 | 16 | Parallel ScyllaDB batch writers |
| `PROJECTOR_BATCH_SIZE` | 2000 | 500 | Rows per flush; chunked to max 100 per CQL batch |
| `PROJECTOR_UPDATE_BATCH_SIZE` | 2000 | 500 | Update rows per flush |
| `PROJECTOR_FLUSH_INTERVAL_MS` | 100 | 100 | Max time between flushes |

### sms-gateway

| Setting | Default | Load Value | Notes |
|---------|---------|------------|-------|
| `SMS_SENDER_WORKERS` | 8 | 64 | Kafka consumer workers |
| `SMPP_CONNECTIONS` | 5 | 12 | SMPP pool connections |
| `SMPP_WINDOW_SIZE` | 10 | 20 | Max concurrent submits per connection |
| `SMPP_DELIVER_WORKERS` | 8 | 16 | DLR/MO handler goroutines per connection |

## What we learned

Best measured single-host results: **~400 TPS** sustained on 200k-card campaigns (4 ARM CPUs, 24GB RAM).

### Effective optimizations (in order of impact)

1. **Kafka consumer tuning** — Changing `MaxWait` from 50ms to 500ms and `MinBytes` from 1 to 10KB reduced idle CPU by ~96% (241% total to 9.3%). The default 50ms caused 6 consumers to poll the broker 20x/sec each when idle.

2. **Card state durability** — Moved card state from async Kafka projection (`card-state-log` topic) to direct synchronous ScyllaDB writes from the executor. Card state is a correctness requirement and must be on a durable path. Increases disk I/O but eliminates data loss risk.

3. **Per-topic worker counts** — `handleMO` is ~10x more expensive than `handleDLR` (crypto parsing + multiple I/O calls). Giving MO 80 workers while activate/DLR use 32 prevents MO from becoming the bottleneck.

4. **Projector batch chunking** — ScyllaDB rejects CQL batches over `batch_size_fail_threshold_in_kb` (50KB default). Each message log row generates ~7 INSERT statements, so batches of 2000+ rows always fail. `CreateBatch` now chunks to max 100 rows per CQL batch regardless of config. This eliminated all "Batch too large" errors and prevented silent data loss.

5. **SMPP deliver backpressure** — Non-blocking enqueue with 5s bounded backpressure timeout (was: infinite block that deadlocks ReadLoop). OTel metrics for queue depth, backpressure events, and drops.

6. **Partition reduction** — 192 to 64 partitions. 64 is ample for single-broker with 4 CPUs; 192 caused excessive metadata overhead per fetch.

### Things that did NOT help

- **Async log producer** — Flooding the single Kafka broker with unbuffered async writes degraded throughput on all critical topics. Keep log producer synchronous with `RequireOne` acks.
- **Multiple readers per topic** — Adding N kafka-go Readers to the same consumer group didn't help because the bottleneck was worker I/O (Kafka publishes, ScyllaDB writes), not fetch throughput. Infrastructure is in place but defaults to 1.
- **Increasing worker count beyond saturation** — Once workers are I/O-bound (waiting on Kafka ACKs or ScyllaDB), more workers just add goroutine overhead.

### Scaling guidance

- **Single host**: 1 Kafka broker, 1 instance of each service. Scale workers before adding service replicas.
- **CPU bottleneck signs**: >90% utilization, high `iowait` in node-exporter dashboard. Check `docker stats` to identify which container is hottest (usually Kafka broker or executor).
- **MO bottleneck signs**: `card-mo` consumer lag growing while other topics stay flat. Increase `CARD_MO_WORKERS`.
- **Projector lag**: `message-log` consumer lag growing. Check for "Batch too large" errors. Reduce `PROJECTOR_BATCH_SIZE` or increase `PROJECTOR_FLUSH_WORKERS`.
- **Disk I/O**: Direct ScyllaDB card state writes + Kafka log segments are disk-intensive. On VMs with shared storage, iowait can dominate. Monitor via node-exporter dashboard.
- **Clean deploys**: Use `docker compose down -v` to wipe all volumes when changing partition counts (Kafka topics cannot reduce partitions in-place).
- **Test timeout**: Set `E2E_TEST_TIMEOUT` for large runs (default 30m). The Go test binary has a 10m default timeout that kills long tests.

## Stop

```bash
docker compose -f deployments/docker-compose.yml -f deployments/docker-compose.load.yml down --remove-orphans
```
