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
  - `OTA Local Performance`
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

## Local performance profile used in testing

These settings produced the best single-host local result so far.

- planners: `2`
- executors: `24`
- gateways: `8`
- projectors: `1`
- Kafka partitions: `192`
- `CARD_WORKER_CONCURRENCY=16`
- `SMS_SENDER_WORKERS=64`
- `SMPP_CONNECTIONS=12`
- `SMPP_WINDOW_SIZE=20`
- `PROJECTOR_WORKERS=32`
- `PROJECTOR_FLUSH_WORKERS=16`
- `PROJECTOR_BATCH_SIZE=5000`
- `PROJECTOR_UPDATE_BATCH_SIZE=5000`
- `KAFKA_PRODUCER_BATCH_SIZE=10000`
- `KAFKA_PRODUCER_BATCH_TIMEOUT_MS=100`
- `KAFKA_CONSUMER_MAX_WAIT_MS=100`
- `KAFKA_CONSUMER_QUEUE_CAPACITY=10000`
- planner shard size: `5000`
- planner claim batch size: `5000`

## What we learned

- Single-host local best measured full E2E result: about `131.62 TPS` on a `10k` campaign.
- Increasing projector replicas above `1` on the same host hurt throughput.
- Increasing Kafka brokers from `1` to `3` on the same host hurt throughput.
- The most effective local improvements were:
  - larger Kafka batching
  - larger planner shard batches
  - reduced executor message-log churn
  - parallel projector flush workers
  - executor hot-path caching
  - removing head-of-line blocking in the SMPP receive path
- For local benchmarking, use one Kafka broker and scale executors/gateways before scaling projectors.

## Stop

```bash
docker compose -f deployments/docker-compose.yml -f deployments/docker-compose.load.yml down --remove-orphans
```
