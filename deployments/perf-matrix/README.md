# SMSC Performance Matrix

This folder defines a proposed benchmark rig for comparing SMPP gateways and
open-source SMSCs under the same load profile.

The goal is to keep the benchmark neutral:

- one fixed load generator layer
- one fixed downstream peer layer
- one system under test (SUT) swapped in at a time
- one shared observability layer

## Topology

The intended topology is:

`loadgen-esme cluster -> central-sut -> downstream-smsc`

Optional sidecars:

- `toxiproxy-north`
- `toxiproxy-south`
- `prometheus`
- `grafana`
- `node-exporter`
- `cadvisor`

## Current Status

This folder is a scaffold.

Usable today:

- `compose.base.yml`
- `compose.observability.yml`
- `compose.sut-gateway.yml`
- `compose.downstream-mocksmsc.yml`
- `compose.downstream-probe.yml`
- `compose.downstream-smppsim.yml`
- `compose.loadgen.yml`
- `compose.chaos.yml`

Placeholders that still need real image/config work:

- `compose.sut-jasmin.yml`
- `compose.sut-kannel.yml`

## Compose Model

Bring the matrix up by combining:

1. one base file
2. one SUT overlay
3. one downstream overlay
4. optional loadgen / observability / chaos overlays

Example combinations:

```bash
# Gateway under test with mock-smsc, loadgen, and observability
docker compose \
  -f deployments/perf-matrix/compose.base.yml \
  -f deployments/perf-matrix/compose.sut-gateway.yml \
  -f deployments/perf-matrix/compose.downstream-mocksmsc.yml \
  -f deployments/perf-matrix/compose.loadgen.yml \
  -f deployments/perf-matrix/compose.observability.yml \
  up -d

# Run one benchmark and write artifacts to deployments/perf-matrix/artifacts
docker compose \
  -f deployments/perf-matrix/compose.base.yml \
  -f deployments/perf-matrix/compose.sut-gateway.yml \
  -f deployments/perf-matrix/compose.downstream-mocksmsc.yml \
  -f deployments/perf-matrix/compose.loadgen.yml \
  run --rm loadgen-esme
```

```bash
# Gateway under test with probe downstream and observability
docker compose \
  -f deployments/perf-matrix/compose.base.yml \
  -f deployments/perf-matrix/compose.sut-gateway.yml \
  -f deployments/perf-matrix/compose.downstream-probe.yml \
  -f deployments/perf-matrix/compose.observability.yml \
  up -d

# Gateway under test with SMPPSim and southbound chaos
docker compose \
  -f deployments/perf-matrix/compose.base.yml \
  -f deployments/perf-matrix/compose.sut-gateway.yml \
  -f deployments/perf-matrix/compose.downstream-smppsim.yml \
  -f deployments/perf-matrix/compose.chaos.yml \
  up -d
```

## Recommended Benchmark Tracks

### Track A: Correctness + Failure

Use:

- `compose.sut-gateway.yml`
- `compose.downstream-probe.yml`
- optional `compose.chaos.yml`

Why:

- exact downstream control
- raw capture visibility
- deterministic DLR / MO injection

### Track B: Throughput + Interop

Use:

- `compose.sut-gateway.yml`
- `compose.downstream-mocksmsc.yml`
- `compose.loadgen.yml`
- `compose.observability.yml`

Why:

- fully in-repo runnable path
- automatic DLR generation
- benchmark artifacts written locally

### Track C: Throughput + External Peer

Use:

- `compose.sut-gateway.yml`
- `compose.downstream-smppsim.yml`
- `compose.loadgen.yml`
- `compose.observability.yml`

Why:

- independent SMPP implementation
- closer to interop testing than the in-repo probe

### Track D: Comparative SUT Swap (Roadmap)

Use the same loadgen and downstream overlay while swapping:

- `compose.sut-gateway.yml` (runnable now)
- `compose.sut-jasmin.yml` (placeholder — not runnable)
- `compose.sut-kannel.yml` (placeholder — not runnable)

The benchmark is only meaningful if everything else stays fixed.
The Jasmin and Kannel overlays are stubs documenting the intended network
contract. Making them runnable requires image selection, config provisioning,
SMPP connector/user setup, healthchecks, and credential wiring.

## What Still Needs To Be Built

### Load Generator

`compose.loadgen.yml` now uses the in-repo `cmd/smpp-loadgen/` binary. It
provides:

- configurable bind count
- configurable SMPP window
- fixed message corpus replay
- CSV and JSON result export
- DLR round-trip latency tracking

Artifacts are written under:

- `deployments/perf-matrix/artifacts/`

### Third-Party SUT Profiles

`compose.sut-jasmin.yml` and `compose.sut-kannel.yml` are placeholders.
They document the target role and network contract, but still need:

- image choice
- config files
- healthchecks
- credential wiring

## Benchmark Rules

For fair comparison:

- pin the same CPU and memory limits for every SUT
- keep the same bind counts and window sizes
- keep the same downstream peer and DLR timing
- run warm-up, measured run, and cool-down for each candidate
- repeat each run at least 3 times
- record median and worst run

## Metrics To Collect

- sustained TPS
- p50 / p95 / p99 submit latency
- p50 / p95 / p99 DLR round-trip latency
- DLR loss rate
- duplicate DLR rate
- CPU
- RSS
- file descriptors
- disk write rate
- retry queue depth
- buffered message count

## Notes

- The existing `deployments/smsc-matrix/` files were used as the starting
  point for the gateway, probe, SMPPSim, and chaos overlays.
- The existing observability stack in
  `/home/ubuntu/ota-platform/deployments/docker-compose.engine.yml`
  was reused as the model for `compose.observability.yml`.
