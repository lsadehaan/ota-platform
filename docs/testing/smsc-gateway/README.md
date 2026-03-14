# SMSC Gateway Verification Plan

This folder defines the verification strategy for the standalone SMSC gateway.
The goal is to prove four properties:

1. SMPP interoperability with existing open-source SMSCs.
2. Correct routing of MT, DLR, and MO traffic through the gateway.
3. Correct behavior under disconnects, restarts, duplicates, and partial failures.
4. Acceptable throughput and latency under steady load and burst load.

## Scope

The plan covers:

- Python `smpplib` as the northbound test ESME (protocol oracle).
- OSS SMSC simulators (mock-smsc, SMPPSim) as downstream peers.
- A Docker Compose-based matrix of open-source SMSCs behind and around the gateway.
- Fault injection for northbound and southbound links.
- A split between PR, nightly, and manual/weekly suites.

The plan does not assume that real MNO-facing semantics are already finalized.
Any ambiguous behavior must be documented first in the expected-behavior matrix
before tests are implemented.

## Test Layers

### 1. Python smpplib Black-Box Correctness

Use Python `smpplib` as the northbound ESME and OSS SMSC simulators as the
downstream peer:

- **smpplib ESME** (northbound)
  - Connects northbound to the gateway as a standard SMPP client.
  - Sends exact `bind_transceiver`, `submit_sm`, `enquire_link`, and `unbind`.
  - Can delay or suppress `deliver_sm_resp`.
  - Field-level assertions on all response PDUs via smpplib object attributes.
- **mock-smsc** (southbound, default downstream)
  - Our own mock SMSC simulator, run as the default downstream peer.
  - Returns controlled `submit_sm_resp`.
  - Injects deterministic DLR and MO payloads.
  - Supports configurable delays and error responses.

Python smpplib field-level assertions serve as the source of truth for protocol
correctness. Real OSS SMSCs are used for interoperability, not as the
byte-level oracle.

### 2. Open-Source SMSC Interoperability

Use a Compose matrix with at least these downstream peers:

- `mock-smsc` (our own simulator)
- `SMPPSim`
- `Melrose Labs open-source SMPP SMSC Simulator`
- `Jasmin`

Recommended ordering:

1. `mock-smsc`
2. `SMPPSim`
3. `Melrose open-source simulator`
4. `Jasmin`

Kannel is deferred to a future phase because it needs additional SMPP
proxy/server components rather than a simple single-container setup.

### 3. Fault Injection

Inject controlled failures with:

- `Toxiproxy` for TCP faults:
  - latency
  - jitter
  - timeout
  - reset
  - bandwidth limiting
  - one-way blackhole
- optional `Pumba` or equivalent container chaos:
  - kill/restart gateway
  - kill/restart engine
  - kill/restart downstream SMSC

## Compose Topology

Create a dedicated matrix under `deployments/smsc-matrix/`:

- `compose.base.yml`
- `compose.mocksmsc.yml`
- `compose.smppsim.yml`
- `compose.melrose.yml`
- `compose.jasmin.yml`
- `compose.chaos.yml`

Each stack should include:

- `smsc-gateway`
- one downstream SMSC profile at a time (mock-smsc, SMPPSim, etc.)
- `toxiproxy-north`
- `toxiproxy-south`
- `prometheus`
- optional `grafana`
- optional `tcpdump` sidecar for packet capture

The Python smpplib test suite runs on the host (or in a lightweight container)
and connects to the gateway's northbound SMPP port.

## Required Test Categories

### Protocol Correctness

- bind accept/reject
- enquire_link handling
- unbind handling
- `submit_sm_resp` status handling
- `deliver_sm_resp` handling
- malformed PDU handling
- duplicate PDU handling
- rate limiting behavior
- MSISDN blacklist behavior
- proactive enquire_link from gateway
- stale connection eviction

### PDU Transparency

For northbound `submit_sm`, the southbound `submit_sm` body must remain
byte-for-byte identical unless an explicit rewrite is configured.

Coverage:

- GSM 7-bit payload
- UCS-2 payload
- binary OTA payload
- UDH multipart payload
- `message_payload` TLV
- non-default source and destination TON/NPI
- non-default PID
- non-default DCS
- registered_delivery variations
- unknown/vendor TLVs

### Routing Correctness

- one engine, one MSISDN
- two engines, one MSISDN, last-submit-wins
- multiple engines, multiple MSISDNs
- DLR returns to original submitting engine
- MO routes by source MSISDN affinity
- reconnect within grace window
- reconnect after grace window
- DLR message ID translation (smscMsgID to gwMsgID)
- MO with no engines connected

### Persistence and Recovery

- gateway restart before southbound response
- gateway restart after southbound response, before DLR
- engine disconnect before `deliver_sm_resp`
- southbound disconnect before `submit_sm_resp`
- retry queue replay after restart
- southbound submit retry and synthetic DLR
- correlation cleanup

### Chaos

- northbound latency spike
- southbound latency spike
- northbound disconnect
- southbound disconnect
- gateway process kill
- engine process kill
- downstream SMSC process kill
- duplicate engine identity reconnect
- graceful drain on shutdown

### Performance

- low steady rate
- medium steady rate
- high steady rate
- burst submit load
- burst DLR fan-in
- soak run

Suggested thresholds: p99 submit latency below 1ms at 500 TPS, memory growth below 50MB over 1 hour soak, queue depth bounded under recoverable faults.

## CI Split

### PR Suite

- smpplib protocol correctness against mock-smsc
- smpplib PDU transparency checks
- `SMPPSim` basic interop
- basic Toxiproxy disconnect tests

### Nightly Suite

- `Melrose` simulator interop
- `Jasmin` interop
- restart and recovery scenarios
- 30 to 60 minute soak

### Weekly or Manual Suite

- extended chaos matrix
- external public SMPP testing services
- pcap review against golden traces
- Kannel stack (future phase)

## Deliverables

Implementation should produce:

1. Compose files under `deployments/smsc-matrix/`.
2. Python smpplib test suite and reusable fixtures.
3. Python-based transparency checks via smpplib field-level assertions.
4. An executable test runner (`tests/smsc-gateway/run_tests.sh`) for local and CI use.
5. Dashboards and alert thresholds for queue growth, retry growth, and latency.

## Exit Criteria

The gateway is considered verified only when:

- All PDU transparency checks pass.
- All routing and reconnect tests pass.
- Recovery tests prove no silent loss in the documented supported cases.
- At least two independent OSS SMSCs pass the interop suite.
- Soak tests show bounded memory, queue depth, and disk growth.

See [TEST_MATRIX.md](TEST_MATRIX.md) for the concrete scenario table and [SOURCES.md](SOURCES.md) for the external systems and references this plan relies on.
