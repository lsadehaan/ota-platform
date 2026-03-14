# SMSC Gateway Test Matrix

This file defines the concrete scenarios that the verification harness must run.

## 1. Protocol Cases

| ID | Scenario | Northbound Peer | Southbound Peer | Expected Result |
| --- | --- | --- | --- | --- |
| P01 | Valid bind_transceiver | smpplib ESME | mock-smsc | Bind succeeds |
| P02 | Invalid password | smpplib ESME | mock-smsc | Bind rejected |
| P03 | Unsupported command before bind | smpplib ESME | mock-smsc | Correct SMPP error or disconnect |
| P04 | enquire_link round-trip | smpplib ESME | mock-smsc | Timely enquire_link_resp |
| P05 | unbind round-trip | smpplib ESME | mock-smsc | Graceful close |
| P06 | malformed PDU header | smpplib ESME | mock-smsc | No panic; connection closed safely |
| P07 | duplicate deliver_sm | smpplib ESME | mock-smsc | Both deliver_sm forwarded to engine (gateway does not deduplicate) |
| P08 | Rate limiting | smpplib ESME | mock-smsc | Engine exceeds `RateLimitTPS` and gets `StatusThrottled` |
| P09 | MSISDN blacklist | smpplib ESME | mock-smsc | Submit to blacklisted MSISDN gets `StatusSubmitFail` |
| P10 | Proactive enquire_link | smpplib ESME | mock-smsc | Gateway sends enquire_link to engines every `EnquireLinkSec` |
| P11 | Stale connection eviction | smpplib ESME | mock-smsc | Engine idle beyond `IdleTimeoutSec` triggers gateway connection close |

## 2. PDU Transparency Cases

| ID | Scenario | Payload Shape | Expected Result |
| --- | --- | --- | --- |
| T01 | GSM 7-bit single-part submit | text | Raw `submit_sm` body preserved |
| T02 | UCS-2 single-part submit | text | Raw body preserved |
| T03 | Binary OTA packet | binary | Raw body preserved |
| T04 | Multipart UDH submit | binary | Raw body preserved for every part |
| T05 | `message_payload` TLV submit | TLV | Raw body preserved |
| T06 | Non-default source TON/NPI | addressing | Raw body preserved |
| T07 | Non-default dest TON/NPI | addressing | Raw body preserved |
| T08 | Non-default PID | protocol | Raw body preserved |
| T09 | Non-default DCS | coding | Raw body preserved |
| T10 | Unknown vendor TLV | TLV | Raw body preserved |
| T11 | `registered_delivery` variations | flags | `0x00` vs `0x01` passed through unchanged |

Pass rule:

- The exact northbound `submit_sm` PDU body must equal the southbound
  `submit_sm` PDU body.
- Only `submit_sm_resp.message_id` and return-path DLR payload translation may
  differ, if that is the defined design.

## 3. Routing Cases

| ID | Scenario | Setup | Expected Result |
| --- | --- | --- | --- |
| R01 | Single engine, single MSISDN | 1 engine | DLR routes back to same engine |
| R02 | Two engines, distinct MSISDNs | 2 engines | DLR/MO route by stored affinity |
| R03 | Two engines, same MSISDN, engine B last | 2 engines | Last submit wins affinity |
| R04 | MO with existing affinity | 2 engines | MO delivered to affined engine |
| R05 | Reconnect within grace period | disconnect/reconnect | Buffered DLR/MO drains to same identity |
| R06 | Reconnect after grace expiry | disconnect/reconnect | Fallback follows documented policy |
| R07 | Two sessions with same identity | duplicate `system_id` | Old connection closed, new connection gets same connID, existing affinity mappings remain valid |
| R08 | MO with no engines connected | 0 engines | MO buffered in Pebble retry queue |
| R09 | DLR message ID translation | 1 engine | DLR receipt text contains `id:GW-*` (gateway ID), not downstream SMSC ID |

## 4. Recovery Cases

| ID | Scenario | Fault Point | Expected Result |
| --- | --- | --- | --- |
| C01 | Gateway restart before southbound submit response | MT submit | Submit record persisted in Pebble before ACK; on restart, submit retry loop replays unforwarded submits; after max retries exhausted, synthetic UNDELIV DLR sent to engine |
| C02 | Gateway restart after southbound submit response | pre-DLR | Correlation recovered from disk |
| C03 | Gateway restart with queued retries | retry queue | Retries resume after restart |
| C04 | Engine disconnect before `deliver_sm_resp` | DLR/MO delivery | Message not silently lost |
| C05 | Southbound disconnect before submit response | MT submit | Engine received immediate submit_sm_resp with StatusOK and gateway message ID; southbound submit retried up to MaxSubmitRetries; if all retries fail, engine receives synthetic stat:UNDELIV DLR with same gateway message ID |
| C06 | Duplicate DLR after restart | DLR | Idempotent terminal handling |
| C07 | Southbound submit retry | MT submit | Downstream submit fails, retried N times, synthetic failure DLR after max retries |
| C08 | Correlation cleanup | correlation store | Correlations older than maxAge evicted by CleanupCorrelations |

## 5. Chaos Cases

| ID | Scenario | Tool | Expected Result |
| --- | --- | --- | --- |
| X01 | Add 500ms southbound latency | Toxiproxy | No crash; latency metrics rise |
| X02 | Add northbound packet loss | Toxiproxy | Retries or disconnects match policy |
| X03 | Reset southbound connection mid-submit | Toxiproxy | No deadlock; recovery path engages |
| X04 | Reset northbound connection during DLR | Toxiproxy | DLR buffered or retried |
| X05 | Kill gateway container | Docker or Pumba | Restart recovers persistent state |
| X06 | Kill engine container | Docker or Pumba | Buffered DLR/MO waits or falls back per policy |
| X07 | Kill downstream SMSC | Docker or Pumba | Southbound reconnect works |
| X08 | Graceful drain | Docker or Pumba | Gateway shutdown with in-flight forwards waits up to DrainTimeoutSec before closing |

## 6. Interoperability Matrix

| Peer | Purpose | Priority | Notes |
| --- | --- | --- | --- |
| mock-smsc (our own) | Default downstream simulator | P0 | Required |
| SMPPSim | First OSS compatibility target | P0 | Required |
| Melrose open-source simulator | Independent SMPP implementation | P1 | Strongly recommended |
| Jasmin | Real SMS gateway interop | P1 | Strongly recommended |
| Kannel plus SMPP add-on stack | Additional compatibility target | Future | Add after P0/P1 stability |

## 7. Performance Cases

| ID | Scenario | Target | Key Metrics |
| --- | --- | --- | --- |
| L01 | 100 TPS steady | 10 min | submit latency, queue depth |
| L02 | 500 TPS steady | 10 min | submit latency, CPU, memory |
| L03 | 1k TPS steady | 10 min | backpressure, retry growth |
| L04 | Burst 5k submits | short burst | peak latency, drop behavior |
| L05 | DLR fan-in burst | short burst | DLR routing latency |
| L06 | 1 hour soak | long run | bounded memory, disk, queue growth |

## 8. Harness Implementation Plan

### Test Artifacts

- `tests/smsc-gateway/conftest.py` -- shared fixtures (smpplib client factory, mock-smsc helpers)
- `tests/smsc-gateway/test_protocol.py` -- protocol correctness cases (P01-P11)
- `tests/smsc-gateway/test_transparency.py` -- PDU transparency cases (T01-T11)
- `tests/smsc-gateway/test_routing.py` -- routing cases (R01-R09)
- `tests/smsc-gateway/test_recovery.py` -- recovery cases (C01-C08)
- `tests/smsc-gateway/test_chaos.py` -- chaos cases (X01-X08)
- `tests/smsc-gateway/test_performance.py` -- performance cases (L01-L06)

### Compose Artifacts

- `deployments/smsc-matrix/compose.base.yml`
- `deployments/smsc-matrix/compose.mocksmsc.yml`
- `deployments/smsc-matrix/compose.smppsim.yml`
- `deployments/smsc-matrix/compose.melrose.yml`
- `deployments/smsc-matrix/compose.jasmin.yml`
- `deployments/smsc-matrix/compose.chaos.yml`

Kannel compose (`compose.kannel.yml`) is deferred to a future phase.

### Automation

- `tests/smsc-gateway/run_tests.sh`
- `tests/smsc-gateway/capture-pcap.sh`

## 9. Acceptance Rules

The gateway fails verification if any of these occur:

- northbound and southbound `submit_sm` bodies differ in transparency mode
- DLR or MO routes to the wrong engine while affinity is valid
- accepted submit is silently lost
- DLR/MO is treated as delivered before the documented acknowledgment point
- retry queue grows without bound under recoverable faults
- restart loses durable correlation or retry state
