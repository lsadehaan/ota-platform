# SMSC Gateway External Systems and References

This file records the external systems the verification plan expects to use.

## Test ESME Library

### Python smpplib

Purpose:

- Northbound test ESME for all protocol correctness and transparency assertions
- Provides field-level access to all SMPP PDU attributes for precise validation
- Serves as the black-box protocol oracle in place of custom probe binaries

Reference:

- https://github.com/python-smpplib/python-smpplib
- Install: `pip install smpplib`

## Default Downstream Simulator

### mock-smsc (our own)

Purpose:

- Default southbound SMSC simulator for all test scenarios
- Configurable DLR delay, success rate, and MO message injection
- Runs as a Docker container alongside the gateway in compose stacks

Reference:

- Internal to this repository (see `cmd/mock-smsc/` or equivalent)

## Open-Source SMSCs and Simulators

### SMPPSim

Purpose:

- First external interoperability target
- Deterministic simulator for submit, DLR, and basic SMPP behavior
- Configurable via properties file for system_id, password, port, DLR behavior, and MO injection

Reference:

- https://smppsim.com/
- Docker images available on Docker Hub (search `smppsim`)

### Melrose Labs Open-Source SMPP SMSC Simulator

Purpose:

- Independent SMPP simulator implementation
- Docker-based compatibility target

Reference:

- https://github.com/melroselabs/smpp-smsc-simulator

### Jasmin

Purpose:

- Higher-fidelity interoperability target
- Can be used both as a northbound SMPP client peer and as a southbound SMPP server-side peer depending on configuration

References:

- https://github.com/jookies/jasmin
- https://docs.jasminsms.com/en/latest/apis/smpp-server/
- https://docs.jasminsms.com/en/latest/architecture/index.html
- https://docs.jasminsms.com/en/latest/routing/index.html

### Future: Kannel

Purpose:

- Additional compatibility target after the initial simulator and Jasmin matrix is stable

Notes:

- Kannel likely needs an additional SMPP proxy/server component for the exact
  topology we want.
- Deferred to a future phase; not part of the initial implementation.

Reference:

- https://www.kannel.org/download/kannel-userguide-snapshot/userguide.html

## External Validation Tools

### Melrose Labs SMPP Testing

Purpose:

- External black-box validation of bind and throughput behavior

Reference:

- https://melroselabs.com/services/smpp-testing/

### Melrose Labs SMPP Analyser

Purpose:

- Capture and inspect live SMPP traffic
- Useful for validating gateway relay behavior against pcap traces

Reference:

- https://melroselabs.com/tools/smppanalyser/

## Fault Injection

### Toxiproxy

Purpose:

- Deterministic TCP fault injection for both northbound and southbound links

Reference:

- https://github.com/Shopify/toxiproxy

### Pumba

Purpose:

- Container-level chaos such as kill, pause, and network disruption

Reference:

- https://github.com/alexei-led/pumba

## Implementation Guidance

The harness should prefer:

1. Python smpplib as the northbound ESME for all protocol and transparency assertions.
2. mock-smsc as the default downstream peer for deterministic control over responses.
3. OSS simulators and SMSCs for interoperability validation.
4. Toxiproxy for deterministic network faults before broader chaos tools.

The smpplib-based test suite provides field-level assertion capability for:

- exact PDU field transparency (source/dest addr, TON/NPI, DCS, PID, TLVs)
- response status code validation
- ack boundary correctness
- duplicate handling semantics
- retry and timeout behavior

OSS SMSCs validate that the gateway works with independent third-party
implementations, but are not the primary oracle for protocol correctness.
