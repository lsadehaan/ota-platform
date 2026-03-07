# Schema Pack

This directory contains wire-level schema artifacts for the OTA platform.

> **Note:** Proto files are schema documentation only. The `.proto` files are not
> compiled to Go code. The codebase uses JSON serialization for Kafka messages.
> The runtime event type definitions live in `internal/contracts/events/` as
> Go structs with JSON struct tags.

## Contents

- `openapi/ota-api-control-v1.yaml` — Control plane REST API
- `openapi/ota-api-readmodels-v1.yaml` — Read model query API
- `proto/ota/common/v1/common.proto` — Common protobuf type definitions
- `proto/ota/events/v1/events.proto` — Kafka event message definitions

## Contract Rules

1. Kafka wire format is currently JSON. Protobuf is the intended future wire format.
2. JSON examples in markdown documents are explanatory only.
3. v1 keeps IDs as strings for debuggability and easier cross-system inspection.
