# Safety Review — OTA Platform

Source reviewed: `/home/ubuntu/.claude/projects/-home-ubuntu-ota-platform/memory/safety-review.md`

Last reviewed: 2026-03-08 (current repo state after post-`a7762c6` safety fixes)

## Summary

Several of the original findings are now fixed. The main changes from the original review are:
- `M5` should be downgraded from a transport-read-loop risk to a bounded gateway-handler latency concern, because inbound `deliver_sm` handling is now off the SMPP socket read loop.
- `H5` should stay as a clarification item, not a firm bug, unless the intended 23.048 profile for this platform requires counter-derived IV use for the configured cipher modes.
- There is one newer security/performance item worth adding: the executor caches materially retain key/process state without TTL or bounds.

## Critical

### C1. Async log producer can silently lose writes
- **File**: `internal/executor/runtime.go`
- `logProducer` is configured with `Async: true` and there is no completion callback, retry confirmation, or dead-letter path.
- If Kafka accepts the write into the client buffer but the async flush fails later, the executor does not observe the failure.
- **Status**: Fixed.
- **Recommended fix**: Either switch `message-log` back to sync-with-batching, or wrap async publishing with explicit error reporting/drain semantics.

### C2. `RequiredAcks` zero-value ambiguity
- **File**: `internal/kafka/producer.go`
- `ProducerOptions.RequiredAcks` is a value type. `RequireNone` is `0`, but the constructor treats `0` as "unset" and overrides it to `RequireAll`.
- **Status**: Fixed.
- **Recommended fix**: use `*kafka.RequiredAcks`, or an explicit `HasRequiredAcks bool` sentinel.

## High

### H1. Start-offset policy can still replay retained history when configured to `first`
- **File**: `internal/kafka/consumer.go`
- Consumer start offset is now configurable and defaults to `last`.
- If a consumer group is new or its offsets expire, retained topic history is replayed.
- On `send-sms` or `card-events`, that can replay work unless upstream/event handlers are perfectly idempotent.
- **Status**: Mostly fixed for current defaults.
- **Recommended fix**: keep the default at `last` and make any `first` usage an explicit, topic-specific choice.

### H2. Unbounded in-process caches retain key/process state indefinitely
- **File**: `internal/executor/worker.go`
- `cardKeyCache`, `profileCache`, and `campaignCache` were unbounded `sync.Map`; they now use bounded TTL caches.
- This is now more important than before because throughput work made these caches central to the hot path.
- **Status**: Fixed.
- **Recommended fix**: monitor the chosen TTL/max-entry caps under load and adjust if they are too loose or too aggressive.

### H3. Dead retry fields in Kafka consumers
- **File**: `internal/kafka/consumer.go`
- `retries` and `retriesMu` fields exist on both consumer structs but are not used by the current retry implementation.
- **Status**: Fixed.
- **Recommended fix**: remove them.

### H4. `partitionTracker.pending` is unused
- **File**: `internal/kafka/consumer.go`
- `pending` is allocated and deleted from, but never populated.
- **Status**: Fixed.
- **Recommended fix**: remove it or wire it into real in-flight tracking.

### H5. Zero-IV encryption behavior needs explicit design confirmation
- **File**: `pkg/gsm0348/builder.go`
- `encryptData` uses `zeroIV16[:blockSize]` and ignores the `counter` argument.
- Whether this is a bug depends on the exact 23.048 profile/cipher mode expected by this platform.
- **Status**: Keep as clarification item, not a confirmed defect.
- **Recommended fix**: document the intended OTA security profile and either remove the unused `counter` parameter or use it explicitly.

### H6. AES-CBC is declared but not implemented in `encryptData`
- **File**: `pkg/gsm0348/builder.go`
- `cipherBlockSize()` and enum naming recognize `CipherAES_CBC`, but `encryptData()` only handles DES/3DES and falls through to unsupported-mode error.
- **Status**: Fixed.
- **Recommended fix**: implement AES-CBC or remove/disable the advertised mode.

## Medium

### M1. Delayed activate retry goroutine is not lifecycle-managed
- **File**: `internal/executor/worker.go`
- `scheduleActivateRetry` launches an unmanaged goroutine and later publishes with `context.Background()`-derived timeout.
- On shutdown, retries can be dropped or race with producer close.
- **Status**: Still valid.
- **Recommended fix**: track these goroutines with a worker-local `WaitGroup` or move delayed retries into Kafka/durable scheduling.

### M2. Redis encryption key remains resident after AEAD construction
- **File**: `internal/redis/client.go`
- Raw `encryptionKey` was retained after `cipher.AEAD` construction; the client now zeros the input key material and does not keep it on the struct.
- **Status**: Fixed.
- **Recommended fix**: do not retain the raw key unless there is a concrete need; zero the input buffer after AEAD construction if practical.

### M3. Dedupe is fail-open on Redis failure
- **File**: `internal/executor/worker.go`
- If `CheckAndSetDedupe` fails, the executor logs a warning and processes the event anyway.
- **Status**: Still valid.
- **Recommended fix**: at minimum emit a metric; optionally make fail-open vs fail-closed configurable by environment.

### M4. Correlation cache sizing and cleanup need monitoring
- **File**: `internal/transport/gateway.go`
- The local correlation cache is now a performance optimization and is cleaned once per minute.
- It is not a correctness problem by itself, but the memory footprint under sustained high TPS should be measured.
- **Status**: Still valid, but the severity depends on observed traffic.
- **Recommended fix**: expose cache size metrics and tighten cleanup if needed.

### M5. DLR correlation backoff still blocks the gateway handler, but no longer blocks the SMPP read loop
- **File**: `internal/transport/gateway.go`
- The current 1-second retry/backoff on correlation miss is now off the SMPP socket read loop because `deliver_sm` handling was moved async.
- It still occupies a gateway handler worker while retrying.
- **Status**: Partially valid; impact is lower than before.
- **Recommended fix**: keep the bound, but add a metric and consider a deferred retry queue if it becomes hot.

### M6. Consumer fetch error handling is inconsistent
- **File**: `internal/kafka/consumer.go`
- Basic consumer now retries fetch errors instead of exiting immediately, bringing it closer to the concurrent consumer behavior.
- **Status**: Fixed.
- **Recommended fix**: make this an explicit policy choice and align both implementations.

### M7. Failed-card reset path can transiently drive counters negative before clamp
- **File**: `internal/scylla/query_store.go`
- `ResetFailedCards` issues `failed = failed - 1, pending = pending + 1`; later normalization hides negatives.
- **Status**: Still valid.
- **Recommended fix**: make reset idempotent against latest state, not raw counter arithmetic.

## Additional findings worth adding

### A1. Executor cache contents now materially affect key-material exposure window
- **File**: `internal/executor/worker.go`
- Because the executor now relies more heavily on process-local caches for throughput, the operational security model should explicitly acknowledge that decrypted card key material can remain in-process longer than before.
- **Recommended fix**: TTL/size bounds plus process-memory assumptions in the threat model.

## Verified improvements since the original review

### V1. SMPP deliver handling moved off the socket read loop
- **File**: `internal/smpp/client.go`
- `deliver_sm` processing is no longer executed inline on the connection read goroutine.
- This materially reduces return-path head-of-line blocking.
- **Impact on original review**: any prior concern that DLR/MO handling blocks the socket reader should be considered fixed.

### V2. Card-state updates no longer use LWT/CAS on the Scylla hot path
- **File**: `internal/scylla/execution_store.go`
- The store now uses timestamped `INSERT ... USING TIMESTAMP` for last-write-wins semantics instead of the earlier compare-and-set approach.
- **Impact on original review**: old warnings about Scylla LWT hot-path cost are no longer current.
