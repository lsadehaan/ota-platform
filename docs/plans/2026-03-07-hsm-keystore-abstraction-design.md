# HSM Keystore Abstraction Design

## Goal

Add an abstraction layer for OTA key retrieval and cryptographic operations, enabling both software-based key storage (open core) and HSM-backed key storage with optional crypto offload (enterprise).

## Architecture

The design uses two separate interfaces: `KeyStore` for key retrieval and `CryptoProvider` for cryptographic operations. A Redis caching decorator wraps any `KeyStore` implementation transparently. Enterprise HSM backends (PKCS#11, Cloud KMS) implement one or both interfaces depending on deployment requirements.

## Interfaces

### `KeyStore`

Retrieves key material for a card.

```go
// internal/keystore/keystore.go

type CardKeyMaterial struct {
    EncKey    []byte // KIC — ciphering key
    AuthKey   []byte // KID — signing/MAC key
    KEK       []byte // Key encryption key (for key provisioning to card)
    ProfileID string
    MSISDN    string
}

type KeyStore interface {
    GetKeys(ctx context.Context, cardID string) (*CardKeyMaterial, error)
}
```

### `CryptoProvider`

Performs MAC computation and encryption without exposing raw key material.

```go
// internal/keystore/crypto.go

type CryptoProvider interface {
    ComputeMAC(ctx context.Context, cardID string, algo, mode string, data []byte) ([]byte, error)
    Encrypt(ctx context.Context, cardID string, algo, mode string, data []byte) ([]byte, error)
}
```

When a `CryptoProvider` is available, the GSM 03.48 builder delegates MAC and encryption operations to it instead of performing software crypto with raw key bytes.

## Implementations

| Implementation | Repo | KeyStore | CryptoProvider | Caching |
|---|---|---|---|---|
| `SoftwareKeyStore` | open | Yes — reads from PostgreSQL | No | Wrapped by `CachedKeyStore` |
| `CachedKeyStore` | open | Yes — Redis decorator | No | Is the cache layer |
| `PKCS11KeyStore` | enterprise | Yes — extracts from HSM | Yes — HSM performs crypto | No cache when crypto offload enabled |
| `CloudKMSKeyStore` | enterprise | Yes — extracts from KMS | Yes — KMS performs crypto | No cache when crypto offload enabled |

### SoftwareKeyStore (open repo)

Replaces the current direct DB access in the executor worker. Reads `cards` table via GORM, returns `CardKeyMaterial`. This is the default backend.

### CachedKeyStore decorator (open repo)

Wraps any `KeyStore`. Checks Redis first (using existing `CardKeys` cache logic with optional AES-GCM encryption), falls back to the inner `KeyStore` on miss, caches the result. Skipped entirely when the backend provides a `CryptoProvider` (keys never leave the HSM).

### PKCS#11 and Cloud KMS (enterprise repo)

Enterprise implementations registered via a plugin/factory pattern. The open repo provides a `Register(name, factory)` function. Enterprise `init()` functions register HSM backends at startup.

## GSM 03.48 Builder Changes

The `CommandPacketInput` struct gains an optional `CryptoProvider` field:

```go
type CommandPacketInput struct {
    TAR            [3]byte
    Counter        [5]byte
    CipheringKey   []byte          // Used when CryptoProvider is nil
    SigningKey      []byte          // Used when CryptoProvider is nil
    CryptoProvider keystore.CryptoProvider // Optional — if set, keys above are ignored
    CardID         string          // Required when CryptoProvider is set
    UserData       []byte
}
```

- If `CryptoProvider` is nil: existing software crypto path (no change)
- If `CryptoProvider` is set: builder calls `ComputeMAC()` and `Encrypt()` instead

## Executor Worker Changes

The worker receives a `KeyStore` (and optionally a `CryptoProvider`) via dependency injection instead of accessing DB and Redis directly for keys.

Current flow:
```
Redis cache → DB fallback → raw bytes to builder
```

New flow:
```
KeyStore.GetKeys(cardID) → key material to builder
  (CachedKeyStore wraps SoftwareKeyStore by default)

If CryptoProvider available:
  builder delegates MAC/encrypt to CryptoProvider
```

## Configuration

```yaml
keystore:
  backend: "software"    # software | pkcs11 | cloudkms
  cache:
    enabled: true        # wrap with CachedKeyStore decorator
    ttl: "5m"
  pkcs11:                # enterprise only
    module: "/usr/lib/softhsm/libsofthsm2.so"
    slot: 0
    pin_env: "HSM_PIN"
    crypto_offload: true
  cloudkms:              # enterprise only
    provider: "aws"      # aws | gcp | azure
    key_ring: "ota-keys"
    crypto_offload: false
```

## Open vs Enterprise Boundary

### Open repo (`ota-platform`)

- `internal/keystore/keystore.go` — `KeyStore` interface, `CardKeyMaterial` struct
- `internal/keystore/crypto.go` — `CryptoProvider` interface
- `internal/keystore/software.go` — `SoftwareKeyStore` (Postgres-backed)
- `internal/keystore/cached.go` — `CachedKeyStore` Redis decorator
- `internal/keystore/registry.go` — backend registration and factory
- `pkg/gsm0348/` — builder changes for optional `CryptoProvider`
- `internal/executor/worker.go` — refactored to use `KeyStore` injection

### Enterprise repo (`ota-platform-enterprise`)

- PKCS#11 `KeyStore` + `CryptoProvider` implementation
- Cloud KMS `KeyStore` + `CryptoProvider` implementation
- HSM configuration wiring
- Integration tests with SoftHSM

## KEK Note

The KEK field is not used for protecting stored KIC/KID keys. It is used for OTA key provisioning — wrapping keys before sending them to the card during keyset updates or application personalization. It is retrieved alongside EncKey and AuthKey as a regular key field.

## Security Considerations

1. Software backend: keys are in memory during processing (same as today)
2. HSM with crypto offload: keys never leave the HSM boundary
3. Redis cache encryption (AES-GCM) remains available for the software path
4. HSM PIN/credentials loaded from environment variables, never from config files
5. `CryptoProvider` mode eliminates key material exposure in logs, dumps, or memory
