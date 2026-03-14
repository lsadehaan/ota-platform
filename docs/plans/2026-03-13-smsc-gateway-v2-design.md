# SMSC Gateway v2 — Multi-Protocol SMS Gateway

**Date:** 2026-03-13
**Status:** Approved

## Context

The SMSC Gateway (v1) is a standalone SMPP-to-SMPP proxy that routes between
OTA Engine instances and downstream SMSCs. It provides MSISDN-based sticky
routing, Pebble-backed persistence, retry with synthetic failure DLRs, bounded
worker pools, and Prometheus metrics.

v2 evolves it into a multi-protocol SMS gateway with:

- **TLS/SSL** on all interfaces (SMPP, HTTP)
- **Advanced routing** — Prefix, Failover, RoundRobin, LeastCost
- **REST API** — HTTP submit with per-message DLR/MO callback URLs
- **MO routing** — inbound messages routed by destination number/shortcode to
  SMPP connections or HTTP callback URLs
- **Admin UI** — embedded web dashboard for configuration and monitoring
- **Multi-user auth** — API keys for REST consumers, username/password for admin

## Architecture

```
  Northbound:                                              Southbound:
                          ┌──────────────────────────────┐
  SMPP engines ◄─────────►│                              │◄────────► SMSC Pool A (MNO 1)
                          │  ┌──────────┐  ┌──────────┐  │
  REST API     ──────────►│  │ MT Route │  │ Outbound │  │◄────────► SMSC Pool B (MNO 2)
               ◄──────────│  │ Table    │  │ Pools    │  │
  (DLR/MO callbacks)      │  └──────────┘  └──────────┘  │◄────────► SMSC Pool C (fallback)
                          │  ┌──────────┐                │
  Admin UI     ──────────►│  │ MO Route │                │
                          │  │ Table    │                │
                          │  └──────────┘                │
                          └──────────────────────────────┘
```

Traffic flows bidirectionally:

- **MT (Mobile Terminated):** Northbound → MT Route Table → Southbound SMSC pool
- **MO (Mobile Originated):** Southbound SMSC → MO Route Table → Northbound
  SMPP connection or REST callback URL
- **DLR:** Southbound SMSC → correlation lookup → Northbound SMPP connection or
  REST callback URL

## 1. TLS

Go standard `crypto/tls`. Purely opt-in — if no cert/key configured, listeners
run without TLS (current behavior).

### Config

```
# Northbound SMPP server
GW_TLS_CERT=/path/to/cert.pem
GW_TLS_KEY=/path/to/key.pem

# Southbound SMPP client (per-pool via route config)
# Each pool config includes tls_enabled and tls_insecure_skip_verify

# REST API + Admin UI (defaults to GW_TLS_CERT/KEY if not set)
GW_HTTP_TLS_CERT=
GW_HTTP_TLS_KEY=
```

### Implementation

- `tls.go` — loads cert/key, builds `tls.Config`
- Northbound: `tls.NewListener(listener, tlsConfig)` wrapping the existing
  `net.Listener`
- Southbound: `tls.Dial` in `smpp.Client.Connect()` when pool config has
  `tls_enabled=true`
- HTTP: `http.Server.ServeTLS()` for REST API and admin UI

## 2. Routing

### Pool Manager

`PoolManager` manages multiple named `smpp.Pool` instances. Each pool
corresponds to one SMSC destination (host:port + credentials).

```go
type PoolManager struct {
    pools map[string]*smpp.Pool  // name → pool
    mu    sync.RWMutex
}

func (pm *PoolManager) Get(name string) *smpp.Pool
func (pm *PoolManager) Add(name string, cfg PoolConfig) error
func (pm *PoolManager) Remove(name string) error
func (pm *PoolManager) Health(name string) PoolHealth
```

### MT Route Table

Evaluates destination MSISDN against configured routes to select a southbound
pool.

**Strategies:**

| Strategy | Description |
|----------|-------------|
| Prefix (longest-match) | Binary search on sorted prefix list. Longest match wins. |
| Failover | Ordered pool list. Try first healthy pool. Health = active connections > 0 + error rate below threshold. |
| RoundRobin | Atomic counter mod pool count. Distributes evenly across equivalent pools. |
| LeastCost | Each pool has a cost-per-message. Pick cheapest healthy pool. Ties broken by round-robin. |

Strategies compose: prefix match selects a route group, then
failover/RR/least-cost within that group.

```go
type MTRoute struct {
    Prefix   string       `json:"prefix"`
    Strategy string       `json:"strategy"`  // prefix, failover, round_robin, least_cost
    Pools    []RoutePool  `json:"pools"`
}

type RoutePool struct {
    Name string  `json:"name"`
    Cost float64 `json:"cost,omitempty"`  // for least_cost
}

type MTRouteTable struct {
    routes []MTRoute  // sorted by prefix length descending
    mu     sync.RWMutex
}

func (t *MTRouteTable) Resolve(msisdn string) (*smpp.Pool, error)
```

### MO Route Table

Routes inbound MO and DLR messages by destination number (shortcode, long
number, or prefix) to a northbound target.

```go
type MORoute struct {
    DestPattern  string `json:"dest_pattern"`   // exact or prefix match
    SourcePrefix string `json:"source_prefix"`  // optional additional filter
    Target       MOTarget `json:"target"`
}

type MOTarget struct {
    Type        string `json:"type"`         // "smpp" or "http"
    ConnID      string `json:"conn_id"`      // for type=smpp
    CallbackURL string `json:"callback_url"` // for type=http
}

func (t *MORouteTable) Resolve(sourceAddr, destAddr string) (*MOTarget, error)
```

## 3. REST API

Base path: `/api/v1`. Auth: `Authorization: Bearer <api-key>`.

### Endpoints

#### Submit
```
POST /api/v1/sms
{
  "to": "+27831234567",
  "from": "MYAPP",
  "body": "Hello world",
  "encoding": "auto",                          // gsm7 | ucs2 | auto
  "callback_url": "https://partner.com/dlr",   // optional per-message
  "reference": "order-12345",                   // optional client ref
  "registered_delivery": true                   // default true
}
→ {"id": "GW-4821", "status": "accepted", "to": "+27831234567", "reference": "order-12345"}
```

#### Batch Submit
```
POST /api/v1/sms/batch
{
  "messages": [{"to": "...", "from": "...", "body": "..."}],
  "callback_url": "https://partner.com/dlr",
  "reference_prefix": "campaign-99"
}
→ [{"id": "GW-4821", "status": "accepted"}, ...]
```

Max 1,000 per batch.

#### Query
```
GET /api/v1/sms/{id}
→ {"id": "GW-4821", "status": "delivered", "to": "...", "delivered_at": "..."}
```

### DLR Callbacks

```
POST https://partner.com/dlr
{
  "event": "dlr",
  "id": "GW-4821",
  "status": "DELIVRD",
  "to": "+27831234567",
  "from": "MYAPP",
  "reference": "order-12345",
  "timestamp": "2026-03-13T22:15:00Z"
}
```

### MO Callbacks

```
POST https://partner.com/mo
{
  "event": "mo",
  "from": "+27831234567",
  "to": "12345",
  "body": "STOP",
  "timestamp": "2026-03-13T22:16:00Z"
}
```

### Callback Delivery

At-least-once with retry. Failed callbacks (non-2xx or timeout after 10s)
retried 3 times with exponential backoff (5s, 30s, 180s). State tracked in
Pebble under `callback-retry:` prefix. Background drain loop processes retries.

## 4. Admin UI

Embedded SPA served from the Go binary via `embed.FS`. Built with Preact +
Pico CSS — small bundle, no Node.js runtime needed.

### Auth

Username/password login → JWT session token (HS256, 24h expiry). Admin accounts
stored in Pebble under `user:` prefix. Passwords hashed with bcrypt.

First-run bootstrap: if no admin users exist, create default `admin/admin` and
force password change on first login.

### Pages

| Page | Content |
|------|---------|
| Dashboard | Real-time TPS (per-pool, aggregate), active connections, error rate, retry queue depth, store size |
| Connections | Northbound SMPP sessions: system_id, IP, bound since, TPS, in-flight. Southbound pools: host, connections, window utilization, health |
| Routes | MT and MO route tables. CRUD. Drag-to-reorder priority. |
| API Keys | Create/revoke API keys. Per-key label, created date, last used, rate limit |
| Users | Admin account management (create, delete, change password) |
| Logs | Recent submit/deliver events, errors, connection events. Filterable. |

### Real-Time Updates

WebSocket at `/admin/ws` pushing metrics every 1s. Metrics struct:

```go
type RealtimeMetrics struct {
    Timestamp       time.Time              `json:"ts"`
    TotalTPS        float64                `json:"total_tps"`
    PoolMetrics     map[string]PoolStats   `json:"pools"`
    NorthboundConns []ConnStats            `json:"northbound"`
    StoreSize       int                    `json:"store_size"`
    RetryQueueSize  int                    `json:"retry_queue"`
    ErrorRate       float64                `json:"error_rate"`
}
```

### Admin API

All UI actions go through `/admin/api/` (separate from `/api/v1/`):

```
POST   /admin/api/login              — get JWT
GET    /admin/api/stats              — dashboard metrics
GET    /admin/api/connections        — list all connections
GET    /admin/api/routes/mt          — list MT routes
POST   /admin/api/routes/mt          — create MT route
PUT    /admin/api/routes/mt/{prefix} — update MT route
DELETE /admin/api/routes/mt/{prefix} — delete MT route
GET    /admin/api/routes/mo          — list MO routes
POST   /admin/api/routes/mo          — create MO route
PUT    /admin/api/routes/mo/{id}     — update MO route
DELETE /admin/api/routes/mo/{id}     — delete MO route
GET    /admin/api/pools              — list southbound pools
POST   /admin/api/pools              — create pool
PUT    /admin/api/pools/{name}       — update pool
DELETE /admin/api/pools/{name}       — delete pool
GET    /admin/api/apikeys            — list API keys
POST   /admin/api/apikeys            — create API key
DELETE /admin/api/apikeys/{id}       — revoke API key
GET    /admin/api/users              — list admin users
POST   /admin/api/users              — create admin user
DELETE /admin/api/users/{username}   — delete admin user
PUT    /admin/api/users/{username}/password — change password
GET    /admin/api/logs               — recent events (paginated)
```

## 5. Data Model

### Pebble Key Namespaces

```
# Existing (unchanged):
msg:{smscMsgID}             → SubmitRecord
gw:{gwMsgID}                → SubmitRecord
retry:{ts}:{id}             → PendingDeliver
submit-retry:{ts}:{id}      → PendingSubmit

# New:
route:mt:{prefix}           → MTRoute
route:mo:{dest}:{prefix}    → MORoute
pool:{name}                 → PoolConfig (host, port, creds, TLS, window size)
apikey:{key-hash}           → APIKey (label, rate limit, created, last used)
user:{username}             → AdminUser (bcrypt hash, role, created)
callback:{gwMsgID}          → CallbackRecord (URL, reference, retries, next)
callback-retry:{ts}:{id}    → PendingCallback
```

## 6. Package Structure

```
internal/smscgw/
  # Existing (unchanged):
  shardmap.go, store.go, connection.go, server.go,
  router.go, config.go, metrics.go

  # New:
  tls.go              — TLS config loading, cert helpers
  pool_manager.go     — manages multiple named smpp.Pool instances
  route_table.go      — MT/MO route evaluation (prefix, failover, RR, least-cost)
  route_config.go     — route CRUD, Pebble persistence
  rest_api.go         — /api/v1/ HTTP handlers (submit, batch, query)
  rest_auth.go        — API key validation middleware
  callback.go         — DLR/MO HTTP callback delivery + retry
  admin_api.go        — /admin/api/ handlers
  admin_auth.go       — JWT session auth, bcrypt passwords
  admin_ui.go         — embed.FS serving + SPA routing

internal/smpp/
  # Modified:
  client.go           — add TLS dial option
  pool.go             — add TLS config field to PoolConfig

cmd/smsc-gateway/
  main.go             — updated wiring
  admin-ui/           — Preact SPA source
    index.html
    app.js
    style.css
```

## 7. Implementation Priority

1. **TLS** — smallest scope, unblocks production deployments
2. **Pool Manager + MT Route Table** — core routing engine
3. **REST API** — HTTP submit + DLR callbacks
4. **MO Route Table** — inbound routing to SMPP or REST callbacks
5. **Admin UI** — management interface

## 8. Jasmin/Kannel ARM64 Testing

Third-party SMSC comparison images are amd64-only. Two paths:

- **Jasmin**: Build ARM64 Dockerfile from `python:3.11-slim` + `pip install jasmin`
- **SMPPSim**: Build ARM64 Dockerfile from `eclipse-temurin:21-jre` + SMPPSim JAR

No separate VM needed. QEMU emulation available as fallback:
`docker run --privileged --rm tonistiigi/binfmt --install all`

## 9. Non-Goals (v2)

- HLR/MNP lookup-based routing (future)
- Multi-protocol southbound (CIMD2, EMI/UCP) — SMPP only
- Message transformation / content modification
- Billing / CDR generation
- Multi-tenant isolation (single tenant, multiple API keys)
