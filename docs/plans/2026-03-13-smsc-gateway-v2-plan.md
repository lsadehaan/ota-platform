# SMSC Gateway v2 Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Evolve the SMPP-to-SMPP proxy into a multi-protocol SMS gateway with TLS, advanced routing, REST API, and embedded admin UI.

**Architecture:** The gateway gains multiple southbound SMPP pools managed by a PoolManager, two route tables (MT and MO) with composable strategies (prefix, failover, round-robin, least-cost), an HTTP REST API for SMS submit with webhook DLR/MO callbacks, and an embedded Preact admin SPA. All config and auth state persisted in Pebble.

**Tech Stack:** Go stdlib `crypto/tls`, `net/http`, `embed`; Preact + Pico CSS for admin UI; Pebble KV for persistence; bcrypt for passwords; JWT (HS256) for admin sessions.

**Design doc:** `docs/plans/2026-03-13-smsc-gateway-v2-design.md`

---

## Task 1: TLS Support — SMPP Client (Southbound)

Add optional TLS dial to the SMPP client so southbound connections to real MNO SMSCs can use TLS.

**Files:**
- Modify: `internal/smpp/client.go:137-202` (Connect method)
- Modify: `internal/smpp/pool.go:13-21` (PoolConfig struct)
- Create: `internal/smpp/client_test.go` (TLS test, append to existing)

**Step 1: Add TLS fields to Config and PoolConfig**

In `internal/smpp/client.go`, add to the `Config` struct (around line 20):
```go
// TLS
TLSEnabled           bool
TLSInsecureSkipVerify bool
```

In `internal/smpp/pool.go`, no changes needed — TLS config flows through `smpp.Config` which Pool already passes to Client.

**Step 2: Modify Connect() to use TLS when configured**

In `internal/smpp/client.go`, replace the dial block at line 149:
```go
var conn net.Conn
if c.config.TLSEnabled {
    tlsCfg := &tls.Config{
        InsecureSkipVerify: c.config.TLSInsecureSkipVerify,
        ServerName:         c.config.Host,
    }
    conn, err = tls.DialWithDialer(&dialer, "tcp", addr, tlsCfg)
} else {
    conn, err = dialer.DialContext(ctx, "tcp", addr)
}
```

Add `"crypto/tls"` to imports.

**Step 3: Write test**

Add test in `internal/smpp/client_test.go` that starts a TLS listener, connects a client with TLSEnabled=true, and verifies the connection succeeds.

**Step 4: Run tests**

```bash
go test ./internal/smpp/... -v -count=1 -run TestTLS
```

**Step 5: Commit**

```bash
git add internal/smpp/client.go internal/smpp/pool.go internal/smpp/client_test.go
git commit -m "feat(smpp): add optional TLS support for outbound connections"
```

---

## Task 2: TLS Support — SMPP Server (Northbound)

Wrap the northbound TCP listener with optional TLS.

**Files:**
- Create: `internal/smscgw/tls.go`
- Modify: `internal/smscgw/server.go:53-63` (Start method)
- Modify: `internal/smscgw/config.go:11-67` (Config struct)

**Step 1: Create tls.go helper**

```go
package smscgw

import (
    "crypto/tls"
    "fmt"
)

// LoadTLSConfig loads a TLS configuration from cert and key file paths.
// Returns nil if both paths are empty (TLS disabled).
func LoadTLSConfig(certFile, keyFile string) (*tls.Config, error) {
    if certFile == "" && keyFile == "" {
        return nil, nil
    }
    if certFile == "" || keyFile == "" {
        return nil, fmt.Errorf("both TLS cert and key must be provided")
    }
    cert, err := tls.LoadX509KeyPair(certFile, keyFile)
    if err != nil {
        return nil, fmt.Errorf("load TLS cert/key: %w", err)
    }
    return &tls.Config{
        Certificates: []tls.Certificate{cert},
        MinVersion:   tls.VersionTLS12,
    }, nil
}
```

**Step 2: Add TLS config fields**

In `config.go`, add to Config struct:
```go
// TLS
TLSCertFile string
TLSKeyFile  string
```

In `LoadConfig()`, add:
```go
TLSCertFile: config.GetEnv("GW_TLS_CERT", ""),
TLSKeyFile:  config.GetEnv("GW_TLS_KEY", ""),
```

**Step 3: Wrap listener in server.go**

In `server.go` Start(), after `net.Listen`, add:
```go
tlsCfg, err := LoadTLSConfig(s.cfg.TLSCertFile, s.cfg.TLSKeyFile)
if err != nil {
    listener.Close()
    return fmt.Errorf("TLS config: %w", err)
}
if tlsCfg != nil {
    s.listener = tls.NewListener(listener, tlsCfg)
    s.logger.Info("TLS enabled on northbound listener")
} else {
    s.listener = listener
}
```

**Step 4: Build and verify**

```bash
CGO_ENABLED=0 go build ./cmd/smsc-gateway/
```

**Step 5: Commit**

```bash
git add internal/smscgw/tls.go internal/smscgw/server.go internal/smscgw/config.go
git commit -m "feat(smscgw): add optional TLS for northbound SMPP server"
```

---

## Task 3: Pool Manager — Multiple Southbound Pools

Replace the single `*smpp.Pool` in Router with a `PoolManager` that holds named pools. This is the foundation for all routing strategies.

**Files:**
- Create: `internal/smscgw/pool_manager.go`
- Create: `internal/smscgw/pool_manager_test.go`

**Step 1: Write PoolManager**

```go
package smscgw

import (
    "context"
    "fmt"
    "sync"

    "go.uber.org/zap"
    "ota-platform/internal/smpp"
)

// PoolConfig defines a named southbound SMSC connection pool.
type SouthboundPoolConfig struct {
    Name       string `json:"name"`
    Host       string `json:"host"`
    Port       int    `json:"port"`
    SystemID   string `json:"system_id"`
    Password   string `json:"password"`
    SourceAddr string `json:"source_addr"`
    Connections int   `json:"connections"`
    WindowSize  int   `json:"window_size"`
    TLSEnabled  bool  `json:"tls_enabled"`
    TLSInsecureSkipVerify bool `json:"tls_insecure_skip_verify"`
}

// PoolHealth reports the health of a named pool.
type PoolHealth struct {
    Name              string `json:"name"`
    ActiveConnections int    `json:"active_connections"`
    Healthy           bool   `json:"healthy"`
}

// PoolManager manages multiple named smpp.Pool instances.
type PoolManager struct {
    pools   map[string]*smpp.Pool
    configs map[string]*SouthboundPoolConfig
    handler smpp.DeliverHandler
    logger  *zap.Logger
    mu      sync.RWMutex
}

func NewPoolManager(handler smpp.DeliverHandler, logger *zap.Logger) *PoolManager {
    return &PoolManager{
        pools:   make(map[string]*smpp.Pool),
        configs: make(map[string]*SouthboundPoolConfig),
        handler: handler,
        logger:  logger,
    }
}

func (pm *PoolManager) Add(ctx context.Context, cfg *SouthboundPoolConfig) error {
    pm.mu.Lock()
    defer pm.mu.Unlock()

    if _, exists := pm.pools[cfg.Name]; exists {
        return fmt.Errorf("pool %q already exists", cfg.Name)
    }

    smppCfg := smpp.Config{
        Host:                  cfg.Host,
        Port:                  cfg.Port,
        SystemID:              cfg.SystemID,
        Password:              cfg.Password,
        SourceAddr:            cfg.SourceAddr,
        SourceAddrTON:         0x05,
        SourceAddrNPI:         0x00,
        EnquireLinkSec:        30,
        TLSEnabled:            cfg.TLSEnabled,
        TLSInsecureSkipVerify: cfg.TLSInsecureSkipVerify,
    }
    conns := cfg.Connections
    if conns <= 0 { conns = 2 }
    window := cfg.WindowSize
    if window <= 0 { window = 10 }
    poolCfg := smpp.PoolConfig{
        Connections:    conns,
        WindowSize:     window,
        DeliverWorkers: 16,
        DeliverQueueSize: 25000,
        SubmitTimeout:  60 * time.Second,
    }

    pool := smpp.NewPool(smppCfg, poolCfg, pm.handler, pm.logger.Named(cfg.Name))
    if err := pool.Connect(ctx); err != nil {
        return fmt.Errorf("connect pool %q: %w", cfg.Name, err)
    }

    pm.pools[cfg.Name] = pool
    pm.configs[cfg.Name] = cfg
    return nil
}

func (pm *PoolManager) Get(name string) (*smpp.Pool, bool) {
    pm.mu.RLock()
    defer pm.mu.RUnlock()
    p, ok := pm.pools[name]
    return p, ok
}

func (pm *PoolManager) Remove(name string) error {
    pm.mu.Lock()
    defer pm.mu.Unlock()
    p, ok := pm.pools[name]
    if !ok {
        return fmt.Errorf("pool %q not found", name)
    }
    p.Close()
    delete(pm.pools, name)
    delete(pm.configs, name)
    return nil
}

func (pm *PoolManager) Health(name string) PoolHealth {
    pm.mu.RLock()
    defer pm.mu.RUnlock()
    p, ok := pm.pools[name]
    if !ok {
        return PoolHealth{Name: name, Healthy: false}
    }
    active := p.ActiveConnections()
    return PoolHealth{Name: name, ActiveConnections: active, Healthy: active > 0}
}

func (pm *PoolManager) AllHealth() []PoolHealth {
    pm.mu.RLock()
    defer pm.mu.RUnlock()
    result := make([]PoolHealth, 0, len(pm.pools))
    for name, p := range pm.pools {
        active := p.ActiveConnections()
        result = append(result, PoolHealth{Name: name, ActiveConnections: active, Healthy: active > 0})
    }
    return result
}

func (pm *PoolManager) Names() []string {
    pm.mu.RLock()
    defer pm.mu.RUnlock()
    names := make([]string, 0, len(pm.pools))
    for n := range pm.pools {
        names = append(names, n)
    }
    return names
}

func (pm *PoolManager) Close() {
    pm.mu.Lock()
    defer pm.mu.Unlock()
    for _, p := range pm.pools {
        p.Close()
    }
    pm.pools = make(map[string]*smpp.Pool)
    pm.configs = make(map[string]*SouthboundPoolConfig)
}
```

**Step 2: Write unit test**

Test Add, Get, Remove, Health, Names, Close.

**Step 3: Build and test**

```bash
go test ./internal/smscgw/... -v -count=1 -run TestPoolManager
```

**Step 4: Commit**

```bash
git add internal/smscgw/pool_manager.go internal/smscgw/pool_manager_test.go
git commit -m "feat(smscgw): add PoolManager for multiple named southbound pools"
```

---

## Task 4: MT Route Table

Implement the MT route table with four strategies: prefix, failover, round-robin, least-cost.

**Files:**
- Create: `internal/smscgw/route_table.go`
- Create: `internal/smscgw/route_table_test.go`

**Step 1: Define route types**

```go
package smscgw

type MTRoute struct {
    Prefix   string      `json:"prefix"`
    Strategy string      `json:"strategy"` // prefix, failover, round_robin, least_cost
    Pools    []RoutePool `json:"pools"`
    rrIndex  atomic.Uint64 // for round-robin
}

type RoutePool struct {
    Name string  `json:"name"`
    Cost float64 `json:"cost,omitempty"`
}

type MTRouteTable struct {
    routes []*MTRoute // sorted by prefix length descending (longest first)
    mu     sync.RWMutex
}
```

**Step 2: Implement Resolve()**

```go
func (t *MTRouteTable) Resolve(msisdn string, pm *PoolManager) (*smpp.Pool, string, error)
```

Logic:
1. Iterate routes (sorted longest-prefix-first)
2. If `msisdn` starts with route prefix (or prefix is `*`), match
3. Apply strategy:
   - `failover`: iterate pools in order, return first healthy
   - `round_robin`: atomic increment mod len(pools), skip unhealthy
   - `least_cost`: sort by cost, return cheapest healthy
4. Return `(pool, poolName, nil)` or error if no healthy pool found

**Step 3: Implement AddRoute / RemoveRoute / ListRoutes**

CRUD methods that maintain sorted order. AddRoute re-sorts by prefix length descending after insertion.

**Step 4: Write comprehensive tests**

- Test longest-prefix-match ("+2347" beats "+234")
- Test failover (first pool down, second pool returned)
- Test round-robin distribution
- Test least-cost selection
- Test default route (`*` prefix)
- Test no matching route → error

**Step 5: Build and test**

```bash
go test ./internal/smscgw/... -v -count=1 -run TestMTRoute
```

**Step 6: Commit**

```bash
git add internal/smscgw/route_table.go internal/smscgw/route_table_test.go
git commit -m "feat(smscgw): MT route table with prefix, failover, round-robin, least-cost"
```

---

## Task 5: MO Route Table

Route inbound MO/DLR by destination number (shortcode/prefix) to SMPP connection or HTTP callback.

**Files:**
- Modify: `internal/smscgw/route_table.go` (append MO types)
- Modify: `internal/smscgw/route_table_test.go` (append MO tests)

**Step 1: Define MO route types**

```go
type MORoute struct {
    DestPattern  string   `json:"dest_pattern"`   // exact or prefix
    SourcePrefix string   `json:"source_prefix"`  // optional filter
    Target       MOTarget `json:"target"`
    Priority     int      `json:"priority"`
}

type MOTarget struct {
    Type        string `json:"type"`         // "smpp" or "http"
    ConnID      string `json:"conn_id"`      // for smpp
    CallbackURL string `json:"callback_url"` // for http
}

type MORouteTable struct {
    routes []*MORoute // sorted by priority (highest first)
    mu     sync.RWMutex
}
```

**Step 2: Implement Resolve()**

```go
func (t *MORouteTable) Resolve(sourceAddr, destAddr string) (*MOTarget, error)
```

Logic:
1. Iterate routes by priority
2. Match: dest matches pattern (exact or prefix) AND source matches prefix (if set)
3. Return first match, or nil if no match (caller falls back to MSISDN affinity)

**Step 3: Write tests**

- Exact shortcode match ("12345" → HTTP callback)
- Prefix match ("+27*" → SMPP connID)
- Source prefix filter
- Priority ordering
- No match → nil (fallback)

**Step 4: Build and test**

```bash
go test ./internal/smscgw/... -v -count=1 -run TestMORoute
```

**Step 5: Commit**

```bash
git add internal/smscgw/route_table.go internal/smscgw/route_table_test.go
git commit -m "feat(smscgw): MO route table with dest pattern and source prefix matching"
```

---

## Task 6: Route Config Persistence

Persist MT routes, MO routes, and pool configs in Pebble. CRUD operations.

**Files:**
- Create: `internal/smscgw/route_config.go`
- Modify: `internal/smscgw/store.go` (add generic Get/Set/Delete/Scan helpers)

**Step 1: Add generic Pebble helpers to store.go**

```go
func (s *MessageStore) SetJSON(key string, v any) error
func (s *MessageStore) GetJSON(key string, v any) error
func (s *MessageStore) DeleteKey(key string) error
func (s *MessageStore) ScanPrefix(prefix string, fn func(key string, data []byte) error) error
```

**Step 2: Create route_config.go**

```go
// RouteConfigStore provides CRUD for MT routes, MO routes, and pool configs.
type RouteConfigStore struct {
    store *MessageStore
}

func (rc *RouteConfigStore) SaveMTRoute(route *MTRoute) error    // key: route:mt:{prefix}
func (rc *RouteConfigStore) DeleteMTRoute(prefix string) error
func (rc *RouteConfigStore) LoadAllMTRoutes() ([]*MTRoute, error)

func (rc *RouteConfigStore) SaveMORoute(route *MORoute) error    // key: route:mo:{dest}:{source}
func (rc *RouteConfigStore) DeleteMORoute(dest, source string) error
func (rc *RouteConfigStore) LoadAllMORoutes() ([]*MORoute, error)

func (rc *RouteConfigStore) SavePoolConfig(cfg *SouthboundPoolConfig) error  // key: pool:{name}
func (rc *RouteConfigStore) DeletePoolConfig(name string) error
func (rc *RouteConfigStore) LoadAllPoolConfigs() ([]*SouthboundPoolConfig, error)
```

**Step 3: Build and test**

```bash
go test ./internal/smscgw/... -v -count=1 -run TestRouteConfig
```

**Step 4: Commit**

```bash
git add internal/smscgw/route_config.go internal/smscgw/store.go
git commit -m "feat(smscgw): Pebble-backed persistence for routes and pool configs"
```

---

## Task 7: Integrate Routing into Router

Replace the single `*smpp.Pool` in Router with PoolManager + route tables. Update HandleSubmit and handleMO to use the route tables.

**Files:**
- Modify: `internal/smscgw/router.go:62-89` (Router struct)
- Modify: `internal/smscgw/router.go:310-384` (forwardSubmitRaw — use route table)
- Modify: `internal/smscgw/router.go:463-497` (handleMO — use MO route table)
- Modify: `cmd/smsc-gateway/main.go:31-124` (wiring)

**Step 1: Update Router struct**

Replace `southbound *smpp.Pool` with:
```go
poolManager  *PoolManager
mtRoutes     *MTRouteTable
moRoutes     *MORouteTable
routeConfig  *RouteConfigStore
```

Keep `SetSouthbound(pool)` as a compatibility shim that adds the pool as "default" to the PoolManager.

**Step 2: Update forwardSubmitRaw**

Replace `r.southbound.SubmitRaw(rawBody)` at line 323 with:
```go
pool, poolName, err := r.mtRoutes.Resolve(destAddr, r.poolManager)
if err != nil {
    // No route found — use default pool (backward compat)
    pool, ok := r.poolManager.Get("default")
    if !ok {
        // No pools at all — enqueue retry
        r.enqueueSubmitRetryOrFail(gwMsgID, connID, destAddr, sourceAddr, rawBody, 0)
        return
    }
    poolName = "default"
    _ = poolName // used for logging
}
resp, err := pool.SubmitRaw(rawBody)
```

**Step 3: Update handleMO**

Before the existing MSISDN affinity lookup, check MO route table:
```go
target, err := r.moRoutes.Resolve(sourceAddr, destAddr)
if err == nil && target != nil {
    if target.Type == "http" {
        // Deliver via HTTP callback (Task 9)
        r.deliverMOCallback(target.CallbackURL, sourceAddr, destAddr, payload)
        return
    }
    if target.Type == "smpp" {
        connID = target.ConnID
        // Fall through to existing deliver logic
    }
}
// Existing MSISDN affinity fallback below...
```

**Step 4: Update main.go wiring**

Load route configs and pool configs from Pebble on startup. Create PoolManager, connect all configured pools. If no pools configured, fall back to the single-pool env var config (backward compat).

**Step 5: Build and verify**

```bash
CGO_ENABLED=0 go build ./cmd/smsc-gateway/
```

**Step 6: Run existing tests**

```bash
go test ./internal/smscgw/... -v -count=1
```

All existing tests must still pass — the default pool backward compat path ensures this.

**Step 7: Commit**

```bash
git add internal/smscgw/router.go cmd/smsc-gateway/main.go
git commit -m "feat(smscgw): integrate PoolManager and route tables into Router"
```

---

## Task 8: REST API — Auth (API Keys)

API key management and authentication middleware.

**Files:**
- Create: `internal/smscgw/rest_auth.go`
- Create: `internal/smscgw/rest_auth_test.go`

**Step 1: Define APIKey type**

```go
type APIKey struct {
    ID        string    `json:"id"`
    KeyHash   string    `json:"key_hash"`   // bcrypt hash
    Label     string    `json:"label"`
    RateLimit int       `json:"rate_limit"` // TPS, 0=unlimited
    CreatedAt time.Time `json:"created_at"`
    LastUsed  time.Time `json:"last_used"`
}
```

**Step 2: Implement key store**

```go
type APIKeyStore struct {
    store *MessageStore
}

func (ks *APIKeyStore) Create(label string, rateLimit int) (plainKey string, err error)
func (ks *APIKeyStore) Validate(bearerToken string) (*APIKey, error)
func (ks *APIKeyStore) Revoke(id string) error
func (ks *APIKeyStore) List() ([]*APIKey, error)
```

Key format: `sk_live_{32 random hex chars}`. Stored as `apikey:{sha256(key)}` in Pebble. The plain key is returned only on creation.

**Step 3: Write auth middleware**

```go
func APIKeyAuthMiddleware(ks *APIKeyStore) func(http.Handler) http.Handler
```

Extracts `Authorization: Bearer <key>`, validates, sets APIKey in request context.

**Step 4: Write tests**

- Create key, validate with correct key → success
- Validate with wrong key → 401
- Revoke key, validate → 401
- List keys (plain key not exposed)

**Step 5: Commit**

```bash
git add internal/smscgw/rest_auth.go internal/smscgw/rest_auth_test.go
git commit -m "feat(smscgw): API key auth for REST API"
```

---

## Task 9: REST API — Submit + Callbacks

HTTP endpoint for SMS submission and DLR/MO webhook callbacks.

**Files:**
- Create: `internal/smscgw/rest_api.go`
- Create: `internal/smscgw/callback.go`
- Create: `internal/smscgw/rest_api_test.go`

**Step 1: Implement submit handler**

```go
func (r *Router) HandleHTTPSubmit(w http.ResponseWriter, req *http.Request)
```

Parses JSON body, builds submit_sm PDU internally (using `smpp.BuildSubmitSM`), routes through MT route table, stores callback URL in Pebble under `callback:{gwMsgID}`.

**Step 2: Implement batch submit**

```go
func (r *Router) HandleHTTPBatchSubmit(w http.ResponseWriter, req *http.Request)
```

Iterates messages array, calls single submit logic per message, returns array of results.

**Step 3: Implement query handler**

```go
func (r *Router) HandleHTTPQuery(w http.ResponseWriter, req *http.Request)
```

Looks up `gw:{id}` or `msg:{id}` in Pebble, returns status.

**Step 4: Implement callback delivery**

```go
type CallbackRecord struct {
    GwMsgID     string    `json:"gw_msg_id"`
    CallbackURL string    `json:"callback_url"`
    Reference   string    `json:"reference"`
    Retries     int       `json:"retries"`
    NextAttempt time.Time `json:"next_attempt"`
}

func (r *Router) deliverDLRCallback(gwMsgID, callbackURL, reference, status string)
func (r *Router) deliverMOCallback(callbackURL, sourceAddr, destAddr string, payload []byte)
func (r *Router) RunCallbackRetryLoop(ctx context.Context, interval time.Duration)
```

POST to callback URL with JSON body. On failure (non-2xx, timeout 10s), enqueue to `callback-retry:{ts}:{id}` in Pebble. Background loop drains retries with exponential backoff (5s, 30s, 180s). Max 3 retries.

**Step 5: Wire HTTP routes**

```go
func (r *Router) RegisterRESTRoutes(mux *http.ServeMux, keyStore *APIKeyStore) {
    auth := APIKeyAuthMiddleware(keyStore)
    mux.Handle("POST /api/v1/sms", auth(http.HandlerFunc(r.HandleHTTPSubmit)))
    mux.Handle("POST /api/v1/sms/batch", auth(http.HandlerFunc(r.HandleHTTPBatchSubmit)))
    mux.Handle("GET /api/v1/sms/{id}", auth(http.HandlerFunc(r.HandleHTTPQuery)))
}
```

**Step 6: Update DLR routing in handleDLR**

After translating the DLR message ID, check if there's a callback record for the gwMsgID. If so, POST to the callback URL instead of (or in addition to) delivering via SMPP.

**Step 7: Write tests**

- Submit via REST → verify accepted, gwMsgID returned
- Submit with callback URL → verify callback POSTed on DLR
- Batch submit → verify array response
- Query → verify status
- Invalid auth → 401
- Callback retry on failure

**Step 8: Commit**

```bash
git add internal/smscgw/rest_api.go internal/smscgw/callback.go internal/smscgw/rest_api_test.go
git commit -m "feat(smscgw): REST API for SMS submit with DLR/MO webhook callbacks"
```

---

## Task 10: Admin Auth — Users + JWT

Username/password authentication for the admin UI.

**Files:**
- Create: `internal/smscgw/admin_auth.go`
- Create: `internal/smscgw/admin_auth_test.go`

**Step 1: Define AdminUser type**

```go
type AdminUser struct {
    Username     string    `json:"username"`
    PasswordHash string    `json:"password_hash"` // bcrypt
    Role         string    `json:"role"`           // "admin"
    CreatedAt    time.Time `json:"created_at"`
    MustChange   bool      `json:"must_change"`    // force password change
}
```

Stored in Pebble as `user:{username}`.

**Step 2: Implement AdminUserStore**

```go
type AdminUserStore struct {
    store     *MessageStore
    jwtSecret []byte
}

func (us *AdminUserStore) Create(username, password, role string) error
func (us *AdminUserStore) Authenticate(username, password string) (jwtToken string, err error)
func (us *AdminUserStore) ValidateJWT(token string) (*AdminUser, error)
func (us *AdminUserStore) ChangePassword(username, oldPass, newPass string) error
func (us *AdminUserStore) Delete(username string) error
func (us *AdminUserStore) List() ([]*AdminUser, error)
func (us *AdminUserStore) Bootstrap() error  // create admin/admin if no users exist
```

JWT: HS256, 24h expiry, claims: `{sub: username, role: role, exp: ...}`.

**Step 3: Write JWT middleware**

```go
func AdminAuthMiddleware(us *AdminUserStore) func(http.Handler) http.Handler
```

Extracts `Authorization: Bearer <jwt>`, validates, sets user in context.

**Step 4: Write tests**

- Create user, authenticate → JWT
- Validate JWT → user
- Wrong password → error
- Expired JWT → error
- Bootstrap creates default admin
- Change password works

**Step 5: Commit**

```bash
git add internal/smscgw/admin_auth.go internal/smscgw/admin_auth_test.go
git commit -m "feat(smscgw): admin user auth with bcrypt + JWT"
```

---

## Task 11: Admin API

REST endpoints for managing routes, pools, API keys, users, and viewing stats.

**Files:**
- Create: `internal/smscgw/admin_api.go`

**Step 1: Implement admin handlers**

```go
type AdminAPI struct {
    router      *Router
    poolManager *PoolManager
    routeConfig *RouteConfigStore
    keyStore    *APIKeyStore
    userStore   *AdminUserStore
    metrics     *Metrics
    server      *Server
    logger      *zap.Logger
}

func (a *AdminAPI) RegisterRoutes(mux *http.ServeMux)
```

Endpoints (all behind AdminAuthMiddleware):
- `POST /admin/api/login` — authenticate, return JWT (no middleware on this one)
- `GET /admin/api/stats` — dashboard metrics snapshot
- `GET /admin/api/connections` — list northbound + southbound connections
- CRUD for `/admin/api/routes/mt`, `/admin/api/routes/mo`
- CRUD for `/admin/api/pools` (add/remove triggers PoolManager.Add/Remove)
- CRUD for `/admin/api/apikeys`
- CRUD for `/admin/api/users`
- `GET /admin/api/logs` — recent events from ring buffer

**Step 2: Implement stats endpoint**

Collects from: metrics (Prometheus counters/gauges), PoolManager.AllHealth(), Server connection list, Store message count, retry queue size.

**Step 3: Implement WebSocket for real-time metrics**

```go
func (a *AdminAPI) HandleWebSocket(w http.ResponseWriter, r *http.Request)
```

Upgrades to WebSocket (use `golang.org/x/net/websocket` or `nhooyr.io/websocket`), pushes `RealtimeMetrics` JSON every 1s.

**Step 4: Commit**

```bash
git add internal/smscgw/admin_api.go
git commit -m "feat(smscgw): admin REST API for gateway management"
```

---

## Task 12: Admin UI — Embedded SPA

Build the embedded web dashboard using Preact + Pico CSS.

**Files:**
- Create: `internal/smscgw/admin_ui.go`
- Create: `cmd/smsc-gateway/admin-ui/index.html`
- Create: `cmd/smsc-gateway/admin-ui/app.js`
- Create: `cmd/smsc-gateway/admin-ui/style.css`

**Step 1: Create admin_ui.go with embed**

```go
package smscgw

import (
    "embed"
    "io/fs"
    "net/http"
)

//go:embed admin-ui
var adminUIFS embed.FS

func AdminUIHandler() http.Handler {
    sub, _ := fs.Sub(adminUIFS, "admin-ui")
    fileServer := http.FileServer(http.FS(sub))
    return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
        // SPA routing: serve index.html for non-file paths
        // ...
        fileServer.ServeHTTP(w, r)
    })
}
```

Note: The embed directive must be in the same package as the `admin-ui/` directory, so the files go in `internal/smscgw/admin-ui/` and the embed is in `admin_ui.go`.

**Step 2: Build the SPA**

A single-page app with Preact (loaded via CDN or bundled), Pico CSS for styling, and vanilla JS fetch calls to `/admin/api/`.

Pages:
- Login form
- Dashboard (TPS chart, connection status, queue depths)
- Connections table (northbound + southbound)
- Routes editor (MT and MO, CRUD forms)
- API Keys management
- Users management

Use `<script type="module">` with Preact from CDN (`https://esm.sh/preact`) for zero build step. Or bundle with esbuild at compile time.

**Step 3: Wire into main.go**

```go
// Admin UI + API on a single HTTP server
adminMux := http.NewServeMux()
adminAPI.RegisterRoutes(adminMux)
adminMux.Handle("/admin/", AdminUIHandler())

adminAddr := config.GetEnv("GW_ADMIN_ADDR", ":8080")
go func() {
    srv := &http.Server{Addr: adminAddr, Handler: adminMux}
    // TLS if configured
    srv.ListenAndServe()
}()
```

**Step 4: Commit**

```bash
git add internal/smscgw/admin_ui.go internal/smscgw/admin-ui/
git commit -m "feat(smscgw): embedded admin UI dashboard"
```

---

## Task 13: Wire Everything in main.go

Update the binary entry point to initialize all new components.

**Files:**
- Modify: `cmd/smsc-gateway/main.go`
- Modify: `internal/smscgw/config.go`

**Step 1: Add new config fields**

```go
// REST API
RESTAddr string // ":8080" — shared with admin UI

// Admin
AdminDefaultPassword string
JWTSecret            string

// HTTP TLS (for REST + admin)
HTTPTLSCertFile string
HTTPTLSKeyFile  string
```

**Step 2: Update run() wiring order**

```go
func run(ctx context.Context, logger *zap.Logger) error {
    cfg := smscgw.LoadConfig()
    metrics := smscgw.NewMetrics()
    store := smscgw.NewMessageStore(cfg.DataDir, logger)
    routeConfig := smscgw.NewRouteConfigStore(store)

    // Router (with pool manager instead of single pool)
    router := smscgw.NewRouter(store, metrics, cfg, logger)
    poolManager := smscgw.NewPoolManager(router.HandleDeliver, logger)
    router.SetPoolManager(poolManager)

    // Load persisted pool configs and connect
    poolConfigs := routeConfig.LoadAllPoolConfigs()
    for _, pc := range poolConfigs {
        poolManager.Add(ctx, pc)
    }

    // Backward compat: if no pools configured, create "default" from env vars
    if len(poolConfigs) == 0 {
        poolManager.Add(ctx, &SouthboundPoolConfig{Name: "default", Host: cfg.SMSCHost, ...})
    }

    // Load route tables
    mtRoutes := routeConfig.LoadAllMTRoutes()
    moRoutes := routeConfig.LoadAllMORoutes()
    router.SetMTRoutes(mtRoutes)
    router.SetMORoutes(moRoutes)

    // Northbound SMPP server (with TLS)
    server := smscgw.NewServer(cfg, metrics, logger)
    server.SetRouter(router)
    router.SetServer(server)
    server.Start()

    // Forward workers + background loops (unchanged)
    router.StartForwardWorkers(cfg.ForwardWorkers)
    // ... all background goroutines ...

    // Auth stores
    keyStore := smscgw.NewAPIKeyStore(store)
    userStore := smscgw.NewAdminUserStore(store, []byte(cfg.JWTSecret))
    userStore.Bootstrap()

    // REST API + Admin UI on single HTTP server
    httpMux := http.NewServeMux()
    router.RegisterRESTRoutes(httpMux, keyStore)
    adminAPI := smscgw.NewAdminAPI(router, poolManager, routeConfig, keyStore, userStore, metrics, server, logger)
    adminAPI.RegisterRoutes(httpMux)
    httpMux.Handle("/", smscgw.AdminUIHandler())

    go serveHTTP(cfg.RESTAddr, httpMux, cfg.HTTPTLSCertFile, cfg.HTTPTLSKeyFile, logger)

    // Callback retry loop
    go router.RunCallbackRetryLoop(ctx, 10*time.Second)

    <-ctx.Done()
    return nil
}
```

**Step 3: Build and verify**

```bash
CGO_ENABLED=0 go build ./cmd/smsc-gateway/
```

**Step 4: Commit**

```bash
git add cmd/smsc-gateway/main.go internal/smscgw/config.go
git commit -m "feat(smscgw): wire TLS, routing, REST API, and admin UI in main"
```

---

## Task 14: New Prometheus Metrics

Add metrics for the new components.

**Files:**
- Modify: `internal/smscgw/metrics.go`

**Step 1: Add new metrics**

```go
// REST API
RESTSubmitTotal    *prometheus.CounterVec  // labels: status
RESTCallbackTotal  *prometheus.CounterVec  // labels: status (success, retry, failed)

// Routing
RouteResolutions   *prometheus.CounterVec  // labels: strategy, pool

// Pools
PoolHealthGauge    *prometheus.GaugeVec    // labels: pool_name

// Admin
AdminLoginTotal    *prometheus.CounterVec  // labels: status (success, failed)
```

**Step 2: Commit**

```bash
git add internal/smscgw/metrics.go
git commit -m "feat(smscgw): add Prometheus metrics for REST API, routing, pools"
```

---

## Task 15: Integration Tests

End-to-end tests for REST API and routing.

**Files:**
- Create: `tests/smsc-gateway/test_rest_api.py`
- Create: `tests/smsc-gateway/test_routing.py` (extend existing)

**Step 1: REST API tests**

- Submit via REST, verify DLR callback received
- Batch submit
- Query message status
- Invalid API key → 401
- Rate-limited API key → 429

**Step 2: Routing tests**

- Configure two pools via admin API, add prefix route, submit to each prefix, verify correct pool receives
- Failover: stop one pool, verify submits go to backup
- MO routing: configure shortcode → HTTP callback, trigger MO, verify callback

**Step 3: Admin UI smoke test**

- Login via admin API
- List routes, create route, delete route
- Create/revoke API key

**Step 4: Commit**

```bash
git add tests/smsc-gateway/test_rest_api.py tests/smsc-gateway/test_routing.py
git commit -m "test(smscgw): integration tests for REST API and routing"
```

---

## Task 16: Jasmin ARM64 Dockerfile

Build an ARM64-compatible Jasmin image for comparative testing.

**Files:**
- Create: `deployments/perf-matrix/jasmin/Dockerfile`
- Create: `deployments/perf-matrix/jasmin/jasmin.cfg`
- Modify: `deployments/perf-matrix/compose.sut-jasmin.yml`

**Step 1: Create Dockerfile**

```dockerfile
FROM python:3.11-slim
RUN pip install --no-cache-dir jasmin
COPY jasmin.cfg /etc/jasmin/jasmin.cfg
EXPOSE 2775 8990 1401
CMD ["jasmind", "--enable-smpp-server"]
```

**Step 2: Create minimal config**

SMPP server on 2776, SMPP client connector to downstream-smsc:2775, system_id/password matching our test harness.

**Step 3: Update compose overlay**

Replace the sleeping alpine container with the real Jasmin build.

**Step 4: Test build**

```bash
docker build -t jasmin-arm64 -f deployments/perf-matrix/jasmin/Dockerfile .
```

**Step 5: Commit**

```bash
git add deployments/perf-matrix/jasmin/ deployments/perf-matrix/compose.sut-jasmin.yml
git commit -m "feat(perf): ARM64-compatible Jasmin Docker image for comparative testing"
```

---

## Task Summary

| Task | Component | Depends On |
|------|-----------|------------|
| 1 | TLS — SMPP client (southbound) | — |
| 2 | TLS — SMPP server (northbound) | — |
| 3 | Pool Manager | — |
| 4 | MT Route Table | 3 |
| 5 | MO Route Table | — |
| 6 | Route Config Persistence | 4, 5 |
| 7 | Integrate routing into Router | 3, 4, 5, 6 |
| 8 | REST API auth (API keys) | — |
| 9 | REST API submit + callbacks | 7, 8 |
| 10 | Admin auth (users + JWT) | — |
| 11 | Admin API | 3, 6, 7, 8, 10 |
| 12 | Admin UI (embedded SPA) | 11 |
| 13 | Wire everything in main.go | 1-12 |
| 14 | New Prometheus metrics | 9, 11 |
| 15 | Integration tests | 9, 11, 13 |
| 16 | Jasmin ARM64 Dockerfile | — |
