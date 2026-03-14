# Campaign Stats Counters Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Use the existing `campaign_progress_by_bucket` counter table for `CampaignStats` reads, and update counters from `CardStateStore.WriteCardState` — eliminating the 600k-row scatter-gather scan.

**Architecture:** The bucketed counter table and initialization already exist. The old projector path (`ExecutionStore.UpdateCampaignCard`) already updates counters. The direct write path (`CardStateStore.WriteCardState`) does not. We add `StatsFrom`/`StatsTo` fields to `CardStateChange`, update counters in `WriteCardState`, switch `CampaignStats` to read from counters, and add reconciler drift repair.

**Tech Stack:** Go, ScyllaDB (gocql), existing `campaign_progress_by_bucket` counter table

---

### Task 1: Add StatsFrom/StatsTo to CardStateChange

**Files:**
- Modify: `internal/kafka/messages.go:53-64`

**Step 1: Add fields to CardStateChange**

```go
type CardStateChange struct {
	CampaignID  string    `json:"campaign_id"`
	CardID      string    `json:"card_id"`
	Status      string    `json:"status"`
	CurrentStep int       `json:"current_step,omitempty"`
	RetryCount  int       `json:"retry_count,omitempty"`
	LastMsgID   string    `json:"last_msg_id,omitempty"`
	LastSMPPID  string    `json:"last_smpp_message_id,omitempty"`
	LastError   string    `json:"last_error,omitempty"`
	LastErrCode string    `json:"last_error_code,omitempty"`
	Timestamp   time.Time `json:"timestamp"`
	StatsFrom   string    `json:"stats_from,omitempty"` // normalized previous status for counter update
	StatsTo     string    `json:"stats_to,omitempty"`   // normalized new status for counter update
}
```

**Step 2: Verify build**

Run: `go build ./...`
Expected: PASS

---

### Task 2: Set StatsFrom/StatsTo at every WriteCardState call site in executor

**Files:**
- Modify: `internal/executor/worker.go` — all `WriteCardState` call sites

The executor knows the state machine transitions. Set `StatsFrom`/`StatsTo` using normalized status values (`pending`, `in_progress`, `completed`, `failed`, `skipped`). The normalization is: `awaiting_dlr`/`awaiting_mo` → `in_progress`, `activating` → `pending`.

**Call sites and their transitions:**

1. **Line ~362** (`handleActivate` success): `pending → in_progress`
   ```go
   StatsFrom: "pending",
   StatsTo:   "in_progress",
   ```

2. **Line ~504** (`handleActivate` partial submit failure): `pending → failed`
   ```go
   StatsFrom: "pending",
   StatsTo:   "failed",
   ```

3. **Line ~581** (`handleActivate` DLR timeout / max retries): `in_progress → failed`
   ```go
   StatsFrom: "in_progress",
   StatsTo:   "failed",
   ```

4. **Line ~663** (retry overflow force-fail): `in_progress → failed`
   ```go
   StatsFrom: "in_progress",
   StatsTo:   "failed",
   ```

5. **Line ~853** (MO PoR error → failed): `in_progress → failed`
   ```go
   StatsFrom: "in_progress",
   StatsTo:   "failed",
   ```

6. **Line ~908** (`advanceOrComplete` → completed): `in_progress → completed`
   ```go
   StatsFrom: "in_progress",
   StatsTo:   "completed",
   ```

For DLR/MO intermediate state changes where the normalized status doesn't change (e.g., `in_progress` → `in_progress` for `awaiting_dlr`), leave `StatsFrom`/`StatsTo` empty — no counter update needed.

**Step: Verify build**

Run: `go build ./...`
Expected: PASS

---

### Task 3: Set StatsFrom/StatsTo in reconciler WriteCardState calls

**Files:**
- Modify: `internal/reconciler/service.go:214` — `recoverStaleCard`

**Transition:** `in_progress → failed` (stale cards are always in_progress when recovered)

```go
StatsFrom: "in_progress",
StatsTo:   "failed",
```

**Step: Verify build**

Run: `go build ./...`
Expected: PASS

---

### Task 4: Update counters in CardStateStore.WriteCardState

**Files:**
- Modify: `internal/scylla/card_state_store.go:22-83`

After the existing batch execution, add the counter update query if `StatsFrom` and `StatsTo` are set and different:

```go
// Update bucketed campaign stats counters.
if ev.StatsFrom != "" && ev.StatsTo != "" && ev.StatsFrom != ev.StatsTo {
    campaignBucket := s.client.CampaignBucket(ev.CampaignID, ev.CardID)
    if err := s.client.Session().Query(
        fmt.Sprintf(`UPDATE campaign_progress_by_bucket SET %s = %s - 1, %s = %s + 1 WHERE campaign_id = ? AND campaign_bucket = ?`,
            ev.StatsFrom, ev.StatsFrom, ev.StatsTo, ev.StatsTo),
        campaignUUID, campaignBucket,
    ).WithContext(ctx).Exec(); err != nil {
        return fmt.Errorf("update campaign progress counters: %w", err)
    }
}
```

Note: The counter column names (`pending`, `in_progress`, `completed`, `failed`, `skipped`) match the normalized `StatsFrom`/`StatsTo` values exactly, so they can be interpolated directly. The values are set by our own code (not user input), so this is safe.

Also update `WriteBatch` with the same logic.

**Step: Verify build**

Run: `go build ./...`
Expected: PASS

---

### Task 5: Add CampaignStatsFromCounters to QueryStore

**Files:**
- Modify: `internal/scylla/query_store.go`

Add a new method that reads from the counter table:

```go
// CampaignStatsFromCounters reads campaign stats from the bucketed counter table.
// Returns aggregated stats across all buckets. O(bucket_count) narrow rows
// instead of O(total_card_state_rows).
func (s *QueryStore) CampaignStatsFromCounters(ctx context.Context, campaignID uuid.UUID) (CampaignStats, error) {
    iter := s.client.Session().Query(
        `SELECT pending, in_progress, completed, failed, skipped FROM campaign_progress_by_bucket WHERE campaign_id = ?`,
        toGocqlUUID(campaignID),
    ).WithContext(ctx).Iter()

    var stats CampaignStats
    var p, ip, c, f, sk int64
    for iter.Scan(&p, &ip, &c, &f, &sk) {
        stats.Pending += p
        stats.InProgress += ip
        stats.Completed += c
        stats.Failed += f
        stats.Skipped += sk
    }
    if err := iter.Close(); err != nil {
        return CampaignStats{}, fmt.Errorf("read campaign stats counters: %w", err)
    }
    stats.Total = stats.Pending + stats.InProgress + stats.Completed + stats.Failed + stats.Skipped
    return normalizeCampaignStats(stats), nil
}
```

**Step: Verify build**

Run: `go build ./...`
Expected: PASS

---

### Task 6: Switch CampaignStats to use counters

**Files:**
- Modify: `internal/scylla/query_store.go:175-199`

Replace the existing `CampaignStats` method body:

```go
func (s *QueryStore) CampaignStats(ctx context.Context, campaignID uuid.UUID) (CampaignStats, error) {
    return s.CampaignStatsFromCounters(ctx, campaignID)
}
```

Keep the old `latestCampaignCardViews`-based scan — it's still used by `ListCampaignCards`, `CampaignStatsWithStaleCards`, and the reconciler.

**Step: Verify build**

Run: `go build ./...`
Expected: PASS

---

### Task 7: Update getCachedCampaignStats to not double-scan

**Files:**
- Modify: `internal/controlplane/api_campaigns.go`

The `getCachedCampaignStats` method currently calls `ListCampaignCards` (full scan) to compute both stats and card list. Now that stats come from counters, separate the two:

- Stats: `a.query.CampaignStats()` (counter-based, fast)
- Card list: `a.query.ListCampaignCards()` (scan, only for pagination)

Update the cache to store stats and cards independently. Stats cache can use 5s TTL. Card list cache can use the same or longer TTL since it's only for pagination display.

```go
func (a *API) getCachedCampaignStats(ctx context.Context, campaignID string, statusFilter string) (scyllastore.CampaignStats, []scyllastore.CampaignCardView, error) {
    a.statsCacheMu.Lock()
    if a.statsCache == nil {
        a.statsCache = make(map[string]cachedCampaignStats)
    }
    if cached, ok := a.statsCache[campaignID]; ok && time.Since(cached.at) < campaignStatsCacheTTL {
        a.statsCacheMu.Unlock()
        cards := cached.cards
        if statusFilter != "" {
            filtered := make([]scyllastore.CampaignCardView, 0)
            for _, c := range cards {
                if c.Status == statusFilter {
                    filtered = append(filtered, c)
                }
            }
            cards = filtered
        }
        return cached.stats, cards, nil
    }
    a.statsCacheMu.Unlock()

    if a.query == nil {
        return scyllastore.CampaignStats{}, nil, nil
    }

    // Stats from counters (fast — 128 narrow rows).
    stats, err := a.query.CampaignStats(ctx, uuid.MustParse(campaignID))
    if err != nil {
        return scyllastore.CampaignStats{}, nil, err
    }

    // Card list from scan (still needed for pagination).
    cards, err := a.query.ListCampaignCards(ctx, uuid.MustParse(campaignID), "")
    if err != nil {
        return scyllastore.CampaignStats{}, nil, err
    }

    a.statsCacheMu.Lock()
    a.statsCache[campaignID] = cachedCampaignStats{stats: stats, cards: cards, at: time.Now()}
    a.statsCacheMu.Unlock()

    if statusFilter != "" {
        filtered := make([]scyllastore.CampaignCardView, 0)
        for _, c := range cards {
            if c.Status == statusFilter {
                filtered = append(filtered, c)
            }
        }
        cards = filtered
    }
    return stats, cards, nil
}
```

Note: The card list scan is still cached for 5s, so it only runs once every 5 seconds. Stats from counters are cheap enough to run every time but we cache them together.

**Step: Verify build**

Run: `go build ./...`
Expected: PASS

---

### Task 8: Add reconciler drift repair

**Files:**
- Modify: `internal/reconciler/service.go`
- Modify: `internal/scylla/query_store.go` — add `RepairCampaignStatsCounters` method

**Step 1: Add RepairCampaignStatsCounters to QueryStore**

This method recomputes stats from the authoritative `campaign_card_status_by_bucket` table and compares with the counter table. If mismatched, it corrects counters.

```go
// RepairCampaignStatsCounters recomputes campaign stats from the authoritative
// campaign_card_status_by_bucket table and corrects any counter drift.
func (s *QueryStore) RepairCampaignStatsCounters(ctx context.Context, campaignID uuid.UUID) error {
    // 1. Get authoritative stats from full scan.
    rows, err := s.latestCampaignCardViews(ctx, campaignID)
    if err != nil {
        return fmt.Errorf("scan authoritative state: %w", err)
    }

    // 2. Compute per-bucket stats from authoritative data.
    bucketStats := make(map[int]CampaignStats)
    for _, row := range rows {
        bucket := s.client.CampaignBucket(campaignID.String(), row.CardID)
        bs := bucketStats[bucket]
        switch counterColumn(row.Status) {
        case "pending":    bs.Pending++
        case "in_progress": bs.InProgress++
        case "completed":  bs.Completed++
        case "failed":     bs.Failed++
        case "skipped":    bs.Skipped++
        }
        bucketStats[bucket] = bs
    }

    // 3. Read current counters per bucket.
    iter := s.client.Session().Query(
        `SELECT campaign_bucket, pending, in_progress, completed, failed, skipped FROM campaign_progress_by_bucket WHERE campaign_id = ?`,
        toGocqlUUID(campaignID),
    ).WithContext(ctx).Iter()

    counterStats := make(map[int]CampaignStats)
    var bucket int
    var p, ip, c, f, sk int64
    for iter.Scan(&bucket, &p, &ip, &c, &f, &sk) {
        counterStats[bucket] = CampaignStats{
            Pending: p, InProgress: ip, Completed: c, Failed: f, Skipped: sk,
        }
    }
    if err := iter.Close(); err != nil {
        return fmt.Errorf("read counter stats: %w", err)
    }

    // 4. Compute and apply corrections.
    allBuckets := make(map[int]bool)
    for b := range bucketStats { allBuckets[b] = true }
    for b := range counterStats { allBuckets[b] = true }

    for b := range allBuckets {
        auth := bucketStats[b]
        curr := counterStats[b]
        dp := auth.Pending - curr.Pending
        dip := auth.InProgress - curr.InProgress
        dc := auth.Completed - curr.Completed
        df := auth.Failed - curr.Failed
        dsk := auth.Skipped - curr.Skipped
        if dp == 0 && dip == 0 && dc == 0 && df == 0 && dsk == 0 {
            continue
        }
        if err := s.client.Session().Query(
            `UPDATE campaign_progress_by_bucket SET pending = pending + ?, in_progress = in_progress + ?, completed = completed + ?, failed = failed + ?, skipped = skipped + ? WHERE campaign_id = ? AND campaign_bucket = ?`,
            dp, dip, dc, df, dsk, toGocqlUUID(campaignID), b,
        ).WithContext(ctx).Exec(); err != nil {
            return fmt.Errorf("repair counter bucket %d: %w", b, err)
        }
    }
    return nil
}
```

**Step 2: Add repair to reconciler**

In `completeTerminalCampaigns`, after computing stats via `CampaignStatsWithStaleCards` (which still uses the full scan), compare with counter stats. If they diverge, repair.

Add a `RepairCampaignStatsCounters` call to the reconciler's `completeTerminalCampaigns` method — after processing stale cards, call repair for each running campaign. This runs every minute (the reconciler's `reclaimInterval`).

```go
// After stale card recovery, repair counter drift if needed.
counterStats, counterErr := s.query.CampaignStatsFromCounters(ctx, campaign.ID)
if counterErr == nil {
    scanTotal := stats.Completed + stats.Failed + stats.Skipped + stats.InProgress + stats.Pending
    counterTotal := counterStats.Completed + counterStats.Failed + counterStats.Skipped + counterStats.InProgress + counterStats.Pending
    if scanTotal != counterTotal || stats.Completed != counterStats.Completed || stats.Failed != counterStats.Failed {
        s.logger.Warn("campaign stats counter drift detected, repairing",
            zap.String("campaign_id", campaign.ID.String()),
            zap.Int64("scan_total", scanTotal),
            zap.Int64("counter_total", counterTotal),
        )
        if err := s.query.RepairCampaignStatsCounters(ctx, campaign.ID); err != nil {
            s.logger.Error("failed to repair campaign stats counters", zap.Error(err))
        }
    }
}
```

Add `QueryStore` method `CampaignStatsFromCounters` to the reconciler's available interface (it already has `*scyllastore.QueryStore` directly).

**Step 3: Verify build**

Run: `go build ./...`
Expected: PASS

---

### Task 9: Run tests and verify

**Step 1: Run unit tests**

Run: `go test ./internal/scylla/... ./internal/executor/... ./internal/controlplane/... ./internal/reconciler/... -v -count=1`
Expected: PASS

**Step 2: Run full test suite**

Run: `go test ./... -count=1`
Expected: PASS

**Step 3: Run linter**

Run: `make lint`
Expected: PASS (or only pre-existing warnings)

---

### Task 10: Rebuild, deploy, and run 200k E2E test

**Step 1: Rebuild all services**

```bash
docker compose -f deployments/docker-compose.yml -f deployments/docker-compose.load.yml build
```

**Step 2: Clean restart**

```bash
docker compose -f deployments/docker-compose.yml -f deployments/docker-compose.load.yml down -v
docker compose -f deployments/docker-compose.yml -f deployments/docker-compose.load.yml up -d
```

**Step 3: Wait for healthy, then run E2E test**

```bash
E2E_BASE_URL=http://localhost:8080 E2E_CARD_COUNT=200000 E2E_EXPECT_RESPONSE=true E2E_CAMPAIGN_TIMEOUT=30m E2E_TEST_TIMEOUT=35m go test -tags=e2e ./e2e -v -count=1 -timeout 35m
```

Expected: PASS with improved TPS (reduced ScyllaDB reactor pressure from stats queries)

**Step 4: Monitor TPS and CPU during test**

Check that:
- ScyllaDB CPU stays lower during campaign (stats no longer scan 600k rows)
- API CPU stays near 0% (counter reads are fast)
- Sustained TPS should be higher than 218 TPS baseline

---

### Task 11: Commit

```bash
git add -A
git commit -m "feat: use bucketed campaign stats counters for O(buckets) reads

Replace O(cards × buckets) scatter-gather scan with bucketed counter reads
for CampaignStats. Counter table campaign_progress_by_bucket already existed
but was only updated by the old projector path.

Changes:
- Add StatsFrom/StatsTo fields to CardStateChange for explicit transitions
- Update counters in CardStateStore.WriteCardState on every state change
- Switch CampaignStats to read from counter table (128 rows vs 600k)
- Add reconciler drift repair (compares counters vs authoritative scan)
- Keep full scan for ListCampaignCards, stale detection, resume

Co-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>"
```
