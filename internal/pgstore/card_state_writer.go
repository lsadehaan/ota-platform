package pgstore

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
	"go.uber.org/zap"
	"gorm.io/gorm"

	"ota-platform/internal/pipeline"
)

// CardStateWriterConfig holds tuning parameters for the batched card state writer.
type CardStateWriterConfig struct {
	MaxBatch      int
	FlushInterval time.Duration
}

// DefaultCardStateWriterConfig returns a CardStateWriterConfig populated from
// environment variables with sensible defaults.
func DefaultCardStateWriterConfig() CardStateWriterConfig {
	return CardStateWriterConfig{
		MaxBatch:      envIntPgstore("ENGINE_STATE_WRITER_BATCH", 10000),
		FlushInterval: time.Duration(envIntPgstore("ENGINE_STATE_WRITER_FLUSH_MS", 2000)) * time.Millisecond,
	}
}

// statsDelta tracks cumulative counter adjustments per campaign.
type statsDelta struct {
	pending, inProgress, completed, failed, skipped int64
}

// RunCardStateWriter consumes CardStateChange events from the channel and
// batch-upserts them into card_execution_states + campaign_stats.
// It flushes when the batch reaches cfg.MaxBatch or the flush interval fires.
func RunCardStateWriter(ctx context.Context, ch <-chan pipeline.CardStateChange, database *gorm.DB, cfg CardStateWriterConfig, logger *zap.Logger) {
	meter := otel.Meter("ota-engine")
	rowsWritten, _ := meter.Int64Counter("ota.state_writer.rows_written",
		metric.WithDescription("Total card_execution_states rows written"))
	flushDuration, _ := meter.Float64Histogram("ota.state_writer.flush_duration_ms",
		metric.WithDescription("Duration of card state writer flush in ms"))
	batchSizeHist, _ := meter.Float64Histogram("ota.state_writer.batch_size",
		metric.WithDescription("Number of events per flush"))

	batch := make([]pipeline.CardStateChange, 0, cfg.MaxBatch)
	ticker := time.NewTicker(cfg.FlushInterval)
	defer ticker.Stop()

	flush := func() {
		if len(batch) == 0 {
			return
		}
		n := len(batch)
		start := time.Now()
		if err := writeCardStateBatch(ctx, database, batch, logger); err != nil {
			logger.Error("card state batch write failed", zap.Error(err), zap.Int("batch_size", n))
		} else {
			elapsed := float64(time.Since(start).Microseconds()) / 1000.0
			if flushDuration != nil {
				flushDuration.Record(ctx, elapsed)
			}
			if batchSizeHist != nil {
				batchSizeHist.Record(ctx, float64(n))
			}
			if rowsWritten != nil {
				rowsWritten.Add(ctx, int64(n))
			}
		}
		batch = batch[:0]
	}

	for {
		select {
		case ev, ok := <-ch:
			if !ok {
				flush()
				return
			}
			batch = append(batch, ev)
			if len(batch) >= cfg.MaxBatch {
				flush()
			}
		case <-ticker.C:
			flush()
		case <-ctx.Done():
			flush()
			return
		}
	}
}

// writeCardStateBatch deduplicates events per (card_id, campaign_id), appends
// card_execution_states rows (no UPDATEs), and updates campaign_stats counters.
func writeCardStateBatch(ctx context.Context, database *gorm.DB, events []pipeline.CardStateChange, logger *zap.Logger) error {
	// Deduplicate: keep the latest event per (card_id, campaign_id) by TransitionSeq.
	type key struct{ cardID, campaignID string }
	latest := make(map[key]*pipeline.CardStateChange, len(events))
	for i := range events {
		ev := &events[i]
		k := key{ev.CardID, ev.CampaignID}
		if existing, ok := latest[k]; !ok {
			latest[k] = ev
		} else if ev.TransitionSeq > existing.TransitionSeq ||
			(ev.TransitionSeq == existing.TransitionSeq && ev.Timestamp.After(existing.Timestamp)) {
			latest[k] = ev
		}
	}

	// Aggregate counter deltas from ALL events (not just deduped) for campaign_stats.
	counterDeltas := make(map[string]*statsDelta)
	for i := range events {
		ev := &events[i]
		if ev.StatsFrom == "" && ev.StatsTo == "" {
			continue
		}
		d, ok := counterDeltas[ev.CampaignID]
		if !ok {
			d = &statsDelta{}
			counterDeltas[ev.CampaignID] = d
		}
		applyDelta(d, ev.StatsFrom, -1)
		applyDelta(d, ev.StatsTo, +1)
	}

	// Build COPY rows for card_execution_states.
	columns := []string{
		"card_id", "campaign_id", "status", "current_step", "retry_count",
		"last_msg_id", "last_smpp_id", "last_error", "transition_seq", "updated_at",
	}
	copyRows := make([][]interface{}, 0, len(latest))
	for _, ev := range latest {
		cardID, err := uuid.Parse(ev.CardID)
		if err != nil {
			logger.Warn("skip card state: bad card_id", zap.String("card_id", ev.CardID), zap.Error(err))
			continue
		}
		campID, err := uuid.Parse(ev.CampaignID)
		if err != nil {
			logger.Warn("skip card state: bad campaign_id", zap.String("campaign_id", ev.CampaignID), zap.Error(err))
			continue
		}

		var lastMsgID *uuid.UUID
		if ev.LastMsgID != "" {
			if parsed, pErr := uuid.Parse(ev.LastMsgID); pErr == nil {
				lastMsgID = &parsed
			}
		}

		copyRows = append(copyRows, []interface{}{
			cardID, campID, ev.Status, ev.CurrentStep, ev.RetryCount,
			lastMsgID, ev.LastSMPPID, ev.LastError, ev.TransitionSeq, ev.Timestamp,
		})
	}

	if len(copyRows) == 0 && len(counterDeltas) == 0 {
		return nil
	}

	// Write card_execution_states via COPY (temp table → INSERT ON CONFLICT DO NOTHING).
	if len(copyRows) > 0 {
		if err := flushCardStatesCopy(ctx, database, columns, copyRows, logger); err != nil {
			return err
		}
	}

	// Update campaign_stats counters (single row per campaign — trivial UPDATE).
	for campIDStr, d := range counterDeltas {
		campID, err := uuid.Parse(campIDStr)
		if err != nil {
			continue
		}
		sql, args := buildStatsUpdateSQL(d, campID)
		if sql == "" {
			continue
		}
		if err := database.WithContext(ctx).Exec(sql, args...).Error; err != nil {
			logger.Warn("campaign stats counter update failed",
				zap.String("campaign_id", campIDStr), zap.Error(err))
		}
	}

	return nil
}

// flushCardStatesCopy uses pgx COPY FROM STDIN directly into
// card_execution_states (append-only). Each row has a unique
// (card_id, campaign_id, transition_seq) so no conflicts occur.
func flushCardStatesCopy(ctx context.Context, database *gorm.DB, columns []string, copyRows [][]interface{}, logger *zap.Logger) error {
	sqlDB, err := database.DB()
	if err != nil {
		return fmt.Errorf("get sql.DB: %w", err)
	}
	conn, err := sqlDB.Conn(ctx)
	if err != nil {
		return fmt.Errorf("get conn: %w", err)
	}
	defer conn.Close()

	return conn.Raw(func(driverConn interface{}) error {
		stdlibConn, ok := driverConn.(*stdlib.Conn)
		if !ok {
			return flushCardStatesFallback(ctx, database, columns, copyRows)
		}
		pgxConn := stdlibConn.Conn()

		_, err := pgxConn.CopyFrom(ctx,
			pgx.Identifier{"card_execution_states"},
			columns,
			pgx.CopyFromRows(copyRows),
		)
		if err != nil {
			// Duplicate on crash recovery — fall back to INSERT ON CONFLICT DO NOTHING.
			logger.Warn("COPY failed, falling back to bulk INSERT", zap.Error(err))
			return flushCardStatesFallback(ctx, database, columns, copyRows)
		}
		return nil
	})
}

// flushCardStatesFallback uses bulk INSERT when pgx COPY is unavailable.
// Chunks rows to stay under PostgreSQL's 65,535 bind parameter limit.
func flushCardStatesFallback(ctx context.Context, database *gorm.DB, _ []string, copyRows [][]interface{}) error {
	const cols = 10
	const maxRows = 65535 / cols // 6553 rows per chunk

	for start := 0; start < len(copyRows); start += maxRows {
		end := start + maxRows
		if end > len(copyRows) {
			end = len(copyRows)
		}
		chunk := copyRows[start:end]

		placeholders := make([]string, 0, len(chunk))
		args := make([]interface{}, 0, len(chunk)*cols)
		for i, row := range chunk {
			base := i * cols
			placeholders = append(placeholders, fmt.Sprintf(
				"($%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d)",
				base+1, base+2, base+3, base+4, base+5,
				base+6, base+7, base+8, base+9, base+10,
			))
			args = append(args, row...)
		}

		sql := fmt.Sprintf(`INSERT INTO card_execution_states
			(card_id, campaign_id, status, current_step, retry_count,
			 last_msg_id, last_smpp_id, last_error, transition_seq, updated_at)
			VALUES %s
			ON CONFLICT (card_id, campaign_id, transition_seq) DO NOTHING`,
			strings.Join(placeholders, ","))

		if err := database.WithContext(ctx).Exec(sql, args...).Error; err != nil {
			return err
		}
	}
	return nil
}

// applyDelta adjusts the delta for a normalized status column.
func applyDelta(d *statsDelta, status string, adj int64) {
	switch statusColumn(status) {
	case "pending":
		d.pending += adj
	case "in_progress":
		d.inProgress += adj
	case "completed":
		d.completed += adj
	case "failed":
		d.failed += adj
	case "skipped":
		d.skipped += adj
	}
}

// statusColumn normalizes card statuses to their counter column names.
func statusColumn(status string) string {
	switch status {
	case "pending", "activating":
		return "pending"
	case "in_progress", "awaiting_dlr", "awaiting_mo":
		return "in_progress"
	case "completed":
		return "completed"
	case "failed":
		return "failed"
	case "skipped":
		return "skipped"
	default:
		return ""
	}
}

// buildStatsUpdateSQL builds a dynamic UPDATE for campaign_stats with only non-zero deltas.
func buildStatsUpdateSQL(d *statsDelta, campaignID uuid.UUID) (string, []interface{}) {
	var sets []string
	var args []interface{}

	if d.pending != 0 {
		sets = append(sets, fmt.Sprintf("pending = pending + $%d", len(args)+1))
		args = append(args, d.pending)
	}
	if d.inProgress != 0 {
		sets = append(sets, fmt.Sprintf("in_progress = in_progress + $%d", len(args)+1))
		args = append(args, d.inProgress)
	}
	if d.completed != 0 {
		sets = append(sets, fmt.Sprintf("completed = completed + $%d", len(args)+1))
		args = append(args, d.completed)
	}
	if d.failed != 0 {
		sets = append(sets, fmt.Sprintf("failed = failed + $%d", len(args)+1))
		args = append(args, d.failed)
	}
	if d.skipped != 0 {
		sets = append(sets, fmt.Sprintf("skipped = skipped + $%d", len(args)+1))
		args = append(args, d.skipped)
	}

	if len(sets) == 0 {
		return "", nil
	}

	args = append(args, campaignID)
	sql := fmt.Sprintf("UPDATE campaign_stats SET %s WHERE campaign_id = $%d", strings.Join(sets, ", "), len(args))
	return sql, args
}
