package pgstore

import (
	"context"
	"fmt"
	"time"

	"strings"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
	"go.uber.org/zap"
	"gorm.io/gorm"

	"ota-platform/internal/pipeline"
)

// MessageWriterConfig holds tuning parameters for the batched message writer.
type MessageWriterConfig struct {
	MaxBatch      int
	FlushInterval time.Duration
}

// DefaultMessageWriterConfig returns a MessageWriterConfig populated from
// environment variables with sensible defaults.
func DefaultMessageWriterConfig() MessageWriterConfig {
	return MessageWriterConfig{
		MaxBatch:      envIntPgstore("ENGINE_MSG_WRITER_BATCH", 10000),
		FlushInterval: time.Duration(envIntPgstore("ENGINE_MSG_WRITER_FLUSH_MS", 2000)) * time.Millisecond,
	}
}

// RunMessageWriter consumes MessageLogAction events from the channel and
// batch-writes them into the message_history table.
func RunMessageWriter(ctx context.Context, ch <-chan pipeline.MessageLogAction, database *gorm.DB, cfg MessageWriterConfig, logger *zap.Logger) {
	meter := otel.Meter("ota-engine")
	rowsWritten, _ := meter.Int64Counter("ota.msg_writer.rows_written",
		metric.WithDescription("Total message_history rows written"))
	rowsUpdated, _ := meter.Int64Counter("ota.msg_writer.rows_updated",
		metric.WithDescription("Total message_history rows updated"))
	flushDuration, _ := meter.Float64Histogram("ota.msg_writer.flush_duration_ms",
		metric.WithDescription("Duration of message writer flush in ms"))
	batchSizeHist, _ := meter.Float64Histogram("ota.msg_writer.batch_size",
		metric.WithDescription("Number of rows per flush"))

	// Separate creates and updates for efficient batching.
	creates := make(map[string]*msgWriterRow, cfg.MaxBatch)
	updates := make(map[string]map[string]interface{}, cfg.MaxBatch)
	ticker := time.NewTicker(cfg.FlushInterval)
	defer ticker.Stop()

	flush := func() {
		if len(creates) > 0 {
			n := len(creates)
			start := time.Now()
			if err := flushMessageCreates(ctx, database, creates, logger); err != nil {
				logger.Error("message create batch failed", zap.Error(err), zap.Int("batch_size", n))
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
			for k := range creates {
				delete(creates, k)
			}
		}
		if len(updates) > 0 {
			n := len(updates)
			start := time.Now()
			if err := flushMessageUpdates(ctx, database, updates, logger); err != nil {
				logger.Error("message update batch failed", zap.Error(err), zap.Int("batch_size", n))
			} else {
				elapsed := float64(time.Since(start).Microseconds()) / 1000.0
				if flushDuration != nil {
					flushDuration.Record(ctx, elapsed)
				}
				if rowsUpdated != nil {
					rowsUpdated.Add(ctx, int64(n))
				}
			}
			for k := range updates {
				delete(updates, k)
			}
		}
	}

	for {
		select {
		case action, ok := <-ch:
			if !ok {
				flush()
				return
			}
			switch action.Action {
			case "create":
				if action.Log == nil {
					continue
				}
				row, err := convertLogEntry(action.Log)
				if err != nil {
					logger.Warn("skip message create: conversion error", zap.Error(err))
					continue
				}
				// If an update is already pending for this ID, merge it into the create.
				if pending, ok := updates[action.Log.ID]; ok {
					applyUpdatesToRow(row, pending)
					delete(updates, action.Log.ID)
				}
				creates[action.Log.ID] = row
				if len(creates) >= cfg.MaxBatch {
					flush()
				}
			case "update":
				if action.ID == "" || len(action.Updates) == 0 {
					continue
				}
				// If create is pending, merge update into create.
				if create, ok := creates[action.ID]; ok {
					applyUpdatesToRow(create, action.Updates)
				} else {
					existing, ok := updates[action.ID]
					if !ok {
						existing = make(map[string]interface{})
					}
					for k, v := range action.Updates {
						existing[k] = v
					}
					updates[action.ID] = existing
				}
				if len(updates) >= cfg.MaxBatch {
					flush()
				}
			}
		case <-ticker.C:
			flush()
		case <-ctx.Done():
			flush()
			return
		}
	}
}

// msgWriterRow holds the parsed values ready for INSERT.
type msgWriterRow struct {
	ID             uuid.UUID
	CampaignID     *uuid.UUID
	CardID         uuid.UUID
	Direction      string
	Status         string
	RawPayload     []byte
	SecuredPayload []byte
	CounterHex     *string
	SMPPMessageID  *string
	CreatedAt      time.Time
	UpdatedAt      time.Time
}

// convertLogEntry converts a Kafka MessageLogEntry to a messageHistoryRow.
func convertLogEntry(entry *pipeline.MessageLogEntry) (*msgWriterRow, error) {
	id, err := uuid.Parse(entry.ID)
	if err != nil {
		return nil, fmt.Errorf("parse id %s: %w", entry.ID, err)
	}
	cardID, err := uuid.Parse(entry.CardID)
	if err != nil {
		return nil, fmt.Errorf("parse card_id %s: %w", entry.CardID, err)
	}

	row := &msgWriterRow{
		ID:        id,
		CardID:    cardID,
		Direction: entry.Direction,
		Status:    entry.Status,
	}

	if entry.CampaignID != "" {
		cid, err := uuid.Parse(entry.CampaignID)
		if err != nil {
			return nil, fmt.Errorf("parse campaign_id %s: %w", entry.CampaignID, err)
		}
		row.CampaignID = &cid
	}

	if !entry.CreatedAt.IsZero() {
		row.CreatedAt = entry.CreatedAt.UTC()
	} else {
		row.CreatedAt = time.Now().UTC()
	}
	if !entry.UpdatedAt.IsZero() {
		row.UpdatedAt = entry.UpdatedAt.UTC()
	} else {
		row.UpdatedAt = row.CreatedAt
	}

	row.RawPayload = entry.RawPayload
	row.SecuredPayload = entry.SecuredPayload
	if entry.CounterHex != "" {
		row.CounterHex = &entry.CounterHex
	}
	if entry.SMPPMessageID != "" {
		row.SMPPMessageID = &entry.SMPPMessageID
	}

	return row, nil
}

// flushMessageCreates uses COPY FROM STDIN (pgx CopyFrom) for maximum
// throughput into the message_history TimescaleDB hypertable.
// COPY is 10-50x faster than bulk INSERT for large batches.
func flushMessageCreates(ctx context.Context, database *gorm.DB, rows map[string]*msgWriterRow, logger *zap.Logger) error {
	if len(rows) == 0 {
		return nil
	}

	columns := []string{
		"id", "campaign_id", "card_id", "direction", "status",
		"raw_payload", "secured_payload", "counter_hex", "smpp_message_id",
		"created_at", "updated_at",
	}

	copyRows := make([][]interface{}, 0, len(rows))
	for _, row := range rows {
		var counterHex, smppMsgID interface{}
		if row.CounterHex != nil {
			counterHex = *row.CounterHex
		}
		if row.SMPPMessageID != nil {
			smppMsgID = *row.SMPPMessageID
		}
		copyRows = append(copyRows, []interface{}{
			row.ID, row.CampaignID, row.CardID, row.Direction, row.Status,
			row.RawPayload, row.SecuredPayload, counterHex, smppMsgID,
			row.CreatedAt, row.UpdatedAt,
		})
	}

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
		// GORM's postgres driver uses pgx/v5/stdlib — unwrap to *pgx.Conn.
		stdlibConn, ok := driverConn.(*stdlib.Conn)
		if !ok {
			return flushMessageCreatesFallback(ctx, database, rows)
		}
		pgxConn := stdlibConn.Conn()
		_, err := pgxConn.CopyFrom(ctx,
			pgx.Identifier{"message_history"},
			columns,
			pgx.CopyFromRows(copyRows),
		)
		if err != nil {
			return flushMessageCreatesFallback(ctx, database, rows)
		}
		return nil
	})
}

// flushMessageCreatesFallback uses bulk INSERT when pgx COPY is unavailable.
// Chunks rows to stay under PostgreSQL's 65,535 bind parameter limit.
func flushMessageCreatesFallback(ctx context.Context, database *gorm.DB, rows map[string]*msgWriterRow) error {
	const cols = 11
	const maxRows = 65535 / cols // 5957 rows per chunk

	// Collect into a slice for chunking.
	allRows := make([]*msgWriterRow, 0, len(rows))
	for _, row := range rows {
		allRows = append(allRows, row)
	}

	for start := 0; start < len(allRows); start += maxRows {
		end := start + maxRows
		if end > len(allRows) {
			end = len(allRows)
		}
		chunk := allRows[start:end]

		placeholders := make([]string, 0, len(chunk))
		args := make([]interface{}, 0, len(chunk)*cols)
		for i, row := range chunk {
			base := i * cols
			placeholders = append(placeholders, fmt.Sprintf(
				"($%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d)",
				base+1, base+2, base+3, base+4, base+5,
				base+6, base+7, base+8, base+9, base+10, base+11,
			))
			args = append(args,
				row.ID, row.CampaignID, row.CardID, row.Direction, row.Status,
				row.RawPayload, row.SecuredPayload, row.CounterHex, row.SMPPMessageID,
				row.CreatedAt, row.UpdatedAt,
			)
		}

		sql := fmt.Sprintf(`INSERT INTO message_history
			(id, campaign_id, card_id, direction, status,
			 raw_payload, secured_payload, counter_hex, smpp_message_id,
			 created_at, updated_at)
			VALUES %s
			ON CONFLICT (id, created_at) DO NOTHING`, strings.Join(placeholders, ","))

		if err := database.WithContext(ctx).Exec(sql, args...).Error; err != nil {
			return err
		}
	}
	return nil
}

// flushMessageUpdates applies batched updates to message_history rows.
// Updates are executed individually (not in a single transaction) because
// TimescaleDB hypertable updates can fail for individual rows (e.g., row not
// yet inserted by another writer), and we don't want one failure to abort all.
func flushMessageUpdates(ctx context.Context, database *gorm.DB, updates map[string]map[string]interface{}, logger *zap.Logger) error {
	if len(updates) == 0 {
		return nil
	}

	// Allowed update columns (prevent arbitrary column writes).
	allowed := map[string]bool{
		"status": true, "smpp_message_id": true, "dlr_status": true,
		"counter_hex": true, "por_status_code": true, "por_data": true,
		"error_code": true, "updated_at": true,
	}

	for idStr, fields := range updates {
		id, err := uuid.Parse(idStr)
		if err != nil {
			logger.Warn("skip message update: bad id", zap.String("id", idStr))
			continue
		}

		filtered := make(map[string]interface{})
		for k, v := range fields {
			if allowed[k] {
				filtered[k] = v
			}
		}
		if len(filtered) == 0 {
			continue
		}
		if _, ok := filtered["updated_at"]; !ok {
			filtered["updated_at"] = time.Now().UTC()
		}

		if err := database.WithContext(ctx).
			Table("message_history").
			Where("id = ?", id).
			Updates(filtered).Error; err != nil {
			logger.Warn("message_history update failed", zap.String("id", idStr), zap.Error(err))
		}
	}
	return nil
}

// applyUpdatesToRow merges update fields into a pending create row.
func applyUpdatesToRow(row *msgWriterRow, updates map[string]interface{}) {
	if v, ok := updates["status"].(string); ok {
		row.Status = v
	}
	if v, ok := updates["smpp_message_id"].(string); ok {
		row.SMPPMessageID = &v
	}
	if v, ok := updates["counter_hex"].(string); ok {
		row.CounterHex = &v
	}
}
