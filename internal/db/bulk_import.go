package db

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
)

// BulkInsertResult holds the outcome of a bulk card insert via COPY.
type BulkInsertResult struct {
	Inserted int64
	Skipped  int64
	// InsertedIDs contains the UUIDs of cards that were actually inserted
	// (not skipped due to unique constraint conflicts).
	InsertedIDs []uuid.UUID
}

// BulkInsertCards uses PostgreSQL COPY protocol to efficiently insert cards.
// It copies data into a temp table, then inserts into the real cards table
// with ON CONFLICT DO NOTHING. This is 5-10x faster than individual INSERTs
// for large batches.
//
// The caller must provide a *sql.Conn to ensure all operations happen on the
// same underlying connection (required for temp table visibility).
func BulkInsertCards(ctx context.Context, conn *sql.Conn, cards []Card) (*BulkInsertResult, error) {
	if len(cards) == 0 {
		return &BulkInsertResult{}, nil
	}

	// Assign UUIDs to cards that don't have them.
	for i := range cards {
		if cards[i].ID == uuid.Nil {
			cards[i].ID = uuid.New()
		}
	}

	// Create temp table without constraints (no indexes = fast COPY).
	_, err := conn.ExecContext(ctx, `
		CREATE TEMP TABLE IF NOT EXISTS _card_import (
			id UUID,
			iccid TEXT,
			imsi TEXT,
			msisdn TEXT,
			profile_id UUID,
			enc_key BYTEA,
			auth_key BYTEA,
			kek BYTEA,
			status TEXT
		)
	`)
	if err != nil {
		return nil, fmt.Errorf("create temp table: %w", err)
	}

	// COPY into temp table using pgx's binary protocol.
	err = conn.Raw(func(driverConn any) error {
		pgxConn := driverConn.(*stdlib.Conn).Conn()
		_, copyErr := pgxConn.CopyFrom(ctx,
			pgx.Identifier{"_card_import"},
			[]string{"id", "iccid", "imsi", "msisdn", "profile_id", "enc_key", "auth_key", "kek", "status"},
			pgx.CopyFromSlice(len(cards), func(i int) ([]any, error) {
				c := cards[i]
				return []any{c.ID, c.ICCID, c.IMSI, c.MSISDN, c.ProfileID, c.EncKey, c.AuthKey, c.KEK, c.Status}, nil
			}),
		)
		return copyErr
	})
	if err != nil {
		// Clean up temp table on error.
		_, _ = conn.ExecContext(ctx, `TRUNCATE _card_import`)
		return nil, fmt.Errorf("copy to temp table: %w", err)
	}

	// INSERT from temp to real table, skipping conflicts.
	rows, err := conn.QueryContext(ctx, `
		INSERT INTO cards (id, iccid, imsi, msisdn, profile_id, enc_key, auth_key, kek, status, created_at)
		SELECT id, iccid, imsi, msisdn, profile_id, enc_key, auth_key, kek, status, now()
		FROM _card_import
		ON CONFLICT DO NOTHING
		RETURNING id
	`)
	if err != nil {
		_, _ = conn.ExecContext(ctx, `TRUNCATE _card_import`)
		return nil, fmt.Errorf("insert from temp: %w", err)
	}

	result := &BulkInsertResult{}
	for rows.Next() {
		var id uuid.UUID
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			_, _ = conn.ExecContext(ctx, `TRUNCATE _card_import`)
			return nil, fmt.Errorf("scan inserted id: %w", err)
		}
		result.InsertedIDs = append(result.InsertedIDs, id)
		result.Inserted++
	}
	if err := rows.Err(); err != nil {
		_, _ = conn.ExecContext(ctx, `TRUNCATE _card_import`)
		return nil, fmt.Errorf("rows error: %w", err)
	}
	rows.Close()

	result.Skipped = int64(len(cards)) - result.Inserted

	// Clean up for next batch.
	_, _ = conn.ExecContext(ctx, `TRUNCATE _card_import`)

	return result, nil
}
