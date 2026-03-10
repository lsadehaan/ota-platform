package main

import (
	"fmt"
	"os"
	"time"

	"go.uber.org/zap"

	"ota-platform/internal/bootstrap"
	"ota-platform/internal/db"
	"ota-platform/internal/scylla"
)

const cardBatchSize = 1000

func main() {
	logger := bootstrap.NewLogger()
	defer logger.Sync()

	ctx, cancel := bootstrap.SignalContext()
	defer cancel()

	// Connect to Postgres.
	gormDB := bootstrap.MustGormDB(logger)
	logger.Info("connected to Postgres")

	// Connect to ScyllaDB.
	scyllaClient := bootstrap.MustScylla(logger)
	defer scyllaClient.Close()
	logger.Info("connected to ScyllaDB")

	cardKeyStore := scylla.NewCardKeyStore(scyllaClient)
	counterStore := scylla.NewCounterStore(scyllaClient)

	// ── Phase 1: Migrate card keys ──────────────────────────────────────
	logger.Info("starting card key migration")
	start := time.Now()
	var totalCards int64

	for offset := 0; ; offset += cardBatchSize {
		if err := ctx.Err(); err != nil {
			logger.Warn("migration interrupted", zap.Error(err))
			os.Exit(1)
		}

		var cards []db.Card
		result := gormDB.WithContext(ctx).
			Select("id, enc_key, auth_key, kek, profile_id, msisdn").
			Order("id").
			Offset(offset).
			Limit(cardBatchSize).
			Find(&cards)
		if result.Error != nil {
			logger.Fatal("failed to read cards from Postgres",
				zap.Int("offset", offset),
				zap.Error(result.Error),
			)
		}
		if len(cards) == 0 {
			break
		}

		records := make([]scylla.CardKeyRecord, len(cards))
		for i, c := range cards {
			records[i] = scylla.CardKeyRecord{
				CardID:    c.ID.String(),
				EncKey:    c.EncKey,
				AuthKey:   c.AuthKey,
				KEK:       c.KEK,
				ProfileID: c.ProfileID.String(),
				MSISDN:    c.MSISDN,
			}
		}

		if err := cardKeyStore.WriteCardKeysBatch(ctx, records); err != nil {
			logger.Fatal("failed to write card keys batch to ScyllaDB",
				zap.Int("offset", offset),
				zap.Error(err),
			)
		}

		totalCards += int64(len(cards))
		if totalCards%10000 == 0 {
			logger.Info("card key migration progress",
				zap.Int64("cards_migrated", totalCards),
				zap.Duration("elapsed", time.Since(start)),
			)
		}
	}

	logger.Info("card key migration complete",
		zap.Int64("total_cards", totalCards),
		zap.Duration("elapsed", time.Since(start)),
	)

	// ── Phase 2: Migrate counters ───────────────────────────────────────
	logger.Info("starting counter migration")
	counterStart := time.Now()
	var totalCounters int64

	var counters []db.CardCounter
	result := gormDB.WithContext(ctx).Find(&counters)
	if result.Error != nil {
		logger.Fatal("failed to read card counters from Postgres", zap.Error(result.Error))
	}

	for _, c := range counters {
		if err := ctx.Err(); err != nil {
			logger.Warn("migration interrupted during counter seeding", zap.Error(err))
			os.Exit(1)
		}

		if err := counterStore.SeedCounter(ctx, c.CardID.String(), c.ApplicationID.String(), c.CounterValue); err != nil {
			logger.Fatal("failed to seed counter in ScyllaDB",
				zap.String("card_id", c.CardID.String()),
				zap.String("application_id", c.ApplicationID.String()),
				zap.Error(err),
			)
		}

		totalCounters++
		if totalCounters%10000 == 0 {
			logger.Info("counter migration progress",
				zap.Int64("counters_migrated", totalCounters),
				zap.Duration("elapsed", time.Since(counterStart)),
			)
		}
	}

	logger.Info("counter migration complete",
		zap.Int64("total_counters", totalCounters),
		zap.Duration("elapsed", time.Since(counterStart)),
	)

	// ── Summary ─────────────────────────────────────────────────────────
	elapsed := time.Since(start)
	logger.Info("migration finished",
		zap.Int64("cards_migrated", totalCards),
		zap.Int64("counters_migrated", totalCounters),
		zap.Duration("total_elapsed", elapsed),
	)
	fmt.Printf("migration complete: %d cards, %d counters migrated in %s\n", totalCards, totalCounters, elapsed.Round(time.Millisecond))
}
