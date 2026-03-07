package planner

import (
	"context"
	"encoding/json"
	"os"
	"testing"

	"github.com/google/uuid"
	"go.uber.org/zap"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"

	"ota-platform/internal/db"
	kafkapkg "ota-platform/internal/kafka"
)

type benchmarkPublisher struct {
	count int
}

func (p *benchmarkPublisher) Publish(_ context.Context, _ string, _ interface{}) error {
	p.count++
	return nil
}

func BenchmarkPublishClaimedShards(b *testing.B) {
	const (
		shardCount     = 32
		eventsPerShard = 500
	)

	items := make([][]byte, shardCount)
	for i := 0; i < shardCount; i++ {
		events := make([]kafkapkg.CardEvent, 0, eventsPerShard)
		for j := 0; j < eventsPerShard; j++ {
			events = append(events, kafkapkg.CardEvent{
				Type:       "card.activate",
				EventID:    uuid.NewString(),
				CardID:     uuid.NewString(),
				CampaignID: uuid.NewString(),
				Step:       1,
			})
		}
		payload, err := json.Marshal(events)
		if err != nil {
			b.Fatalf("marshal shard events: %v", err)
		}
		items[i] = payload
	}

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		database := openPlannerBenchmarkDB(b)
		campaignID := uuid.New()
		if err := database.Exec(
			`INSERT INTO campaigns (id, name, status, campaign_type, max_retries) VALUES (?, ?, ?, ?, ?)`,
			campaignID.String(), "bench", "running", "script", 3,
		).Error; err != nil {
			b.Fatalf("create campaign: %v", err)
		}
		shards := make([]db.CampaignShard, 0, shardCount)
		for shardIdx := 0; shardIdx < shardCount; shardIdx++ {
			shards = append(shards, db.CampaignShard{
				ID:         uuid.New(),
				CampaignID: campaignID,
				Sequence:   shardIdx + 1,
				Status:     "publishing",
				ItemCount:  eventsPerShard,
				Items:      items[shardIdx],
			})
		}
		if err := database.CreateInBatches(&shards, 8).Error; err != nil {
			b.Fatalf("create shards: %v", err)
		}
		publisher := &benchmarkPublisher{}
		svc := NewService(database, publisher, zap.NewNop())
		b.StartTimer()

		svc.publishClaimedShards(context.Background(), shards)

		b.StopTimer()
		expected := shardCount * eventsPerShard
		if publisher.count != expected {
			b.Fatalf("expected %d publishes, got %d", expected, publisher.count)
		}
	}
}

func openPlannerBenchmarkDB(tb testing.TB) *gorm.DB {
	tb.Helper()

	tmp, err := os.CreateTemp("", "planner-bench-*.sqlite")
	if err != nil {
		tb.Fatalf("create temp sqlite file: %v", err)
	}
	path := tmp.Name()
	if err := tmp.Close(); err != nil {
		tb.Fatalf("close temp sqlite file: %v", err)
	}
	tb.Cleanup(func() { _ = os.Remove(path) })

	database, err := gorm.Open(sqlite.Open(path), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	if err != nil {
		tb.Fatalf("open sqlite: %v", err)
	}
	for _, stmt := range []string{
		`CREATE TABLE campaigns (
			id TEXT PRIMARY KEY,
			name TEXT NOT NULL,
			status TEXT NOT NULL,
			campaign_type TEXT NOT NULL,
			max_retries INTEGER NOT NULL
		)`,
		`CREATE TABLE campaign_shards (
			id TEXT PRIMARY KEY,
			campaign_id TEXT NOT NULL,
			sequence INTEGER NOT NULL,
			status TEXT NOT NULL,
			item_count INTEGER NOT NULL,
			items BLOB NOT NULL,
			claimed_at DATETIME,
			published_at DATETIME,
			created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
			updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
		)`,
	} {
		if err := database.Exec(stmt).Error; err != nil {
			tb.Fatalf("exec schema %q: %v", stmt, err)
		}
	}
	return database
}
