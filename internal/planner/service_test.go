package planner

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/google/uuid"
	"go.uber.org/zap"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"

	"ota-platform/internal/db"
	kafkapkg "ota-platform/internal/kafka"
)

type plannerTestPublisher struct {
	events []kafkapkg.CardEvent
}

func (p *plannerTestPublisher) Publish(_ context.Context, _ string, message interface{}) error {
	event, ok := message.(kafkapkg.CardEvent)
	if !ok {
		return nil
	}
	p.events = append(p.events, event)
	return nil
}

func TestPublishClaimedShardsPublishesAndMarksShardPublished(t *testing.T) {
	database, err := gorm.Open(sqlite.Open("file::memory:?cache=shared"), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	if err := database.Exec(`
		CREATE TABLE campaigns (
			id TEXT PRIMARY KEY,
			name TEXT NOT NULL,
			status TEXT NOT NULL,
			campaign_type TEXT NOT NULL,
			max_retries INTEGER NOT NULL
		)
	`).Error; err != nil {
		t.Fatalf("create campaigns table: %v", err)
	}
	if err := database.Exec(`
		CREATE TABLE campaign_shards (
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
		)
	`).Error; err != nil {
		t.Fatalf("create campaign_shards table: %v", err)
	}

	campaignID := uuid.New()
	if err := database.Exec(
		`INSERT INTO campaigns (id, name, status, campaign_type, max_retries) VALUES (?, ?, ?, ?, ?)`,
		campaignID.String(), "c1", "running", "script", 3,
	).Error; err != nil {
		t.Fatalf("create campaign: %v", err)
	}

	items, err := json.Marshal([]kafkapkg.CardEvent{
		{Type: "card.activate", EventID: uuid.New().String(), CardID: uuid.New().String(), CampaignID: campaignID.String(), Step: 1},
		{Type: "card.activate", EventID: uuid.New().String(), CardID: uuid.New().String(), CampaignID: campaignID.String(), Step: 1},
	})
	if err != nil {
		t.Fatalf("marshal items: %v", err)
	}

	shardID := uuid.New()
	if err := database.Exec(
		`INSERT INTO campaign_shards (id, campaign_id, sequence, status, item_count, items) VALUES (?, ?, ?, ?, ?, ?)`,
		shardID.String(), campaignID.String(), 1, "pending", 2, items,
	).Error; err != nil {
		t.Fatalf("create shard: %v", err)
	}

	pub := &plannerTestPublisher{}
	svc := NewService(database, pub, zap.NewNop())
	svc.publishClaimedShards(context.Background(), []db.CampaignShard{{
		ID:         shardID,
		CampaignID: campaignID,
		Sequence:   1,
		Status:     "publishing",
		ItemCount:  2,
		Items:      items,
	}})

	if len(pub.events) != 2 {
		t.Fatalf("expected 2 published events, got %d", len(pub.events))
	}

	var updated struct {
		Status      string
		PublishedAt *string
	}
	if err := database.Raw(`SELECT status, published_at FROM campaign_shards WHERE id = ?`, shardID.String()).Scan(&updated).Error; err != nil {
		t.Fatalf("reload shard: %v", err)
	}
	if updated.Status != "published" {
		t.Fatalf("expected shard status published, got %s", updated.Status)
	}
	if updated.PublishedAt == nil {
		t.Fatal("expected published_at to be set")
	}
}
