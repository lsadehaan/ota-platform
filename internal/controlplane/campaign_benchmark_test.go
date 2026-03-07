package controlplane

import (
	"context"
	"fmt"
	"testing"

	"github.com/google/uuid"
	"go.uber.org/zap"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

func BenchmarkStartCampaignShardPlanning(b *testing.B) {
	const targetCount = 5000

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		database := openControlplaneBenchmarkDB(b)
		profileID := uuid.New()
		appID := uuid.New()
		campaignID := uuid.New()
		commandID := uuid.New()
		if err := database.Exec(`INSERT INTO profiles (id, name, max_concat_sms, buffer_size) VALUES (?, ?, ?, ?)`, profileID.String(), "p1", 7, 180).Error; err != nil {
			b.Fatalf("create profile: %v", err)
		}
		if err := database.Exec(`INSERT INTO applications (id, profile_id, name, tar, kic_algo, kic_mode, kic_keyset_id, kid_algo, kid_mode, kid_keyset_id, certification_mode, ciphered, counter_mode, por_mode, por_protocol, por_ciphered, por_cert_mode) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			appID.String(), profileID.String(), "app1", []byte{0x01, 0x02, 0x03}, "DES", "TRIPLE_DES_CBC_2_KEYS", 1, "DES", "TRIPLE_DES_CBC_2_KEYS", 1, "CC", true, "COUNTER_REPLAY_OR_CHECK", "REPLY_ALWAYS", "SMS_SUBMIT", false, "NO_SECURITY").Error; err != nil {
			b.Fatalf("create app: %v", err)
		}
		if err := database.Exec(`INSERT INTO campaigns (id, name, status, campaign_type, max_retries) VALUES (?, ?, ?, ?, ?)`,
			campaignID.String(), "bench", "pending", "script", 3).Error; err != nil {
			b.Fatalf("create campaign: %v", err)
		}
		if err := database.Exec(`INSERT INTO campaign_commands (id, campaign_id, sequence, application_id, script, expect_response) VALUES (?, ?, ?, ?, ?, ?)`,
			commandID.String(), campaignID.String(), 1, appID.String(), []byte{0xA0}, true).Error; err != nil {
			b.Fatalf("create command: %v", err)
		}
		targets := make([]map[string]interface{}, 0, targetCount)
		for j := 0; j < targetCount; j++ {
			targets = append(targets, map[string]interface{}{
				"campaign_id": campaignID.String(),
				"card_id":     uuid.NewString(),
			})
		}
		if err := database.Table("campaign_targets").CreateInBatches(targets, 1000).Error; err != nil {
			b.Fatalf("create targets: %v", err)
		}
		svc := NewCampaignService(database, campaignTestStore{}, &trackingCampaignQueryStore{}, nil, zap.NewNop())
		b.StartTimer()

		if err := svc.StartCampaign(context.Background(), campaignID); err != nil {
			b.Fatalf("start campaign: %v", err)
		}
	}
}

func openControlplaneBenchmarkDB(tb testing.TB) *gorm.DB {
	tb.Helper()

	dsn := fmt.Sprintf("file:%s?mode=memory&cache=shared", uuid.NewString())
	database, err := gorm.Open(sqlite.Open(dsn), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	if err != nil {
		tb.Fatalf("open sqlite: %v", err)
	}
	for _, stmt := range []string{
		`CREATE TABLE profiles (id TEXT PRIMARY KEY, name TEXT NOT NULL, max_concat_sms INTEGER NOT NULL, buffer_size INTEGER NOT NULL, pid INTEGER NOT NULL DEFAULT 0, dcs INTEGER NOT NULL DEFAULT 0, security_bytes_type TEXT NOT NULL DEFAULT 'WITH_LENGTHS_AND_UDHL', created_at DATETIME DEFAULT CURRENT_TIMESTAMP)`,
		`CREATE TABLE applications (id TEXT PRIMARY KEY, profile_id TEXT NOT NULL, name TEXT NOT NULL, tar BLOB NOT NULL, kic_algo TEXT NOT NULL, kic_mode TEXT NOT NULL, kic_keyset_id INTEGER NOT NULL, kid_algo TEXT NOT NULL, kid_mode TEXT NOT NULL, kid_keyset_id INTEGER NOT NULL, certification_mode TEXT NOT NULL, ciphered BOOLEAN NOT NULL, counter_mode TEXT NOT NULL, por_mode TEXT NOT NULL, por_protocol TEXT NOT NULL, por_ciphered BOOLEAN NOT NULL, por_cert_mode TEXT NOT NULL)`,
		`CREATE TABLE campaigns (id TEXT PRIMARY KEY, name TEXT NOT NULL, status TEXT NOT NULL, campaign_type TEXT NOT NULL, max_retries INTEGER NOT NULL, created_at DATETIME DEFAULT CURRENT_TIMESTAMP, updated_at DATETIME DEFAULT CURRENT_TIMESTAMP, started_at DATETIME, completed_at DATETIME)`,
		`CREATE TABLE campaign_commands (id TEXT PRIMARY KEY, campaign_id TEXT NOT NULL, sequence INTEGER NOT NULL, application_id TEXT NOT NULL, script BLOB NOT NULL, expect_response BOOLEAN NOT NULL)`,
		`CREATE TABLE campaign_targets (campaign_id TEXT NOT NULL, card_id TEXT NOT NULL, created_at DATETIME DEFAULT CURRENT_TIMESTAMP, PRIMARY KEY (campaign_id, card_id))`,
		`CREATE TABLE campaign_shards (id TEXT PRIMARY KEY, campaign_id TEXT NOT NULL, sequence INTEGER NOT NULL, status TEXT NOT NULL, item_count INTEGER NOT NULL, items BLOB NOT NULL, claimed_at DATETIME, published_at DATETIME, created_at DATETIME DEFAULT CURRENT_TIMESTAMP, updated_at DATETIME DEFAULT CURRENT_TIMESTAMP)`,
	} {
		if err := database.Exec(stmt).Error; err != nil {
			tb.Fatalf("exec schema %q: %v", stmt, err)
		}
	}
	return database
}
