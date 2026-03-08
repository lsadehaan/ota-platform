package controlplane

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/google/uuid"
	"go.uber.org/zap"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
	kafkapkg "ota-platform/internal/kafka"
	redispkg "ota-platform/internal/redis"
	scyllastore "ota-platform/internal/scylla"
)

type campaignTestStore struct{}

func (campaignTestStore) CacheCampaignCommands(context.Context, string, []redispkg.CampaignCommandCache) error {
	return nil
}
func (campaignTestStore) SetCampaignStatus(context.Context, string, string) error { return nil }

type campaignTestQueryStore struct{}

func (campaignTestQueryStore) GetCardState(context.Context, uuid.UUID) (*scyllastore.CardStateRecord, error) {
	return nil, nil
}
func (campaignTestQueryStore) CampaignStats(context.Context, uuid.UUID) (scyllastore.CampaignStats, error) {
	return scyllastore.CampaignStats{}, nil
}
func (campaignTestQueryStore) CampaignStatsBatch(context.Context, []uuid.UUID) (map[uuid.UUID]scyllastore.CampaignStats, error) {
	return map[uuid.UUID]scyllastore.CampaignStats{}, nil
}
func (campaignTestQueryStore) ListCampaignCards(context.Context, uuid.UUID, string) ([]scyllastore.CampaignCardView, error) {
	return nil, nil
}
func (campaignTestQueryStore) ListFailedCardIDs(context.Context, uuid.UUID) ([]uuid.UUID, error) {
	return nil, nil
}
func (campaignTestQueryStore) ResetFailedCards(context.Context, uuid.UUID, []uuid.UUID, int, time.Time) error {
	return nil
}
func (campaignTestQueryStore) InitializeCampaign(context.Context, uuid.UUID, []uuid.UUID, int, time.Time) error {
	return nil
}
func (campaignTestQueryStore) AbortCampaign(context.Context, uuid.UUID, time.Time) (int, error) {
	return 0, nil
}
func (campaignTestQueryStore) GetMessage(context.Context, uuid.UUID) (*scyllastore.MessageRecord, error) {
	return nil, nil
}
func (campaignTestQueryStore) ListCardMessages(context.Context, uuid.UUID, int) ([]scyllastore.MessageRecord, error) {
	return nil, nil
}
func (campaignTestQueryStore) ListMessages(context.Context, scyllastore.MessageFilter) ([]scyllastore.MessageRecord, int64, error) {
	return nil, 0, nil
}
func (campaignTestQueryStore) RecentActivity(context.Context, int) ([]scyllastore.MessageRecord, error) {
	return nil, nil
}
func (campaignTestQueryStore) MessageMetrics(context.Context, time.Time) (scyllastore.MessageMetrics, error) {
	return scyllastore.MessageMetrics{}, nil
}
func (campaignTestQueryStore) CampaignMessageMetrics(context.Context, uuid.UUID, time.Time) (scyllastore.MessageMetrics, error) {
	return scyllastore.MessageMetrics{}, nil
}
func (campaignTestQueryStore) Throughput(context.Context, time.Time) ([]scyllastore.ThroughputPoint, error) {
	return nil, nil
}
func (campaignTestQueryStore) CampaignThroughput(context.Context, uuid.UUID, time.Time) ([]scyllastore.ThroughputPoint, error) {
	return nil, nil
}
func (campaignTestQueryStore) MinuteThroughput(context.Context) ([]scyllastore.ThroughputPoint, error) {
	return nil, nil
}
func (campaignTestQueryStore) ErrorSummary(context.Context, time.Time) ([]scyllastore.ErrorCount, []scyllastore.ErrorCount, []scyllastore.ErrorCount, error) {
	return nil, nil, nil, nil
}
func (campaignTestQueryStore) CampaignErrorSummary(context.Context, uuid.UUID, time.Time) ([]scyllastore.ErrorCount, []scyllastore.ErrorCount, []scyllastore.ErrorCount, error) {
	return nil, nil, nil, nil
}

type trackingCampaignQueryStore struct {
	initializeCalls int
	initializeCards int
}

func (s *trackingCampaignQueryStore) GetCardState(context.Context, uuid.UUID) (*scyllastore.CardStateRecord, error) {
	return nil, nil
}
func (s *trackingCampaignQueryStore) CampaignStats(context.Context, uuid.UUID) (scyllastore.CampaignStats, error) {
	return scyllastore.CampaignStats{}, nil
}
func (s *trackingCampaignQueryStore) CampaignStatsBatch(context.Context, []uuid.UUID) (map[uuid.UUID]scyllastore.CampaignStats, error) {
	return map[uuid.UUID]scyllastore.CampaignStats{}, nil
}
func (s *trackingCampaignQueryStore) ListCampaignCards(context.Context, uuid.UUID, string) ([]scyllastore.CampaignCardView, error) {
	return nil, nil
}
func (s *trackingCampaignQueryStore) ListFailedCardIDs(context.Context, uuid.UUID) ([]uuid.UUID, error) {
	return nil, nil
}
func (s *trackingCampaignQueryStore) ResetFailedCards(context.Context, uuid.UUID, []uuid.UUID, int, time.Time) error {
	return nil
}
func (s *trackingCampaignQueryStore) InitializeCampaign(_ context.Context, _ uuid.UUID, cardIDs []uuid.UUID, _ int, _ time.Time) error {
	s.initializeCalls++
	s.initializeCards += len(cardIDs)
	return nil
}
func (s *trackingCampaignQueryStore) AbortCampaign(context.Context, uuid.UUID, time.Time) (int, error) {
	return 0, nil
}
func (s *trackingCampaignQueryStore) GetMessage(context.Context, uuid.UUID) (*scyllastore.MessageRecord, error) {
	return nil, nil
}
func (s *trackingCampaignQueryStore) ListCardMessages(context.Context, uuid.UUID, int) ([]scyllastore.MessageRecord, error) {
	return nil, nil
}
func (s *trackingCampaignQueryStore) ListMessages(context.Context, scyllastore.MessageFilter) ([]scyllastore.MessageRecord, int64, error) {
	return nil, 0, nil
}
func (s *trackingCampaignQueryStore) RecentActivity(context.Context, int) ([]scyllastore.MessageRecord, error) {
	return nil, nil
}
func (s *trackingCampaignQueryStore) MessageMetrics(context.Context, time.Time) (scyllastore.MessageMetrics, error) {
	return scyllastore.MessageMetrics{}, nil
}
func (s *trackingCampaignQueryStore) CampaignMessageMetrics(context.Context, uuid.UUID, time.Time) (scyllastore.MessageMetrics, error) {
	return scyllastore.MessageMetrics{}, nil
}
func (s *trackingCampaignQueryStore) Throughput(context.Context, time.Time) ([]scyllastore.ThroughputPoint, error) {
	return nil, nil
}
func (s *trackingCampaignQueryStore) CampaignThroughput(context.Context, uuid.UUID, time.Time) ([]scyllastore.ThroughputPoint, error) {
	return nil, nil
}
func (s *trackingCampaignQueryStore) MinuteThroughput(context.Context) ([]scyllastore.ThroughputPoint, error) {
	return nil, nil
}
func (s *trackingCampaignQueryStore) ErrorSummary(context.Context, time.Time) ([]scyllastore.ErrorCount, []scyllastore.ErrorCount, []scyllastore.ErrorCount, error) {
	return nil, nil, nil, nil
}
func (s *trackingCampaignQueryStore) CampaignErrorSummary(context.Context, uuid.UUID, time.Time) ([]scyllastore.ErrorCount, []scyllastore.ErrorCount, []scyllastore.ErrorCount, error) {
	return nil, nil, nil, nil
}

func testLogger() *zap.Logger { return zap.NewNop() }

func openCampaignTestDB(t *testing.T) *gorm.DB {
	t.Helper()

	dsn := "file:" + uuid.NewString() + "?mode=memory&cache=shared"
	database, err := gorm.Open(sqlite.Open(dsn), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	return database
}

func TestStartCampaignCreatesPlannerShards(t *testing.T) {
	database := openCampaignTestDB(t)

	for _, stmt := range []string{
		`CREATE TABLE profiles (id TEXT PRIMARY KEY, name TEXT NOT NULL, max_concat_sms INTEGER NOT NULL, buffer_size INTEGER NOT NULL, pid INTEGER NOT NULL DEFAULT 0, dcs INTEGER NOT NULL DEFAULT 0, security_bytes_type TEXT NOT NULL DEFAULT 'WITH_LENGTHS_AND_UDHL', created_at DATETIME DEFAULT CURRENT_TIMESTAMP)`,
		`CREATE TABLE applications (id TEXT PRIMARY KEY, profile_id TEXT NOT NULL, name TEXT NOT NULL, tar BLOB NOT NULL, kic_algo TEXT NOT NULL, kic_mode TEXT NOT NULL, kic_keyset_id INTEGER NOT NULL, kid_algo TEXT NOT NULL, kid_mode TEXT NOT NULL, kid_keyset_id INTEGER NOT NULL, certification_mode TEXT NOT NULL, ciphered BOOLEAN NOT NULL, counter_mode TEXT NOT NULL, por_mode TEXT NOT NULL, por_protocol TEXT NOT NULL, por_ciphered BOOLEAN NOT NULL, por_cert_mode TEXT NOT NULL)`,
		`CREATE TABLE cards (id TEXT PRIMARY KEY, iccid TEXT NOT NULL, imsi TEXT NOT NULL, msisdn TEXT NOT NULL, profile_id TEXT NOT NULL, enc_key BLOB NOT NULL, auth_key BLOB NOT NULL, kek BLOB, status TEXT NOT NULL, created_at DATETIME DEFAULT CURRENT_TIMESTAMP)`,
		`CREATE TABLE campaigns (id TEXT PRIMARY KEY, name TEXT NOT NULL, status TEXT NOT NULL, campaign_type TEXT NOT NULL, max_retries INTEGER NOT NULL, created_at DATETIME DEFAULT CURRENT_TIMESTAMP, updated_at DATETIME DEFAULT CURRENT_TIMESTAMP, started_at DATETIME, completed_at DATETIME)`,
		`CREATE TABLE campaign_commands (id TEXT PRIMARY KEY, campaign_id TEXT NOT NULL, sequence INTEGER NOT NULL, application_id TEXT NOT NULL, script BLOB NOT NULL, expect_response BOOLEAN NOT NULL)`,
		`CREATE TABLE campaign_targets (campaign_id TEXT NOT NULL, card_id TEXT NOT NULL, created_at DATETIME DEFAULT CURRENT_TIMESTAMP, PRIMARY KEY (campaign_id, card_id))`,
		`CREATE TABLE campaign_shards (id TEXT PRIMARY KEY, campaign_id TEXT NOT NULL, sequence INTEGER NOT NULL, status TEXT NOT NULL, item_count INTEGER NOT NULL, items BLOB NOT NULL, claimed_at DATETIME, published_at DATETIME, created_at DATETIME DEFAULT CURRENT_TIMESTAMP, updated_at DATETIME DEFAULT CURRENT_TIMESTAMP)`,
	} {
		if err := database.Exec(stmt).Error; err != nil {
			t.Fatalf("exec schema %q: %v", stmt, err)
		}
	}

	profileID := uuid.New()
	appID := uuid.New()
	card1ID := uuid.New()
	card2ID := uuid.New()
	campaignID := uuid.New()
	commandID := uuid.New()

	if err := database.Exec(`INSERT INTO profiles (id, name, max_concat_sms, buffer_size) VALUES (?, ?, ?, ?)`, profileID.String(), "p1", 7, 180).Error; err != nil {
		t.Fatalf("create profile: %v", err)
	}
	if err := database.Exec(`INSERT INTO applications (id, profile_id, name, tar, kic_algo, kic_mode, kic_keyset_id, kid_algo, kid_mode, kid_keyset_id, certification_mode, ciphered, counter_mode, por_mode, por_protocol, por_ciphered, por_cert_mode) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		appID.String(), profileID.String(), "app1", []byte{0x01, 0x02, 0x03}, "DES", "TRIPLE_DES_CBC_2_KEYS", 1, "DES", "TRIPLE_DES_CBC_2_KEYS", 1, "CC", true, "COUNTER_REPLAY_OR_CHECK", "REPLY_ALWAYS", "SMS_SUBMIT", false, "NO_SECURITY").Error; err != nil {
		t.Fatalf("create app: %v", err)
	}
	if err := database.Exec(`INSERT INTO cards (id, iccid, imsi, msisdn, profile_id, enc_key, auth_key, status) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		card1ID.String(), "iccid1", "imsi1", "111", profileID.String(), []byte{0x01}, []byte{0x02}, "active").Error; err != nil {
		t.Fatalf("create card1: %v", err)
	}
	if err := database.Exec(`INSERT INTO cards (id, iccid, imsi, msisdn, profile_id, enc_key, auth_key, status) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		card2ID.String(), "iccid2", "imsi2", "222", profileID.String(), []byte{0x01}, []byte{0x02}, "active").Error; err != nil {
		t.Fatalf("create card2: %v", err)
	}
	if err := database.Exec(`INSERT INTO campaigns (id, name, status, campaign_type, max_retries) VALUES (?, ?, ?, ?, ?)`,
		campaignID.String(), "c1", "pending", "script", 3).Error; err != nil {
		t.Fatalf("create campaign: %v", err)
	}
	if err := database.Exec(`INSERT INTO campaign_commands (id, campaign_id, sequence, application_id, script, expect_response) VALUES (?, ?, ?, ?, ?, ?)`,
		commandID.String(), campaignID.String(), 1, appID.String(), []byte{0xA0}, true).Error; err != nil {
		t.Fatalf("create command: %v", err)
	}
	if err := database.Exec(`INSERT INTO campaign_targets (campaign_id, card_id) VALUES (?, ?)`, campaignID.String(), card1ID.String()).Error; err != nil {
		t.Fatalf("create campaign target1: %v", err)
	}
	if err := database.Exec(`INSERT INTO campaign_targets (campaign_id, card_id) VALUES (?, ?)`, campaignID.String(), card2ID.String()).Error; err != nil {
		t.Fatalf("create campaign target2: %v", err)
	}

	svc := NewCampaignService(database, campaignTestStore{}, campaignTestQueryStore{}, nil, testLogger())
	if err := svc.StartCampaign(context.Background(), campaignID); err != nil {
		t.Fatalf("start campaign: %v", err)
	}

	var updated struct {
		Status string
	}
	if err := database.Raw(`SELECT status FROM campaigns WHERE id = ?`, campaignID.String()).Scan(&updated).Error; err != nil {
		t.Fatalf("reload campaign: %v", err)
	}
	if updated.Status != "running" {
		t.Fatalf("expected campaign status running, got %s", updated.Status)
	}

	var targets []struct{ CardID string }
	if err := database.Raw(`SELECT card_id FROM campaign_targets WHERE campaign_id = ?`, campaignID.String()).Scan(&targets).Error; err != nil {
		t.Fatalf("load campaign targets: %v", err)
	}
	if len(targets) != 2 {
		t.Fatalf("expected 2 campaign targets, got %d", len(targets))
	}

	var shards []struct {
		ItemCount int
		Items     []byte
	}
	if err := database.Raw(`SELECT item_count, items FROM campaign_shards WHERE campaign_id = ? ORDER BY sequence ASC`, campaignID.String()).Scan(&shards).Error; err != nil {
		t.Fatalf("load shards: %v", err)
	}
	if len(shards) != 1 {
		t.Fatalf("expected 1 shard, got %d", len(shards))
	}
	if shards[0].ItemCount != 2 {
		t.Fatalf("expected shard item_count 2, got %d", shards[0].ItemCount)
	}
	var events []kafkapkg.CardEvent
	if err := json.Unmarshal(shards[0].Items, &events); err != nil {
		t.Fatalf("unmarshal shard events: %v", err)
	}
	if len(events) != 2 {
		t.Fatalf("expected 2 shard events, got %d", len(events))
	}
	if events[0].CardID == uuid.Nil.String() || events[1].CardID == uuid.Nil.String() {
		t.Fatalf("expected non-zero card ids in shard events, got %+v", events)
	}
}

func TestStartCampaignStreamsTargetsIntoBatches(t *testing.T) {
	shardSize := campaignShardSize()
	database := openCampaignTestDB(t)

	for _, stmt := range []string{
		`CREATE TABLE profiles (id TEXT PRIMARY KEY, name TEXT NOT NULL, max_concat_sms INTEGER NOT NULL, buffer_size INTEGER NOT NULL, pid INTEGER NOT NULL DEFAULT 0, dcs INTEGER NOT NULL DEFAULT 0, security_bytes_type TEXT NOT NULL DEFAULT 'WITH_LENGTHS_AND_UDHL', created_at DATETIME DEFAULT CURRENT_TIMESTAMP)`,
		`CREATE TABLE applications (id TEXT PRIMARY KEY, profile_id TEXT NOT NULL, name TEXT NOT NULL, tar BLOB NOT NULL, kic_algo TEXT NOT NULL, kic_mode TEXT NOT NULL, kic_keyset_id INTEGER NOT NULL, kid_algo TEXT NOT NULL, kid_mode TEXT NOT NULL, kid_keyset_id INTEGER NOT NULL, certification_mode TEXT NOT NULL, ciphered BOOLEAN NOT NULL, counter_mode TEXT NOT NULL, por_mode TEXT NOT NULL, por_protocol TEXT NOT NULL, por_ciphered BOOLEAN NOT NULL, por_cert_mode TEXT NOT NULL)`,
		`CREATE TABLE campaigns (id TEXT PRIMARY KEY, name TEXT NOT NULL, status TEXT NOT NULL, campaign_type TEXT NOT NULL, max_retries INTEGER NOT NULL, created_at DATETIME DEFAULT CURRENT_TIMESTAMP, updated_at DATETIME DEFAULT CURRENT_TIMESTAMP, started_at DATETIME, completed_at DATETIME)`,
		`CREATE TABLE campaign_commands (id TEXT PRIMARY KEY, campaign_id TEXT NOT NULL, sequence INTEGER NOT NULL, application_id TEXT NOT NULL, script BLOB NOT NULL, expect_response BOOLEAN NOT NULL)`,
		`CREATE TABLE campaign_targets (campaign_id TEXT NOT NULL, card_id TEXT NOT NULL, created_at DATETIME DEFAULT CURRENT_TIMESTAMP, PRIMARY KEY (campaign_id, card_id))`,
		`CREATE TABLE campaign_shards (id TEXT PRIMARY KEY, campaign_id TEXT NOT NULL, sequence INTEGER NOT NULL, status TEXT NOT NULL, item_count INTEGER NOT NULL, items BLOB NOT NULL, claimed_at DATETIME, published_at DATETIME, created_at DATETIME DEFAULT CURRENT_TIMESTAMP, updated_at DATETIME DEFAULT CURRENT_TIMESTAMP)`,
	} {
		if err := database.Exec(stmt).Error; err != nil {
			t.Fatalf("exec schema %q: %v", stmt, err)
		}
	}

	profileID := uuid.New()
	appID := uuid.New()
	campaignID := uuid.New()
	commandID := uuid.New()
	if err := database.Exec(`INSERT INTO profiles (id, name, max_concat_sms, buffer_size) VALUES (?, ?, ?, ?)`, profileID.String(), "p1", 7, 180).Error; err != nil {
		t.Fatalf("create profile: %v", err)
	}
	if err := database.Exec(`INSERT INTO applications (id, profile_id, name, tar, kic_algo, kic_mode, kic_keyset_id, kid_algo, kid_mode, kid_keyset_id, certification_mode, ciphered, counter_mode, por_mode, por_protocol, por_ciphered, por_cert_mode) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		appID.String(), profileID.String(), "app1", []byte{0x01, 0x02, 0x03}, "DES", "TRIPLE_DES_CBC_2_KEYS", 1, "DES", "TRIPLE_DES_CBC_2_KEYS", 1, "CC", true, "COUNTER_REPLAY_OR_CHECK", "REPLY_ALWAYS", "SMS_SUBMIT", false, "NO_SECURITY").Error; err != nil {
		t.Fatalf("create app: %v", err)
	}
	if err := database.Exec(`INSERT INTO campaigns (id, name, status, campaign_type, max_retries) VALUES (?, ?, ?, ?, ?)`,
		campaignID.String(), "c1", "pending", "script", 3).Error; err != nil {
		t.Fatalf("create campaign: %v", err)
	}
	if err := database.Exec(`INSERT INTO campaign_commands (id, campaign_id, sequence, application_id, script, expect_response) VALUES (?, ?, ?, ?, ?, ?)`,
		commandID.String(), campaignID.String(), 1, appID.String(), []byte{0xA0}, true).Error; err != nil {
		t.Fatalf("create command: %v", err)
	}
	for i := 0; i < shardSize+23; i++ {
		cardID := uuid.New()
		if err := database.Exec(`INSERT INTO campaign_targets (campaign_id, card_id) VALUES (?, ?)`, campaignID.String(), cardID.String()).Error; err != nil {
			t.Fatalf("create campaign target %d: %v", i, err)
		}
	}

	queryStore := &trackingCampaignQueryStore{}
	svc := NewCampaignService(database, campaignTestStore{}, queryStore, nil, testLogger())
	if err := svc.StartCampaign(context.Background(), campaignID); err != nil {
		t.Fatalf("start campaign: %v", err)
	}

	if queryStore.initializeCalls != 2 {
		t.Fatalf("expected 2 initialize calls, got %d", queryStore.initializeCalls)
	}
	if queryStore.initializeCards != shardSize+23 {
		t.Fatalf("expected %d initialized cards, got %d", shardSize+23, queryStore.initializeCards)
	}

	var shards []struct {
		Sequence  int
		ItemCount int
	}
	if err := database.Raw(`SELECT sequence, item_count FROM campaign_shards WHERE campaign_id = ? ORDER BY sequence ASC`, campaignID.String()).Scan(&shards).Error; err != nil {
		t.Fatalf("load shards: %v", err)
	}
	if len(shards) != 2 {
		t.Fatalf("expected 2 shards, got %d", len(shards))
	}
	if shards[0].ItemCount != shardSize || shards[1].ItemCount != 23 {
		t.Fatalf("unexpected shard sizes: %+v", shards)
	}
}
