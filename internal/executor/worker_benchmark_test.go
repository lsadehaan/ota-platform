package executor

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"go.uber.org/zap"

	"ota-platform/internal/db"
	kafkapkg "ota-platform/internal/kafka"
	"ota-platform/internal/keystore"
	redispkg "ota-platform/internal/redis"
)

type benchmarkKeyStore struct {
	keys *keystore.CardKeyMaterial
}

func (s *benchmarkKeyStore) GetKeys(context.Context, string) (*keystore.CardKeyMaterial, error) {
	return s.keys, nil
}

type benchmarkCoordinationStore struct {
	cardKeys       *redispkg.CardKeys
	commands       []redispkg.CampaignCommandCache
	campaignParams *redispkg.CampaignParams
	profile        db.Profile
}

func (s *benchmarkCoordinationStore) CheckAndSetDedupe(context.Context, string) (bool, error) {
	return true, nil
}
func (s *benchmarkCoordinationStore) GetCampaignStatus(context.Context, string) (string, error) {
	return "running", nil
}
func (s *benchmarkCoordinationStore) SetCampaignStatus(context.Context, string, string) error {
	return nil
}
func (s *benchmarkCoordinationStore) GetCardKeys(context.Context, string) (*redispkg.CardKeys, error) {
	return s.cardKeys, nil
}
func (s *benchmarkCoordinationStore) CacheCardKeys(context.Context, string, *redispkg.CardKeys) error {
	return nil
}
func (s *benchmarkCoordinationStore) IncrCounter(context.Context, string, string) (int64, error) {
	return 1, nil
}
func (s *benchmarkCoordinationStore) AcquireThrottle(context.Context, string, int) (bool, error) {
	return true, nil
}
func (s *benchmarkCoordinationStore) SetCardState(context.Context, string, *redispkg.CardState) error {
	return nil
}
func (s *benchmarkCoordinationStore) GetCardState(context.Context, string) (*redispkg.CardState, error) {
	return nil, nil
}
func (s *benchmarkCoordinationStore) UpdateProgress(context.Context, string, string, string) (*redispkg.CampaignProgress, error) {
	return &redispkg.CampaignProgress{}, nil
}
func (s *benchmarkCoordinationStore) GetProgress(context.Context, string) (*redispkg.CampaignProgress, error) {
	return &redispkg.CampaignProgress{}, nil
}
func (s *benchmarkCoordinationStore) GetCachedProfile(_ context.Context, _ string, out interface{}) error {
	if p, ok := out.(*db.Profile); ok {
		*p = s.profile
	}
	return nil
}
func (s *benchmarkCoordinationStore) CacheProfile(context.Context, string, interface{}) error {
	return nil
}
func (s *benchmarkCoordinationStore) GetCachedCampaignParams(context.Context, string) (*redispkg.CampaignParams, error) {
	return s.campaignParams, nil
}
func (s *benchmarkCoordinationStore) CacheCampaignParams(context.Context, string, *redispkg.CampaignParams) error {
	return nil
}
func (s *benchmarkCoordinationStore) GetCampaignCommands(context.Context, string) ([]redispkg.CampaignCommandCache, error) {
	return s.commands, nil
}
func (s *benchmarkCoordinationStore) CacheCampaignCommands(context.Context, string, []redispkg.CampaignCommandCache) error {
	return nil
}

type benchmarkProducer struct{}

func (benchmarkProducer) Publish(context.Context, string, interface{}) error { return nil }
func (benchmarkProducer) Close() error                                       { return nil }

type benchmarkExecutionStore struct{}

func (benchmarkExecutionStore) UpdateCampaignCard(context.Context, string, string, map[string]interface{}) error {
	return nil
}
func (benchmarkExecutionStore) CompleteCampaignIfRunning(context.Context, string, string, time.Time) (bool, error) {
	return false, nil
}

func BenchmarkHandleActivate(b *testing.B) {
	ks := &benchmarkKeyStore{
		keys: &keystore.CardKeyMaterial{
			EncKey:    []byte{0x40, 0x41, 0x42, 0x43, 0x44, 0x45, 0x46, 0x47, 0x48, 0x49, 0x4A, 0x4B, 0x4C, 0x4D, 0x4E, 0x4F},
			AuthKey:   []byte{0x50, 0x51, 0x52, 0x53, 0x54, 0x55, 0x56, 0x57, 0x58, 0x59, 0x5A, 0x5B, 0x5C, 0x5D, 0x5E, 0x5F},
			ProfileID: "profile-1",
			MSISDN:    "1234567890",
		},
	}
	store := &benchmarkCoordinationStore{
		commands: []redispkg.CampaignCommandCache{{
			Sequence:       1,
			ApplicationID:  "app-1",
			Script:         []byte{0xA0, 0xCA, 0x00, 0x00, 0x00},
			ExpectResponse: true,
			TAR:            []byte{0x01, 0x02, 0x03},
			KIcAlgo:        "DES",
			KIcMode:        "TRIPLE_DES_CBC_2_KEYS",
			KIcKeysetID:    1,
			KIdAlgo:        "DES",
			KIdMode:        "TRIPLE_DES_CBC_2_KEYS",
			KIdKeysetID:    1,
			CertMode:       "CC",
			Ciphered:       true,
			CounterMode:    "COUNTER_REPLAY_OR_CHECK",
			PORMode:        "REPLY_ALWAYS",
			PORProtocol:    "SMS_SUBMIT",
			PORCiphered:    false,
			PORCertMode:    "NO_SECURITY",
		}},
		campaignParams: &redispkg.CampaignParams{MaxRetries: 3},
		profile: db.Profile{
			MaxConcatSMS: 5,
			BufferSize:   140,
			PID:          0,
			DCS:          0,
		},
	}

	worker := NewCardWorker(nil, benchmarkExecutionStore{}, store, ks, nil, benchmarkProducer{}, benchmarkProducer{}, benchmarkProducer{}, nil, zap.NewNop())
	payload, err := json.Marshal(kafkapkg.CardEvent{
		Type:       "card.activate",
		EventID:    "evt-1",
		CardID:     "card-1",
		CampaignID: "campaign-1",
		Step:       1,
		Timestamp:  time.Now(),
	})
	if err != nil {
		b.Fatalf("marshal event: %v", err)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := worker.HandleEvent(context.Background(), []byte("card-1"), payload); err != nil {
			b.Fatalf("handle activate: %v", err)
		}
	}
}
