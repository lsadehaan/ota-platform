package planner

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"go.uber.org/zap"

	"ota-platform/internal/db"
	"ota-platform/internal/executor"
	kafkapkg "ota-platform/internal/kafka"
	"ota-platform/internal/keystore"
	redispkg "ota-platform/internal/redis"
	scyllastore "ota-platform/internal/scylla"
	"ota-platform/internal/smpp"
	"ota-platform/internal/transport"
)

type pipelineKeyStore struct {
	keys *keystore.CardKeyMaterial
}

func (s *pipelineKeyStore) GetKeys(context.Context, string) (*keystore.CardKeyMaterial, error) {
	return s.keys, nil
}

func newPipelineKeyStore() *pipelineKeyStore {
	return &pipelineKeyStore{
		keys: &keystore.CardKeyMaterial{
			EncKey:    []byte{0x40, 0x41, 0x42, 0x43, 0x44, 0x45, 0x46, 0x47, 0x48, 0x49, 0x4A, 0x4B, 0x4C, 0x4D, 0x4E, 0x4F},
			AuthKey:   []byte{0x50, 0x51, 0x52, 0x53, 0x54, 0x55, 0x56, 0x57, 0x58, 0x59, 0x5A, 0x5B, 0x5C, 0x5D, 0x5E, 0x5F},
			ProfileID: "profile-1",
			MSISDN:    "1234567890",
		},
	}
}

type pipelineCoordinationStore struct {
	commands       []redispkg.CampaignCommandCache
	campaignParams *redispkg.CampaignParams
	profile        db.Profile

	cardStateByID   map[string]*redispkg.CardState
	counterByKey    map[string]int64
	correlationByID map[string]*redispkg.SMPPMapping
}

func newPipelineCoordinationStore() *pipelineCoordinationStore {
	return &pipelineCoordinationStore{
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
		cardStateByID:   make(map[string]*redispkg.CardState),
		counterByKey:    make(map[string]int64),
		correlationByID: make(map[string]*redispkg.SMPPMapping),
	}
}

func (s *pipelineCoordinationStore) CheckAndSetDedupe(context.Context, string) (bool, error) {
	return true, nil
}
func (s *pipelineCoordinationStore) GetCampaignStatus(context.Context, string) (string, error) {
	return "running", nil
}
func (s *pipelineCoordinationStore) SetCampaignStatus(context.Context, string, string) error {
	return nil
}
func (s *pipelineCoordinationStore) IncrCounter(_ context.Context, cardID, appID string) (int64, error) {
	key := cardID + ":" + appID
	s.counterByKey[key]++
	return s.counterByKey[key], nil
}
func (s *pipelineCoordinationStore) AcquireThrottle(context.Context, string, int) (bool, error) {
	return true, nil
}
func (s *pipelineCoordinationStore) SetCardState(_ context.Context, cardID string, state *redispkg.CardState) error {
	if state == nil {
		delete(s.cardStateByID, cardID)
		return nil
	}
	copied := *state
	s.cardStateByID[cardID] = &copied
	return nil
}
func (s *pipelineCoordinationStore) GetCardState(_ context.Context, cardID string) (*redispkg.CardState, error) {
	state, ok := s.cardStateByID[cardID]
	if !ok {
		return nil, nil
	}
	copied := *state
	return &copied, nil
}
func (s *pipelineCoordinationStore) GetCachedProfile(_ context.Context, _ string, out interface{}) error {
	if profile, ok := out.(*db.Profile); ok {
		*profile = s.profile
	}
	return nil
}
func (s *pipelineCoordinationStore) CacheProfile(context.Context, string, interface{}) error {
	return nil
}
func (s *pipelineCoordinationStore) GetCachedCampaignParams(context.Context, string) (*redispkg.CampaignParams, error) {
	return s.campaignParams, nil
}
func (s *pipelineCoordinationStore) CacheCampaignParams(context.Context, string, *redispkg.CampaignParams) error {
	return nil
}
func (s *pipelineCoordinationStore) GetCampaignCommands(context.Context, string) ([]redispkg.CampaignCommandCache, error) {
	return s.commands, nil
}
func (s *pipelineCoordinationStore) CacheCampaignCommands(context.Context, string, []redispkg.CampaignCommandCache) error {
	return nil
}
func (s *pipelineCoordinationStore) StoreSMPPCorrelation(_ context.Context, smppMsgID string, mapping *redispkg.SMPPMapping) error {
	s.correlationByID[smppMsgID] = mapping
	return nil
}
func (s *pipelineCoordinationStore) LoadSMPPCorrelation(_ context.Context, smppMsgID string) (*redispkg.SMPPMapping, error) {
	return s.correlationByID[smppMsgID], nil
}
func (s *pipelineCoordinationStore) LookupCardByMSISDN(context.Context, string) (string, error) {
	return "card-1", nil
}

type pipelineProducer struct {
	smsMessages []kafkapkg.SendSMSMessage
	logActions  []kafkapkg.MessageLogAction
	cardEvents  []kafkapkg.CardEvent
}

func (p *pipelineProducer) Publish(_ context.Context, _ string, message interface{}) error {
	switch v := message.(type) {
	case kafkapkg.SendSMSMessage:
		p.smsMessages = append(p.smsMessages, v)
	case kafkapkg.MessageLogAction:
		p.logActions = append(p.logActions, v)
	case kafkapkg.CardEvent:
		p.cardEvents = append(p.cardEvents, v)
	}
	return nil
}

func (p *pipelineProducer) Close() error { return nil }

type pipelineExecutionStore struct{}

func (pipelineExecutionStore) UpdateCampaignCard(context.Context, string, string, map[string]interface{}) error {
	return nil
}
func (pipelineExecutionStore) CompleteCampaignIfRunning(context.Context, string, string, time.Time) (bool, error) {
	return false, nil
}
func (pipelineExecutionStore) CampaignStats(context.Context, string) (scyllastore.CampaignStats, error) {
	return scyllastore.CampaignStats{}, nil
}

type pipelineSMPPClient struct{}

func (pipelineSMPPClient) Connect(context.Context) error { return nil }
func (pipelineSMPPClient) ActiveConnections() int        { return 1 }
func (pipelineSMPPClient) Close() error                  { return nil }
func (pipelineSMPPClient) Submit(*smpp.SubmitRequest) (*smpp.SubmitResponse, error) {
	return &smpp.SubmitResponse{MessageID: uuid.NewString()}, nil
}

type pipelineProjectorStore struct {
	creates int
	updates int
}

func (s *pipelineProjectorStore) ApplyActions(actions []kafkapkg.MessageLogAction) error {
	for _, action := range actions {
		switch action.Action {
		case "create":
			if action.Log == nil {
				continue
			}
			if _, err := base64.StdEncoding.DecodeString(action.Log.RawPayload); err != nil {
				return err
			}
			if action.Log.SecuredPayload != "" {
				if _, err := base64.StdEncoding.DecodeString(action.Log.SecuredPayload); err != nil {
					return err
				}
			}
			s.creates++
		case "update":
			s.updates++
		}
	}
	return nil
}

type executorBridgePublisher struct {
	worker *executor.Service
	ctx    context.Context
}

func (p *executorBridgePublisher) Publish(_ context.Context, key string, message interface{}) error {
	event, ok := message.(kafkapkg.CardEvent)
	if !ok {
		return nil
	}
	payload, err := json.Marshal(event)
	if err != nil {
		return err
	}
	return p.worker.HandleEvent(p.ctx, []byte(key), payload)
}

func (p *executorBridgePublisher) Close() error { return nil }

func BenchmarkActivationPipeline(b *testing.B) {
	const (
		shardCount     = 8
		eventsPerShard = 250
	)

	items := make([][]byte, shardCount)
	for i := 0; i < shardCount; i++ {
		events := make([]kafkapkg.CardEvent, 0, eventsPerShard)
		for j := 0; j < eventsPerShard; j++ {
			events = append(events, kafkapkg.CardEvent{
				Type:       "card.activate",
				EventID:    uuid.NewString(),
				CardID:     "card-1",
				CampaignID: "campaign-1",
				Step:       1,
				Timestamp:  time.Now(),
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

		coordination := newPipelineCoordinationStore()
		smsProducer := &pipelineProducer{}
		logProducer := &pipelineProducer{}
		eventProducer := &pipelineProducer{}
		worker := executor.NewService(nil, pipelineExecutionStore{}, coordination, newPipelineKeyStore(), nil, smsProducer, logProducer, eventProducer, nil, zap.NewNop())
		plannerSvc := NewService(database, &executorBridgePublisher{worker: worker, ctx: context.Background()}, zap.NewNop())
		projectorStore := &pipelineProjectorStore{}
		gateway := transport.NewServiceWithDeps(pipelineSMPPClient{}, &pipelineProducer{}, coordination, zap.NewNop())
		b.StartTimer()

		plannerSvc.publishClaimedShards(context.Background(), shards)
		if err := projectorStore.ApplyActions(logProducer.logActions); err != nil {
			b.Fatalf("apply projector actions: %v", err)
		}
		for _, smsMsg := range smsProducer.smsMessages {
			payload, err := json.Marshal(smsMsg)
			if err != nil {
				b.Fatalf("marshal send-sms: %v", err)
			}
			if err := gateway.HandleSendSMSMessage(context.Background(), []byte(smsMsg.CardID), payload); err != nil {
				b.Fatalf("gateway handle send-sms: %v", err)
			}
		}
		b.StopTimer()

		expectedMessages := shardCount * eventsPerShard
		if len(smsProducer.smsMessages) != expectedMessages {
			b.Fatalf("expected %d sms messages, got %d", expectedMessages, len(smsProducer.smsMessages))
		}
		if projectorStore.creates == 0 {
			b.Fatal("expected projector create actions")
		}
	}
}

func BenchmarkActivationResponsePipeline(b *testing.B) {
	coordination := newPipelineCoordinationStore()
	smsProducer := &pipelineProducer{}
	logProducer := &pipelineProducer{}
	eventProducer := &pipelineProducer{}
	worker := executor.NewService(nil, pipelineExecutionStore{}, coordination, newPipelineKeyStore(), nil, smsProducer, logProducer, eventProducer, nil, zap.NewNop())
	bridge := &executorBridgePublisher{worker: worker, ctx: context.Background()}
	gateway := transport.NewServiceWithDeps(pipelineSMPPClient{}, bridge, coordination, zap.NewNop())

	activatePayload, err := json.Marshal(kafkapkg.CardEvent{
		Type:       "card.activate",
		EventID:    uuid.NewString(),
		CardID:     "card-1",
		CampaignID: "campaign-1",
		Step:       1,
		Timestamp:  time.Now(),
	})
	if err != nil {
		b.Fatalf("marshal activate event: %v", err)
	}

	dlrTemplate := "id:%s sub:001 dlvrd:001 submit date:2603071200 done date:2603071201 stat:DELIVRD err:000 text:"
	porPayload := buildBenchmarkPoR()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		smsProducer.smsMessages = smsProducer.smsMessages[:0]
		logProducer.logActions = logProducer.logActions[:0]
		eventProducer.cardEvents = eventProducer.cardEvents[:0]
		coordination.correlationByID = make(map[string]*redispkg.SMPPMapping)
		coordination.cardStateByID = make(map[string]*redispkg.CardState)

		if err := worker.HandleEvent(context.Background(), []byte("card-1"), activatePayload); err != nil {
			b.Fatalf("activate event: %v", err)
		}
		if len(smsProducer.smsMessages) != 1 {
			b.Fatalf("expected 1 send-sms message, got %d", len(smsProducer.smsMessages))
		}

		msg := smsProducer.smsMessages[0]
		sendPayload, err := json.Marshal(msg)
		if err != nil {
			b.Fatalf("marshal send-sms: %v", err)
		}
		if err := gateway.HandleSendSMSMessage(context.Background(), []byte(msg.CardID), sendPayload); err != nil {
			b.Fatalf("gateway send: %v", err)
		}

		var smppID string
		for id := range coordination.correlationByID {
			smppID = id
			break
		}
		if smppID == "" {
			b.Fatal("expected smpp correlation")
		}

		gateway.HandleDLRReceipt(context.Background(), "", []byte(
			fmt.Sprintf(dlrTemplate, smppID),
		))
		gateway.HandleMOPayload(context.Background(), "1234567890", "", porPayload)

		state, err := coordination.GetCardState(context.Background(), "card-1")
		if err != nil {
			b.Fatalf("load final card state: %v", err)
		}
		if state == nil || state.Status != "completed" {
			b.Fatalf("expected completed card state, got %#v", state)
		}
	}
}

func buildBenchmarkPoR() []byte {
	return []byte{
		0x00, 0x0B,
		0x0A,
		0x01, 0x02, 0x03,
		0x00, 0x00, 0x00, 0x00, 0x00,
		0x00,
		0x00,
	}
}
