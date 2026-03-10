package executor

import (
	"context"
	"go.uber.org/zap"
	"gorm.io/gorm"

	kafkapkg "ota-platform/internal/kafka"
	"ota-platform/internal/keystore"
	redispkg "ota-platform/internal/redis"
)

// CoordinationStore captures the coordination operations the executor depends on.
type CoordinationStore interface {
	CheckAndSetDedupe(ctx context.Context, eventID string) (bool, error)
	GetCampaignStatus(ctx context.Context, campaignID string) (string, error)
	SetCampaignStatus(ctx context.Context, campaignID, status string) error
	AcquireThrottle(ctx context.Context, campaignID string, ratePerSec int) (bool, error)
	SetCardState(ctx context.Context, cardID string, state *redispkg.CardState) error
	GetCardState(ctx context.Context, cardID string) (*redispkg.CardState, error)
	GetCachedProfile(ctx context.Context, profileID string, out interface{}) error
	CacheProfile(ctx context.Context, profileID string, profile interface{}) error
	GetCachedCampaignParams(ctx context.Context, campaignID string) (*redispkg.CampaignParams, error)
	CacheCampaignParams(ctx context.Context, campaignID string, params *redispkg.CampaignParams) error
	GetCampaignCommands(ctx context.Context, campaignID string) ([]redispkg.CampaignCommandCache, error)
	CacheCampaignCommands(ctx context.Context, campaignID string, cmds []redispkg.CampaignCommandCache) error
}

// CounterStore abstracts atomic counter operations backed by ScyllaDB.
type CounterStore interface {
	IncrCounter(ctx context.Context, cardID, applicationID string) (int64, error)
}

// WSEvent is the websocket payload type shared with the control plane.
type WSEvent struct {
	Type       string      `json:"type"`
	CampaignID string      `json:"campaign_id,omitempty"`
	CardID     string      `json:"card_id,omitempty"`
	Data       interface{} `json:"data"`
}

// WSHub captures the websocket fanout behavior needed by the executor.
type WSHub interface {
	Broadcast(event *WSEvent)
	BroadcastToCampaign(campaignID string, event *WSEvent)
}

type Producer interface {
	Publish(ctx context.Context, key string, message interface{}) error
	Close() error
}

// CardStateWriter writes card state changes directly to ScyllaDB.
type CardStateWriter interface {
	WriteCardState(ctx context.Context, ev kafkapkg.CardStateChange) error
}

// Service is the executor service type.
type Service = CardWorker

// NewService constructs the executor service.
func NewService(database *gorm.DB, executionStore ExecutionStore, store CoordinationStore, ks keystore.KeyStore, cp keystore.CryptoProvider, smsProducer, logProducer, eventProducer Producer, cardState CardStateWriter, counterStore CounterStore, wsHub WSHub, logger *zap.Logger) *Service {
	return NewCardWorker(database, executionStore, store, ks, cp, smsProducer, logProducer, eventProducer, cardState, counterStore, wsHub, logger)
}
