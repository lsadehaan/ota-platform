package controlplane

import (
	"context"
	"time"

	"go.uber.org/zap"

	"github.com/google/uuid"
	"gorm.io/gorm"

	redispkg "ota-platform/internal/redis"
	"ota-platform/internal/store"
)

// CoordinationStore captures the coordination operations required by the control plane.
type CoordinationStore interface {
	CacheCampaignCommands(ctx context.Context, campaignID string, cmds []redispkg.CampaignCommandCache) error
	SetCampaignStatus(ctx context.Context, campaignID, status string) error
}

type QueryStore interface {
	CampaignStats(ctx context.Context, campaignID uuid.UUID) (store.CampaignStats, error)
	CampaignStatsBatch(ctx context.Context, campaignIDs []uuid.UUID) (map[uuid.UUID]store.CampaignStats, error)
	ListCampaignCards(ctx context.Context, campaignID uuid.UUID, statusFilter string) ([]store.CampaignCardView, error)
	ListFailedCardIDs(ctx context.Context, campaignID uuid.UUID) ([]uuid.UUID, error)
	ResetFailedCards(ctx context.Context, campaignID uuid.UUID, cardIDs []uuid.UUID, step int, updatedAt time.Time) error
	InitializeCampaign(ctx context.Context, campaignID uuid.UUID, cardIDs []uuid.UUID, initialStep int, updatedAt time.Time) error
	AbortCampaign(ctx context.Context, campaignID uuid.UUID, updatedAt time.Time) (int, error)
	GetMessage(ctx context.Context, msgID uuid.UUID) (*store.MessageRecord, error)
	GetCardState(ctx context.Context, cardID uuid.UUID) (*store.CardStateRecord, error)
	ListCardMessages(ctx context.Context, cardID uuid.UUID, limit int) ([]store.MessageRecord, error)
	ListMessages(ctx context.Context, filter store.MessageFilter) ([]store.MessageRecord, int64, error)
	RecentActivity(ctx context.Context, limit int) ([]store.MessageRecord, error)
	MessageMetrics(ctx context.Context, since time.Time) (store.MessageMetrics, error)
	CampaignMessageMetrics(ctx context.Context, campaignID uuid.UUID, since time.Time) (store.MessageMetrics, error)
	Throughput(ctx context.Context, since time.Time) ([]store.ThroughputPoint, error)
	CampaignThroughput(ctx context.Context, campaignID uuid.UUID, since time.Time) ([]store.ThroughputPoint, error)
	MinuteThroughput(ctx context.Context) ([]store.ThroughputPoint, error)
	ErrorSummary(ctx context.Context, since time.Time) ([]store.ErrorCount, []store.ErrorCount, []store.ErrorCount, error)
	CampaignErrorSummary(ctx context.Context, campaignID uuid.UUID, since time.Time) ([]store.ErrorCount, []store.ErrorCount, []store.ErrorCount, error)
}

// ChannelStats provides pipeline channel depth information.
type ChannelStats interface {
	ChannelDepths() map[string]int
}

// CardKeyWriter writes card key material to ScyllaDB.
type CardKeyWriter interface {
	WriteCardKeys(ctx context.Context, cardID string, encKey, authKey, kek []byte, profileID, msisdn string) error
	WriteCardKeysBatch(ctx context.Context, records []store.CardKeyRecord) error
	DeleteCardKeys(ctx context.Context, cardID string) error
	DeleteMSISDNMapping(ctx context.Context, msisdn string) error
}

// CounterReader reads card counters from ScyllaDB.
type CounterReader interface {
	GetCountersByCard(ctx context.Context, cardID string) ([]store.CardCounterRecord, error)
}

func NewAPI(database *gorm.DB, rdb *redispkg.Client, campaignSvc *CampaignService, queryStore QueryStore, cardKeys CardKeyWriter, counterRead CounterReader, wsHub *WSHub, logger *zap.Logger) *API {
	return newAPI(database, rdb, campaignSvc, queryStore, cardKeys, counterRead, wsHub, logger)
}
