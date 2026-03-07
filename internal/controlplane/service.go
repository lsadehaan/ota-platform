package controlplane

import (
	"context"
	"time"

	"go.uber.org/zap"

	"github.com/google/uuid"
	"gorm.io/gorm"

	redispkg "ota-platform/internal/redis"
	scyllastore "ota-platform/internal/scylla"
)

// CoordinationStore captures the coordination operations required by the control plane.
type CoordinationStore interface {
	CacheCampaignCommands(ctx context.Context, campaignID string, cmds []redispkg.CampaignCommandCache) error
	SetCampaignStatus(ctx context.Context, campaignID, status string) error
	InitProgress(ctx context.Context, campaignID string, totalCards int64) error
}

type QueryStore interface {
	CampaignStats(ctx context.Context, campaignID uuid.UUID) (scyllastore.CampaignStats, error)
	CampaignStatsBatch(ctx context.Context, campaignIDs []uuid.UUID) (map[uuid.UUID]scyllastore.CampaignStats, error)
	ListCampaignCards(ctx context.Context, campaignID uuid.UUID, statusFilter string) ([]scyllastore.CampaignCardView, error)
	ListFailedCardIDs(ctx context.Context, campaignID uuid.UUID) ([]uuid.UUID, error)
	ResetFailedCards(ctx context.Context, campaignID uuid.UUID, cardIDs []uuid.UUID, step int, updatedAt time.Time) error
	InitializeCampaign(ctx context.Context, campaignID uuid.UUID, cardIDs []uuid.UUID, initialStep int, updatedAt time.Time) error
	AbortCampaign(ctx context.Context, campaignID uuid.UUID, updatedAt time.Time) (int, error)
	GetMessage(ctx context.Context, msgID uuid.UUID) (*scyllastore.MessageRecord, error)
	ListCardMessages(ctx context.Context, cardID uuid.UUID, limit int) ([]scyllastore.MessageRecord, error)
	ListMessages(ctx context.Context, filter scyllastore.MessageFilter) ([]scyllastore.MessageRecord, int64, error)
	RecentActivity(ctx context.Context, limit int) ([]scyllastore.MessageRecord, error)
	MessageMetrics(ctx context.Context, since time.Time) (scyllastore.MessageMetrics, error)
	CampaignMessageMetrics(ctx context.Context, campaignID uuid.UUID, since time.Time) (scyllastore.MessageMetrics, error)
	Throughput(ctx context.Context, since time.Time) ([]scyllastore.ThroughputPoint, error)
	CampaignThroughput(ctx context.Context, campaignID uuid.UUID, since time.Time) ([]scyllastore.ThroughputPoint, error)
	ErrorSummary(ctx context.Context, since time.Time) ([]scyllastore.ErrorCount, []scyllastore.ErrorCount, []scyllastore.ErrorCount, error)
	CampaignErrorSummary(ctx context.Context, campaignID uuid.UUID, since time.Time) ([]scyllastore.ErrorCount, []scyllastore.ErrorCount, []scyllastore.ErrorCount, error)
}

func NewAPI(database *gorm.DB, campaignSvc *CampaignService, queryStore QueryStore, wsHub *WSHub, logger *zap.Logger) *API {
	return newAPI(database, campaignSvc, queryStore, wsHub, logger)
}
