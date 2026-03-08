package executor

import (
	"context"
	"time"

	scyllastore "ota-platform/internal/scylla"
)

// ExecutionStore owns durable execution-state writes.
type ExecutionStore interface {
	UpdateCampaignCard(ctx context.Context, campaignID, cardID string, updates map[string]interface{}) error
	CompleteCampaignIfRunning(ctx context.Context, campaignID, finalStatus string, completedAt time.Time) (bool, error)
	CampaignStats(ctx context.Context, campaignID string) (scyllastore.CampaignStats, error)
}
