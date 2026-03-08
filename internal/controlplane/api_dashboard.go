package controlplane

import (
	"context"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"go.uber.org/zap"

	"ota-platform/internal/db"
	scyllastore "ota-platform/internal/scylla"
)

// GetDashboardKPIs handles GET /api/v1/dashboard/kpis.
func (a *API) GetDashboardKPIs(c *gin.Context) {
	kpi := a.computeKPIsFromTables(c.Request.Context())
	var successRate float64
	if kpi.MTMessages > 0 {
		successRate = float64(kpi.DeliveredMessages) / float64(kpi.MTMessages) * 100
	}

	c.JSON(http.StatusOK, gin.H{
		"data": gin.H{
			"total_cards":          kpi.TotalCards,
			"active_cards":         kpi.ActiveCards,
			"inactive_cards":       kpi.InactiveCards,
			"blocked_cards":        kpi.BlockedCards,
			"total_campaigns":      kpi.TotalCampaigns,
			"active_campaigns":     kpi.RunningCampaigns,
			"completed_campaigns":  kpi.CompletedCampaigns,
			"failed_campaigns":     kpi.FailedCampaigns,
			"total_messages":       kpi.TotalMessages,
			"mt_messages":          kpi.MTMessages,
			"mo_messages":          kpi.MOMessages,
			"delivered_messages":   kpi.DeliveredMessages,
			"undelivered_messages": kpi.UndeliveredMessages,
			"total_cap_files":      kpi.TotalCAPFiles,
			"total_scripts":        kpi.TotalScripts,
			"refreshed_at":         kpi.RefreshedAt,
			"messages_today":       kpi.MessagesToday,
			"success_rate":         successRate,
			"campaigns_by_status": map[string]int64{
				"pending":   kpi.TotalCampaigns - kpi.RunningCampaigns - kpi.CompletedCampaigns - kpi.FailedCampaigns,
				"running":   kpi.RunningCampaigns,
				"completed": kpi.CompletedCampaigns,
				"failed":    kpi.FailedCampaigns,
			},
		},
	})
}

// GetRecentActivity handles GET /api/v1/dashboard/activity.
// Returns the last 50 message records with card info from the Scylla read model.
func (a *API) GetRecentActivity(c *gin.Context) {
	messages, err := a.query.RecentActivity(c.Request.Context(), 50)
	if err != nil {
		a.logger.Error("failed to get recent activity", zap.Error(err))
		errorResponse(c, http.StatusInternalServerError, "failed to get recent activity")
		return
	}

	type activityEntry struct {
		ID            string    `json:"id"`
		CampaignID    *string   `json:"campaign_id,omitempty"`
		CardID        string    `json:"card_id"`
		MSISDN        string    `json:"msisdn"`
		ICCID         string    `json:"iccid"`
		Direction     string    `json:"direction"`
		Status        string    `json:"status"`
		DLRStatus     *string   `json:"dlr_status,omitempty"`
		PORStatusCode *int16    `json:"por_status_code,omitempty"`
		CreatedAt     time.Time `json:"created_at"`
		UpdatedAt     time.Time `json:"updated_at"`
	}

	cardIDs := make([]string, 0, len(messages))
	seen := make(map[string]struct{}, len(messages))
	for _, m := range messages {
		id := m.CardID.String()
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		cardIDs = append(cardIDs, id)
	}
	var cards []db.Card
	if len(cardIDs) > 0 {
		if err := a.db.Where("id IN ?", cardIDs).Find(&cards).Error; err != nil {
			a.logger.Error("failed to load recent activity cards", zap.Error(err))
			errorResponse(c, http.StatusInternalServerError, "failed to get recent activity")
			return
		}
	}
	cardByID := make(map[string]db.Card, len(cards))
	for _, card := range cards {
		cardByID[card.ID.String()] = card
	}

	result := make([]activityEntry, 0, len(messages))
	for _, m := range messages {
		card := cardByID[m.CardID.String()]
		entry := activityEntry{
			ID:            m.ID.String(),
			CardID:        m.CardID.String(),
			MSISDN:        card.MSISDN,
			ICCID:         card.ICCID,
			Direction:     m.Direction,
			Status:        m.Status,
			DLRStatus:     m.DLRStatus,
			PORStatusCode: m.PORStatusCode,
			CreatedAt:     m.CreatedAt,
			UpdatedAt:     m.UpdatedAt,
		}
		if m.CampaignID != nil {
			s := m.CampaignID.String()
			entry.CampaignID = &s
		}
		result = append(result, entry)
	}

	c.JSON(http.StatusOK, gin.H{"data": result})
}

// GetSMSThroughput handles GET /api/v1/dashboard/sms-throughput.
// Returns per-minute average TPS for the last hour (or hourly totals for a specific campaign).
func (a *API) GetSMSThroughput(c *gin.Context) {
	var (
		results []scyllastore.ThroughputPoint
		err     error
	)
	if campaignID := c.Query("campaign_id"); campaignID != "" {
		id, parseErr := uuid.Parse(campaignID)
		if parseErr != nil {
			errorResponse(c, http.StatusBadRequest, "invalid campaign_id")
			return
		}
		since := time.Now().UTC().Add(-24 * time.Hour)
		results, err = a.query.CampaignThroughput(c.Request.Context(), id, since)
	} else {
		results, err = a.query.MinuteThroughput(c.Request.Context())
	}
	if err != nil {
		a.logger.Error("failed to get SMS throughput", zap.Error(err))
		errorResponse(c, http.StatusInternalServerError, "failed to get SMS throughput")
		return
	}

	if results == nil {
		results = []scyllastore.ThroughputPoint{}
	}

	c.JSON(http.StatusOK, gin.H{"data": results})
}

type dashboardKPI struct {
	TotalCards          int64
	ActiveCards         int64
	InactiveCards       int64
	BlockedCards        int64
	TotalCampaigns      int64
	RunningCampaigns    int64
	CompletedCampaigns  int64
	FailedCampaigns     int64
	TotalMessages       int64
	MTMessages          int64
	MOMessages          int64
	DeliveredMessages   int64
	UndeliveredMessages int64
	TotalCAPFiles       int64
	TotalScripts        int64
	RefreshedAt         time.Time
	MessagesToday       int64
}

// computeKPIsFromTables calculates KPIs directly from the active control-plane and Scylla read-model tables.
func (a *API) computeKPIsFromTables(ctx context.Context) dashboardKPI {
	now := time.Now().UTC()

	a.kpiMu.Lock()
	if !a.kpiCachedUntil.IsZero() && now.Before(a.kpiCachedUntil) {
		cached := a.kpiCached
		a.kpiMu.Unlock()
		return cached
	}
	a.kpiMu.Unlock()

	var kpi dashboardKPI

	// Card counts
	a.db.Model(&db.Card{}).Count(&kpi.TotalCards)
	a.db.Model(&db.Card{}).Where("status = 'active'").Count(&kpi.ActiveCards)
	a.db.Model(&db.Card{}).Where("status = 'inactive'").Count(&kpi.InactiveCards)
	a.db.Model(&db.Card{}).Where("status = 'blocked'").Count(&kpi.BlockedCards)

	// Campaign counts
	a.db.Model(&db.Campaign{}).Count(&kpi.TotalCampaigns)
	a.db.Model(&db.Campaign{}).Where("status = 'running'").Count(&kpi.RunningCampaigns)
	a.db.Model(&db.Campaign{}).Where("status IN ('completed','completed_with_errors')").Count(&kpi.CompletedCampaigns)
	a.db.Model(&db.Campaign{}).Where("status = 'failed'").Count(&kpi.FailedCampaigns)

	if metrics, err := a.query.MessageMetrics(ctx, now.Add(-24*time.Hour)); err == nil {
		kpi.TotalMessages = metrics.Total
		kpi.MTMessages = metrics.MT
		kpi.MOMessages = metrics.MO
		kpi.DeliveredMessages = metrics.Delivered
		kpi.UndeliveredMessages = metrics.Undelivered
	} else {
		a.logger.Warn("failed to compute 24h Scylla message KPIs", zap.Error(err))
	}
	if todayMetrics, err := a.query.MessageMetrics(ctx, now.Truncate(24*time.Hour)); err == nil {
		kpi.MessagesToday = todayMetrics.MT
	} else {
		a.logger.Warn("failed to compute today Scylla message KPIs", zap.Error(err))
	}

	// Asset counts
	a.db.Model(&db.CAPFile{}).Count(&kpi.TotalCAPFiles)
	a.db.Model(&db.Script{}).Count(&kpi.TotalScripts)

	kpi.RefreshedAt = now

	a.kpiMu.Lock()
	a.kpiCached = kpi
	a.kpiCachedUntil = now.Add(2 * time.Second)
	a.kpiMu.Unlock()

	return kpi
}
