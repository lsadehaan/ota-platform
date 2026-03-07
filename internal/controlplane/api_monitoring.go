package controlplane

import (
	"net/http"
	"sort"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"go.uber.org/zap"
	"gorm.io/gorm"

	"ota-platform/internal/db"
	scyllastore "ota-platform/internal/scylla"
	"ota-platform/pkg/hexutil"
)

// GetSystemHealth handles GET /api/v1/monitoring/health.
func (a *API) GetSystemHealth(c *gin.Context) {
	// Check database connectivity
	dbStatus := "up"
	sqlDB, err := a.db.DB()
	if err != nil {
		dbStatus = "down"
	} else if err := sqlDB.Ping(); err != nil {
		dbStatus = "down"
	}

	status := "healthy"
	httpStatus := http.StatusOK
	if dbStatus != "up" {
		status = "degraded"
		httpStatus = http.StatusServiceUnavailable
	}

	type healthComponent struct {
		Name      string  `json:"name"`
		Status    string  `json:"status"`
		Details   string  `json:"details,omitempty"`
		LatencyMs float64 `json:"latency_ms"`
	}

	// Measure database latency
	var dbLatencyMs float64
	if dbStatus == "up" {
		start := time.Now()
		_ = sqlDB.Ping()
		dbLatencyMs = float64(time.Since(start).Microseconds()) / 1000.0
	}

	components := []healthComponent{
		{
			Name:      "database",
			Status:    dbStatus,
			LatencyMs: dbLatencyMs,
		},
	}

	c.JSON(httpStatus, gin.H{
		"status":     status,
		"timestamp":  time.Now().UTC(),
		"components": components,
	})
}

// ListMessages handles GET /api/v1/monitoring/messages with filters and pagination.
func (a *API) ListMessages(c *gin.Context) {
	page, pageSize, offset := paginationParams(c)
	filter := scyllaMessageFilterFromRequest(c, page, pageSize)
	if msisdn := c.Query("msisdn"); msisdn != "" {
		var card db.Card
		if err := a.db.Select("id").First(&card, "msisdn = ?", msisdn).Error; err == nil {
			filter.CardID = &card.ID
		} else if err == gorm.ErrRecordNotFound {
			paginatedResponse(c, http.StatusOK, []interface{}{}, 0, page, pageSize)
			return
		} else {
			a.logger.Error("failed to resolve message msisdn filter", zap.Error(err))
			errorResponse(c, http.StatusInternalServerError, "failed to resolve msisdn")
			return
		}
	}
	if campaignID := c.Query("campaign_id"); campaignID != "" {
		id, err := uuid.Parse(campaignID)
		if err != nil {
			errorResponse(c, http.StatusBadRequest, "invalid campaign_id")
			return
		}
		filter.CampaignID = &id
	}
	// Accept both "from" and "date_from" as aliases
	fromParam := c.Query("from")
	if fromParam == "" {
		fromParam = c.Query("date_from")
	}
	if fromParam != "" {
		t, err := time.Parse(time.RFC3339, fromParam)
		if err == nil {
			filter.From = t.UTC()
		}
	}
	// Accept both "to" and "date_to" as aliases
	toParam := c.Query("to")
	if toParam == "" {
		toParam = c.Query("date_to")
	}
	if toParam != "" {
		t, err := time.Parse(time.RFC3339, toParam)
		if err == nil {
			filter.To = t.UTC()
		}
	}
	filter.Page = page
	filter.PageSize = pageSize

	messages, total, err := a.query.ListMessages(c.Request.Context(), filter)
	if err != nil {
		a.logger.Error("failed to list messages", zap.Error(err))
		errorResponse(c, http.StatusInternalServerError, "failed to list messages")
		return
	}
	_ = offset

	paginatedResponse(c, http.StatusOK, messages, total, page, pageSize)
}

// GetMessage handles GET /api/v1/monitoring/messages/:id with hex payloads.
func (a *API) GetMessage(c *gin.Context) {
	id, ok := parseUUID(c, "id")
	if !ok {
		return
	}

	msgID := uuid.MustParse(id)
	msg, err := a.query.GetMessage(c.Request.Context(), msgID)
	if err != nil {
		a.logger.Error("failed to get message", zap.Error(err))
		errorResponse(c, http.StatusInternalServerError, "failed to get message")
		return
	}
	if msg == nil {
		errorResponse(c, http.StatusNotFound, "message not found")
		return
	}

	var card db.Card
	if err := a.db.First(&card, "id = ?", msg.CardID).Error; err != nil {
		a.logger.Error("failed to get message card", zap.Error(err))
		errorResponse(c, http.StatusInternalServerError, "failed to get message card")
		return
	}
	var campaign *db.Campaign
	if msg.CampaignID != nil {
		var loaded db.Campaign
		if err := a.db.First(&loaded, "id = ?", *msg.CampaignID).Error; err == nil {
			campaign = &loaded
		}
	}

	// Build detailed response with hex payloads
	response := gin.H{
		"id":              msg.ID,
		"campaign_id":     msg.CampaignID,
		"card_id":         msg.CardID,
		"direction":       msg.Direction,
		"status":          msg.Status,
		"smpp_message_id": msg.SMPPMessageID,
		"dlr_status":      msg.DLRStatus,
		"por_status_code": msg.PORStatusCode,
		"created_at":      msg.CreatedAt,
		"updated_at":      msg.UpdatedAt,
		"card":            card,
		"campaign":        campaign,
	}

	if len(msg.RawPayload) > 0 {
		response["raw_payload_hex"] = hexutil.Encode(msg.RawPayload)
	}
	if len(msg.SecuredPayload) > 0 {
		response["secured_payload_hex"] = hexutil.Encode(msg.SecuredPayload)
	}
	if len(msg.PORData) > 0 {
		response["por_data_hex"] = hexutil.Encode(msg.PORData)
	}

	c.JSON(http.StatusOK, gin.H{"data": response})
}

// GetErrorSummary handles GET /api/v1/monitoring/errors.
// Aggregates recent transport and PoR errors from the Scylla read model.
func (a *API) GetErrorSummary(c *gin.Context) {
	since := time.Now().UTC().Add(-24 * time.Hour)
	var (
		statusCounts []scyllastore.ErrorCount
		dlrCounts    []scyllastore.ErrorCount
		porCounts    []scyllastore.ErrorCount
		err          error
		campaignID   *uuid.UUID
	)
	if rawCampaignID := c.Query("campaign_id"); rawCampaignID != "" {
		id, parseErr := uuid.Parse(rawCampaignID)
		if parseErr != nil {
			errorResponse(c, http.StatusBadRequest, "invalid campaign_id")
			return
		}
		campaignID = &id
		statusCounts, dlrCounts, porCounts, err = a.query.CampaignErrorSummary(c.Request.Context(), id, since)
	} else {
		statusCounts, dlrCounts, porCounts, err = a.query.ErrorSummary(c.Request.Context(), since)
	}
	if err != nil {
		a.logger.Error("failed to get error summary", zap.Error(err))
		errorResponse(c, http.StatusInternalServerError, "failed to get error summary")
		return
	}

	// Campaign card failures
	type campaignErrorCount struct {
		CampaignID   string `json:"campaign_id"`
		CampaignName string `json:"campaign_name"`
		FailedCount  int64  `json:"failed_count"`
	}

	var campaigns []db.Campaign
	if err := a.db.Select("id, name").Find(&campaigns).Error; err != nil {
		a.logger.Error("failed to load campaigns for error summary", zap.Error(err))
		errorResponse(c, http.StatusInternalServerError, "failed to get error summary")
		return
	}
	ids := make([]uuid.UUID, 0, len(campaigns))
	names := make(map[uuid.UUID]string, len(campaigns))
	for _, campaign := range campaigns {
		if campaignID != nil && campaign.ID != *campaignID {
			continue
		}
		ids = append(ids, campaign.ID)
		names[campaign.ID] = campaign.Name
	}
	statsByCampaign, err := a.query.CampaignStatsBatch(c.Request.Context(), ids)
	if err != nil {
		a.logger.Error("failed to load campaign failure stats", zap.Error(err))
		errorResponse(c, http.StatusInternalServerError, "failed to get error summary")
		return
	}
	campaignErrors := make([]campaignErrorCount, 0)
	for id, stats := range statsByCampaign {
		if stats.Failed == 0 {
			continue
		}
		campaignErrors = append(campaignErrors, campaignErrorCount{
			CampaignID:   id.String(),
			CampaignName: names[id],
			FailedCount:  stats.Failed,
		})
	}
	sort.Slice(campaignErrors, func(i, j int) bool { return campaignErrors[i].FailedCount > campaignErrors[j].FailedCount })
	if len(campaignErrors) > 20 {
		campaignErrors = campaignErrors[:20]
	}

	if campaignErrors == nil {
		campaignErrors = []campaignErrorCount{}
	}

	c.JSON(http.StatusOK, gin.H{
		"status_errors":   statusCounts,
		"dlr_errors":      dlrCounts,
		"por_errors":      porCounts,
		"campaign_errors": campaignErrors,
	})
}

func scyllaMessageFilterFromRequest(c *gin.Context, page, pageSize int) scyllastore.MessageFilter {
	return scyllastore.MessageFilter{
		Direction: c.Query("direction"),
		Status:    c.Query("status"),
		From:      time.Now().UTC().Add(-24 * time.Hour),
		To:        time.Now().UTC(),
		Page:      page,
		PageSize:  pageSize,
	}
}

// GetSettings handles GET /api/v1/settings.
func (a *API) GetSettings(c *gin.Context) {
	// Return platform-level settings. For now, these are read from context or defaults.
	// In a full implementation these would be stored in a settings table.
	settings := gin.H{
		"sms_gateway": gin.H{
			"type":    "smpp",
			"host":    "",
			"port":    2775,
			"enabled": true,
		},
		"kafka": gin.H{
			"brokers": []string{},
			"enabled": true,
		},
		"ota": gin.H{
			"default_max_retries":    3,
			"default_max_concat_sms": 7,
			"default_buffer_size":    180,
		},
	}

	c.JSON(http.StatusOK, gin.H{"data": settings})
}

// UpdateSettings handles PUT /api/v1/settings.
func (a *API) UpdateSettings(c *gin.Context) {
	var settings map[string]interface{}
	if err := c.ShouldBindJSON(&settings); err != nil {
		errorResponse(c, http.StatusBadRequest, err.Error())
		return
	}

	// In a full implementation, persist settings to a database table.
	// For now, acknowledge receipt.
	a.logger.Info("settings update received", zap.Any("settings", settings))

	c.JSON(http.StatusOK, gin.H{
		"message": "settings updated",
		"data":    settings,
	})
}
