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
)

func (a *API) GetDebugCard(c *gin.Context) {
	id, ok := parseUUID(c, "id")
	if !ok {
		return
	}
	cardID := uuid.MustParse(id)

	var card db.Card
	if err := a.db.Preload("Profile").First(&card, "id = ?", cardID).Error; err != nil {
		if err == gorm.ErrRecordNotFound {
			errorResponse(c, http.StatusNotFound, "card not found")
			return
		}
		errorResponse(c, http.StatusInternalServerError, "failed to load card")
		return
	}

	var counters []db.CardCounter
	if err := a.db.Where("card_id = ?", cardID).Find(&counters).Error; err != nil {
		errorResponse(c, http.StatusInternalServerError, "failed to load card counters")
		return
	}

	state, err := a.query.GetCardState(c.Request.Context(), cardID)
	if err != nil {
		a.logger.Error("failed to load card debug state", zap.Error(err))
		errorResponse(c, http.StatusInternalServerError, "failed to load card state")
		return
	}
	messages, err := a.query.ListCardMessages(c.Request.Context(), cardID, 50)
	if err != nil {
		a.logger.Error("failed to load card debug messages", zap.Error(err))
		errorResponse(c, http.StatusInternalServerError, "failed to load card messages")
		return
	}

	var targets []db.CampaignTarget
	if err := a.db.Where("card_id = ?", cardID).Order("created_at DESC").Limit(20).Find(&targets).Error; err != nil {
		errorResponse(c, http.StatusInternalServerError, "failed to load card targets")
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"card":             card,
		"counters":         counters,
		"execution_state":  state,
		"recent_messages":  messages,
		"campaign_targets": targets,
	})
}

func (a *API) GetDebugCampaign(c *gin.Context) {
	id, ok := parseUUID(c, "id")
	if !ok {
		return
	}
	campaignID := uuid.MustParse(id)

	var campaign db.Campaign
	if err := a.db.Preload("CampaignCommands").First(&campaign, "id = ?", campaignID).Error; err != nil {
		if err == gorm.ErrRecordNotFound {
			errorResponse(c, http.StatusNotFound, "campaign not found")
			return
		}
		errorResponse(c, http.StatusInternalServerError, "failed to load campaign")
		return
	}

	stats, err := a.query.CampaignStats(c.Request.Context(), campaignID)
	if err != nil {
		errorResponse(c, http.StatusInternalServerError, "failed to load campaign stats")
		return
	}
	cards, err := a.query.ListCampaignCards(c.Request.Context(), campaignID, "")
	if err != nil {
		errorResponse(c, http.StatusInternalServerError, "failed to load campaign cards")
		return
	}
	if len(cards) > 50 {
		cards = cards[:50]
	}
	failedCardIDs, err := a.query.ListFailedCardIDs(c.Request.Context(), campaignID)
	if err != nil {
		errorResponse(c, http.StatusInternalServerError, "failed to load failed cards")
		return
	}

	var shardRows []struct {
		Status string
		Count  int64
	}
	if err := a.db.Model(&db.CampaignShard{}).
		Select("status, COUNT(*) AS count").
		Where("campaign_id = ?", campaignID).
		Group("status").
		Scan(&shardRows).Error; err != nil {
		errorResponse(c, http.StatusInternalServerError, "failed to load shard summary")
		return
	}
	shards := make(map[string]int64, len(shardRows))
	for _, row := range shardRows {
		shards[row.Status] = row.Count
	}

	var targetCount int64
	if err := a.db.Model(&db.CampaignTarget{}).Where("campaign_id = ?", campaignID).Count(&targetCount).Error; err != nil {
		errorResponse(c, http.StatusInternalServerError, "failed to load target count")
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"campaign":     campaign,
		"stats":        stats,
		"target_count": targetCount,
		"shards":       shards,
		"sample_cards": cards,
		"failed_cards": failedCardIDs,
	})
}

func (a *API) GetDebugMessage(c *gin.Context) {
	id, ok := parseUUID(c, "id")
	if !ok {
		return
	}
	msgID := uuid.MustParse(id)
	msg, err := a.query.GetMessage(c.Request.Context(), msgID)
	if err != nil {
		errorResponse(c, http.StatusInternalServerError, "failed to load message")
		return
	}
	if msg == nil {
		errorResponse(c, http.StatusNotFound, "message not found")
		return
	}
	messages, err := a.query.ListCardMessages(c.Request.Context(), msg.CardID, 50)
	if err != nil {
		errorResponse(c, http.StatusInternalServerError, "failed to load message chain")
		return
	}
	c.JSON(http.StatusOK, gin.H{
		"message":       msg,
		"card_messages": messages,
	})
}

func (a *API) GetDebugQueues(c *gin.Context) {
	ctx := c.Request.Context()
	var campaigns []db.Campaign
	if err := a.db.Select("id, name, status, started_at, completed_at, created_at, updated_at").Find(&campaigns).Error; err != nil {
		errorResponse(c, http.StatusInternalServerError, "failed to load campaigns")
		return
	}
	ids := make([]uuid.UUID, 0, len(campaigns))
	for _, campaign := range campaigns {
		ids = append(ids, campaign.ID)
	}
	statsByCampaign, err := a.query.CampaignStatsBatch(ctx, ids)
	if err != nil {
		errorResponse(c, http.StatusInternalServerError, "failed to load campaign stats")
		return
	}

	var shardRows []struct {
		CampaignID uuid.UUID
		Status     string
		Count      int64
		Oldest     *time.Time
	}
	if err := a.db.Model(&db.CampaignShard{}).
		Select("campaign_id, status, COUNT(*) AS count, MIN(created_at) AS oldest").
		Group("campaign_id, status").
		Scan(&shardRows).Error; err != nil {
		errorResponse(c, http.StatusInternalServerError, "failed to load shard queues")
		return
	}

	shardsByCampaign := make(map[uuid.UUID]map[string]gin.H)
	for _, row := range shardRows {
		entry := shardsByCampaign[row.CampaignID]
		if entry == nil {
			entry = make(map[string]gin.H)
			shardsByCampaign[row.CampaignID] = entry
		}
		entry[row.Status] = gin.H{"count": row.Count, "oldest": row.Oldest}
	}

	response := make([]gin.H, 0, len(campaigns))
	for _, campaign := range campaigns {
		stats := statsByCampaign[campaign.ID]
		response = append(response, gin.H{
			"campaign_id": campaign.ID,
			"name":        campaign.Name,
			"status":      campaign.Status,
			"stats":       stats,
			"shards":      shardsByCampaign[campaign.ID],
			"started_at":  campaign.StartedAt,
			"updated_at":  campaign.UpdatedAt,
		})
	}
	c.JSON(http.StatusOK, gin.H{"campaigns": response})
}

func (a *API) GetDebugStuck(c *gin.Context) {
	ctx := c.Request.Context()
	cutoff := time.Now().UTC().Add(-5 * time.Minute)

	var running []db.Campaign
	if err := a.db.Where("status = ?", "running").Find(&running).Error; err != nil {
		errorResponse(c, http.StatusInternalServerError, "failed to load running campaigns")
		return
	}
	ids := make([]uuid.UUID, 0, len(running))
	for _, campaign := range running {
		ids = append(ids, campaign.ID)
	}
	statsByCampaign, err := a.query.CampaignStatsBatch(ctx, ids)
	if err != nil {
		errorResponse(c, http.StatusInternalServerError, "failed to load running campaign stats")
		return
	}

	staleRunning := make([]gin.H, 0)
	for _, campaign := range running {
		stats := statsByCampaign[campaign.ID]
		finished := stats.Total > 0 && stats.Pending == 0 && stats.InProgress == 0 && (stats.Completed+stats.Failed+stats.Skipped) == stats.Total
		if finished {
			staleRunning = append(staleRunning, gin.H{
				"campaign_id": campaign.ID,
				"name":        campaign.Name,
				"status":      campaign.Status,
				"stats":       stats,
				"updated_at":  campaign.UpdatedAt,
			})
		}
	}

	var stalePublishing []db.CampaignShard
	if err := a.db.Where("status = ? AND updated_at < ?", "publishing", cutoff).Order("updated_at ASC").Limit(100).Find(&stalePublishing).Error; err != nil {
		errorResponse(c, http.StatusInternalServerError, "failed to load stale publishing shards")
		return
	}
	var stalePending []db.CampaignShard
	if err := a.db.Where("status = ? AND created_at < ?", "pending", cutoff).Order("created_at ASC").Limit(100).Find(&stalePending).Error; err != nil {
		errorResponse(c, http.StatusInternalServerError, "failed to load stale pending shards")
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"stale_running_campaigns": staleRunning,
		"stale_publishing_shards": stalePublishing,
		"stale_pending_shards":    stalePending,
	})
}
