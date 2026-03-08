package controlplane

import (
	"encoding/hex"
	"fmt"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"go.uber.org/zap"
	"gorm.io/gorm"

	"ota-platform/internal/db"
	scyllastore "ota-platform/internal/scylla"
	"ota-platform/pkg/hexutil"
)

var allowedCampaignTypes = map[string]struct{}{
	"cap_load":       {},
	"applet_install": {},
	"applet_delete":  {},
	"script":         {},
	"install_applet": {},
	"delete_applet":  {},
	"update_applet":  {},
	"send_script":    {},
	"custom_apdu":    {},
}

// CreateCampaignRequest is the request body for POST /api/v1/campaigns.
type CreateCampaignRequest struct {
	Name              string           `json:"name" binding:"required"`
	CampaignType      string           `json:"campaign_type"`
	Type              string           `json:"type"`
	CardIDs           []string         `json:"card_ids"`
	ProfileID         string           `json:"profile_id"`
	CardGroupID       string           `json:"card_group_id"`
	CAPFileID         string           `json:"cap_file_id"`
	ScriptID          string           `json:"script_id"`
	MaxRetries        int              `json:"max_retries"`
	ThrottleSMSPerSec int              `json:"throttle_sms_per_sec"`
	MaxConcatOverride int              `json:"max_concat_override"`
	ScheduledAt       string           `json:"scheduled_at"`
	Commands          []CommandRequest `json:"commands"`
	StartImmediately  bool             `json:"start_immediately"`
	// Type-specific parameters sent by frontend wizard
	MaxBlockSize      int    `json:"max_block_size"`
	LoadFileAID       string `json:"load_file_aid"`
	ModuleAID         string `json:"module_aid"`
	AppAID            string `json:"app_aid"`
	InstallPrivileges string `json:"install_privileges"`
	STKParams         string `json:"stk_params"`
	DeleteAID         string `json:"delete_aid"`
	DeleteRelated     bool   `json:"delete_related"`
	ScriptHex         string `json:"script_hex"`
	TAR               string `json:"tar"`
	ApplicationID     string `json:"application_id"`
}

// CommandRequest represents a single command within a campaign creation request.
type CommandRequest struct {
	ApplicationID  string `json:"application_id"`
	Script         string `json:"script" binding:"required"`
	Sequence       int    `json:"sequence" binding:"required"`
	ExpectResponse bool   `json:"expect_response"`
}

// ListCampaigns handles GET /api/v1/campaigns with pagination, status filter, and search.
func (a *API) ListCampaigns(c *gin.Context) {
	page, pageSize, offset := paginationParams(c)

	query := a.db.Model(&db.Campaign{})

	if status := c.Query("status"); status != "" {
		query = query.Where("status = ?", status)
	}
	if search := c.Query("search"); search != "" {
		query = query.Where("name ILIKE ? ESCAPE '\\'", "%"+escapeLike(search)+"%")
	}

	var total int64
	if err := query.Count(&total).Error; err != nil {
		a.logger.Error("failed to count campaigns", zap.Error(err))
		errorResponse(c, http.StatusInternalServerError, "failed to count campaigns")
		return
	}

	var campaigns []db.Campaign
	if err := query.
		Order("created_at DESC").
		Offset(offset).
		Limit(pageSize).
		Find(&campaigns).Error; err != nil {
		a.logger.Error("failed to list campaigns", zap.Error(err))
		errorResponse(c, http.StatusInternalServerError, "failed to list campaigns")
		return
	}

	// Get campaign IDs for batch stat query
	campaignIDs := make([]uuid.UUID, len(campaigns))
	for i, camp := range campaigns {
		campaignIDs[i] = camp.ID
	}

	statsMap := make(map[uuid.UUID]scyllastore.CampaignStats)
	if a.query != nil && len(campaignIDs) > 0 {
		var err error
		statsMap, err = a.query.CampaignStatsBatch(c.Request.Context(), campaignIDs)
		if err != nil {
			a.logger.Error("failed to load campaign stats from scylla", zap.Error(err))
			errorResponse(c, http.StatusInternalServerError, "failed to load campaign stats")
			return
		}
	}

	// Build enriched response
	type campaignResponse struct {
		db.Campaign
		TotalCards      int64   `json:"total_cards"`
		SuccessCards    int64   `json:"success_cards"`
		FailedCards     int64   `json:"failed_cards"`
		ProgressPercent float64 `json:"progress_percent"`
	}
	enriched := make([]campaignResponse, len(campaigns))
	for i, camp := range campaigns {
		s := statsMap[camp.ID]
		var pct float64
		if s.Total > 0 {
			pct = float64(s.Completed+s.Failed+s.Skipped) / float64(s.Total) * 100
		}
		enriched[i] = campaignResponse{
			Campaign:        camp,
			TotalCards:      s.Total,
			SuccessCards:    s.Completed,
			FailedCards:     s.Failed,
			ProgressPercent: pct,
		}
	}

	paginatedResponse(c, http.StatusOK, enriched, total, page, pageSize)
}

// CreateCampaign handles POST /api/v1/campaigns.
func (a *API) CreateCampaign(c *gin.Context) {
	var req CreateCampaignRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		errorResponse(c, http.StatusBadRequest, err.Error())
		return
	}

	// Accept "type" as alias for "campaign_type"
	if req.CampaignType == "" && req.Type != "" {
		req.CampaignType = req.Type
	}
	if req.CampaignType == "" {
		errorResponse(c, http.StatusBadRequest, "campaign_type or type is required")
		return
	}
	if _, ok := allowedCampaignTypes[req.CampaignType]; !ok {
		errorResponse(c, http.StatusBadRequest, "invalid campaign_type")
		return
	}

	// Auto-generate a placeholder command when the frontend sends type-specific
	// parameters instead of an explicit commands array.
	if len(req.Commands) == 0 {
		// Wizard campaigns require an explicit application_id for correct OTA security.
		if req.ApplicationID == "" {
			errorResponse(c, http.StatusBadRequest, "application_id is required for wizard campaigns")
			return
		}
		// Validate the application exists and get its profile.
		appID, err := uuid.Parse(req.ApplicationID)
		if err != nil {
			errorResponse(c, http.StatusBadRequest, "invalid application_id")
			return
		}
		var app db.Application
		if err := a.db.First(&app, "id = ?", appID).Error; err != nil {
			if err == gorm.ErrRecordNotFound {
				errorResponse(c, http.StatusBadRequest, "application not found")
				return
			}
			errorResponse(c, http.StatusInternalServerError, "failed to look up application")
			return
		}

		placeholder := CommandRequest{
			Sequence:       1,
			ExpectResponse: true,
			ApplicationID:  req.ApplicationID,
		}
		switch req.CampaignType {
		case "install_applet":
			script := "install"
			if req.LoadFileAID != "" {
				script += ":load_file_aid=" + req.LoadFileAID
			}
			if req.ModuleAID != "" {
				script += ":module_aid=" + req.ModuleAID
			}
			if req.AppAID != "" {
				script += ":app_aid=" + req.AppAID
			}
			if req.InstallPrivileges != "" {
				script += ":privileges=" + req.InstallPrivileges
			}
			if req.STKParams != "" {
				script += ":stk_params=" + req.STKParams
			}
			placeholder.Script = hex.EncodeToString([]byte(script))
		case "delete_applet":
			script := "delete"
			if req.DeleteAID != "" {
				script += ":aid=" + req.DeleteAID
			}
			if req.DeleteRelated {
				script += ":related=true"
			}
			placeholder.Script = hex.EncodeToString([]byte(script))
		case "send_script":
			if req.ScriptHex != "" {
				placeholder.Script = req.ScriptHex
			} else {
				placeholder.Script = hex.EncodeToString([]byte("noop"))
			}
		case "custom_apdu":
			placeholder.Script = hex.EncodeToString([]byte("custom"))
		default:
			placeholder.Script = hex.EncodeToString([]byte("placeholder"))
		}
		req.Commands = []CommandRequest{placeholder}
	}

	campaign := db.Campaign{
		Name:         req.Name,
		CampaignType: req.CampaignType,
		Status:       "pending",
		MaxRetries:   req.MaxRetries,
	}

	if req.ThrottleSMSPerSec > 0 {
		v := req.ThrottleSMSPerSec
		campaign.ThrottleSMSPerSec = &v
	}
	if req.MaxConcatOverride > 0 {
		v := req.MaxConcatOverride
		campaign.MaxConcatOverride = &v
	}

	if req.CAPFileID != "" {
		capID, err := uuid.Parse(req.CAPFileID)
		if err != nil {
			errorResponse(c, http.StatusBadRequest, "invalid cap_file_id")
			return
		}
		campaign.CAPFileID = &capID
	}

	if req.ScriptID != "" {
		scriptID, err := uuid.Parse(req.ScriptID)
		if err != nil {
			errorResponse(c, http.StatusBadRequest, "invalid script_id")
			return
		}
		campaign.ScriptID = &scriptID
	}

	if req.ScheduledAt != "" {
		t, err := time.Parse(time.RFC3339, req.ScheduledAt)
		if err != nil {
			errorResponse(c, http.StatusBadRequest, "invalid scheduled_at format, use RFC3339")
			return
		}
		campaign.ScheduledAt = &t
	}

	// Resolve target cards
	var cardIDs []uuid.UUID

	// Explicit card IDs
	for _, idStr := range req.CardIDs {
		id, err := uuid.Parse(idStr)
		if err != nil {
			errorResponse(c, http.StatusBadRequest, "invalid card_id: "+idStr)
			return
		}
		cardIDs = append(cardIDs, id)
	}

	// Cards by profile
	if req.ProfileID != "" {
		profileID, err := uuid.Parse(req.ProfileID)
		if err != nil {
			errorResponse(c, http.StatusBadRequest, "invalid profile_id")
			return
		}
		var profileCards []db.Card
		if err := a.db.Where("profile_id = ? AND status = 'active'", profileID).Find(&profileCards).Error; err != nil {
			a.logger.Error("failed to fetch profile cards", zap.Error(err))
			errorResponse(c, http.StatusInternalServerError, "failed to fetch profile cards")
			return
		}
		for _, card := range profileCards {
			cardIDs = append(cardIDs, card.ID)
		}
	}

	// Cards by group
	if req.CardGroupID != "" {
		groupID, err := uuid.Parse(req.CardGroupID)
		if err != nil {
			errorResponse(c, http.StatusBadRequest, "invalid card_group_id")
			return
		}
		var members []db.CardGroupMember
		if err := a.db.Where("card_group_id = ?", groupID).Find(&members).Error; err != nil {
			a.logger.Error("failed to fetch group members", zap.Error(err))
			errorResponse(c, http.StatusInternalServerError, "failed to fetch group members")
			return
		}
		for _, m := range members {
			cardIDs = append(cardIDs, m.CardID)
		}
	}

	// Deduplicate card IDs
	seen := make(map[uuid.UUID]bool)
	var uniqueCardIDs []uuid.UUID
	for _, id := range cardIDs {
		if !seen[id] {
			seen[id] = true
			uniqueCardIDs = append(uniqueCardIDs, id)
		}
	}

	if len(uniqueCardIDs) == 0 {
		errorResponse(c, http.StatusBadRequest, "no target cards specified")
		return
	}

	// For wizard campaigns, validate all target cards belong to the same profile as the application.
	if req.ApplicationID != "" {
		appID, _ := uuid.Parse(req.ApplicationID)
		var app db.Application
		if err := a.db.Select("profile_id").First(&app, "id = ?", appID).Error; err != nil {
			if err == gorm.ErrRecordNotFound {
				errorResponse(c, http.StatusBadRequest, "application not found")
				return
			}
			a.logger.Error("failed to fetch application for profile validation", zap.Error(err))
			errorResponse(c, http.StatusInternalServerError, "failed to validate application profile")
			return
		}
		var mismatchCount int64
		a.db.Model(&db.Card{}).
			Where("id IN ? AND profile_id != ?", uniqueCardIDs, app.ProfileID).
			Count(&mismatchCount)
		if mismatchCount > 0 {
			errorResponse(c, http.StatusBadRequest,
				fmt.Sprintf("%d cards do not belong to the application's profile", mismatchCount))
			return
		}
	}

	// Use a transaction for campaign + commands + cards
	err := a.db.Transaction(func(tx *gorm.DB) error {
		if err := tx.Create(&campaign).Error; err != nil {
			return err
		}

		// Create campaign commands
		for _, cmdInput := range req.Commands {
			appID, err := uuid.Parse(cmdInput.ApplicationID)
			if err != nil {
				return err
			}
			scriptBytes, err := hexutil.Decode(cmdInput.Script)
			if err != nil {
				return err
			}
			cmd := db.CampaignCommand{
				CampaignID:     campaign.ID,
				Sequence:       cmdInput.Sequence,
				ApplicationID:  appID,
				Script:         scriptBytes,
				ExpectResponse: cmdInput.ExpectResponse,
			}
			if err := tx.Create(&cmd).Error; err != nil {
				return err
			}
		}

		// Create campaign targets in batches to avoid large per-row ORM overhead.
		targets := make([]db.CampaignTarget, 0, len(uniqueCardIDs))
		for _, cardID := range uniqueCardIDs {
			targets = append(targets, db.CampaignTarget{
				CampaignID: campaign.ID,
				CardID:     cardID,
			})
		}
		if err := tx.CreateInBatches(targets, 500).Error; err != nil {
			return err
		}

		return nil
	})

	if err != nil {
		a.logger.Error("failed to create campaign", zap.Error(err))
		errorResponse(c, http.StatusInternalServerError, "failed to create campaign")
		return
	}

	// Reload with associations
	a.db.Preload("CampaignCommands").First(&campaign, "id = ?", campaign.ID)

	// Start immediately if requested
	if req.StartImmediately {
		if startErr := a.campaign.StartCampaign(c.Request.Context(), campaign.ID); startErr != nil {
			a.logger.Error("campaign created but failed to start immediately", zap.Error(startErr))
			// Return the campaign anyway; it was created successfully
			c.JSON(http.StatusCreated, gin.H{
				"data":        campaign,
				"card_count":  len(uniqueCardIDs),
				"start_error": startErr.Error(),
			})
			return
		}
		// Reload to reflect running status
		a.db.First(&campaign, "id = ?", campaign.ID)
	}

	c.JSON(http.StatusCreated, gin.H{
		"data":       campaign,
		"card_count": len(uniqueCardIDs),
	})
}

// GetCampaign handles GET /api/v1/campaigns/:id with stats and paginated cards.
// Returns a flat response matching the frontend contract.
func (a *API) GetCampaign(c *gin.Context) {
	id, ok := parseUUID(c, "id")
	if !ok {
		return
	}

	var campaign db.Campaign
	if err := a.db.
		Preload("CampaignCommands", func(tx *gorm.DB) *gorm.DB {
			return tx.Order("sequence ASC")
		}).
		Preload("CampaignCommands.Application").
		Preload("CAPFile").
		Preload("Script").
		First(&campaign, "id = ?", id).Error; err != nil {
		if err == gorm.ErrRecordNotFound {
			errorResponse(c, http.StatusNotFound, "campaign not found")
			return
		}
		a.logger.Error("failed to get campaign", zap.Error(err))
		errorResponse(c, http.StatusInternalServerError, "failed to get campaign")
		return
	}

	stats := scyllastore.CampaignStats{}
	if a.query != nil {
		var err error
		stats, err = a.query.CampaignStats(c.Request.Context(), uuid.MustParse(id))
		if err != nil {
			a.logger.Error("failed to load campaign stats from scylla", zap.Error(err))
			errorResponse(c, http.StatusInternalServerError, "failed to load campaign stats")
			return
		}
	}

	var progressPct float64
	if stats.Total > 0 {
		progressPct = float64(stats.Completed+stats.Failed+stats.Skipped) / float64(stats.Total) * 100
	}

	// Paginated cards query from Scylla query tables
	cardPage, cardPageSize, cardOffset := paginationParams(c)
	statusFilter := c.Query("status")
	var campaignCards []scyllastore.CampaignCardView
	if a.query != nil {
		var err error
		campaignCards, err = a.query.ListCampaignCards(c.Request.Context(), uuid.MustParse(id), statusFilter)
		if err != nil {
			a.logger.Error("failed to load campaign cards from scylla", zap.Error(err))
			errorResponse(c, http.StatusInternalServerError, "failed to load campaign cards")
			return
		}
	}
	cardTotal := int64(len(campaignCards))
	cardTotalPages := int(cardTotal) / cardPageSize
	if int(cardTotal)%cardPageSize != 0 {
		cardTotalPages++
	}
	if cardOffset > len(campaignCards) {
		cardOffset = len(campaignCards)
	}
	cardEnd := cardOffset + cardPageSize
	if cardEnd > len(campaignCards) {
		cardEnd = len(campaignCards)
	}
	pageCards := campaignCards[cardOffset:cardEnd]

	// Build card response items matching frontend expectations
	type cardItem struct {
		ID             string  `json:"id"`
		ICCID          string  `json:"iccid"`
		MSISDN         string  `json:"msisdn"`
		CurrentCommand int     `json:"current_command"`
		TotalCommands  int     `json:"total_commands"`
		Status         string  `json:"status"`
		ErrorMessage   *string `json:"error_message"`
		Attempts       int     `json:"attempts"`
		UpdatedAt      string  `json:"updated_at"`
	}
	totalCommands := len(campaign.CampaignCommands)
	cardIDs := make([]uuid.UUID, 0, len(pageCards))
	for _, cc := range pageCards {
		if parsed, err := uuid.Parse(cc.CardID); err == nil {
			cardIDs = append(cardIDs, parsed)
		}
	}
	var cards []db.Card
	cardMap := make(map[string]db.Card, len(cardIDs))
	if len(cardIDs) > 0 {
		if err := a.db.Where("id IN ?", cardIDs).Find(&cards).Error; err != nil {
			a.logger.Error("failed to load cards for campaign detail", zap.Error(err))
			errorResponse(c, http.StatusInternalServerError, "failed to load campaign cards")
			return
		}
		for _, card := range cards {
			cardMap[card.ID.String()] = card
		}
	}
	cardItems := make([]cardItem, len(pageCards))
	for i, cc := range pageCards {
		card := cardMap[cc.CardID]
		cardItems[i] = cardItem{
			ID:             cc.CardID,
			ICCID:          card.ICCID,
			MSISDN:         card.MSISDN,
			CurrentCommand: cc.CurrentStep,
			TotalCommands:  totalCommands,
			Status:         cc.Status,
			ErrorMessage:   stringPtrOrNil(cc.LastError),
			Attempts:       cc.RetryCount,
			UpdatedAt:      cc.UpdatedAt.Format(time.RFC3339),
		}
	}

	// Build commands response matching frontend expectations
	type commandItem struct {
		ID             string `json:"id"`
		Sequence       int    `json:"sequence"`
		Type           string `json:"type"`
		Description    string `json:"description"`
		APDU           string `json:"apdu"`
		ExpectResponse bool   `json:"expect_response"`
		TimeoutSeconds int    `json:"timeout_seconds"`
	}
	cmdItems := make([]commandItem, len(campaign.CampaignCommands))
	for i, cmd := range campaign.CampaignCommands {
		cmdItems[i] = commandItem{
			ID:             cmd.ID.String(),
			Sequence:       cmd.Sequence,
			Type:           campaign.CampaignType,
			Description:    cmd.Application.Name,
			APDU:           hex.EncodeToString(cmd.Script),
			ExpectResponse: cmd.ExpectResponse,
			TimeoutSeconds: 30,
		}
	}

	// Return flat response — no {data: ...} envelope
	c.JSON(http.StatusOK, gin.H{
		"id":                campaign.ID,
		"name":              campaign.Name,
		"status":            campaign.Status,
		"campaign_type":     campaign.CampaignType,
		"cap_file_id":       campaign.CAPFileID,
		"script_id":         campaign.ScriptID,
		"scheduled_at":      campaign.ScheduledAt,
		"max_retries":       campaign.MaxRetries,
		"started_at":        campaign.StartedAt,
		"completed_at":      campaign.CompletedAt,
		"created_at":        campaign.CreatedAt,
		"updated_at":        campaign.UpdatedAt,
		"total_cards":       stats.Total,
		"pending_cards":     stats.Pending,
		"in_progress_cards": stats.InProgress,
		"success_cards":     stats.Completed,
		"failed_cards":      stats.Failed,
		"progress_percent":  progressPct,
		"commands":          cmdItems,
		"cards": gin.H{
			"data":        cardItems,
			"total":       cardTotal,
			"page":        cardPage,
			"page_size":   cardPageSize,
			"total_pages": cardTotalPages,
		},
	})
}

// StartCampaign handles POST /api/v1/campaigns/:id/start.
func (a *API) StartCampaign(c *gin.Context) {
	id, ok := parseUUID(c, "id")
	if !ok {
		return
	}

	campaignID, _ := uuid.Parse(id)
	if err := a.campaign.StartCampaign(c.Request.Context(), campaignID); err != nil {
		a.logger.Error("failed to start campaign", zap.Error(err))
		errorResponse(c, http.StatusBadRequest, err.Error())
		return
	}

	c.JSON(http.StatusOK, gin.H{"message": "campaign started"})
}

// PauseCampaign handles POST /api/v1/campaigns/:id/pause.
func (a *API) PauseCampaign(c *gin.Context) {
	id, ok := parseUUID(c, "id")
	if !ok {
		return
	}

	campaignID, _ := uuid.Parse(id)
	if err := a.campaign.PauseCampaign(c.Request.Context(), campaignID); err != nil {
		a.logger.Error("failed to pause campaign", zap.Error(err))
		errorResponse(c, http.StatusBadRequest, err.Error())
		return
	}

	c.JSON(http.StatusOK, gin.H{"message": "campaign paused"})
}

// ResumeCampaign handles POST /api/v1/campaigns/:id/resume.
func (a *API) ResumeCampaign(c *gin.Context) {
	id, ok := parseUUID(c, "id")
	if !ok {
		return
	}

	campaignID, _ := uuid.Parse(id)
	if err := a.campaign.ResumeCampaign(c.Request.Context(), campaignID); err != nil {
		a.logger.Error("failed to resume campaign", zap.Error(err))
		errorResponse(c, http.StatusBadRequest, err.Error())
		return
	}

	c.JSON(http.StatusOK, gin.H{"message": "campaign resumed"})
}

// AbortCampaign handles POST /api/v1/campaigns/:id/abort.
func (a *API) AbortCampaign(c *gin.Context) {
	id, ok := parseUUID(c, "id")
	if !ok {
		return
	}

	campaignID, _ := uuid.Parse(id)
	if err := a.campaign.AbortCampaign(c.Request.Context(), campaignID); err != nil {
		a.logger.Error("failed to abort campaign", zap.Error(err))
		errorResponse(c, http.StatusBadRequest, err.Error())
		return
	}

	c.JSON(http.StatusOK, gin.H{"message": "campaign aborted"})
}

// RetryFailedCards handles POST /api/v1/campaigns/:id/retry-failed.
func (a *API) RetryFailedCards(c *gin.Context) {
	id, ok := parseUUID(c, "id")
	if !ok {
		return
	}

	campaignID, err := uuid.Parse(id)
	if err != nil {
		errorResponse(c, http.StatusBadRequest, "invalid campaign id")
		return
	}

	// Verify campaign exists
	var campaign db.Campaign
	if err := a.db.First(&campaign, "id = ?", campaignID).Error; err != nil {
		if err == gorm.ErrRecordNotFound {
			errorResponse(c, http.StatusNotFound, "campaign not found")
			return
		}
		a.logger.Error("failed to get campaign", zap.Error(err))
		errorResponse(c, http.StatusInternalServerError, "failed to get campaign")
		return
	}

	// Find the first command sequence to reset cards to
	var firstCmd db.CampaignCommand
	if err := a.db.Where("campaign_id = ?", campaignID).Order("sequence ASC").First(&firstCmd).Error; err != nil {
		a.logger.Error("failed to find first command", zap.Error(err))
		errorResponse(c, http.StatusInternalServerError, "failed to find campaign commands")
		return
	}

	var failedCardIDs []uuid.UUID
	if a.query != nil {
		failedCardIDs, err = a.query.ListFailedCardIDs(c.Request.Context(), campaignID)
		if err != nil {
			a.logger.Error("failed to load failed cards from scylla", zap.Error(err))
			errorResponse(c, http.StatusInternalServerError, "failed to load failed cards")
			return
		}
		if err := a.query.ResetFailedCards(c.Request.Context(), campaignID, failedCardIDs, firstCmd.Sequence, time.Now()); err != nil {
			a.logger.Error("failed to reset failed cards in scylla", zap.Error(err))
			errorResponse(c, http.StatusInternalServerError, "failed to reset failed cards")
			return
		}
	}

	// If campaign was completed/failed, set it back to pending so it can be restarted
	if campaign.Status == "completed" || campaign.Status == "completed_with_errors" || campaign.Status == "failed" {
		a.db.Model(&campaign).Updates(map[string]interface{}{
			"status":       "pending",
			"completed_at": nil,
		})
	}

	c.JSON(http.StatusOK, gin.H{
		"message":     "failed cards reset to pending",
		"cards_reset": len(failedCardIDs),
	})
}

func stringPtrOrNil(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}
