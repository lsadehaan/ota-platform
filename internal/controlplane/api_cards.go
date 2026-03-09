package controlplane

import (
	"encoding/csv"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"go.uber.org/zap"
	"gorm.io/gorm"

	"ota-platform/internal/db"
	redispkg "ota-platform/internal/redis"
	"ota-platform/pkg/hexutil"
)

// CreateCardRequest is the request body for POST /api/v1/cards.
type CreateCardRequest struct {
	ICCID     string `json:"iccid" binding:"required"`
	IMSI      string `json:"imsi" binding:"required"`
	MSISDN    string `json:"msisdn" binding:"required"`
	ProfileID string `json:"profile_id" binding:"required"`
	EncKey    string `json:"enc_key" binding:"required"`
	AuthKey   string `json:"auth_key" binding:"required"`
	KEK       string `json:"kek"`
	Status    string `json:"status"`
}

// UpdateCardRequest is the request body for PUT /api/v1/cards/:id.
type UpdateCardRequest struct {
	ICCID     *string `json:"iccid"`
	IMSI      *string `json:"imsi"`
	MSISDN    *string `json:"msisdn"`
	ProfileID *string `json:"profile_id"`
	EncKey    *string `json:"enc_key"`
	AuthKey   *string `json:"auth_key"`
	KEK       *string `json:"kek"`
	Status    *string `json:"status"`
}

// CardGroupRequest is the request body for card group operations.
type CardGroupRequest struct {
	Name        string  `json:"name" binding:"required"`
	Description *string `json:"description"`
}

// CardGroupMembersRequest is the request body for adding/removing group members.
type CardGroupMembersRequest struct {
	CardIDs []string `json:"card_ids" binding:"required"`
}

// ListCards handles GET /api/v1/cards with pagination, search, and filters.
func (a *API) ListCards(c *gin.Context) {
	page, pageSize, offset := paginationParams(c)

	query := a.db.Model(&db.Card{})

	if search := c.Query("q"); search != "" {
		escaped := escapeLike(search)
		query = query.Where("iccid ILIKE ? ESCAPE '\\' OR imsi ILIKE ? ESCAPE '\\' OR msisdn ILIKE ? ESCAPE '\\'",
			escaped+"%", escaped+"%", escaped+"%")
	}
	if profileID := c.Query("profile_id"); profileID != "" {
		query = query.Where("profile_id = ?", profileID)
	}
	if status := c.Query("status"); status != "" {
		query = query.Where("status = ?", status)
	}
	if groupID := c.Query("group_id"); groupID != "" {
		query = query.Where("id IN (SELECT card_id FROM card_group_members WHERE card_group_id = ?)", groupID)
	}

	var total int64
	if err := query.Count(&total).Error; err != nil {
		a.logger.Error("failed to count cards", zap.Error(err))
		errorResponse(c, http.StatusInternalServerError, "failed to count cards")
		return
	}

	var cards []db.Card
	if err := query.
		Preload("Profile").
		Order("created_at DESC").
		Offset(offset).
		Limit(pageSize).
		Find(&cards).Error; err != nil {
		a.logger.Error("failed to list cards", zap.Error(err))
		errorResponse(c, http.StatusInternalServerError, "failed to list cards")
		return
	}

	paginatedResponse(c, http.StatusOK, cards, total, page, pageSize)
}

// CreateCard handles POST /api/v1/cards.
func (a *API) CreateCard(c *gin.Context) {
	var req CreateCardRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		errorResponse(c, http.StatusBadRequest, err.Error())
		return
	}

	profileID, err := uuid.Parse(req.ProfileID)
	if err != nil {
		errorResponse(c, http.StatusBadRequest, "invalid profile_id")
		return
	}

	// Verify profile exists
	var profile db.Profile
	if err := a.db.First(&profile, "id = ?", profileID).Error; err != nil {
		if err == gorm.ErrRecordNotFound {
			errorResponse(c, http.StatusBadRequest, "profile not found")
			return
		}
		a.logger.Error("failed to check profile", zap.Error(err))
		errorResponse(c, http.StatusInternalServerError, "failed to check profile")
		return
	}

	encKey, err := hexutil.Decode(req.EncKey)
	if err != nil {
		errorResponse(c, http.StatusBadRequest, "invalid enc_key hex")
		return
	}
	authKey, err := hexutil.Decode(req.AuthKey)
	if err != nil {
		errorResponse(c, http.StatusBadRequest, "invalid auth_key hex")
		return
	}

	card := db.Card{
		ICCID:     req.ICCID,
		IMSI:      req.IMSI,
		MSISDN:    req.MSISDN,
		ProfileID: profileID,
		EncKey:    encKey,
		AuthKey:   authKey,
		Status:    "active",
	}

	if req.KEK != "" {
		kek, err := hexutil.Decode(req.KEK)
		if err != nil {
			errorResponse(c, http.StatusBadRequest, "invalid kek hex")
			return
		}
		card.KEK = kek
	}
	if req.Status != "" {
		card.Status = req.Status
	}

	if err := a.db.Create(&card).Error; err != nil {
		if isUniqueConstraintError(err) {
			errorResponse(c, http.StatusConflict, "a card with this ICCID, IMSI, or MSISDN already exists")
			return
		}
		a.logger.Error("failed to create card", zap.Error(err))
		errorResponse(c, http.StatusInternalServerError, "failed to create card")
		return
	}

	c.JSON(http.StatusCreated, gin.H{"data": card})
}

// GetCard handles GET /api/v1/cards/:id with profile, counters, and recent messages.
func (a *API) GetCard(c *gin.Context) {
	id, ok := parseUUID(c, "id")
	if !ok {
		return
	}

	var card db.Card
	if err := a.db.Preload("Profile").First(&card, "id = ?", id).Error; err != nil {
		if err == gorm.ErrRecordNotFound {
			errorResponse(c, http.StatusNotFound, "card not found")
			return
		}
		a.logger.Error("failed to get card", zap.Error(err))
		errorResponse(c, http.StatusInternalServerError, "failed to get card")
		return
	}

	// Load counters
	var counters []db.CardCounter
	a.db.Preload("Application").Where("card_id = ?", id).Find(&counters)

	// Load recent messages (last 10)
	recentMessages, err := a.query.ListCardMessages(c.Request.Context(), card.ID, 10)
	if err != nil {
		a.logger.Error("failed to get recent card messages", zap.Error(err))
		errorResponse(c, http.StatusInternalServerError, "failed to get card messages")
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"data":            card,
		"counters":        counters,
		"recent_messages": recentMessages,
	})
}

// UpdateCard handles PUT /api/v1/cards/:id.
func (a *API) UpdateCard(c *gin.Context) {
	id, ok := parseUUID(c, "id")
	if !ok {
		return
	}

	var card db.Card
	if err := a.db.First(&card, "id = ?", id).Error; err != nil {
		if err == gorm.ErrRecordNotFound {
			errorResponse(c, http.StatusNotFound, "card not found")
			return
		}
		a.logger.Error("failed to find card", zap.Error(err))
		errorResponse(c, http.StatusInternalServerError, "failed to find card")
		return
	}

	var req UpdateCardRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		errorResponse(c, http.StatusBadRequest, err.Error())
		return
	}

	updates := make(map[string]interface{})

	if req.ICCID != nil {
		updates["iccid"] = *req.ICCID
	}
	if req.IMSI != nil {
		updates["imsi"] = *req.IMSI
	}
	if req.MSISDN != nil {
		updates["msisdn"] = *req.MSISDN
	}
	if req.ProfileID != nil {
		profileID, err := uuid.Parse(*req.ProfileID)
		if err != nil {
			errorResponse(c, http.StatusBadRequest, "invalid profile_id")
			return
		}
		updates["profile_id"] = profileID
	}
	if req.EncKey != nil {
		encKey, err := hexutil.Decode(*req.EncKey)
		if err != nil {
			errorResponse(c, http.StatusBadRequest, "invalid enc_key hex")
			return
		}
		updates["enc_key"] = encKey
	}
	if req.AuthKey != nil {
		authKey, err := hexutil.Decode(*req.AuthKey)
		if err != nil {
			errorResponse(c, http.StatusBadRequest, "invalid auth_key hex")
			return
		}
		updates["auth_key"] = authKey
	}
	if req.KEK != nil {
		kek, err := hexutil.Decode(*req.KEK)
		if err != nil {
			errorResponse(c, http.StatusBadRequest, "invalid kek hex")
			return
		}
		updates["kek"] = kek
	}
	if req.Status != nil {
		updates["status"] = *req.Status
	}

	if len(updates) == 0 {
		errorResponse(c, http.StatusBadRequest, "no fields to update")
		return
	}

	if err := a.db.Model(&card).Updates(updates).Error; err != nil {
		a.logger.Error("failed to update card", zap.Error(err))
		errorResponse(c, http.StatusInternalServerError, "failed to update card")
		return
	}

	a.db.Preload("Profile").First(&card, "id = ?", id)
	c.JSON(http.StatusOK, gin.H{"data": card})
}

// DeleteCard handles DELETE /api/v1/cards/:id.
func (a *API) DeleteCard(c *gin.Context) {
	id, ok := parseUUID(c, "id")
	if !ok {
		return
	}

	result := a.db.Delete(&db.Card{}, "id = ?", id)
	if result.Error != nil {
		a.logger.Error("failed to delete card", zap.Error(result.Error))
		errorResponse(c, http.StatusInternalServerError, "failed to delete card")
		return
	}
	if result.RowsAffected == 0 {
		errorResponse(c, http.StatusNotFound, "card not found")
		return
	}

	c.JSON(http.StatusOK, gin.H{"message": "card deleted"})
}

// GetCardCounters handles GET /api/v1/cards/:id/counters.
func (a *API) GetCardCounters(c *gin.Context) {
	id, ok := parseUUID(c, "id")
	if !ok {
		return
	}

	// Verify card exists
	var card db.Card
	if err := a.db.First(&card, "id = ?", id).Error; err != nil {
		if err == gorm.ErrRecordNotFound {
			errorResponse(c, http.StatusNotFound, "card not found")
			return
		}
		a.logger.Error("failed to find card", zap.Error(err))
		errorResponse(c, http.StatusInternalServerError, "failed to find card")
		return
	}

	var counters []db.CardCounter
	if err := a.db.
		Preload("Application").
		Where("card_id = ?", id).
		Find(&counters).Error; err != nil {
		a.logger.Error("failed to get card counters", zap.Error(err))
		errorResponse(c, http.StatusInternalServerError, "failed to get card counters")
		return
	}

	// Build response with application names
	type counterResponse struct {
		CardID          uuid.UUID `json:"card_id"`
		ApplicationID   uuid.UUID `json:"application_id"`
		ApplicationName string    `json:"application_name"`
		CounterValue    int64     `json:"counter_value"`
	}

	result := make([]counterResponse, 0, len(counters))
	for _, ctr := range counters {
		appName := ""
		if ctr.Application.Name != "" {
			appName = ctr.Application.Name
		}
		result = append(result, counterResponse{
			CardID:          ctr.CardID,
			ApplicationID:   ctr.ApplicationID,
			ApplicationName: appName,
			CounterValue:    ctr.CounterValue,
		})
	}

	c.JSON(http.StatusOK, gin.H{"data": result})
}

// ImportCards handles POST /api/v1/cards/import with CSV file upload.
// Uses PostgreSQL COPY protocol for bulk inserts (5-10x faster than INSERT)
// and pre-warms the Redis card key cache to avoid cold-start DB queries
// when campaigns activate imported cards.
func (a *API) ImportCards(c *gin.Context) {
	c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, 1<<30) // 1GB
	file, _, err := c.Request.FormFile("file")
	if err != nil {
		errorResponse(c, http.StatusBadRequest, "file is required")
		return
	}
	defer file.Close()

	reader := csv.NewReader(file)
	reader.ReuseRecord = true

	// Read and validate header
	header, err := reader.Read()
	if err != nil {
		errorResponse(c, http.StatusBadRequest, "failed to read CSV header")
		return
	}

	// Map header columns to indices
	colMap := make(map[string]int)
	for i, col := range header {
		colMap[strings.TrimSpace(strings.ToLower(col))] = i
	}

	requiredCols := []string{"iccid", "imsi", "msisdn", "profile_name", "enc_key", "auth_key"}
	for _, col := range requiredCols {
		if _, exists := colMap[col]; !exists {
			errorResponse(c, http.StatusBadRequest, "missing required CSV column: "+col)
			return
		}
	}

	// Get a dedicated SQL connection for COPY operations (temp table needs
	// same connection for its entire lifetime).
	sqlDB, err := a.db.DB()
	if err != nil {
		a.logger.Error("failed to get sql.DB", zap.Error(err))
		errorResponse(c, http.StatusInternalServerError, "database error")
		return
	}
	conn, err := sqlDB.Conn(c.Request.Context())
	if err != nil {
		a.logger.Error("failed to get db connection", zap.Error(err))
		errorResponse(c, http.StatusInternalServerError, "database error")
		return
	}
	defer conn.Close()

	// Cache profile name -> ID lookups
	profileCache := make(map[string]uuid.UUID)

	const batchSize = 5000

	var created, skipped, errCount int
	var errors []string
	lineNum := 1
	batch := make([]db.Card, 0, batchSize)

	flushBatch := func() error {
		if len(batch) == 0 {
			return nil
		}

		result, err := db.BulkInsertCards(c.Request.Context(), conn, batch)
		if err != nil {
			return err
		}
		created += int(result.Inserted)
		skipped += int(result.Skipped)

		// Pre-warm Redis card key cache for inserted cards.
		if a.rdb != nil && len(result.InsertedIDs) > 0 {
			insertedSet := make(map[uuid.UUID]struct{}, len(result.InsertedIDs))
			for _, id := range result.InsertedIDs {
				insertedSet[id] = struct{}{}
			}

			keysToCache := make(map[string]*redispkg.CardKeys, len(result.InsertedIDs))
			for i := range batch {
				if _, ok := insertedSet[batch[i].ID]; ok {
					keysToCache[batch[i].ID.String()] = &redispkg.CardKeys{
						EncKey:    batch[i].EncKey,
						AuthKey:   batch[i].AuthKey,
						KEK:       batch[i].KEK,
						ProfileID: batch[i].ProfileID.String(),
						MSISDN:    batch[i].MSISDN,
					}
				}
			}

			if err := a.rdb.BulkCacheCardKeys(c.Request.Context(), keysToCache); err != nil {
				a.logger.Warn("failed to pre-warm card key cache", zap.Error(err))
				// Non-fatal: keys will be loaded on demand.
			}
		}

		batch = batch[:0]
		return nil
	}

	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			lineNum++
			errCount++
			errors = append(errors, fmt.Sprintf("line %d: %s", lineNum, err.Error()))
			continue
		}
		lineNum++

		iccid := strings.TrimSpace(record[colMap["iccid"]])
		imsi := strings.TrimSpace(record[colMap["imsi"]])
		msisdn := strings.TrimSpace(record[colMap["msisdn"]])
		profileName := strings.TrimSpace(record[colMap["profile_name"]])
		encKeyHex := strings.TrimSpace(record[colMap["enc_key"]])
		authKeyHex := strings.TrimSpace(record[colMap["auth_key"]])

		if iccid == "" || imsi == "" || msisdn == "" || profileName == "" || encKeyHex == "" || authKeyHex == "" {
			skipped++
			errors = append(errors, fmt.Sprintf("line %d: missing required fields", lineNum))
			continue
		}

		// Resolve profile
		profileID, ok := profileCache[profileName]
		if !ok {
			var profile db.Profile
			if err := a.db.Where("name = ?", profileName).First(&profile).Error; err != nil {
				errCount++
				errors = append(errors, fmt.Sprintf("line %d: profile not found: %s", lineNum, profileName))
				continue
			}
			profileID = profile.ID
			profileCache[profileName] = profileID
		}

		encKey, err := hexutil.Decode(encKeyHex)
		if err != nil {
			errCount++
			errors = append(errors, fmt.Sprintf("line %d: invalid enc_key hex", lineNum))
			continue
		}
		authKey, err := hexutil.Decode(authKeyHex)
		if err != nil {
			errCount++
			errors = append(errors, fmt.Sprintf("line %d: invalid auth_key hex", lineNum))
			continue
		}

		batch = append(batch, db.Card{
			ICCID:     iccid,
			IMSI:      imsi,
			MSISDN:    msisdn,
			ProfileID: profileID,
			EncKey:    encKey,
			AuthKey:   authKey,
			Status:    "active",
		})

		if len(batch) >= batchSize {
			if err := flushBatch(); err != nil {
				a.logger.Error("failed to import card batch", zap.Error(err))
				errorResponse(c, http.StatusInternalServerError, "failed to import cards")
				return
			}

			if lineNum%50000 == 0 {
				a.logger.Info("import progress", zap.Int("lines", lineNum), zap.Int("created", created), zap.Int("skipped", skipped))
			}
		}
	}

	if err := flushBatch(); err != nil {
		a.logger.Error("failed to import final card batch", zap.Error(err))
		errorResponse(c, http.StatusInternalServerError, "failed to import cards")
		return
	}

	a.logger.Info("card import complete",
		zap.Int("created", created),
		zap.Int("skipped", skipped),
		zap.Int("errors", errCount),
	)

	c.JSON(http.StatusOK, gin.H{
		"created": created,
		"skipped": skipped,
		"errors":  errCount,
		"details": errors,
	})
}

func isUniqueConstraintError(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "duplicate key value") ||
		strings.Contains(msg, "unique constraint failed") ||
		strings.Contains(msg, "violates unique constraint")
}

// ExportCards handles GET /api/v1/cards/export as CSV download.
func (a *API) ExportCards(c *gin.Context) {
	query := a.db.Model(&db.Card{}).Preload("Profile")

	if profileID := c.Query("profile_id"); profileID != "" {
		query = query.Where("profile_id = ?", profileID)
	}
	if status := c.Query("status"); status != "" {
		query = query.Where("status = ?", status)
	}
	if groupID := c.Query("group_id"); groupID != "" {
		query = query.Where("id IN (SELECT card_id FROM card_group_members WHERE card_group_id = ?)", groupID)
	}

	c.Header("Content-Type", "text/csv")
	c.Header("Content-Disposition", "attachment; filename=cards_export.csv")

	writer := csv.NewWriter(c.Writer)
	defer writer.Flush()

	// Write header
	if err := writer.Write([]string{"iccid", "imsi", "msisdn", "profile_name", "status", "created_at"}); err != nil {
		a.logger.Error("failed to write CSV header", zap.Error(err))
		return
	}

	// Stream rows in batches to avoid loading all cards into memory
	var batch []db.Card
	query.Preload("Profile").Order("created_at DESC").FindInBatches(&batch, 500, func(tx *gorm.DB, batchNum int) error {
		for _, card := range batch {
			profileName := ""
			if card.Profile.Name != "" {
				profileName = card.Profile.Name
			}
			row := []string{
				card.ICCID,
				card.IMSI,
				card.MSISDN,
				profileName,
				card.Status,
				card.CreatedAt.Format("2006-01-02T15:04:05Z"),
			}
			if err := writer.Write(row); err != nil {
				a.logger.Error("failed to write CSV row", zap.Error(err))
				return err
			}
		}
		writer.Flush()
		return nil
	})
}

// ListCardGroups handles GET /api/v1/card-groups.
func (a *API) ListCardGroups(c *gin.Context) {
	page, pageSize, offset := paginationParams(c)

	type groupWithCount struct {
		db.CardGroup
		MemberCount int64 `json:"member_count"`
	}
	var groups []groupWithCount
	if err := a.db.Model(&db.CardGroup{}).
		Select("card_groups.*, COALESCE(m.cnt, 0) AS member_count").
		Joins("LEFT JOIN (SELECT card_group_id, COUNT(*) AS cnt FROM card_group_members GROUP BY card_group_id) m ON m.card_group_id = card_groups.id").
		Order("card_groups.created_at DESC").
		Offset(offset).
		Limit(pageSize).
		Scan(&groups).Error; err != nil {
		a.logger.Error("failed to list card groups", zap.Error(err))
		errorResponse(c, http.StatusInternalServerError, "failed to list card groups")
		return
	}

	var total int64
	a.db.Model(&db.CardGroup{}).Count(&total)

	paginatedResponse(c, http.StatusOK, groups, total, page, pageSize)
}

// CreateCardGroup handles POST /api/v1/card-groups.
func (a *API) CreateCardGroup(c *gin.Context) {
	var req CardGroupRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		errorResponse(c, http.StatusBadRequest, err.Error())
		return
	}

	group := db.CardGroup{
		Name:        req.Name,
		Description: req.Description,
	}

	if err := a.db.Create(&group).Error; err != nil {
		a.logger.Error("failed to create card group", zap.Error(err))
		errorResponse(c, http.StatusInternalServerError, "failed to create card group")
		return
	}

	c.JSON(http.StatusCreated, gin.H{"data": group})
}

// GetCardGroup handles GET /api/v1/card-groups/:id.
func (a *API) GetCardGroup(c *gin.Context) {
	id, ok := parseUUID(c, "id")
	if !ok {
		return
	}

	var group db.CardGroup
	if err := a.db.First(&group, "id = ?", id).Error; err != nil {
		if err == gorm.ErrRecordNotFound {
			errorResponse(c, http.StatusNotFound, "card group not found")
			return
		}
		a.logger.Error("failed to get card group", zap.Error(err))
		errorResponse(c, http.StatusInternalServerError, "failed to get card group")
		return
	}

	// Load members with card details
	var members []db.CardGroupMember
	a.db.Preload("Card").Preload("Card.Profile").Where("card_group_id = ?", id).Find(&members)

	c.JSON(http.StatusOK, gin.H{
		"data":         group,
		"members":      members,
		"member_count": len(members),
	})
}

// UpdateCardGroup handles PUT /api/v1/card-groups/:id.
func (a *API) UpdateCardGroup(c *gin.Context) {
	id, ok := parseUUID(c, "id")
	if !ok {
		return
	}

	var group db.CardGroup
	if err := a.db.First(&group, "id = ?", id).Error; err != nil {
		if err == gorm.ErrRecordNotFound {
			errorResponse(c, http.StatusNotFound, "card group not found")
			return
		}
		a.logger.Error("failed to find card group", zap.Error(err))
		errorResponse(c, http.StatusInternalServerError, "failed to find card group")
		return
	}

	var req CardGroupRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		errorResponse(c, http.StatusBadRequest, err.Error())
		return
	}

	group.Name = req.Name
	group.Description = req.Description

	if err := a.db.Save(&group).Error; err != nil {
		a.logger.Error("failed to update card group", zap.Error(err))
		errorResponse(c, http.StatusInternalServerError, "failed to update card group")
		return
	}

	c.JSON(http.StatusOK, gin.H{"data": group})
}

// DeleteCardGroup handles DELETE /api/v1/card-groups/:id.
func (a *API) DeleteCardGroup(c *gin.Context) {
	id, ok := parseUUID(c, "id")
	if !ok {
		return
	}

	err := a.db.Transaction(func(tx *gorm.DB) error {
		// Delete members first
		if err := tx.Where("card_group_id = ?", id).Delete(&db.CardGroupMember{}).Error; err != nil {
			return err
		}
		result := tx.Delete(&db.CardGroup{}, "id = ?", id)
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected == 0 {
			return gorm.ErrRecordNotFound
		}
		return nil
	})

	if err != nil {
		if err == gorm.ErrRecordNotFound {
			errorResponse(c, http.StatusNotFound, "card group not found")
			return
		}
		a.logger.Error("failed to delete card group", zap.Error(err))
		errorResponse(c, http.StatusInternalServerError, "failed to delete card group")
		return
	}

	c.JSON(http.StatusOK, gin.H{"message": "card group deleted"})
}

// AddCardGroupMembers handles POST /api/v1/card-groups/:id/members.
func (a *API) AddCardGroupMembers(c *gin.Context) {
	id, ok := parseUUID(c, "id")
	if !ok {
		return
	}

	groupID, _ := uuid.Parse(id)

	// Verify group exists
	var group db.CardGroup
	if err := a.db.First(&group, "id = ?", groupID).Error; err != nil {
		if err == gorm.ErrRecordNotFound {
			errorResponse(c, http.StatusNotFound, "card group not found")
			return
		}
		a.logger.Error("failed to find card group", zap.Error(err))
		errorResponse(c, http.StatusInternalServerError, "failed to find card group")
		return
	}

	var req CardGroupMembersRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		errorResponse(c, http.StatusBadRequest, err.Error())
		return
	}

	var added int
	for _, cardIDStr := range req.CardIDs {
		cardID, err := uuid.Parse(cardIDStr)
		if err != nil {
			continue
		}
		member := db.CardGroupMember{
			CardGroupID: groupID,
			CardID:      cardID,
		}
		if err := a.db.Create(&member).Error; err != nil {
			// Skip duplicates
			continue
		}
		added++
	}

	c.JSON(http.StatusOK, gin.H{
		"message": "members added",
		"added":   added,
	})
}

// RemoveCardGroupMembers handles DELETE /api/v1/card-groups/:id/members.
func (a *API) RemoveCardGroupMembers(c *gin.Context) {
	id, ok := parseUUID(c, "id")
	if !ok {
		return
	}

	groupID, _ := uuid.Parse(id)

	var req CardGroupMembersRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		errorResponse(c, http.StatusBadRequest, err.Error())
		return
	}

	var removed int64
	for _, cardIDStr := range req.CardIDs {
		cardID, err := uuid.Parse(cardIDStr)
		if err != nil {
			continue
		}
		result := a.db.Where("card_group_id = ? AND card_id = ?", groupID, cardID).
			Delete(&db.CardGroupMember{})
		removed += result.RowsAffected
	}

	c.JSON(http.StatusOK, gin.H{
		"message": "members removed",
		"removed": removed,
	})
}
