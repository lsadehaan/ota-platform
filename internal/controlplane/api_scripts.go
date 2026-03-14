package controlplane

import (
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
	"go.uber.org/zap"
	"gorm.io/gorm"

	"ota-platform/internal/db"
)

// CreateScriptRequest is the request body for POST /api/v1/scripts.
type CreateScriptRequest struct {
	Name        string  `json:"name" binding:"required"`
	Description *string `json:"description"`
	TargetTAR   string  `json:"target_tar" binding:"required"`
	Commands    string  `json:"commands" binding:"required"`
}

// UpdateScriptRequest is the request body for PUT /api/v1/scripts/:id.
type UpdateScriptRequest struct {
	Name        *string `json:"name"`
	Description *string `json:"description"`
	TargetTAR   *string `json:"target_tar"`
	Commands    *string `json:"commands"`
}

// ListScripts handles GET /api/v1/scripts.
func (a *API) ListScripts(c *gin.Context) {
	var scripts []db.Script
	query := a.db.Model(&db.Script{})

	if search := c.Query("search"); search != "" {
		query = query.Where("name ILIKE ? ESCAPE '\\'", "%"+escapeLike(search)+"%")
	}

	if err := query.
		Order("created_at DESC").
		Find(&scripts).Error; err != nil {
		a.logger.Error("failed to list scripts", zap.Error(err))
		errorResponse(c, http.StatusInternalServerError, "failed to list scripts")
		return
	}

	c.JSON(http.StatusOK, gin.H{"data": scripts})
}

// CreateScript handles POST /api/v1/scripts.
func (a *API) CreateScript(c *gin.Context) {
	var req CreateScriptRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		errorResponse(c, http.StatusBadRequest, err.Error())
		return
	}

	commandCount := countCommands(req.Commands)

	script := db.Script{
		Name:         req.Name,
		Description:  req.Description,
		TargetTAR:    req.TargetTAR,
		Commands:     req.Commands,
		CommandCount: commandCount,
	}

	if err := a.db.Create(&script).Error; err != nil {
		a.logger.Error("failed to create script", zap.Error(err))
		errorResponse(c, http.StatusInternalServerError, "failed to create script")
		return
	}

	c.JSON(http.StatusCreated, gin.H{"data": script})
}

// GetScript handles GET /api/v1/scripts/:id.
func (a *API) GetScript(c *gin.Context) {
	id, ok := parseUUID(c, "id")
	if !ok {
		return
	}

	var script db.Script
	if err := a.db.First(&script, "id = ?", id).Error; err != nil {
		if err == gorm.ErrRecordNotFound {
			errorResponse(c, http.StatusNotFound, "script not found")
			return
		}
		a.logger.Error("failed to get script", zap.Error(err))
		errorResponse(c, http.StatusInternalServerError, "failed to get script")
		return
	}

	c.JSON(http.StatusOK, gin.H{"data": script})
}

// UpdateScript handles PUT /api/v1/scripts/:id.
func (a *API) UpdateScript(c *gin.Context) {
	id, ok := parseUUID(c, "id")
	if !ok {
		return
	}

	var script db.Script
	if err := a.db.First(&script, "id = ?", id).Error; err != nil {
		if err == gorm.ErrRecordNotFound {
			errorResponse(c, http.StatusNotFound, "script not found")
			return
		}
		a.logger.Error("failed to find script", zap.Error(err))
		errorResponse(c, http.StatusInternalServerError, "failed to find script")
		return
	}

	var req UpdateScriptRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		errorResponse(c, http.StatusBadRequest, err.Error())
		return
	}

	updates := make(map[string]interface{})
	if req.Name != nil {
		updates["name"] = *req.Name
	}
	if req.Description != nil {
		updates["description"] = *req.Description
	}
	if req.TargetTAR != nil {
		updates["target_tar"] = *req.TargetTAR
	}
	if req.Commands != nil {
		updates["commands"] = *req.Commands
		updates["command_count"] = countCommands(*req.Commands)
	}

	if len(updates) == 0 {
		errorResponse(c, http.StatusBadRequest, "no fields to update")
		return
	}

	if err := a.db.Model(&script).Updates(updates).Error; err != nil {
		a.logger.Error("failed to update script", zap.Error(err))
		errorResponse(c, http.StatusInternalServerError, "failed to update script")
		return
	}

	a.db.First(&script, "id = ?", id)
	c.JSON(http.StatusOK, gin.H{"data": script})
}

// DeleteScript handles DELETE /api/v1/scripts/:id.
func (a *API) DeleteScript(c *gin.Context) {
	id, ok := parseUUID(c, "id")
	if !ok {
		return
	}

	// Check if any campaigns reference this script
	var campaignCount int64
	a.db.Model(&db.Campaign{}).Where("script_id = ?", id).Count(&campaignCount)
	if campaignCount > 0 {
		errorResponse(c, http.StatusConflict, "cannot delete script referenced by campaigns")
		return
	}

	result := a.db.Delete(&db.Script{}, "id = ?", id)
	if result.Error != nil {
		a.logger.Error("failed to delete script", zap.Error(result.Error))
		errorResponse(c, http.StatusInternalServerError, "failed to delete script")
		return
	}
	if result.RowsAffected == 0 {
		errorResponse(c, http.StatusNotFound, "script not found")
		return
	}

	c.JSON(http.StatusOK, gin.H{"message": "script deleted"})
}

// countCommands counts the number of non-empty lines in the commands string,
// treating each line as a separate command.
func countCommands(commands string) int {
	if commands == "" {
		return 0
	}
	count := 0
	for _, line := range strings.Split(commands, "\n") {
		if strings.TrimSpace(line) != "" {
			count++
		}
	}
	return count
}
