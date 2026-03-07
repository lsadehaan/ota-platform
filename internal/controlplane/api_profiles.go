package controlplane

import (
	"fmt"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"go.uber.org/zap"
	"gorm.io/gorm"

	"ota-platform/internal/db"
	"ota-platform/pkg/hexutil"
)

// CreateProfileRequest is the request body for POST /api/v1/profiles.
type CreateProfileRequest struct {
	Name              string                     `json:"name" binding:"required"`
	MaxConcatSMS      *int                       `json:"max_concat_sms"`
	BufferSize        *int                       `json:"buffer_size"`
	PID               *int16                     `json:"pid"`
	DCS               *int16                     `json:"dcs"`
	SecurityBytesType string                     `json:"security_bytes_type"`
	Applications      []CreateApplicationRequest `json:"applications"`
}

// UpdateProfileRequest is the request body for PUT /api/v1/profiles/:id.
type UpdateProfileRequest struct {
	Name              *string `json:"name"`
	MaxConcatSMS      *int    `json:"max_concat_sms"`
	BufferSize        *int    `json:"buffer_size"`
	PID               *int16  `json:"pid"`
	DCS               *int16  `json:"dcs"`
	SecurityBytesType *string `json:"security_bytes_type"`
}

// CreateApplicationRequest is the request body for POST /api/v1/profiles/:id/applications.
type CreateApplicationRequest struct {
	Name              string `json:"name" binding:"required"`
	TAR               string `json:"tar" binding:"required"`
	KIcAlgo           string `json:"kic_algo"`
	KIcMode           string `json:"kic_mode"`
	KIcKeysetID       *int16 `json:"kic_keyset_id"`
	KIdAlgo           string `json:"kid_algo"`
	KIdMode           string `json:"kid_mode"`
	KIdKeysetID       *int16 `json:"kid_keyset_id"`
	CertificationMode string `json:"certification_mode"`
	Ciphered          *bool  `json:"ciphered"`
	CounterMode       string `json:"counter_mode"`
	PORMode           string `json:"por_mode"`
	PORProtocol       string `json:"por_protocol"`
	PORCiphered       *bool  `json:"por_ciphered"`
	PORCertMode       string `json:"por_cert_mode"`
}

// UpdateApplicationRequest is the request body for PUT /api/v1/profiles/:id/applications/:appId.
type UpdateApplicationRequest struct {
	Name              *string `json:"name"`
	TAR               *string `json:"tar"`
	KIcAlgo           *string `json:"kic_algo"`
	KIcMode           *string `json:"kic_mode"`
	KIcKeysetID       *int16  `json:"kic_keyset_id"`
	KIdAlgo           *string `json:"kid_algo"`
	KIdMode           *string `json:"kid_mode"`
	KIdKeysetID       *int16  `json:"kid_keyset_id"`
	CertificationMode *string `json:"certification_mode"`
	Ciphered          *bool   `json:"ciphered"`
	CounterMode       *string `json:"counter_mode"`
	PORMode           *string `json:"por_mode"`
	PORProtocol       *string `json:"por_protocol"`
	PORCiphered       *bool   `json:"por_ciphered"`
	PORCertMode       *string `json:"por_cert_mode"`
}

// ListProfiles handles GET /api/v1/profiles with card count and application count.
func (a *API) ListProfiles(c *gin.Context) {
	var profiles []db.Profile
	query := a.db.Model(&db.Profile{})

	if search := c.Query("search"); search != "" {
		query = query.Where("name ILIKE ? ESCAPE '\\\\'", "%"+escapeLike(search)+"%")
	}

	if err := query.
		Preload("Applications").
		Order("created_at DESC").
		Find(&profiles).Error; err != nil {
		a.logger.Error("failed to list profiles", zap.Error(err))
		errorResponse(c, http.StatusInternalServerError, "failed to list profiles")
		return
	}

	// Build response with counts
	type profileResponse struct {
		db.Profile
		CardCount        int64 `json:"card_count"`
		ApplicationCount int   `json:"application_count"`
	}

	result := make([]profileResponse, 0, len(profiles))
	for _, p := range profiles {
		var cardCount int64
		a.db.Model(&db.Card{}).Where("profile_id = ?", p.ID).Count(&cardCount)
		result = append(result, profileResponse{
			Profile:          p,
			CardCount:        cardCount,
			ApplicationCount: len(p.Applications),
		})
	}

	c.JSON(http.StatusOK, gin.H{"data": result})
}

// CreateProfile handles POST /api/v1/profiles with optional nested applications.
func (a *API) CreateProfile(c *gin.Context) {
	var req CreateProfileRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		errorResponse(c, http.StatusBadRequest, err.Error())
		return
	}

	profile := db.Profile{
		Name: req.Name,
	}

	if req.MaxConcatSMS != nil {
		profile.MaxConcatSMS = *req.MaxConcatSMS
	} else {
		profile.MaxConcatSMS = 7
	}
	if req.BufferSize != nil {
		profile.BufferSize = *req.BufferSize
	} else {
		profile.BufferSize = 180
	}
	if req.PID != nil {
		profile.PID = *req.PID
	}
	if req.DCS != nil {
		profile.DCS = *req.DCS
	}
	if req.SecurityBytesType != "" {
		profile.SecurityBytesType = req.SecurityBytesType
	} else {
		profile.SecurityBytesType = "WITH_LENGTHS_AND_UDHL"
	}

	err := a.db.Transaction(func(tx *gorm.DB) error {
		if err := tx.Create(&profile).Error; err != nil {
			return err
		}

		// Create nested applications if provided
		for _, appReq := range req.Applications {
			app, err := buildApplicationFromRequest(profile.ID, &appReq)
			if err != nil {
				return err
			}
			if err := tx.Create(app).Error; err != nil {
				return err
			}
		}

		return nil
	})

	if err != nil {
		a.logger.Error("failed to create profile", zap.Error(err))
		errorResponse(c, http.StatusInternalServerError, "failed to create profile")
		return
	}

	// Reload with applications
	a.db.Preload("Applications").First(&profile, "id = ?", profile.ID)

	c.JSON(http.StatusCreated, gin.H{"data": profile})
}

// GetProfile handles GET /api/v1/profiles/:id with applications and card count.
func (a *API) GetProfile(c *gin.Context) {
	id, ok := parseUUID(c, "id")
	if !ok {
		return
	}

	var profile db.Profile
	if err := a.db.
		Preload("Applications").
		First(&profile, "id = ?", id).Error; err != nil {
		if err == gorm.ErrRecordNotFound {
			errorResponse(c, http.StatusNotFound, "profile not found")
			return
		}
		a.logger.Error("failed to get profile", zap.Error(err))
		errorResponse(c, http.StatusInternalServerError, "failed to get profile")
		return
	}

	// Count cards for this profile
	var cardCount int64
	a.db.Model(&db.Card{}).Where("profile_id = ?", id).Count(&cardCount)

	c.JSON(http.StatusOK, gin.H{
		"data":       profile,
		"card_count": cardCount,
	})
}

// UpdateProfile handles PUT /api/v1/profiles/:id.
func (a *API) UpdateProfile(c *gin.Context) {
	id, ok := parseUUID(c, "id")
	if !ok {
		return
	}

	var profile db.Profile
	if err := a.db.First(&profile, "id = ?", id).Error; err != nil {
		if err == gorm.ErrRecordNotFound {
			errorResponse(c, http.StatusNotFound, "profile not found")
			return
		}
		a.logger.Error("failed to find profile", zap.Error(err))
		errorResponse(c, http.StatusInternalServerError, "failed to find profile")
		return
	}

	var req UpdateProfileRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		errorResponse(c, http.StatusBadRequest, err.Error())
		return
	}

	updates := make(map[string]interface{})
	if req.Name != nil {
		updates["name"] = *req.Name
	}
	if req.MaxConcatSMS != nil {
		updates["max_concat_sms"] = *req.MaxConcatSMS
	}
	if req.BufferSize != nil {
		updates["buffer_size"] = *req.BufferSize
	}
	if req.PID != nil {
		updates["pid"] = *req.PID
	}
	if req.DCS != nil {
		updates["dcs"] = *req.DCS
	}
	if req.SecurityBytesType != nil {
		updates["security_bytes_type"] = *req.SecurityBytesType
	}

	if len(updates) == 0 {
		errorResponse(c, http.StatusBadRequest, "no fields to update")
		return
	}

	if err := a.db.Model(&profile).Updates(updates).Error; err != nil {
		a.logger.Error("failed to update profile", zap.Error(err))
		errorResponse(c, http.StatusInternalServerError, "failed to update profile")
		return
	}

	a.db.Preload("Applications").First(&profile, "id = ?", id)
	c.JSON(http.StatusOK, gin.H{"data": profile})
}

// DeleteProfile handles DELETE /api/v1/profiles/:id.
func (a *API) DeleteProfile(c *gin.Context) {
	id, ok := parseUUID(c, "id")
	if !ok {
		return
	}

	// Check for cards using this profile
	var cardCount int64
	a.db.Model(&db.Card{}).Where("profile_id = ?", id).Count(&cardCount)
	if cardCount > 0 {
		errorResponse(c, http.StatusConflict, "cannot delete profile with associated cards")
		return
	}

	err := a.db.Transaction(func(tx *gorm.DB) error {
		// Delete applications first
		if err := tx.Where("profile_id = ?", id).Delete(&db.Application{}).Error; err != nil {
			return err
		}
		result := tx.Delete(&db.Profile{}, "id = ?", id)
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
			errorResponse(c, http.StatusNotFound, "profile not found")
			return
		}
		a.logger.Error("failed to delete profile", zap.Error(err))
		errorResponse(c, http.StatusInternalServerError, "failed to delete profile")
		return
	}

	c.JSON(http.StatusOK, gin.H{"message": "profile deleted"})
}

// CreateApplication handles POST /api/v1/profiles/:id/applications.
func (a *API) CreateApplication(c *gin.Context) {
	profileIDStr, ok := parseUUID(c, "id")
	if !ok {
		return
	}

	profileID, _ := uuid.Parse(profileIDStr)

	// Verify profile exists
	var profile db.Profile
	if err := a.db.First(&profile, "id = ?", profileID).Error; err != nil {
		if err == gorm.ErrRecordNotFound {
			errorResponse(c, http.StatusNotFound, "profile not found")
			return
		}
		a.logger.Error("failed to find profile", zap.Error(err))
		errorResponse(c, http.StatusInternalServerError, "failed to find profile")
		return
	}

	var req CreateApplicationRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		errorResponse(c, http.StatusBadRequest, err.Error())
		return
	}

	app, err := buildApplicationFromRequest(profileID, &req)
	if err != nil {
		errorResponse(c, http.StatusBadRequest, err.Error())
		return
	}

	if err := a.db.Create(app).Error; err != nil {
		a.logger.Error("failed to create application", zap.Error(err))
		errorResponse(c, http.StatusInternalServerError, "failed to create application")
		return
	}

	c.JSON(http.StatusCreated, gin.H{"data": app})
}

// UpdateApplication handles PUT /api/v1/profiles/:id/applications/:appId.
func (a *API) UpdateApplication(c *gin.Context) {
	profileIDStr, ok := parseUUID(c, "id")
	if !ok {
		return
	}
	appIDStr, ok := parseUUID(c, "appId")
	if !ok {
		return
	}

	var app db.Application
	if err := a.db.First(&app, "id = ? AND profile_id = ?", appIDStr, profileIDStr).Error; err != nil {
		if err == gorm.ErrRecordNotFound {
			errorResponse(c, http.StatusNotFound, "application not found")
			return
		}
		a.logger.Error("failed to find application", zap.Error(err))
		errorResponse(c, http.StatusInternalServerError, "failed to find application")
		return
	}

	var req UpdateApplicationRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		errorResponse(c, http.StatusBadRequest, err.Error())
		return
	}

	updates := make(map[string]interface{})
	if req.Name != nil {
		updates["name"] = *req.Name
	}
	if req.TAR != nil {
		tarBytes, err := hexutil.Decode(*req.TAR)
		if err != nil || len(tarBytes) != 3 {
			errorResponse(c, http.StatusBadRequest, "invalid TAR value (must be 3 bytes)")
			return
		}
		updates["tar"] = tarBytes
	}
	if req.KIcAlgo != nil {
		updates["kic_algo"] = *req.KIcAlgo
	}
	if req.KIcMode != nil {
		updates["kic_mode"] = *req.KIcMode
	}
	if req.KIcKeysetID != nil {
		updates["kic_keyset_id"] = *req.KIcKeysetID
	}
	if req.KIdAlgo != nil {
		updates["kid_algo"] = *req.KIdAlgo
	}
	if req.KIdMode != nil {
		updates["kid_mode"] = *req.KIdMode
	}
	if req.KIdKeysetID != nil {
		updates["kid_keyset_id"] = *req.KIdKeysetID
	}
	if req.CertificationMode != nil {
		updates["certification_mode"] = *req.CertificationMode
	}
	if req.Ciphered != nil {
		updates["ciphered"] = *req.Ciphered
	}
	if req.CounterMode != nil {
		updates["counter_mode"] = *req.CounterMode
	}
	if req.PORMode != nil {
		updates["por_mode"] = *req.PORMode
	}
	if req.PORProtocol != nil {
		updates["por_protocol"] = *req.PORProtocol
	}
	if req.PORCiphered != nil {
		updates["por_ciphered"] = *req.PORCiphered
	}
	if req.PORCertMode != nil {
		updates["por_cert_mode"] = *req.PORCertMode
	}

	if len(updates) == 0 {
		errorResponse(c, http.StatusBadRequest, "no fields to update")
		return
	}

	if err := a.db.Model(&app).Updates(updates).Error; err != nil {
		a.logger.Error("failed to update application", zap.Error(err))
		errorResponse(c, http.StatusInternalServerError, "failed to update application")
		return
	}

	a.db.First(&app, "id = ?", appIDStr)
	c.JSON(http.StatusOK, gin.H{"data": app})
}

// DeleteApplication handles DELETE /api/v1/profiles/:id/applications/:appId.
func (a *API) DeleteApplication(c *gin.Context) {
	profileIDStr, ok := parseUUID(c, "id")
	if !ok {
		return
	}
	appIDStr, ok := parseUUID(c, "appId")
	if !ok {
		return
	}

	result := a.db.Where("id = ? AND profile_id = ?", appIDStr, profileIDStr).Delete(&db.Application{})
	if result.Error != nil {
		a.logger.Error("failed to delete application", zap.Error(result.Error))
		errorResponse(c, http.StatusInternalServerError, "failed to delete application")
		return
	}
	if result.RowsAffected == 0 {
		errorResponse(c, http.StatusNotFound, "application not found")
		return
	}

	c.JSON(http.StatusOK, gin.H{"message": "application deleted"})
}

// buildApplicationFromRequest creates a db.Application from a CreateApplicationRequest.
func buildApplicationFromRequest(profileID uuid.UUID, req *CreateApplicationRequest) (*db.Application, error) {
	tarBytes, err := hexutil.Decode(req.TAR)
	if err != nil {
		return nil, fmt.Errorf("invalid TAR hex value")
	}
	if len(tarBytes) != 3 {
		return nil, fmt.Errorf("TAR must be exactly 3 bytes (6 hex characters)")
	}

	app := &db.Application{
		ProfileID: profileID,
		Name:      req.Name,
		TAR:       tarBytes,
	}

	// Apply defaults or provided values
	if req.KIcAlgo != "" {
		app.KIcAlgo = req.KIcAlgo
	} else {
		app.KIcAlgo = "DES"
	}
	if req.KIcMode != "" {
		app.KIcMode = req.KIcMode
	} else {
		app.KIcMode = "TRIPLE_DES_CBC_2_KEYS"
	}
	if req.KIcKeysetID != nil {
		app.KIcKeysetID = *req.KIcKeysetID
	} else {
		app.KIcKeysetID = 1
	}
	if req.KIdAlgo != "" {
		app.KIdAlgo = req.KIdAlgo
	} else {
		app.KIdAlgo = "DES"
	}
	if req.KIdMode != "" {
		app.KIdMode = req.KIdMode
	} else {
		app.KIdMode = "TRIPLE_DES_CBC_2_KEYS"
	}
	if req.KIdKeysetID != nil {
		app.KIdKeysetID = *req.KIdKeysetID
	} else {
		app.KIdKeysetID = 1
	}
	if req.CertificationMode != "" {
		app.CertificationMode = req.CertificationMode
	} else {
		app.CertificationMode = "CC"
	}
	if req.Ciphered != nil {
		app.Ciphered = *req.Ciphered
	} else {
		app.Ciphered = true
	}
	if req.CounterMode != "" {
		app.CounterMode = req.CounterMode
	} else {
		app.CounterMode = "COUNTER_REPLAY_OR_CHECK"
	}
	if req.PORMode != "" {
		app.PORMode = req.PORMode
	} else {
		app.PORMode = "REPLY_ALWAYS"
	}
	if req.PORProtocol != "" {
		app.PORProtocol = req.PORProtocol
	} else {
		app.PORProtocol = "SMS_SUBMIT"
	}
	if req.PORCiphered != nil {
		app.PORCiphered = *req.PORCiphered
	}
	if req.PORCertMode != "" {
		app.PORCertMode = req.PORCertMode
	} else {
		app.PORCertMode = "NO_SECURITY"
	}

	return app, nil
}
