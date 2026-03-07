package controlplane

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	"github.com/gin-gonic/gin"
	"go.uber.org/zap"
	"gorm.io/gorm"

	"ota-platform/internal/db"
	"ota-platform/pkg/globalplatform"
	"ota-platform/pkg/hexutil"
)

// ListCAPFiles handles GET /api/v1/caps.
func (a *API) ListCAPFiles(c *gin.Context) {
	var caps []db.CAPFile
	if err := a.db.
		Order("created_at DESC").
		Find(&caps).Error; err != nil {
		a.logger.Error("failed to list CAP files", zap.Error(err))
		errorResponse(c, http.StatusInternalServerError, "failed to list CAP files")
		return
	}

	c.JSON(http.StatusOK, gin.H{"data": caps})
}

// UploadCAPFile handles POST /api/v1/caps/upload with multipart file upload.
func (a *API) UploadCAPFile(c *gin.Context) {
	file, header, err := c.Request.FormFile("file")
	if err != nil {
		errorResponse(c, http.StatusBadRequest, "file is required")
		return
	}
	defer file.Close()

	const maxCAPSize = 10 * 1024 * 1024 // 10 MB
	limitedReader := io.LimitReader(file, maxCAPSize+1)
	fileData, err := io.ReadAll(limitedReader)
	if err != nil {
		a.logger.Error("failed to read uploaded file", zap.Error(err))
		errorResponse(c, http.StatusInternalServerError, "failed to read uploaded file")
		return
	}
	if len(fileData) > maxCAPSize {
		errorResponse(c, http.StatusRequestEntityTooLarge, "CAP file exceeds 10 MB size limit")
		return
	}

	// Parse the CAP file
	parsed, err := globalplatform.ParseCAPFile(fileData, header.Filename)
	if err != nil {
		errorResponse(c, http.StatusBadRequest, "failed to parse CAP file: "+err.Error())
		return
	}

	// Compute SHA-256 hash
	hash := sha256.Sum256(fileData)
	hashHex := fmt.Sprintf("%x", hash)

	// Build section sizes for metadata
	sectionSizes := parsed.SectionSizes()
	sectionsJSON, err := json.Marshal(sectionSizes)
	if err != nil {
		a.logger.Error("failed to marshal sections", zap.Error(err))
		errorResponse(c, http.StatusInternalServerError, "failed to marshal section metadata")
		return
	}

	capRecord := db.CAPFile{
		Filename:    header.Filename,
		AID:         hexutil.Encode(parsed.AID),
		FileData:    fileData,
		FileSize:    len(fileData),
		SHA256Hash:  hashHex,
		LoadFileHex: hexutil.Encode(parsed.LoadFile),
		Sections:    sectionsJSON,
	}

	if err := a.db.Create(&capRecord).Error; err != nil {
		a.logger.Error("failed to store CAP file", zap.Error(err))
		errorResponse(c, http.StatusInternalServerError, "failed to store CAP file")
		return
	}

	c.JSON(http.StatusCreated, gin.H{"data": capRecord})
}

// GetCAPFile handles GET /api/v1/caps/:id.
func (a *API) GetCAPFile(c *gin.Context) {
	id, ok := parseUUID(c, "id")
	if !ok {
		return
	}

	var capFile db.CAPFile
	if err := a.db.First(&capFile, "id = ?", id).Error; err != nil {
		if err == gorm.ErrRecordNotFound {
			errorResponse(c, http.StatusNotFound, "CAP file not found")
			return
		}
		a.logger.Error("failed to get CAP file", zap.Error(err))
		errorResponse(c, http.StatusInternalServerError, "failed to get CAP file")
		return
	}

	c.JSON(http.StatusOK, gin.H{"data": capFile})
}

// DeleteCAPFile handles DELETE /api/v1/caps/:id.
func (a *API) DeleteCAPFile(c *gin.Context) {
	id, ok := parseUUID(c, "id")
	if !ok {
		return
	}

	// Check if any campaigns reference this CAP file
	var campaignCount int64
	a.db.Model(&db.Campaign{}).Where("cap_file_id = ?", id).Count(&campaignCount)
	if campaignCount > 0 {
		errorResponse(c, http.StatusConflict, "cannot delete CAP file referenced by campaigns")
		return
	}

	result := a.db.Delete(&db.CAPFile{}, "id = ?", id)
	if result.Error != nil {
		a.logger.Error("failed to delete CAP file", zap.Error(result.Error))
		errorResponse(c, http.StatusInternalServerError, "failed to delete CAP file")
		return
	}
	if result.RowsAffected == 0 {
		errorResponse(c, http.StatusNotFound, "CAP file not found")
		return
	}

	c.JSON(http.StatusOK, gin.H{"message": "CAP file deleted"})
}

// PreviewCAPAPDUs handles GET /api/v1/caps/:id/apdu-preview.
func (a *API) PreviewCAPAPDUs(c *gin.Context) {
	id, ok := parseUUID(c, "id")
	if !ok {
		return
	}

	var capFile db.CAPFile
	if err := a.db.First(&capFile, "id = ?", id).Error; err != nil {
		if err == gorm.ErrRecordNotFound {
			errorResponse(c, http.StatusNotFound, "CAP file not found")
			return
		}
		a.logger.Error("failed to get CAP file", zap.Error(err))
		errorResponse(c, http.StatusInternalServerError, "failed to get CAP file")
		return
	}

	// Default max block size is 0xD8 (216) if not specified
	maxBlockSize := 0xD8
	if v := c.Query("max_block_size"); v != "" {
		if p := parseInt(v); p > 0 {
			maxBlockSize = p
		}
	}

	// Decode the load file hex
	loadFileData, err := hexutil.Decode(capFile.LoadFileHex)
	if err != nil {
		a.logger.Error("failed to decode load file hex", zap.Error(err))
		errorResponse(c, http.StatusInternalServerError, "failed to decode load file")
		return
	}

	// Decode the AID
	aidBytes, err := hexutil.Decode(capFile.AID)
	if err != nil {
		a.logger.Error("failed to decode AID", zap.Error(err))
		errorResponse(c, http.StatusInternalServerError, "failed to decode AID")
		return
	}

	// ISD AID (default Issuer Security Domain)
	isdAID := hexutil.MustDecode("A000000003000000")

	// Build INSTALL [for LOAD] APDU
	installForLoad := globalplatform.BuildInstallForLoadAPDU(aidBytes, isdAID)

	// Build LOAD block APDUs
	loadBlocks := globalplatform.BuildLoadBlockAPDUs(loadFileData, maxBlockSize)

	// Build DELETE APDU
	deleteAPDU := globalplatform.BuildDeleteAPDU(aidBytes, true)

	// Format APDUs as hex strings
	apdus := make([]map[string]string, 0, 2+len(loadBlocks))

	apdus = append(apdus, map[string]string{
		"type": "INSTALL_FOR_LOAD",
		"apdu": hexutil.Encode(installForLoad),
	})

	for i, block := range loadBlocks {
		label := "LOAD_BLOCK"
		if i == len(loadBlocks)-1 {
			label = "LOAD_BLOCK_LAST"
		}
		apdus = append(apdus, map[string]string{
			"type": label,
			"apdu": hexutil.Encode(block),
		})
	}

	apdus = append(apdus, map[string]string{
		"type": "DELETE",
		"apdu": hexutil.Encode(deleteAPDU),
	})

	c.JSON(http.StatusOK, gin.H{
		"cap_file_id":    capFile.ID,
		"aid":            capFile.AID,
		"max_block_size": maxBlockSize,
		"total_apdus":    len(apdus),
		"apdus":          apdus,
	})
}
