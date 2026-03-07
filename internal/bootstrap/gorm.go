package bootstrap

import (
	"os"

	"go.uber.org/zap"
	"gorm.io/gorm"

	"ota-platform/internal/db"
)

func MustGormDB(logger *zap.Logger) *gorm.DB {
	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		logger.Fatal("DATABASE_URL required")
	}

	database, err := db.Connect(databaseURL)
	if err != nil {
		logger.Fatal("database connection failed", zap.Error(err))
	}
	return database
}
