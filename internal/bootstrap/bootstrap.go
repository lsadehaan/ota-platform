package bootstrap

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"go.uber.org/zap"
	"gorm.io/gorm"

	"ota-platform/internal/db"
)

func NewLogger() *zap.Logger {
	cfg := zap.NewProductionConfig()
	if lvl := os.Getenv("LOG_LEVEL"); lvl != "" {
		var zapLevel zap.AtomicLevel
		if err := zapLevel.UnmarshalText([]byte(lvl)); err == nil {
			cfg.Level = zapLevel
		}
	}
	logger, err := cfg.Build()
	if err != nil {
		panic(err)
	}
	return logger
}

func SignalContext() (context.Context, context.CancelFunc) {
	return signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
}

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
