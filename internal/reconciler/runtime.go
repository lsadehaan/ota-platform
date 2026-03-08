package reconciler

import (
	"context"
	"fmt"
	"time"

	"go.uber.org/zap"

	"ota-platform/internal/bootstrap"
	"ota-platform/internal/observability"
	scyllastore "ota-platform/internal/scylla"
)

func Run(ctx context.Context, logger *zap.Logger) error {
	shutdownTelemetry, err := observability.Init(ctx, "reconciler", logger)
	if err != nil {
		return fmt.Errorf("init telemetry: %w", err)
	}
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := shutdownTelemetry(shutdownCtx); err != nil {
			logger.Warn("telemetry shutdown failed", zap.Error(err))
		}
	}()

	database := bootstrap.MustGormDB(logger)
	scyllaClient := bootstrap.MustScylla(logger)
	defer scyllaClient.Close()
	queryStore := scyllastore.NewQueryStore(scyllaClient)
	service := NewService(database, queryStore, logger.Named("reconciler"))
	return service.Run(ctx)
}
