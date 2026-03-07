package projector

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
	shutdownTelemetry, err := observability.Init(ctx, "read-model-projector", logger)
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

	scylla := bootstrap.MustScylla(logger)
	defer scylla.Close()
	store := scyllastore.NewMessageLogStore(scylla)
	logWriter := NewService(store, bootstrap.KafkaBrokers(), logger.Named("log-writer"))
	defer logWriter.Close()

	done := make(chan struct{})
	go func() {
		logWriter.Start(ctx)
		close(done)
	}()

	select {
	case <-ctx.Done():
		<-done
		logger.Info("read-model projector shutdown complete")
		return nil
	case <-done:
		return nil
	}
}
