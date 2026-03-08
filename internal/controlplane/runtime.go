package controlplane

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"go.uber.org/zap"

	"ota-platform/internal/bootstrap"
	"ota-platform/internal/config"
	"ota-platform/internal/observability"
	scyllastore "ota-platform/internal/scylla"
)

func Run(ctx context.Context, logger *zap.Logger) error {
	shutdownTelemetry, err := observability.Init(ctx, "ota-api", logger)
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
	scylla := bootstrap.MustScylla(logger)
	defer scylla.Close()
	rdb := bootstrap.MustCoordinationStore(logger)
	defer rdb.Close()

	wsHub := NewWSHub(logger.Named("ws"))
	go wsHub.Run(ctx)

	queryStore := scyllastore.NewQueryStore(scylla)
	campaignSvc := NewCampaignService(database, rdb, queryStore, wsHub, logger.Named("campaign"))
	api := NewAPI(database, campaignSvc, queryStore, wsHub, logger.Named("api"))
	router := api.SetupRouter()

	port := config.GetEnv("OTA_API_PORT", "8080")
	srv := &http.Server{
		Addr:         ":" + port,
		Handler:      router,
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 5 * time.Minute,
		IdleTimeout:  120 * time.Second,
	}

	errCh := make(chan error, 1)
	go func() {
		logger.Info("OTA API server starting", zap.String("port", port))
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			errCh <- err
		}
		close(errCh)
	}()

	select {
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		if err := srv.Shutdown(shutdownCtx); err != nil {
			logger.Error("OTA API shutdown error", zap.Error(err))
			return err
		}
		logger.Info("OTA API shutdown complete")
		return nil
	case err, ok := <-errCh:
		if !ok {
			return nil
		}
		return err
	}
}
