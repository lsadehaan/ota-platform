// Package main provides the OTA Engine single-binary entry point that wires
// together the control plane API, campaign planner, card executor workers,
// and an in-process SMPP gateway connected via Go channels instead of Kafka.
package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"time"

	"go.uber.org/zap"
	"gorm.io/gorm"

	"ota-platform/internal/bootstrap"
	"ota-platform/internal/config"
	"ota-platform/internal/controlplane"
	"ota-platform/internal/db"
	"ota-platform/internal/executor"
	"ota-platform/internal/observability"
	"ota-platform/internal/pgstore"
	"ota-platform/internal/pipeline"
	"ota-platform/internal/planner"
	"github.com/idnteq/go-smsc/smpp"
	"ota-platform/internal/transport"
)

func main() {
	logger := bootstrap.NewLogger()
	ctx, cancel := bootstrap.SignalContext()
	defer cancel()

	if err := run(ctx, logger); err != nil {
		logger.Fatal("ota-engine failed", zap.Error(err))
	}
}

func run(ctx context.Context, logger *zap.Logger) error {
	// 1. OTel
	shutdownTelemetry, err := observability.Init(ctx, "ota-engine", logger)
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

	// 2. Postgres (GORM) — sole authoritative data store
	database := bootstrap.MustGormDB(logger)

	// 3. Pipeline Bus (Go channels replace Kafka)
	bus := pipeline.NewBus()
	bus.RegisterMetrics()

	// 4. Stores (Postgres-backed via pgstore, except counterStore which is process-local)
	queryStore := pgstore.NewQueryStore(database)
	cardKeyStore := pgstore.NewCardKeyStore(database, logger.Named("card-keys"))
	counterStore := pgstore.NewCounterStore() // process-local cache; batch-preloaded by planner
	coordination := pgstore.NewCoordinationStore(database)
	gwCoordination := pgstore.NewGatewayCoordinationStore(database, coordination)
	msisdnResolver := pgstore.NewMSISDNResolver(database)

	// 5. Channel producers (satisfy executor interfaces)
	smsProducer := pipeline.NewSMSProducer(bus.SendSMSCh)
	logProducer := pipeline.NewLogProducer(bus.MessageLogCh)
	eventProducer := pipeline.NewEventProducer(bus.ActivateCh)
	stateWriter := pipeline.NewChannelCardStateWriter(bus.CardStateCh)

	// Register log producer drop metric
	logProducer.RegisterDropMetric()

	// 6. WebSocket hub
	wsHub := controlplane.NewWSHub(logger.Named("ws"))
	go wsHub.Run(ctx)

	// 7. Executor (CardWorker)
	wsAdapter := &wsHubAdapter{hub: wsHub}
	cardWorker := executor.NewService(
		database,
		nil, // executionStore — card state written via batched channel writer
		coordination,
		cardKeyStore, // keystore.KeyStore (reads keys from Postgres cards table)
		nil,          // cryptoProvider — nil uses software crypto
		smsProducer, logProducer, eventProducer, stateWriter,
		counterStore,
		wsAdapter,
		logger.Named("executor"),
	)
	cardWorker.StartCacheSweeper(ctx)

	// 8. Planner (with batch preloader to eliminate per-card Postgres queries)
	preloader := pgstore.NewBatchPreloader(database, counterStore, cardWorker, msisdnResolver, logger.Named("preloader"))
	plannerPub := pipeline.NewPlannerPublisher(bus.ActivateCh)
	plannerSvc := planner.NewService(database, plannerPub, preloader, logger.Named("planner"))

	// 9. In-process Gateway (SMPP pool → mock-smsc / real SMSC)
	smppCfg := smpp.Config{
		Host:           config.GetEnv("SMPP_HOST", "localhost"),
		Port:           config.GetEnvInt("SMPP_PORT", 2775),
		SystemID:       config.GetEnv("SMPP_SYSTEM_ID", "smppclient"),
		Password:       config.GetEnv("SMPP_PASSWORD", "password"),
		SourceAddr:     config.GetEnv("SMPP_SOURCE_ADDR", ""),
		SourceAddrTON:  0x05,
		SourceAddrNPI:  0x00,
		EnquireLinkSec: config.GetEnvInt("SMPP_ENQUIRE_LINK_SEC", 30),
	}
	poolCfg := smpp.PoolConfig{
		Connections:      config.GetEnvInt("SMPP_CONNECTIONS", 5),
		WindowSize:       config.GetEnvInt("SMPP_WINDOW_SIZE", 10),
		DeliverWorkers:   config.GetEnvInt("SMPP_DELIVER_WORKERS", 32),
		DeliverQueueSize: config.GetEnvInt("SMPP_DELIVER_QUEUE_SIZE", 25000),
		SubmitTimeout:    time.Duration(config.GetEnvInt("SMPP_SUBMIT_TIMEOUT_SEC", 60)) * time.Second,
	}

	gateway := transport.NewInProcessGateway(
		smppCfg, poolCfg,
		bus,
		gwCoordination, // transport.CoordinationStore (process-local)
		msisdnResolver, // transport.MSISDNResolver (Postgres)
		logger.Named("gateway"),
	)

	// 10. Campaign service + API
	campaignSvc := controlplane.NewCampaignService(database, coordination, queryStore, wsHub, logger.Named("campaign"))
	api := controlplane.NewAPI(database, nil, campaignSvc, queryStore, cardKeyStore, cardKeyStore, wsHub, logger.Named("api"))

	api.SetChannelStats(bus)

	// 11. Start worker pools
	activateWorkers := envInt("ENGINE_ACTIVATE_WORKERS", 64)
	gatewayWorkers := envInt("ENGINE_GATEWAY_WORKERS", 32)
	dlrWorkers := envInt("ENGINE_DLR_WORKERS", 128)
	moWorkers := envInt("ENGINE_MO_WORKERS", 64)

	startWorkerPool(ctx, bus.ActivateCh, cardWorker, activateWorkers, logger.Named("activate-pool"))
	startWorkerPool(ctx, bus.DLRCh, cardWorker, dlrWorkers, logger.Named("dlr-pool"))
	startWorkerPool(ctx, bus.MOCh, cardWorker, moWorkers, logger.Named("mo-pool"))

	// Gateway workers consume from SendSMSCh
	for i := 0; i < gatewayWorkers; i++ {
		go func() {
			for {
				select {
				case msg, ok := <-bus.SendSMSCh:
					if !ok {
						return
					}
					if err := gateway.HandleSendSMS(ctx, msg); err != nil {
						logger.Warn("gateway send error", zap.Error(err))
					}
				case <-ctx.Done():
					return
				}
			}
		}()
	}

	// 12. Start batched writers (consume from CardStateCh and MessageLogCh)
	// Append-only INSERTs have no row contention, so multiple writers are safe.
	stateWriterCfg := pgstore.DefaultCardStateWriterConfig()
	stateWriterWorkers := envInt("ENGINE_STATE_WRITER_WORKERS", 4)
	for i := 0; i < stateWriterWorkers; i++ {
		go pgstore.RunCardStateWriter(ctx, bus.CardStateCh, database, stateWriterCfg, logger.Named("card-state-writer"))
	}

	msgWriterCfg := pgstore.DefaultMessageWriterConfig()
	msgWriterWorkers := envInt("ENGINE_MSG_WRITER_WORKERS", 4)
	for i := 0; i < msgWriterWorkers; i++ {
		go pgstore.RunMessageWriter(ctx, bus.MessageLogCh, database, msgWriterCfg, logger.Named("message-writer"))
	}

	// 13. Start campaign completion checker
	go runCompletionChecker(ctx, database, wsHub, logger.Named("completion-checker"))

	// 13b. Crash recovery: re-create planner shards for campaigns that were running
	shardSize := envInt("PLANNER_CLAIM_BATCH_SIZE", 5000)
	if err := pgstore.RecoverRunningCampaigns(ctx, database, shardSize, logger.Named("recovery")); err != nil {
		logger.Error("campaign recovery failed", zap.Error(err))
		// Non-fatal — continue startup
	}

	// 14. Start planner
	plannerDone := make(chan struct{})
	go func() {
		plannerSvc.Run(ctx)
		close(plannerDone)
	}()

	// 15. Connect SMPP gateway
	if err := gateway.Start(ctx); err != nil {
		return fmt.Errorf("gateway start: %w", err)
	}
	defer gateway.Close()

	// 16. Start HTTP server
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
		logger.Info("OTA Engine starting", zap.String("port", port))
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			errCh <- err
		}
		close(errCh)
	}()

	select {
	case <-ctx.Done():
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer shutdownCancel()
		if srvErr := srv.Shutdown(shutdownCtx); srvErr != nil {
			logger.Error("HTTP server shutdown error", zap.Error(srvErr))
		}
		<-plannerDone
		cardWorker.DrainRetries()
		logger.Info("OTA Engine shutdown complete")
		return nil
	case err, ok := <-errCh:
		if !ok {
			return nil
		}
		return err
	}
}

// startWorkerPool launches n goroutines that consume CardEvents from ch and
// dispatch them to the card worker's HandleCardEvent method.
func startWorkerPool(ctx context.Context, ch <-chan pipeline.CardEvent, worker *executor.Service, n int, logger *zap.Logger) {
	for i := 0; i < n; i++ {
		go func() {
			for {
				select {
				case event, ok := <-ch:
					if !ok {
						return
					}
					if err := worker.HandleCardEvent(ctx, &event); err != nil {
						logger.Warn("worker error",
							zap.String("card_id", event.CardID),
							zap.String("type", event.Type),
							zap.Error(err),
						)
					}
				case <-ctx.Done():
					return
				}
			}
		}()
	}
}

// wsHubAdapter converts between executor.WSEvent and controlplane.WSEvent.
// Both types are structurally identical but in different packages.
type wsHubAdapter struct {
	hub *controlplane.WSHub
}

func (a *wsHubAdapter) Broadcast(event *executor.WSEvent) {
	a.hub.Broadcast(&controlplane.WSEvent{
		Type:       event.Type,
		CampaignID: event.CampaignID,
		CardID:     event.CardID,
		Data:       event.Data,
	})
}

func (a *wsHubAdapter) BroadcastToCampaign(campaignID string, event *executor.WSEvent) {
	a.hub.BroadcastToCampaign(campaignID, &controlplane.WSEvent{
		Type:       event.Type,
		CampaignID: event.CampaignID,
		CardID:     event.CardID,
		Data:       event.Data,
	})
}

// runCompletionChecker periodically checks running campaigns for completion.
// A campaign is complete when campaign_stats shows pending=0 and in_progress=0.
func runCompletionChecker(ctx context.Context, database *gorm.DB, wsHub *controlplane.WSHub, logger *zap.Logger) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			var campaigns []db.Campaign
			if err := database.WithContext(ctx).Where("status = ?", "running").Find(&campaigns).Error; err != nil {
				logger.Warn("completion check: query campaigns", zap.Error(err))
				continue
			}
			for _, c := range campaigns {
				var stats db.CampaignStatsRow
				if err := database.WithContext(ctx).Where("campaign_id = ?", c.ID).First(&stats).Error; err != nil {
					continue
				}
				if stats.Pending > 0 || stats.InProgress > 0 {
					continue
				}
				now := time.Now()
				finalStatus := "completed"
				if stats.Completed == 0 && stats.Failed > 0 {
					finalStatus = "failed"
				}
				result := database.WithContext(ctx).
					Model(&db.Campaign{}).
					Where("id = ? AND status = ?", c.ID, "running").
					Updates(map[string]interface{}{"status": finalStatus, "completed_at": now})
				if result.Error != nil {
					logger.Warn("completion check: update campaign", zap.String("campaign_id", c.ID.String()), zap.Error(result.Error))
					continue
				}
				if result.RowsAffected > 0 {
					logger.Info("campaign completed",
						zap.String("campaign_id", c.ID.String()),
						zap.String("status", finalStatus),
						zap.Int64("completed", stats.Completed),
						zap.Int64("failed", stats.Failed),
					)
					if wsHub != nil {
						wsHub.Broadcast(&controlplane.WSEvent{
							Type:       "campaign_completed",
							CampaignID: c.ID.String(),
							Data:       map[string]interface{}{"status": finalStatus},
						})
					}
				}
			}
		case <-ctx.Done():
			return
		}
	}
}

func envInt(key string, fallback int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return n
		}
	}
	return fallback
}
