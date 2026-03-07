package transport

import (
	"context"
	"fmt"
	"os"
	"time"

	"go.uber.org/zap"

	"ota-platform/internal/bootstrap"
	"ota-platform/internal/config"
	"ota-platform/internal/observability"
	"ota-platform/internal/smpp"
)

func Run(ctx context.Context, logger *zap.Logger) error {
	shutdownTelemetry, err := observability.Init(ctx, "sms-gateway", logger)
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

	rdb := bootstrap.MustCoordinationStore(logger)
	defer rdb.Close()

	smppConfig := smpp.Config{
		Host:           config.GetEnv("SMPP_HOST", "localhost"),
		Port:           config.GetEnvInt("SMPP_PORT", 2775),
		SystemID:       config.GetEnv("SMPP_SYSTEM_ID", "smppclient"),
		Password:       config.GetEnv("SMPP_PASSWORD", "password"),
		SourceAddr:     os.Getenv("SMPP_SOURCE_ADDR"),
		SourceAddrTON:  0x05,
		SourceAddrNPI:  0x00,
		EnquireLinkSec: 30,
	}
	poolConfig := smpp.PoolConfig{
		Connections: config.GetEnvInt("SMPP_CONNECTIONS", 5),
		WindowSize:  config.GetEnvInt("SMPP_WINDOW_SIZE", 10),
	}

	svc := NewService(smppConfig, poolConfig, bootstrap.KafkaBrokers(), rdb, logger.Named("sms-gateway"))
	defer svc.Close()

	return svc.Start(ctx)
}
