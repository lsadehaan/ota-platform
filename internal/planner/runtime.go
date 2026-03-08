package planner

import (
	"context"
	"fmt"
	"time"

	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"

	"ota-platform/internal/bootstrap"
	contractevents "ota-platform/internal/contracts/events"
	kafkapkg "ota-platform/internal/kafka"
	"ota-platform/internal/observability"
)

func Run(ctx context.Context, logger *zap.Logger) error {
	shutdownTelemetry, err := observability.Init(ctx, "campaign-planner", logger)
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
	producer := kafkapkg.NewProducerWithOptions(bootstrap.KafkaBrokers(), contractevents.TopicCardEvents, kafkapkg.ProducerOptions{
		RequiredAcks:    kafka.RequireAll,
		RequiredAcksSet: true,
	}, logger.Named("planner-producer"))
	defer producer.Close()

	service := NewService(database, producer, logger.Named("planner"))

	done := make(chan struct{})
	go func() {
		service.Run(ctx)
		close(done)
	}()

	select {
	case <-ctx.Done():
		<-done
		logger.Info("campaign planner shutdown complete")
		return nil
	case <-done:
		return nil
	}
}
