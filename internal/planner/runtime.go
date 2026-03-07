package planner

import (
	"context"

	"go.uber.org/zap"

	"ota-platform/internal/bootstrap"
	contractevents "ota-platform/internal/contracts/events"
	kafkapkg "ota-platform/internal/kafka"
)

func Run(ctx context.Context, logger *zap.Logger) error {
	database := bootstrap.MustGormDB(logger)
	producer := kafkapkg.NewProducer(bootstrap.KafkaBrokers(), contractevents.TopicCardEvents, logger.Named("planner-producer"))
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
