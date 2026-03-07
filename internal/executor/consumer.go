package executor

import (
	"context"
	"os"
	"strconv"

	"go.uber.org/zap"

	contractevents "ota-platform/internal/contracts/events"
	kafkapkg "ota-platform/internal/kafka"
)

const (
	cardEventsTopic   = contractevents.TopicCardEvents
	cardEventsGroupID = "card-workers"
	defaultWorkers    = 16
)

// ConsumerManager manages the card-events Kafka consumer that feeds into
// the CardWorker for event-driven card processing.
type ConsumerManager struct {
	consumer   *kafkapkg.ConcurrentConsumer
	cardWorker *CardWorker
	logger     *zap.Logger
}

// NewConsumerManager creates a ConsumerManager with a ConcurrentConsumer
// wired to the CardWorker for processing card state machine events.
func NewConsumerManager(brokers []string, cardWorker *CardWorker, logger *zap.Logger) *ConsumerManager {
	workers := defaultWorkers
	if v := os.Getenv("CARD_WORKER_CONCURRENCY"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			workers = n
		}
	}

	cm := &ConsumerManager{
		cardWorker: cardWorker,
		logger:     logger,
	}

	cm.consumer = kafkapkg.NewConcurrentConsumer(
		brokers,
		cardEventsTopic,
		cardEventsGroupID,
		workers,
		cardWorker.HandleEvent,
		logger.Named("card-events-consumer"),
	)

	return cm
}

// Start launches the card-events consumer. It blocks until ctx is cancelled.
func (cm *ConsumerManager) Start(ctx context.Context) {
	cm.logger.Info("starting card-events consumer")
	if err := cm.consumer.Start(ctx); err != nil {
		cm.logger.Error("card-events consumer stopped", zap.Error(err))
	}
}

// Close shuts down the consumer.
func (cm *ConsumerManager) Close() {
	if err := cm.consumer.Close(); err != nil {
		cm.logger.Error("failed to close card-events consumer", zap.Error(err))
	}
	cm.logger.Info("consumer manager closed")
}
