package executor

import (
	"context"
	"os"
	"strconv"
	"sync"

	"go.uber.org/zap"

	contractevents "ota-platform/internal/contracts/events"
	kafkapkg "ota-platform/internal/kafka"
)

const (
	activateGroupID = "card-activate-workers"
	dlrGroupID      = "card-dlr-workers"
	moGroupID       = "card-mo-workers"
	defaultWorkers  = 16
)

// ConsumerManager manages per-topic Kafka consumers for the card executor.
// Each event type gets its own ConcurrentConsumer so that bulk activates
// cannot starve DLR/MO processing (head-of-line blocking).
type ConsumerManager struct {
	activateConsumer *kafkapkg.ConcurrentConsumer
	dlrConsumer      *kafkapkg.ConcurrentConsumer
	moConsumer       *kafkapkg.ConcurrentConsumer
	cardWorker       *CardWorker
	logger           *zap.Logger
}

// NewConsumerManager creates a ConsumerManager with separate consumers for
// card-activate, card-dlr, and card-mo topics.
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

	cm.activateConsumer = kafkapkg.NewConcurrentConsumer(
		brokers,
		contractevents.TopicCardActivate,
		activateGroupID,
		workers,
		cardWorker.HandleEvent,
		logger.Named("activate-consumer"),
	)

	cm.dlrConsumer = kafkapkg.NewConcurrentConsumer(
		brokers,
		contractevents.TopicCardDLR,
		dlrGroupID,
		workers,
		cardWorker.HandleEvent,
		logger.Named("dlr-consumer"),
	)

	cm.moConsumer = kafkapkg.NewConcurrentConsumer(
		brokers,
		contractevents.TopicCardMO,
		moGroupID,
		workers,
		cardWorker.HandleEvent,
		logger.Named("mo-consumer"),
	)

	return cm
}

// Start launches all three consumers concurrently. It blocks until ctx is cancelled.
func (cm *ConsumerManager) Start(ctx context.Context) {
	cm.logger.Info("starting card-events consumers (activate, dlr, mo)")

	var wg sync.WaitGroup
	wg.Add(3)

	go func() {
		defer wg.Done()
		if err := cm.activateConsumer.Start(ctx); err != nil {
			cm.logger.Error("activate consumer stopped", zap.Error(err))
		}
	}()

	go func() {
		defer wg.Done()
		if err := cm.dlrConsumer.Start(ctx); err != nil {
			cm.logger.Error("dlr consumer stopped", zap.Error(err))
		}
	}()

	go func() {
		defer wg.Done()
		if err := cm.moConsumer.Start(ctx); err != nil {
			cm.logger.Error("mo consumer stopped", zap.Error(err))
		}
	}()

	wg.Wait()
}

// Close shuts down all consumers.
func (cm *ConsumerManager) Close() {
	if err := cm.activateConsumer.Close(); err != nil {
		cm.logger.Error("failed to close activate consumer", zap.Error(err))
	}
	if err := cm.dlrConsumer.Close(); err != nil {
		cm.logger.Error("failed to close dlr consumer", zap.Error(err))
	}
	if err := cm.moConsumer.Close(); err != nil {
		cm.logger.Error("failed to close mo consumer", zap.Error(err))
	}
	cm.logger.Info("consumer manager closed")
}
