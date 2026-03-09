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
// Topics can be scaled horizontally with multiple readers (each a separate
// ConcurrentConsumer in the same consumer group) to overcome the single
// fetch-pipeline bottleneck of kafka-go Reader.
type ConsumerManager struct {
	activateConsumers []*kafkapkg.ConcurrentConsumer
	dlrConsumers      []*kafkapkg.ConcurrentConsumer
	moConsumers       []*kafkapkg.ConcurrentConsumer
	cardWorker        *CardWorker
	logger            *zap.Logger
}

// NewConsumerManager creates a ConsumerManager with separate consumers for
// card-activate, card-dlr, and card-mo topics. Each topic can have its own
// worker count via CARD_ACTIVATE_WORKERS, CARD_DLR_WORKERS, CARD_MO_WORKERS.
// Falls back to CARD_WORKER_CONCURRENCY, then defaultWorkers.
//
// Each topic can also have multiple readers (CARD_ACTIVATE_READERS, etc.) to
// overcome the single fetch-pipeline bottleneck of kafka-go Reader. Multiple
// readers in the same consumer group each get a subset of partitions.
func NewConsumerManager(brokers []string, cardWorker *CardWorker, logger *zap.Logger) *ConsumerManager {
	baseWorkers := envIntDefault("CARD_WORKER_CONCURRENCY", defaultWorkers)
	activateWorkers := envIntDefault("CARD_ACTIVATE_WORKERS", baseWorkers)
	dlrWorkers := envIntDefault("CARD_DLR_WORKERS", baseWorkers)
	moWorkers := envIntDefault("CARD_MO_WORKERS", baseWorkers)

	activateReaders := envIntDefault("CARD_ACTIVATE_READERS", 1)
	dlrReaders := envIntDefault("CARD_DLR_READERS", 1)
	moReaders := envIntDefault("CARD_MO_READERS", 1)

	cm := &ConsumerManager{
		cardWorker: cardWorker,
		logger:     logger,
	}

	logger.Info("consumer worker config",
		zap.Int("activate_workers", activateWorkers),
		zap.Int("activate_readers", activateReaders),
		zap.Int("dlr_workers", dlrWorkers),
		zap.Int("dlr_readers", dlrReaders),
		zap.Int("mo_workers", moWorkers),
		zap.Int("mo_readers", moReaders),
	)

	cm.activateConsumers = makeConsumers(brokers, contractevents.TopicCardActivate, activateGroupID, activateReaders, activateWorkers, cardWorker.HandleEvent, logger.Named("activate-consumer"))
	cm.dlrConsumers = makeConsumers(brokers, contractevents.TopicCardDLR, dlrGroupID, dlrReaders, dlrWorkers, cardWorker.HandleEvent, logger.Named("dlr-consumer"))
	cm.moConsumers = makeConsumers(brokers, contractevents.TopicCardMO, moGroupID, moReaders, moWorkers, cardWorker.HandleEvent, logger.Named("mo-consumer"))

	return cm
}

// makeConsumers creates N ConcurrentConsumers for the same topic and consumer group.
// Workers are split evenly across readers (remainder goes to the last reader).
func makeConsumers(brokers []string, topic, groupID string, readers, totalWorkers int, handler func(ctx context.Context, key []byte, value []byte) error, logger *zap.Logger) []*kafkapkg.ConcurrentConsumer {
	if readers <= 0 {
		readers = 1
	}
	workersPerReader := totalWorkers / readers
	if workersPerReader < 1 {
		workersPerReader = 1
	}

	consumers := make([]*kafkapkg.ConcurrentConsumer, readers)
	for i := 0; i < readers; i++ {
		w := workersPerReader
		if i == readers-1 {
			// last reader gets remainder
			w = totalWorkers - workersPerReader*(readers-1)
			if w < 1 {
				w = 1
			}
		}
		consumers[i] = kafkapkg.NewConcurrentConsumer(
			brokers, topic, groupID, w, handler,
			logger.With(zap.Int("reader", i)),
		)
	}
	return consumers
}

func envIntDefault(key string, fallback int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return n
		}
	}
	return fallback
}

// Start launches all consumers concurrently. It blocks until ctx is cancelled.
func (cm *ConsumerManager) Start(ctx context.Context) {
	cm.logger.Info("starting card-events consumers (activate, dlr, mo)")

	var wg sync.WaitGroup

	startAll := func(consumers []*kafkapkg.ConcurrentConsumer, name string) {
		for _, c := range consumers {
			wg.Add(1)
			go func(c *kafkapkg.ConcurrentConsumer) {
				defer wg.Done()
				if err := c.Start(ctx); err != nil {
					cm.logger.Error(name+" consumer stopped", zap.Error(err))
				}
			}(c)
		}
	}

	startAll(cm.activateConsumers, "activate")
	startAll(cm.dlrConsumers, "dlr")
	startAll(cm.moConsumers, "mo")

	wg.Wait()
}

// Close shuts down all consumers.
func (cm *ConsumerManager) Close() {
	closeAll := func(consumers []*kafkapkg.ConcurrentConsumer, name string) {
		for _, c := range consumers {
			if err := c.Close(); err != nil {
				cm.logger.Error("failed to close "+name+" consumer", zap.Error(err))
			}
		}
	}

	closeAll(cm.activateConsumers, "activate")
	closeAll(cm.dlrConsumers, "dlr")
	closeAll(cm.moConsumers, "mo")
	cm.logger.Info("consumer manager closed")
}
