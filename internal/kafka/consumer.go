package kafka

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"

	"ota-platform/internal/config"
	"ota-platform/internal/observability"
)

const maxRetries = 3

func consumerMaxWait() time.Duration {
	ms := config.GetEnvInt("KAFKA_CONSUMER_MAX_WAIT_MS", 50)
	if ms <= 0 {
		ms = 50
	}
	return time.Duration(ms) * time.Millisecond
}

func consumerQueueCapacity() int {
	n := config.GetEnvInt("KAFKA_CONSUMER_QUEUE_CAPACITY", 10000)
	if n <= 0 {
		n = 10000
	}
	return n
}

// MessageHandler is called for each consumed message. Returning a non-nil error
// causes the consumer to retry inline before the message is treated as a poison pill.
type MessageHandler func(ctx context.Context, key []byte, value []byte) error

// Consumer wraps a kafka-go reader for consuming messages as part of a consumer group.
type Consumer struct {
	reader    *kafka.Reader
	handler   MessageHandler
	logger    *zap.Logger
	retries   map[string]int
	retriesMu sync.Mutex
}

// NewConsumer creates a new Kafka consumer that joins the specified consumer group.
func NewConsumer(brokers []string, topic string, groupID string, handler MessageHandler, logger *zap.Logger) *Consumer {
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:       brokers,
		Topic:         topic,
		GroupID:       groupID,
		MinBytes:      1,
		MaxBytes:      10e6, // 10 MB
		MaxWait:       consumerMaxWait(),
		QueueCapacity: consumerQueueCapacity(),
	})
	return &Consumer{
		reader:  r,
		handler: handler,
		logger:  logger,
		retries: make(map[string]int),
	}
}

// Start begins consuming messages in a loop. It blocks until ctx is cancelled.
// Each successfully handled message is committed. Handler errors are retried inline;
// if all retries fail the message is treated as a poison pill and committed to skip it.
func (c *Consumer) Start(ctx context.Context) error {
	c.logger.Info("kafka consumer started",
		zap.String("topic", c.reader.Config().Topic),
		zap.String("group", c.reader.Config().GroupID),
	)

	for {
		msg, err := c.reader.FetchMessage(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				c.logger.Info("kafka consumer stopping due to context cancellation")
				return nil
			}
			c.logger.Error("failed to fetch kafka message", zap.Error(err))
			return fmt.Errorf("fetch message: %w", err)
		}

		c.logger.Debug("received kafka message",
			zap.String("topic", msg.Topic),
			zap.Int("partition", msg.Partition),
			zap.Int64("offset", msg.Offset),
			zap.String("key", string(msg.Key)),
		)

		msgCtx := observability.ExtractKafkaContext(ctx, msg.Headers)
		if err := c.handleWithRetry(msgCtx, msg); err != nil {
			c.logger.Error("poison pill: handler failed after max retries, committing to skip",
				zap.String("topic", msg.Topic),
				zap.Int("partition", msg.Partition),
				zap.Int64("offset", msg.Offset),
				zap.Int("retries", maxRetries),
				zap.String("key", string(msg.Key)),
				zap.Error(err),
			)
		}

		if err := c.reader.CommitMessages(ctx, msg); err != nil {
			c.logger.Error("failed to commit kafka message",
				zap.String("topic", msg.Topic),
				zap.Int("partition", msg.Partition),
				zap.Int64("offset", msg.Offset),
				zap.Error(err),
			)
		}
	}
}

func (c *Consumer) handleWithRetry(ctx context.Context, msg kafka.Message) error {
	var lastErr error
	for attempt := 1; attempt <= maxRetries; attempt++ {
		if err := c.handler(ctx, msg.Key, msg.Value); err != nil {
			lastErr = err
			if attempt < maxRetries {
				c.logger.Warn("handler error, retrying inline",
					zap.String("topic", msg.Topic),
					zap.Int("partition", msg.Partition),
					zap.Int64("offset", msg.Offset),
					zap.Int("attempt", attempt),
					zap.Error(err),
				)
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(time.Duration(attempt) * 50 * time.Millisecond):
				}
				continue
			}
			return lastErr
		}
		return nil
	}
	return lastErr
}

// Close closes the consumer, leaving the consumer group.
func (c *Consumer) Close() error {
	c.logger.Info("closing kafka consumer",
		zap.String("topic", c.reader.Config().Topic),
		zap.String("group", c.reader.Config().GroupID),
	)
	return c.reader.Close()
}

// partitionTracker tracks completed offsets for a single partition to enable
// safe contiguous-offset commits.
type partitionTracker struct {
	mu        sync.Mutex
	pending   map[int64]bool // offsets currently being processed
	completed map[int64]bool // offsets that finished processing
	committed int64          // highest committed offset (-1 means none)
}

// ConcurrentConsumer processes Kafka messages using a pool of worker goroutines.
// Messages with the same key are guaranteed to be processed in order by routing
// them to the same worker based on key hash.
// Offsets are committed in contiguous order per partition to avoid skipping messages.
type ConcurrentConsumer struct {
	reader    *kafka.Reader
	handler   func(ctx context.Context, key []byte, value []byte) error
	workers   int
	logger    *zap.Logger
	retries   map[string]int
	retriesMu sync.Mutex
}

// NewConcurrentConsumer creates a consumer with N worker goroutines.
// Messages are dispatched to workers by hashing the message key, ensuring
// per-key ordering (critical for per-card event ordering).
func NewConcurrentConsumer(brokers []string, topic, groupID string, workers int, handler func(ctx context.Context, key []byte, value []byte) error, logger *zap.Logger) *ConcurrentConsumer {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        brokers,
		Topic:          topic,
		GroupID:        groupID,
		MinBytes:       1,
		MaxBytes:       10e6,
		MaxWait:        consumerMaxWait(),
		QueueCapacity:  consumerQueueCapacity(),
		CommitInterval: 0, // manual commits only
		StartOffset:    kafka.FirstOffset,
	})
	return &ConcurrentConsumer{
		reader:  reader,
		handler: handler,
		workers: workers,
		logger:  logger,
		retries: make(map[string]int),
	}
}

// Start begins consuming messages and dispatching them to worker goroutines.
// It blocks until ctx is cancelled. Messages with the same key are always routed
// to the same worker, preserving per-key ordering while allowing parallel processing
// of different keys.
//
// Offsets are committed safely: a commit goroutine tracks completed offsets per
// partition and only commits offset N when all offsets <= N are also completed.
func (cc *ConcurrentConsumer) Start(ctx context.Context) error {
	cc.logger.Info("concurrent kafka consumer started",
		zap.String("topic", cc.reader.Config().Topic),
		zap.String("group", cc.reader.Config().GroupID),
		zap.Int("workers", cc.workers),
	)

	workerChs := make([]chan kafka.Message, cc.workers)
	commitCh := make(chan kafka.Message, 1000)
	var wg sync.WaitGroup

	// Commit goroutine: receives completed messages, tracks per-partition, commits contiguously
	var commitWg sync.WaitGroup
	commitWg.Add(1)
	go func() {
		defer commitWg.Done()
		cc.commitLoop(ctx, commitCh)
	}()

	for i := 0; i < cc.workers; i++ {
		workerChs[i] = make(chan kafka.Message, 100)
		wg.Add(1)
		go func(ch chan kafka.Message, workerID int) {
			defer wg.Done()
			for msg := range ch {
				msgCtx := observability.ExtractKafkaContext(ctx, msg.Headers)
				if err := cc.handleWithRetry(msgCtx, msg, workerID); err != nil {
					cc.logger.Error("poison pill: handler failed after max retries, committing to skip",
						zap.Int("worker", workerID),
						zap.String("topic", msg.Topic),
						zap.Int("partition", msg.Partition),
						zap.Int64("offset", msg.Offset),
						zap.Int("retries", maxRetries),
						zap.String("key", string(msg.Key)),
						zap.Error(err),
					)
				}

				commitCh <- msg
			}
		}(workerChs[i], i)
	}

	// Main fetch loop
	for {
		msg, err := cc.reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				break
			}
			cc.logger.Error("fetch error", zap.Error(err))
			continue
		}

		// Route by key hash (FNV) — ensures per-key ordering
		h := fnv32(msg.Key)
		workerChs[h%uint32(cc.workers)] <- msg
	}

	// Close worker channels and wait for workers to drain
	for _, ch := range workerChs {
		close(ch)
	}
	wg.Wait()

	// Close commit channel and wait for final flush
	close(commitCh)
	commitWg.Wait()

	return ctx.Err()
}

func (cc *ConcurrentConsumer) handleWithRetry(ctx context.Context, msg kafka.Message, workerID int) error {
	var lastErr error
	for attempt := 1; attempt <= maxRetries; attempt++ {
		if err := cc.handler(ctx, msg.Key, msg.Value); err != nil {
			lastErr = err
			if attempt < maxRetries {
				cc.logger.Warn("worker handler error, retrying inline",
					zap.Int("worker", workerID),
					zap.String("topic", msg.Topic),
					zap.Int("partition", msg.Partition),
					zap.Int64("offset", msg.Offset),
					zap.Int("attempt", attempt),
					zap.Error(err),
				)
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(time.Duration(attempt) * 50 * time.Millisecond):
				}
				continue
			}
			return lastErr
		}
		return nil
	}
	return lastErr
}

// commitLoop receives completed messages, tracks them per partition, and commits
// the highest contiguous offset for each partition. It also flushes periodically.
func (cc *ConcurrentConsumer) commitLoop(ctx context.Context, commitCh <-chan kafka.Message) {
	trackers := make(map[int]*partitionTracker)
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	// getTracker returns or creates a tracker for the given partition.
	getTracker := func(partition int) *partitionTracker {
		t, ok := trackers[partition]
		if !ok {
			t = &partitionTracker{
				pending:   make(map[int64]bool),
				completed: make(map[int64]bool),
				committed: -1,
			}
			trackers[partition] = t
		}
		return t
	}

	// flushAll commits contiguous offsets for all partitions.
	flushAll := func() {
		for partition, t := range trackers {
			cc.flushPartition(ctx, partition, t)
		}
	}

	for {
		select {
		case msg, ok := <-commitCh:
			if !ok {
				// Channel closed — final flush
				flushAll()
				return
			}
			t := getTracker(msg.Partition)
			t.mu.Lock()
			t.completed[msg.Offset] = true
			delete(t.pending, msg.Offset)
			t.mu.Unlock()

			cc.flushPartition(ctx, msg.Partition, t)

		case <-ticker.C:
			flushAll()
		}
	}
}

// flushPartition finds the highest contiguous completed offset from committed+1
// and commits it if there is progress.
func (cc *ConcurrentConsumer) flushPartition(ctx context.Context, partition int, t *partitionTracker) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if len(t.completed) == 0 {
		return
	}

	// Find the highest contiguous completed offset starting from committed+1.
	next := t.committed + 1
	highWater := t.committed
	for t.completed[next] {
		highWater = next
		next++
	}

	if highWater <= t.committed {
		return // no contiguous progress
	}

	// Build a commit message for the high-water offset.
	// kafka-go CommitMessages commits offset+1 (the next offset to read).
	commitMsg := kafka.Message{
		Topic:     cc.reader.Config().Topic,
		Partition: partition,
		Offset:    highWater,
	}

	if err := cc.reader.CommitMessages(ctx, commitMsg); err != nil {
		cc.logger.Error("failed to commit contiguous offset",
			zap.Int("partition", partition),
			zap.Int64("offset", highWater),
			zap.Error(err),
		)
		return
	}

	cc.logger.Debug("committed contiguous offset",
		zap.Int("partition", partition),
		zap.Int64("offset", highWater),
		zap.Int64("previous", t.committed),
	)

	// Clean up completed offsets that have been committed.
	offsets := make([]int64, 0, len(t.completed))
	for o := range t.completed {
		offsets = append(offsets, o)
	}
	sort.Slice(offsets, func(i, j int) bool { return offsets[i] < offsets[j] })
	for _, o := range offsets {
		if o <= highWater {
			delete(t.completed, o)
		}
	}

	t.committed = highWater
}

// Close closes the concurrent consumer, leaving the consumer group.
func (cc *ConcurrentConsumer) Close() error {
	cc.logger.Info("closing concurrent kafka consumer",
		zap.String("topic", cc.reader.Config().Topic),
		zap.String("group", cc.reader.Config().GroupID),
	)
	return cc.reader.Close()
}

// fnv32 computes a 32-bit FNV-1a hash of the given data.
func fnv32(data []byte) uint32 {
	h := uint32(2166136261)
	for _, b := range data {
		h ^= uint32(b)
		h *= 16777619
	}
	return h
}
