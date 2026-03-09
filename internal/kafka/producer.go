package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/segmentio/kafka-go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
	"go.uber.org/zap"

	"ota-platform/internal/config"
	"ota-platform/internal/observability"
)

// Producer wraps a kafka-go writer for producing messages.
type Producer struct {
	writer        *kafka.Writer
	logger        *zap.Logger
	publishErrors metric.Int64Counter
}

type ProducerOptions struct {
	RequiredAcks    kafka.RequiredAcks
	RequiredAcksSet bool
	Async           bool
	BatchSize       int
	BatchTimeout    time.Duration
}

const (
	defaultProducerBatchSize      = 1000
	defaultProducerBatchTimeoutMS = 50
)

func producerBatchSize() int {
	size := config.GetEnvInt("KAFKA_PRODUCER_BATCH_SIZE", defaultProducerBatchSize)
	if size <= 0 {
		return defaultProducerBatchSize
	}
	return size
}

func producerBatchTimeout() time.Duration {
	ms := config.GetEnvInt("KAFKA_PRODUCER_BATCH_TIMEOUT_MS", defaultProducerBatchTimeoutMS)
	if ms <= 0 {
		ms = defaultProducerBatchTimeoutMS
	}
	return time.Duration(ms) * time.Millisecond
}

// NewProducer creates a new Kafka producer for the given topic.
func NewProducer(brokers []string, topic string, logger *zap.Logger) *Producer {
	return NewProducerWithOptions(brokers, topic, ProducerOptions{
		RequiredAcks:    kafka.RequireAll,
		RequiredAcksSet: true,
		BatchSize:       producerBatchSize(),
		BatchTimeout:    producerBatchTimeout(),
	}, logger)
}

// NewProducerWithOptions creates a Kafka producer with explicit writer options.
func NewProducerWithOptions(brokers []string, topic string, opts ProducerOptions, logger *zap.Logger) *Producer {
	if !opts.RequiredAcksSet {
		opts.RequiredAcks = kafka.RequireAll
	}
	if opts.BatchSize <= 0 {
		opts.BatchSize = producerBatchSize()
	}
	if opts.BatchTimeout <= 0 {
		opts.BatchTimeout = producerBatchTimeout()
	}
	w := &kafka.Writer{
		Addr:         kafka.TCP(brokers...),
		Topic:        topic,
		Balancer:     &kafka.Hash{},
		RequiredAcks: opts.RequiredAcks,
		Async:        opts.Async,
		BatchSize:    opts.BatchSize,
		BatchTimeout: opts.BatchTimeout,
	}
	meter := otel.Meter("kafka-producer")
	publishErrors, _ := meter.Int64Counter("ota.kafka.publish_errors",
		metric.WithDescription("Total Kafka publish errors"),
	)

	return &Producer{
		writer:        w,
		logger:        logger,
		publishErrors: publishErrors,
	}
}

// Publish sends a message with the given key (used for partitioning) and value
// (JSON-serializable). The value is marshalled to JSON before sending.
func (p *Producer) Publish(ctx context.Context, key string, value interface{}) error {
	data, err := json.Marshal(value)
	if err != nil {
		p.logger.Error("failed to marshal kafka message value",
			zap.String("key", key),
			zap.Error(err),
		)
		return fmt.Errorf("marshal kafka message: %w", err)
	}

	msg := kafka.Message{
		Key:   []byte(key),
		Value: data,
	}
	observability.InjectKafkaHeaders(ctx, &msg.Headers)

	if err := p.writer.WriteMessages(ctx, msg); err != nil {
		if p.publishErrors != nil {
			p.publishErrors.Add(ctx, 1)
		}
		p.logger.Error("failed to publish kafka message",
			zap.String("topic", p.writer.Topic),
			zap.String("key", key),
			zap.Error(err),
		)
		return fmt.Errorf("publish to %s: %w", p.writer.Topic, err)
	}

	p.logger.Debug("published kafka message",
		zap.String("topic", p.writer.Topic),
		zap.String("key", key),
		zap.Int("size", len(data)),
	)
	return nil
}

// BatchItem represents a single message in a batch publish operation.
type BatchItem struct {
	Key   string
	Value interface{}
}

// PublishBatch sends multiple messages in a single Kafka write.
// Each item is a key-value pair where value will be JSON-marshalled.
func (p *Producer) PublishBatch(ctx context.Context, items []BatchItem) error {
	msgs := make([]kafka.Message, 0, len(items))
	for _, item := range items {
		data, err := json.Marshal(item.Value)
		if err != nil {
			p.logger.Error("failed to marshal batch item",
				zap.String("key", item.Key),
				zap.Error(err),
			)
			return fmt.Errorf("marshal batch item (key=%s): %w", item.Key, err)
		}
		msgs = append(msgs, kafka.Message{
			Key:   []byte(item.Key),
			Value: data,
		})
		observability.InjectKafkaHeaders(ctx, &msgs[len(msgs)-1].Headers)
	}

	if err := p.writer.WriteMessages(ctx, msgs...); err != nil {
		if p.publishErrors != nil {
			p.publishErrors.Add(ctx, 1)
		}
		p.logger.Error("failed to publish kafka batch",
			zap.String("topic", p.writer.Topic),
			zap.Int("count", len(msgs)),
			zap.Error(err),
		)
		return fmt.Errorf("publish batch to %s: %w", p.writer.Topic, err)
	}

	p.logger.Debug("published kafka batch",
		zap.String("topic", p.writer.Topic),
		zap.Int("count", len(msgs)),
	)
	return nil
}

// PublishRaw sends a pre-serialized message (no JSON marshalling).
func (p *Producer) PublishRaw(ctx context.Context, key string, value []byte) error {
	msg := kafka.Message{
		Key:   []byte(key),
		Value: value,
	}
	observability.InjectKafkaHeaders(ctx, &msg.Headers)

	if err := p.writer.WriteMessages(ctx, msg); err != nil {
		if p.publishErrors != nil {
			p.publishErrors.Add(ctx, 1)
		}
		p.logger.Error("failed to publish raw kafka message",
			zap.String("topic", p.writer.Topic),
			zap.String("key", key),
			zap.Error(err),
		)
		return fmt.Errorf("publish raw to %s: %w", p.writer.Topic, err)
	}

	p.logger.Debug("published raw kafka message",
		zap.String("topic", p.writer.Topic),
		zap.String("key", key),
		zap.Int("size", len(value)),
	)
	return nil
}

// Close flushes pending writes and closes the producer.
func (p *Producer) Close() error {
	p.logger.Info("closing kafka producer", zap.String("topic", p.writer.Topic))
	return p.writer.Close()
}
