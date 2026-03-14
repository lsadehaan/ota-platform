package pipeline

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync/atomic"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
)

// SMSProducer sends SendSMSMessage to a channel, satisfying executor.Producer.
type SMSProducer struct {
	ch chan<- SendSMSMessage
}

func NewSMSProducer(ch chan<- SendSMSMessage) *SMSProducer {
	return &SMSProducer{ch: ch}
}

func (p *SMSProducer) Publish(ctx context.Context, key string, message interface{}) (*PublishFuture, error) {
	msg, ok := message.(SendSMSMessage)
	if !ok {
		return nil, fmt.Errorf("SMSProducer: expected SendSMSMessage, got %T", message)
	}
	select {
	case p.ch <- msg:
		return ResolvedFuture(nil), nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (p *SMSProducer) Close() error { return nil }

// LogProducer sends MessageLogAction to a channel, satisfying executor.Producer.
// Non-blocking: drops events when the channel is full rather than stalling the
// executor hot path (which cascades into SMPP backpressure and throughput collapse).
type LogProducer struct {
	ch       chan<- MessageLogAction
	dropped  atomic.Int64
	disabled bool
}

// NewLogProducer creates a LogProducer. If ENGINE_MSG_LOG_DISABLED=true, the
// producer silently discards all events (no channel writes, no Postgres load).
func NewLogProducer(ch chan<- MessageLogAction) *LogProducer {
	p := &LogProducer{ch: ch}
	if strings.EqualFold(os.Getenv("ENGINE_MSG_LOG_DISABLED"), "true") {
		p.disabled = true
	}
	return p
}

func (p *LogProducer) Publish(ctx context.Context, key string, message interface{}) (*PublishFuture, error) {
	if p.disabled {
		return ResolvedFuture(nil), nil
	}
	msg, ok := message.(MessageLogAction)
	if !ok {
		return nil, fmt.Errorf("LogProducer: expected MessageLogAction, got %T", message)
	}
	select {
	case p.ch <- msg:
		return ResolvedFuture(nil), nil
	default:
		p.dropped.Add(1)
		return ResolvedFuture(nil), nil
	}
}

// Dropped returns the total number of dropped message log events.
func (p *LogProducer) Dropped() int64 {
	return p.dropped.Load()
}

// RegisterDropMetric registers an OTel gauge that reports dropped message log events.
func (p *LogProducer) RegisterDropMetric() {
	meter := otel.Meter("ota-engine")
	_, _ = meter.Int64ObservableGauge("ota.bus.message_log_dropped",
		metric.WithDescription("Total message log events dropped due to channel backpressure"),
		metric.WithInt64Callback(func(_ context.Context, o metric.Int64Observer) error {
			o.Observe(p.dropped.Load())
			return nil
		}),
	)
}

func (p *LogProducer) Close() error { return nil }

// EventProducer sends CardEvent to a channel (for DLR/MO events back to executor).
type EventProducer struct {
	ch chan<- CardEvent
}

func NewEventProducer(ch chan<- CardEvent) *EventProducer {
	return &EventProducer{ch: ch}
}

func (p *EventProducer) Publish(ctx context.Context, key string, message interface{}) (*PublishFuture, error) {
	msg, ok := message.(CardEvent)
	if !ok {
		return nil, fmt.Errorf("EventProducer: expected CardEvent, got %T", message)
	}
	select {
	case p.ch <- msg:
		return ResolvedFuture(nil), nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (p *EventProducer) Close() error { return nil }
