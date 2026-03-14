package executor

import (
	"context"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
)

type executorTelemetry struct {
	tracer             trace.Tracer
	eventsProcessed    metric.Int64Counter
	cardsProcessed     metric.Int64Counter
	retries            metric.Int64Counter
	dedupeErrors       metric.Int64Counter
	processingDuration metric.Float64Histogram
	opDuration         metric.Float64Histogram
}

func newExecutorTelemetry(logger *zap.Logger) *executorTelemetry {
	meter := otel.Meter("card-executor")

	eventsProcessed, err := meter.Int64Counter(
		"ota.executor.events_processed",
		metric.WithDescription("Card events processed by the executor"),
	)
	if err != nil {
		logger.Warn("create executor events_processed metric", zap.Error(err))
	}

	cardsProcessed, err := meter.Int64Counter(
		"ota.executor.cards_processed",
		metric.WithDescription("Cards reaching a terminal executor state"),
	)
	if err != nil {
		logger.Warn("create executor cards_processed metric", zap.Error(err))
	}

	retries, err := meter.Int64Counter(
		"ota.executor.retries",
		metric.WithDescription("Executor retry attempts"),
	)
	if err != nil {
		logger.Warn("create executor retries metric", zap.Error(err))
	}

	dedupeErrors, err := meter.Int64Counter(
		"ota.executor.dedupe_errors",
		metric.WithDescription("Deduplication check failures (fail-open events)"),
	)
	if err != nil {
		logger.Warn("create executor dedupe_errors metric", zap.Error(err))
	}

	processingDuration, err := meter.Float64Histogram(
		"ota.executor.processing_duration_ms",
		metric.WithDescription("Executor event processing duration in milliseconds"),
	)
	if err != nil {
		logger.Warn("create executor processing_duration metric", zap.Error(err))
	}

	opDuration, err := meter.Float64Histogram(
		"ota.executor.op_duration_ms",
		metric.WithDescription("Per-operation duration within executor handlers"),
	)
	if err != nil {
		logger.Warn("create executor op_duration metric", zap.Error(err))
	}

	return &executorTelemetry{
		tracer:             otel.Tracer("card-executor"),
		eventsProcessed:    eventsProcessed,
		cardsProcessed:     cardsProcessed,
		retries:            retries,
		dedupeErrors:       dedupeErrors,
		processingDuration: processingDuration,
		opDuration:         opDuration,
	}
}

func (t *executorTelemetry) startEvent(ctx context.Context, eventType, campaignID, cardID string, step int) (context.Context, trace.Span, time.Time) {
	if t == nil {
		return ctx, trace.SpanFromContext(ctx), time.Now()
	}
	ctx, span := t.tracer.Start(ctx, "executor."+eventType,
		trace.WithAttributes(
			attribute.String("ota.event.type", eventType),
			attribute.String("ota.campaign_id", campaignID),
			attribute.String("ota.card_id", cardID),
			attribute.Int("ota.step", step),
		),
	)
	return ctx, span, time.Now()
}

func (t *executorTelemetry) finishEvent(ctx context.Context, span trace.Span, started time.Time, eventType string, err error) {
	if t == nil {
		return
	}
	if t.eventsProcessed != nil {
		t.eventsProcessed.Add(ctx, 1, metric.WithAttributes(attribute.String("ota.event.type", eventType)))
	}
	if t.processingDuration != nil {
		t.processingDuration.Record(ctx, float64(time.Since(started).Milliseconds()), metric.WithAttributes(attribute.String("ota.event.type", eventType)))
	}
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	} else {
		span.SetStatus(codes.Ok, "")
	}
	span.End()
}

func (t *executorTelemetry) recordCardProcessed(ctx context.Context, outcome string) {
	if t == nil {
		return
	}
	if t.cardsProcessed != nil {
		t.cardsProcessed.Add(ctx, 1, metric.WithAttributes(attribute.String("ota.outcome", outcome)))
	}
}

func (t *executorTelemetry) recordDedupeError(ctx context.Context) {
	if t == nil {
		return
	}
	if t.dedupeErrors != nil {
		t.dedupeErrors.Add(ctx, 1)
	}
}

func (t *executorTelemetry) recordRetry(ctx context.Context, reason string) {
	if t == nil {
		return
	}
	if t.retries != nil {
		t.retries.Add(ctx, 1, metric.WithAttributes(attribute.String("ota.reason", reason)))
	}
}

func (t *executorTelemetry) recordOp(ctx context.Context, handler, op string, elapsed time.Duration) {
	if t == nil || t.opDuration == nil {
		return
	}
	t.opDuration.Record(ctx, float64(elapsed.Microseconds())/1000.0,
		metric.WithAttributes(
			attribute.String("handler", handler),
			attribute.String("op", op),
		),
	)
}
