package transport

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

type gatewayTelemetry struct {
	tracer            trace.Tracer
	smppSubmits       metric.Int64Counter
	dlrEvents         metric.Int64Counter
	moEvents          metric.Int64Counter
	submitFailures    metric.Int64Counter
	smppLatencyMillis metric.Float64Histogram
}

func newGatewayTelemetry(logger *zap.Logger) *gatewayTelemetry {
	meter := otel.Meter("sms-gateway")

	smppSubmits, err := meter.Int64Counter(
		"ota.transport.smpp_submits",
		metric.WithDescription("SMPP submit attempts"),
	)
	if err != nil {
		logger.Warn("create smpp_submits metric", zap.Error(err))
	}
	dlrEvents, err := meter.Int64Counter(
		"ota.transport.dlr_events",
		metric.WithDescription("Delivery reports processed by the SMS gateway"),
	)
	if err != nil {
		logger.Warn("create dlr_events metric", zap.Error(err))
	}
	moEvents, err := meter.Int64Counter(
		"ota.transport.mo_events",
		metric.WithDescription("Mobile originated responses processed by the SMS gateway"),
	)
	if err != nil {
		logger.Warn("create mo_events metric", zap.Error(err))
	}
	submitFailures, err := meter.Int64Counter(
		"ota.transport.submit_failures",
		metric.WithDescription("SMPP submit failures"),
	)
	if err != nil {
		logger.Warn("create submit_failures metric", zap.Error(err))
	}
	smppLatencyMillis, err := meter.Float64Histogram(
		"ota.transport.smpp_latency_ms",
		metric.WithDescription("SMPP submit latency in milliseconds"),
	)
	if err != nil {
		logger.Warn("create smpp_latency metric", zap.Error(err))
	}

	return &gatewayTelemetry{
		tracer:            otel.Tracer("sms-gateway"),
		smppSubmits:       smppSubmits,
		dlrEvents:         dlrEvents,
		moEvents:          moEvents,
		submitFailures:    submitFailures,
		smppLatencyMillis: smppLatencyMillis,
	}
}

func (t *gatewayTelemetry) startSpan(ctx context.Context, name string, attrs ...attribute.KeyValue) (context.Context, trace.Span) {
	if t == nil {
		return ctx, trace.SpanFromContext(ctx)
	}
	return t.tracer.Start(ctx, name, trace.WithAttributes(attrs...))
}

func (t *gatewayTelemetry) finishSpan(span trace.Span, err error) {
	if t == nil {
		return
	}
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	} else {
		span.SetStatus(codes.Ok, "")
	}
	span.End()
}

func (t *gatewayTelemetry) recordSubmit(ctx context.Context, status string, elapsed time.Duration) {
	if t == nil {
		return
	}
	attrs := metric.WithAttributes(attribute.String("ota.status", status))
	if t.smppSubmits != nil {
		t.smppSubmits.Add(ctx, 1, attrs)
	}
	if t.smppLatencyMillis != nil {
		t.smppLatencyMillis.Record(ctx, float64(elapsed.Milliseconds()), attrs)
	}
}

func (t *gatewayTelemetry) recordSubmitFailure(ctx context.Context, status string) {
	if t == nil {
		return
	}
	if t.submitFailures != nil {
		t.submitFailures.Add(ctx, 1, metric.WithAttributes(attribute.String("ota.status", status)))
	}
}

func (t *gatewayTelemetry) recordDLR(ctx context.Context, status string) {
	if t == nil {
		return
	}
	if t.dlrEvents != nil {
		t.dlrEvents.Add(ctx, 1, metric.WithAttributes(attribute.String("ota.status", status)))
	}
}

func (t *gatewayTelemetry) recordMO(ctx context.Context) {
	if t == nil {
		return
	}
	if t.moEvents != nil {
		t.moEvents.Add(ctx, 1)
	}
}
