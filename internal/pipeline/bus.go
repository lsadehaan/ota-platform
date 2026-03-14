package pipeline

import (
	"context"
	"os"
	"strconv"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
)

type Bus struct {
	ActivateCh   chan CardEvent
	SendSMSCh    chan SendSMSMessage
	DLRCh        chan CardEvent
	MOCh         chan CardEvent
	MessageLogCh chan MessageLogAction
	CardStateCh  chan CardStateChange
}

func NewBus() *Bus {
	plannerBatch := envInt("PLANNER_CLAIM_BATCH_SIZE", 5000)
	bufSize := envInt("ENGINE_CHANNEL_BUFFER", plannerBatch*5)
	// Writer channels get larger buffers to absorb bursts — each card
	// generates ~4 message log events and ~2 state changes, so these
	// channels see higher throughput than the event channels.
	msgLogBuf := envInt("ENGINE_MSG_LOG_BUFFER", 100000)
	cardStateBuf := envInt("ENGINE_CARD_STATE_BUFFER", 50000)
	return &Bus{
		ActivateCh:   make(chan CardEvent, bufSize),
		SendSMSCh:    make(chan SendSMSMessage, bufSize),
		DLRCh:        make(chan CardEvent, bufSize),
		MOCh:         make(chan CardEvent, bufSize),
		MessageLogCh: make(chan MessageLogAction, msgLogBuf),
		CardStateCh:  make(chan CardStateChange, cardStateBuf),
	}
}

func (b *Bus) Close() {
	close(b.ActivateCh)
	close(b.SendSMSCh)
	close(b.DLRCh)
	close(b.MOCh)
	close(b.MessageLogCh)
	close(b.CardStateCh)
}

// RegisterMetrics registers OTel Int64ObservableGauge instruments that report
// the current depth (len) of every Bus channel.
func (b *Bus) RegisterMetrics() {
	meter := otel.Meter("ota-engine")

	mustGauge := func(name, desc string, cb metric.Int64Callback) {
		_, _ = meter.Int64ObservableGauge(name,
			metric.WithDescription(desc),
			metric.WithInt64Callback(cb),
		)
	}

	mustGauge("ota.bus.activate_ch_depth", "Depth of ActivateCh channel",
		func(_ context.Context, o metric.Int64Observer) error {
			o.Observe(int64(len(b.ActivateCh)))
			return nil
		})
	mustGauge("ota.bus.send_sms_ch_depth", "Depth of SendSMSCh channel",
		func(_ context.Context, o metric.Int64Observer) error {
			o.Observe(int64(len(b.SendSMSCh)))
			return nil
		})
	mustGauge("ota.bus.dlr_ch_depth", "Depth of DLRCh channel",
		func(_ context.Context, o metric.Int64Observer) error {
			o.Observe(int64(len(b.DLRCh)))
			return nil
		})
	mustGauge("ota.bus.mo_ch_depth", "Depth of MOCh channel",
		func(_ context.Context, o metric.Int64Observer) error {
			o.Observe(int64(len(b.MOCh)))
			return nil
		})
	mustGauge("ota.bus.message_log_ch_depth", "Depth of MessageLogCh channel",
		func(_ context.Context, o metric.Int64Observer) error {
			o.Observe(int64(len(b.MessageLogCh)))
			return nil
		})
	mustGauge("ota.bus.card_state_ch_depth", "Depth of CardStateCh channel",
		func(_ context.Context, o metric.Int64Observer) error {
			o.Observe(int64(len(b.CardStateCh)))
			return nil
		})
}

// ChannelDepths returns the current depth of every Bus channel.
func (b *Bus) ChannelDepths() map[string]int {
	return map[string]int{
		"activate":    len(b.ActivateCh),
		"send_sms":    len(b.SendSMSCh),
		"dlr":         len(b.DLRCh),
		"mo":          len(b.MOCh),
		"message_log": len(b.MessageLogCh),
		"card_state":  len(b.CardStateCh),
	}
}

func envInt(key string, fallback int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return n
		}
	}
	return fallback
}
