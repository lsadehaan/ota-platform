package pipeline

import (
	"context"
)

// ChannelCardStateWriter satisfies executor.CardStateWriter by sending to a channel.
type ChannelCardStateWriter struct {
	ch chan<- CardStateChange
}

func NewChannelCardStateWriter(ch chan<- CardStateChange) *ChannelCardStateWriter {
	return &ChannelCardStateWriter{ch: ch}
}

func (w *ChannelCardStateWriter) WriteCardState(ctx context.Context, ev CardStateChange) (*PublishFuture, error) {
	select {
	case w.ch <- ev:
		return ResolvedFuture(nil), nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}
