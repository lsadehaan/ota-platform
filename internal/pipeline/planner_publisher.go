package pipeline

import (
	"context"
	"fmt"
)

// PlannerPublisher satisfies planner.Publisher and planner.BatchPublisher.
type PlannerPublisher struct {
	ch chan<- CardEvent
}

func NewPlannerPublisher(ch chan<- CardEvent) *PlannerPublisher {
	return &PlannerPublisher{ch: ch}
}

func (p *PlannerPublisher) Publish(ctx context.Context, key string, message interface{}) (*PublishFuture, error) {
	msg, ok := message.(CardEvent)
	if !ok {
		return nil, fmt.Errorf("PlannerPublisher: expected CardEvent, got %T", message)
	}
	select {
	case p.ch <- msg:
		return ResolvedFuture(nil), nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (p *PlannerPublisher) PublishBatch(ctx context.Context, items []BatchItem) error {
	for _, item := range items {
		msg, ok := item.Value.(CardEvent)
		if !ok {
			return fmt.Errorf("PlannerPublisher.PublishBatch: expected CardEvent, got %T", item.Value)
		}
		select {
		case p.ch <- msg:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}
