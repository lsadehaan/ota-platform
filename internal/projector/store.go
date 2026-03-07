package projector

import (
	"context"

	"ota-platform/internal/db"
	kafkapkg "ota-platform/internal/kafka"
)

// MessageLogStore owns durable message-log persistence.
type MessageLogStore interface {
	CreateBatch(ctx context.Context, logs []db.MessageLog, batchSize int) error
	ApplyUpdates(ctx context.Context, updates []kafkapkg.MessageLogAction) error
}
