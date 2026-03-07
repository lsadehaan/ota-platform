package projector

import "go.uber.org/zap"

// Service is the read-model projector service.
type Service = LogWriter

// NewService constructs the current projector implementation.
func NewService(store MessageLogStore, brokers []string, logger *zap.Logger) *Service {
	return NewLogWriter(store, brokers, logger)
}
