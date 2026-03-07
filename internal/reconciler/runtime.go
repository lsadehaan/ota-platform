package reconciler

import (
	"context"

	"go.uber.org/zap"

	"ota-platform/internal/bootstrap"
)

func Run(ctx context.Context, logger *zap.Logger) error {
	database := bootstrap.MustGormDB(logger)
	service := NewService(database, logger.Named("reconciler"))
	return service.Run(ctx)
}
