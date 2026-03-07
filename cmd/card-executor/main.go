package main

import (
	"go.uber.org/zap"

	"ota-platform/internal/bootstrap"
	"ota-platform/internal/executor"
)

func main() {
	logger := bootstrap.NewLogger()
	defer logger.Sync()

	ctx, cancel := bootstrap.SignalContext()
	defer cancel()

	if err := executor.Run(ctx, logger); err != nil {
		logger.Fatal("card-executor exited with error", zap.Error(err))
	}
}
