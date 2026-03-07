package main

import (
	"go.uber.org/zap"

	"ota-platform/internal/bootstrap"
	"ota-platform/internal/projector"
)

func main() {
	logger := bootstrap.NewLogger()
	defer logger.Sync()

	ctx, cancel := bootstrap.SignalContext()
	defer cancel()

	if err := projector.Run(ctx, logger); err != nil {
		logger.Fatal("read-model-projector exited with error", zap.Error(err))
	}
}
