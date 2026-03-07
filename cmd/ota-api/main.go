package main

import (
	"go.uber.org/zap"

	"ota-platform/internal/bootstrap"
	"ota-platform/internal/controlplane"
)

func main() {
	logger := bootstrap.NewLogger()
	defer logger.Sync()

	ctx, cancel := bootstrap.SignalContext()
	defer cancel()

	if err := controlplane.Run(ctx, logger); err != nil {
		logger.Fatal("ota-api exited with error", zap.Error(err))
	}
}
