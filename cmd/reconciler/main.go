package main

import (
	"go.uber.org/zap"

	"ota-platform/internal/bootstrap"
	"ota-platform/internal/reconciler"
)

func main() {
	logger := bootstrap.NewLogger()
	defer logger.Sync()

	ctx, cancel := bootstrap.SignalContext()
	defer cancel()

	if err := reconciler.Run(ctx, logger); err != nil {
		logger.Fatal("reconciler exited with error", zap.Error(err))
	}
}
