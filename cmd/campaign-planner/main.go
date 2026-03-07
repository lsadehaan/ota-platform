package main

import (
	"go.uber.org/zap"

	"ota-platform/internal/bootstrap"
	"ota-platform/internal/planner"
)

func main() {
	logger := bootstrap.NewLogger()
	defer logger.Sync()

	ctx, cancel := bootstrap.SignalContext()
	defer cancel()

	if err := planner.Run(ctx, logger); err != nil {
		logger.Fatal("campaign-planner exited with error", zap.Error(err))
	}
}
