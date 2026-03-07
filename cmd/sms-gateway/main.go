package main

import (
	"go.uber.org/zap"

	"ota-platform/internal/bootstrap"
	"ota-platform/internal/transport"
)

func main() {
	logger := bootstrap.NewLogger()
	defer logger.Sync()

	ctx, cancel := bootstrap.SignalContext()
	defer cancel()

	if err := transport.Run(ctx, logger); err != nil {
		logger.Fatal("sms-gateway exited with error", zap.Error(err))
	}
}
