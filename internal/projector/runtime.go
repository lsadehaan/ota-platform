package projector

import (
	"context"

	"go.uber.org/zap"

	"ota-platform/internal/bootstrap"
	scyllastore "ota-platform/internal/scylla"
)

func Run(ctx context.Context, logger *zap.Logger) error {
	scylla := bootstrap.MustScylla(logger)
	defer scylla.Close()
	store := scyllastore.NewMessageLogStore(scylla)
	logWriter := NewService(store, bootstrap.KafkaBrokers(), logger.Named("log-writer"))
	defer logWriter.Close()

	done := make(chan struct{})
	go func() {
		logWriter.Start(ctx)
		close(done)
	}()

	select {
	case <-ctx.Done():
		<-done
		logger.Info("read-model projector shutdown complete")
		return nil
	case <-done:
		return nil
	}
}
