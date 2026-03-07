package executor

import (
	"context"

	"go.uber.org/zap"

	"ota-platform/internal/bootstrap"
	contractevents "ota-platform/internal/contracts/events"
	kafkapkg "ota-platform/internal/kafka"
	scyllastore "ota-platform/internal/scylla"
)

func Run(ctx context.Context, logger *zap.Logger) error {
	database := bootstrap.MustGormDB(logger)
	scylla := bootstrap.MustScylla(logger)
	defer scylla.Close()
	rdb := bootstrap.MustCoordinationStore(logger)
	defer rdb.Close()

	kafkaBrokers := bootstrap.KafkaBrokers()
	smsProducer := kafkapkg.NewProducer(kafkaBrokers, contractevents.TopicSendSMS, logger.Named("sms-producer"))
	defer smsProducer.Close()

	logProducer := kafkapkg.NewProducer(kafkaBrokers, contractevents.TopicMessageLog, logger.Named("log-producer"))
	defer logProducer.Close()

	eventProducer := kafkapkg.NewProducer(kafkaBrokers, contractevents.TopicCardEvents, logger.Named("event-producer"))
	defer eventProducer.Close()

	executionStore := scyllastore.NewExecutionStore(scylla, database)
	cardWorker := NewService(database, executionStore, rdb, smsProducer, logProducer, eventProducer, nil, logger.Named("card-worker"))
	consumerMgr := NewConsumerManager(kafkaBrokers, cardWorker, logger.Named("consumer"))
	defer consumerMgr.Close()

	consumerDone := make(chan struct{})
	go func() {
		consumerMgr.Start(ctx)
		close(consumerDone)
	}()

	select {
	case <-ctx.Done():
		<-consumerDone
		logger.Info("card executor shutdown complete")
		return nil
	case <-consumerDone:
		return nil
	}
}
