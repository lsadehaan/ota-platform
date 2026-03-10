package executor

import (
	"context"
	"fmt"
	"time"

	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"

	"ota-platform/internal/bootstrap"
	"ota-platform/internal/config"
	contractevents "ota-platform/internal/contracts/events"
	kafkapkg "ota-platform/internal/kafka"
	"ota-platform/internal/keystore"
	"ota-platform/internal/observability"
	scyllastore "ota-platform/internal/scylla"
)

func Run(ctx context.Context, logger *zap.Logger) error {
	shutdownTelemetry, err := observability.Init(ctx, "card-executor", logger)
	if err != nil {
		return fmt.Errorf("init telemetry: %w", err)
	}
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := shutdownTelemetry(shutdownCtx); err != nil {
			logger.Warn("telemetry shutdown failed", zap.Error(err))
		}
	}()

	database := bootstrap.MustGormDB(logger)
	rdb := bootstrap.MustCoordinationStore(logger)
	defer rdb.Close()

	kafkaBrokers := bootstrap.KafkaBrokers()
	smsProducer := kafkapkg.NewProducerWithOptions(kafkaBrokers, contractevents.TopicSendSMS, kafkapkg.ProducerOptions{
		RequiredAcks:    kafka.RequireAll,
		RequiredAcksSet: true,
	}, logger.Named("sms-producer"))
	defer smsProducer.Close()

	logProducer := kafkapkg.NewProducerWithOptions(kafkaBrokers, contractevents.TopicMessageLog, kafkapkg.ProducerOptions{
		RequiredAcks:    kafka.RequireOne,
		RequiredAcksSet: true,
	}, logger.Named("log-producer"))
	defer logProducer.Close()

	activateProducer := kafkapkg.NewProducerWithOptions(kafkaBrokers, contractevents.TopicCardActivate, kafkapkg.ProducerOptions{
		RequiredAcks:    kafka.RequireAll,
		RequiredAcksSet: true,
	}, logger.Named("activate-producer"))
	defer activateProducer.Close()

	scylla := bootstrap.MustScylla(logger)
	defer scylla.Close()
	cardStateStore := scyllastore.NewCardStateStore(scylla)

	// Build keystore from ScyllaDB.
	cardKeyStore := scyllastore.NewCardKeyStore(scylla)
	counterStore := scyllastore.NewCounterStore(scylla)

	backend := config.GetEnv("KEYSTORE_BACKEND", "software")
	var ks keystore.KeyStore
	var cp keystore.CryptoProvider
	if backend == "software" {
		ks = keystore.NewScyllaKeyStore(&scyllaKeyAdapter{store: cardKeyStore})
	} else {
		ks, cp, _ = keystore.Build(keystore.Config{Backend: backend}, database, logger.Named("keystore"))
	}

	cardWorker := NewService(database, nil, rdb, ks, cp, smsProducer, logProducer, activateProducer, cardStateStore, counterStore, nil, logger.Named("card-worker"))
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
		cardWorker.DrainRetries()
		logger.Info("card executor shutdown complete")
		return nil
	case <-consumerDone:
		cardWorker.DrainRetries()
		return nil
	}
}

// scyllaKeyAdapter adapts scylla.CardKeyStore to the keystore.ScyllaKeyReader interface.
type scyllaKeyAdapter struct {
	store *scyllastore.CardKeyStore
}

func (a *scyllaKeyAdapter) GetCardKeys(ctx context.Context, cardID string) (*keystore.ScyllaCardKeyRecord, error) {
	rec, err := a.store.GetCardKeys(ctx, cardID)
	if err != nil {
		return nil, err
	}
	return &keystore.ScyllaCardKeyRecord{
		CardID:    rec.CardID,
		EncKey:    rec.EncKey,
		AuthKey:   rec.AuthKey,
		KEK:       rec.KEK,
		ProfileID: rec.ProfileID,
		MSISDN:    rec.MSISDN,
	}, nil
}
