package bootstrap

import (
	"context"
	"encoding/hex"
	"fmt"
	"os/signal"
	"strings"
	"syscall"

	"go.uber.org/zap"

	"ota-platform/internal/config"
	redispkg "ota-platform/internal/redis"
)

func NewLogger() *zap.Logger {
	logger, err := zap.NewProduction()
	if err != nil {
		panic(err)
	}
	return logger
}

func SignalContext() (context.Context, context.CancelFunc) {
	return signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
}

func OpenCoordinationStore(logger *zap.Logger) (*redispkg.Client, error) {
	encKey, err := redisEncryptionKeyFromEnv()
	if err != nil {
		return nil, err
	}

	return redispkg.NewClient(redispkg.Config{
		Addr:         config.GetEnv("COORDINATION_ADDR", "localhost:6379"),
		Password:     config.GetEnv("COORDINATION_PASSWORD", ""),
		PoolSize:     config.GetEnvInt("COORDINATION_POOL_SIZE", 0),
		MinIdleConns: config.GetEnvInt("COORDINATION_MIN_IDLE_CONNS", 0),
	}, logger.Named("coordination"), encKey)
}

func MustCoordinationStore(logger *zap.Logger) *redispkg.Client {
	rdb, err := OpenCoordinationStore(logger)
	if err != nil {
		logger.Fatal("coordination store connection failed", zap.Error(err))
	}
	return rdb
}

func KafkaBrokers() []string {
	return strings.Split(config.GetEnv("KAFKA_BROKERS", "localhost:9092"), ",")
}

func redisEncryptionKeyFromEnv() ([]byte, error) {
	encKey := config.GetEnv("COORDINATION_ENCRYPTION_KEY", "")
	if encKey == "" {
		return nil, nil
	}

	encKeyBytes, err := hex.DecodeString(encKey)
	if err != nil {
		return nil, fmt.Errorf("coordination encryption key is not valid hex: %w", err)
	}

	if n := len(encKeyBytes); n != 16 && n != 24 && n != 32 {
		return nil, fmt.Errorf("coordination encryption key must be 16, 24, or 32 bytes, got %d", n)
	}

	return encKeyBytes, nil
}
