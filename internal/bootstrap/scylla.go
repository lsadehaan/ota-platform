package bootstrap

import (
	"strings"

	"go.uber.org/zap"

	"ota-platform/internal/config"
	"ota-platform/internal/scylla"
)

func OpenScylla(logger *zap.Logger) (*scylla.Client, error) {
	return scylla.Open(scylla.Config{
		Hosts:             strings.Split(config.GetEnv("SCYLLA_HOSTS", "localhost:9042"), ","),
		Keyspace:          config.GetEnv("SCYLLA_KEYSPACE", "ota_execution"),
		Username:          config.GetEnv("SCYLLA_USERNAME", ""),
		Password:          config.GetEnv("SCYLLA_PASSWORD", ""),
		BucketCount:       config.GetEnvInt("SCYLLA_BUCKET_COUNT", 128),
		ReplicationClass:  config.GetEnv("SCYLLA_REPLICATION_CLASS", "SimpleStrategy"),
		ReplicationFactor: config.GetEnvInt("SCYLLA_REPLICATION_FACTOR", 1),
	}, logger.Named("scylla"))
}

func MustScylla(logger *zap.Logger) *scylla.Client {
	client, err := OpenScylla(logger)
	if err != nil {
		logger.Fatal("scylla connection failed", zap.Error(err))
	}
	return client
}
