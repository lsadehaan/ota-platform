package main

import (
	"context"
	"fmt"
	"net"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"

	"ota-platform/internal/bootstrap"
	"ota-platform/internal/config"
	contractevents "ota-platform/internal/contracts/events"
)

type topicSpec struct {
	Topic             string
	NumPartitions     int
	ReplicationFactor int
}

func main() {
	logger := bootstrap.NewLogger()
	defer logger.Sync()

	brokers := bootstrap.KafkaBrokers()
	controller, err := waitForController(brokers, 30*time.Second)
	if err != nil {
		logger.Fatal("failed to resolve kafka controller", zap.Error(err))
	}

	conn, err := kafka.DialContext(context.Background(), "tcp", net.JoinHostPort(controller.Host, fmt.Sprintf("%d", controller.Port)))
	if err != nil {
		logger.Fatal("failed to dial kafka controller", zap.Error(err))
	}
	defer conn.Close()

	partitions := config.GetEnvInt("KAFKA_TOPIC_PARTITIONS", 12)
	rf := config.GetEnvInt("KAFKA_TOPIC_REPLICATION_FACTOR", 1)
	topics := []topicSpec{
		{Topic: contractevents.TopicCardEvents, NumPartitions: partitions, ReplicationFactor: rf},
		{Topic: contractevents.TopicSendSMS, NumPartitions: partitions, ReplicationFactor: rf},
		{Topic: contractevents.TopicMessageLog, NumPartitions: partitions, ReplicationFactor: rf},
	}
	cfgs := make([]kafka.TopicConfig, 0, len(topics))
	for _, topic := range topics {
		cfgs = append(cfgs, kafka.TopicConfig{
			Topic:             topic.Topic,
			NumPartitions:     topic.NumPartitions,
			ReplicationFactor: topic.ReplicationFactor,
		})
	}
	if err := conn.CreateTopics(cfgs...); err != nil && !strings.Contains(err.Error(), "already exists") {
		logger.Fatal("failed to create kafka topics", zap.Error(err))
	}
	logger.Info("kafka topics ensured", zap.Int("count", len(cfgs)))
}

func waitForController(brokers []string, timeout time.Duration) (*kafka.Broker, error) {
	deadline := time.Now().Add(timeout)
	var lastErr error
	for time.Now().Before(deadline) {
		for _, broker := range brokers {
			conn, err := kafka.Dial("tcp", broker)
			if err != nil {
				lastErr = err
				continue
			}
			controller, err := conn.Controller()
			_ = conn.Close()
			if err == nil {
				return &controller, nil
			}
			lastErr = err
		}
		time.Sleep(500 * time.Millisecond)
	}
	if lastErr == nil {
		lastErr = fmt.Errorf("timeout waiting for kafka controller")
	}
	return nil, lastErr
}
