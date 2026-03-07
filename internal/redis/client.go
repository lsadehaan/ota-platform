package redis

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"fmt"
	"runtime"
	"time"

	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

// Config holds Redis connection configuration.
type Config struct {
	Addr         string
	Password     string
	DB           int
	PoolSize     int
	MinIdleConns int
}

// Client is a wrapper around the go-redis client with logging.
type Client struct {
	rdb           *redis.Client
	logger        *zap.Logger
	encryptionKey []byte
	aead          cipher.AEAD
}

// NewClient creates a new Redis client, pings to verify the connection, and
// returns the wrapped Client.
func NewClient(cfg Config, logger *zap.Logger, encryptionKey []byte) (*Client, error) {
	poolSize := cfg.PoolSize
	if poolSize == 0 {
		poolSize = 10 * runtime.NumCPU()
	}
	minIdleConns := cfg.MinIdleConns
	if minIdleConns == 0 {
		minIdleConns = 5
	}

	rdb := redis.NewClient(&redis.Options{
		Addr:         cfg.Addr,
		Password:     cfg.Password,
		DB:           cfg.DB,
		PoolSize:     poolSize,
		MinIdleConns: minIdleConns,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := rdb.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("redis ping failed: %w", err)
	}

	logger.Info("connected to redis", zap.String("addr", cfg.Addr), zap.Int("db", cfg.DB))

	var aead cipher.AEAD
	if len(encryptionKey) > 0 {
		block, err := aes.NewCipher(encryptionKey)
		if err != nil {
			return nil, fmt.Errorf("create redis encryption cipher: %w", err)
		}
		aead, err = cipher.NewGCM(block)
		if err != nil {
			return nil, fmt.Errorf("create redis encryption AEAD: %w", err)
		}
	}

	return &Client{
		rdb:           rdb,
		logger:        logger,
		encryptionKey: encryptionKey,
		aead:          aead,
	}, nil
}

// Close closes the underlying Redis connection.
func (c *Client) Close() error {
	return c.rdb.Close()
}

// Raw exposes the underlying go-redis client for pipeline operations.
func (c *Client) Raw() *redis.Client {
	return c.rdb
}

// SetHashFields sets multiple fields in a Redis hash with a 24-hour TTL.
func (c *Client) SetHashFields(ctx context.Context, key string, fields map[string]int64) error {
	ifaceFields := make(map[string]interface{}, len(fields))
	for k, v := range fields {
		ifaceFields[k] = v
	}
	pipe := c.rdb.Pipeline()
	pipe.HSet(ctx, key, ifaceFields)
	pipe.Expire(ctx, key, 24*time.Hour)
	_, err := pipe.Exec(ctx)
	return err
}
