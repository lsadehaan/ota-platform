package keystore

import (
	"context"

	redispkg "ota-platform/internal/redis"
)

// RedisKeyCache adapts the existing redis.Client to satisfy the KeyCache interface.
type RedisKeyCache struct {
	client RedisKeyCacheClient
}

// RedisKeyCacheClient is the subset of redis.Client methods needed for key caching.
type RedisKeyCacheClient interface {
	GetCardKeys(ctx context.Context, cardID string) (*redispkg.CardKeys, error)
	CacheCardKeys(ctx context.Context, cardID string, keys *redispkg.CardKeys) error
}

// NewRedisKeyCache creates a RedisKeyCache backed by the given Redis client.
func NewRedisKeyCache(client RedisKeyCacheClient) *RedisKeyCache {
	return &RedisKeyCache{client: client}
}

// GetCachedKeys retrieves card keys from Redis cache.
// Returns (nil, nil) on cache miss.
func (r *RedisKeyCache) GetCachedKeys(ctx context.Context, cardID string) (*CardKeyMaterial, error) {
	cardKeys, err := r.client.GetCardKeys(ctx, cardID)
	if err != nil {
		return nil, err
	}
	if cardKeys == nil {
		return nil, nil
	}

	return &CardKeyMaterial{
		EncKey:    cardKeys.EncKey,
		AuthKey:   cardKeys.AuthKey,
		KEK:       cardKeys.KEK,
		ProfileID: cardKeys.ProfileID,
		MSISDN:    cardKeys.MSISDN,
	}, nil
}

// CacheKeys stores card keys in Redis cache.
func (r *RedisKeyCache) CacheKeys(ctx context.Context, cardID string, keys *CardKeyMaterial) error {
	return r.client.CacheCardKeys(ctx, cardID, &redispkg.CardKeys{
		EncKey:    keys.EncKey,
		AuthKey:   keys.AuthKey,
		KEK:       keys.KEK,
		ProfileID: keys.ProfileID,
		MSISDN:    keys.MSISDN,
	})
}
