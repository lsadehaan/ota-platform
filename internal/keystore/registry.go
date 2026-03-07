package keystore

import (
	"fmt"
	"sync"

	"go.uber.org/zap"
	"gorm.io/gorm"
)

// Config holds keystore configuration.
type Config struct {
	Backend      string `json:"backend" yaml:"backend"`
	CacheEnabled bool   `json:"cache_enabled" yaml:"cache_enabled"`
}

// Factory creates a KeyStore and optional CryptoProvider from configuration.
type Factory func(cfg Config, db *gorm.DB, cache KeyCache, logger *zap.Logger) (KeyStore, CryptoProvider, error)

var (
	mu        sync.RWMutex
	factories = map[string]Factory{}
)

// Register registers a named backend factory. Enterprise packages call this
// from init() to register HSM backends.
func Register(name string, factory Factory) {
	mu.Lock()
	defer mu.Unlock()
	factories[name] = factory
}

// Build constructs a KeyStore (and optional CryptoProvider) based on config.
// If CacheEnabled is true and the backend does not provide its own CryptoProvider,
// the KeyStore is wrapped with a CachedKeyStore decorator.
func Build(cfg Config, db *gorm.DB, cache KeyCache, logger *zap.Logger) (KeyStore, CryptoProvider, error) {
	backend := cfg.Backend
	if backend == "" {
		backend = "software"
	}

	if backend == "software" {
		ks := NewSoftwareKeyStore(db)
		if cfg.CacheEnabled && cache != nil {
			return NewCachedKeyStore(ks, cache), nil, nil
		}
		return ks, nil, nil
	}

	mu.RLock()
	factory, ok := factories[backend]
	mu.RUnlock()
	if !ok {
		return nil, nil, fmt.Errorf("keystore: unknown backend %q", backend)
	}

	ks, cp, err := factory(cfg, db, cache, logger)
	if err != nil {
		return nil, nil, fmt.Errorf("keystore: build %q: %w", backend, err)
	}

	// Wrap with cache if backend doesn't provide CryptoProvider
	// (i.e., keys are extracted and can be cached).
	if cp == nil && cfg.CacheEnabled && cache != nil {
		ks = NewCachedKeyStore(ks, cache)
	}

	return ks, cp, nil
}
