package keystore

import (
	"fmt"
	"sync"

	"go.uber.org/zap"
	"gorm.io/gorm"
)

// Config holds keystore configuration.
type Config struct {
	Backend string `json:"backend" yaml:"backend"`
}

// Factory creates a KeyStore and optional CryptoProvider from configuration.
type Factory func(cfg Config, db *gorm.DB, logger *zap.Logger) (KeyStore, CryptoProvider, error)

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
func Build(cfg Config, db *gorm.DB, logger *zap.Logger) (KeyStore, CryptoProvider, error) {
	backend := cfg.Backend
	if backend == "" {
		backend = "software"
	}

	if backend == "software" {
		return NewSoftwareKeyStore(db), nil, nil
	}

	mu.RLock()
	factory, ok := factories[backend]
	mu.RUnlock()
	if !ok {
		return nil, nil, fmt.Errorf("keystore: unknown backend %q", backend)
	}

	return factory(cfg, db, logger)
}
