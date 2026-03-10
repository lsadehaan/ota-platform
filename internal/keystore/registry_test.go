package keystore

import (
	"testing"

	"go.uber.org/zap"
)

func TestRegistry_DefaultSoftware(t *testing.T) {
	db := setupTestDB(t)

	cfg := Config{Backend: "software"}
	ks, cp, err := Build(cfg, db, zap.NewNop())
	if err != nil {
		t.Fatalf("Build failed: %v", err)
	}
	if ks == nil {
		t.Fatal("expected non-nil KeyStore")
	}
	if cp != nil {
		t.Fatal("expected nil CryptoProvider for software backend")
	}
}

func TestRegistry_EmptyBackendDefaultsToSoftware(t *testing.T) {
	db := setupTestDB(t)

	cfg := Config{Backend: ""}
	ks, _, err := Build(cfg, db, zap.NewNop())
	if err != nil {
		t.Fatalf("Build failed: %v", err)
	}
	if ks == nil {
		t.Fatal("expected non-nil KeyStore for empty backend")
	}
}

func TestRegistry_UnknownBackend(t *testing.T) {
	cfg := Config{Backend: "nonexistent"}
	_, _, err := Build(cfg, nil, zap.NewNop())
	if err == nil {
		t.Fatal("expected error for unknown backend")
	}
}
