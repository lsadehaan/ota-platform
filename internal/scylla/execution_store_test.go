package scylla

import (
	"testing"

	"github.com/google/uuid"
)

func TestToOptionalGocqlUUIDAcceptsGoogleUUID(t *testing.T) {
	id := uuid.New()

	got, err := toOptionalGocqlUUID(id)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got == nil {
		t.Fatal("expected uuid, got nil")
	}
	if uuid.UUID(*got) != id {
		t.Fatalf("uuid mismatch: got %s want %s", uuid.UUID(*got), id)
	}
}

func TestToOptionalGocqlUUIDParsesString(t *testing.T) {
	id := uuid.New()

	got, err := toOptionalGocqlUUID(id.String())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got == nil {
		t.Fatal("expected uuid, got nil")
	}
	if uuid.UUID(*got) != id {
		t.Fatalf("uuid mismatch: got %s want %s", uuid.UUID(*got), id)
	}
}
