package pgstore

import (
	"fmt"
	"os"
	"strconv"

	"github.com/google/uuid"
)

// envIntPgstore reads an environment variable as int, returning fallback if unset or invalid.
func envIntPgstore(key string, fallback int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return n
		}
	}
	return fallback
}

// parseUUID parses a string UUID and stores it in dst.
func parseUUID(s string, dst *uuid.UUID) error {
	if s == "" {
		*dst = uuid.Nil
		return nil
	}
	u, err := uuid.Parse(s)
	if err != nil {
		return fmt.Errorf("parse uuid %q: %w", s, err)
	}
	*dst = u
	return nil
}

// uuidPtrFromString converts a string to a *uuid.UUID, returning nil for empty strings.
func uuidPtrFromString(s string) *uuid.UUID {
	if s == "" {
		return nil
	}
	u, err := uuid.Parse(s)
	if err != nil {
		return nil
	}
	return &u
}
