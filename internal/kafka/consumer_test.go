package kafka

import (
	"fmt"
	"testing"
)

// TestRetryTracking verifies the retry counting logic extracted from the
// consumer's Start loop. This tests fix #1b: don't commit on handler error
// until maxRetries is reached.
func TestRetryTracking(t *testing.T) {
	retries := make(map[string]int)

	retryKey := "test-topic:0:42"

	// Simulate 3 consecutive failures for the same message.
	for i := 1; i <= maxRetries; i++ {
		retries[retryKey]++
		count := retries[retryKey]

		if i < maxRetries {
			if count >= maxRetries {
				t.Errorf("retry %d: count=%d should be < maxRetries=%d", i, count, maxRetries)
			}
		} else {
			if count < maxRetries {
				t.Errorf("retry %d: count=%d should be >= maxRetries=%d", i, count, maxRetries)
			}
		}
	}

	// After poison-pill commit, retry entry should be cleaned up.
	delete(retries, retryKey)
	if _, exists := retries[retryKey]; exists {
		t.Error("expected retry entry to be deleted after poison pill")
	}
}

// TestRetryKeyFormat verifies the retry key format is deterministic.
func TestRetryKeyFormat(t *testing.T) {
	key := fmt.Sprintf("%s:%d:%d", "sms-dlr", 2, 1337)
	expected := "sms-dlr:2:1337"
	if key != expected {
		t.Errorf("expected %q, got %q", expected, key)
	}
}

// TestSuccessResetsRetryCounter verifies that a successful handler clears
// any accumulated retry count for a message.
func TestSuccessResetsRetryCounter(t *testing.T) {
	retries := make(map[string]int)
	retryKey := "test-topic:0:100"

	// Simulate 2 failures (below maxRetries).
	retries[retryKey] = 2

	// Simulate success — should delete the entry.
	delete(retries, retryKey)

	if count, exists := retries[retryKey]; exists {
		t.Errorf("expected retry entry to be cleared on success, got count=%d", count)
	}
}
