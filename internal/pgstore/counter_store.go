package pgstore

import (
	"context"
	"strings"
	"sync"
)

// CounterStore holds process-local counter values.
// Counters are batch-preloaded by the planner before shard activation.
// During execution, all counter ops are process-local (zero DB).
type CounterStore struct {
	mu       sync.RWMutex
	counters map[string]int64    // "cardID:appID" -> current counter value
	cardApps map[string][]string // cardID -> []appID (for O(1) eviction)
}

func NewCounterStore() *CounterStore {
	return &CounterStore{
		counters: make(map[string]int64),
		cardApps: make(map[string][]string),
	}
}

func counterKey(cardID, applicationID string) string {
	return cardID + ":" + applicationID
}

// GetCounter returns the current process-local counter value.
func (s *CounterStore) GetCounter(_ context.Context, cardID, applicationID string) (int64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.counters[counterKey(cardID, applicationID)], nil
}

// IncrCounter increments the process-local counter and returns the new value.
func (s *CounterStore) IncrCounter(_ context.Context, cardID, applicationID string) (int64, error) {
	key := counterKey(cardID, applicationID)
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.counters[key]; !exists {
		// First time seeing this card/app pair — add to index.
		s.cardApps[cardID] = append(s.cardApps[cardID], applicationID)
	}
	s.counters[key]++
	return s.counters[key], nil
}

// IncrCounterAsync is identical to IncrCounter (no background I/O needed).
func (s *CounterStore) IncrCounterAsync(ctx context.Context, cardID, applicationID string) error {
	_, err := s.IncrCounter(ctx, cardID, applicationID)
	return err
}

// EvictCard removes all counter entries for a card.
// Called when a card reaches terminal state to bound memory growth.
// Each card has exactly one counter per application_id. Since campaigns use
// a single application, we store a secondary index to avoid O(n) map scans.
func (s *CounterStore) EvictCard(cardID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, appID := range s.cardApps[cardID] {
		delete(s.counters, counterKey(cardID, appID))
	}
	delete(s.cardApps, cardID)
}

// PreloadCounters populates the cache from batch-loaded values.
// Called by the planner after batch SELECT/UPDATE from card_counters.
func (s *CounterStore) PreloadCounters(values map[string]int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for k, v := range values {
		s.counters[k] = v
		// Maintain cardApps index: extract cardID and appID from "cardID:appID" key.
		if idx := strings.IndexByte(k, ':'); idx >= 0 {
			cardID := k[:idx]
			appID := k[idx+1:]
			s.cardApps[cardID] = append(s.cardApps[cardID], appID)
		}
	}
}
