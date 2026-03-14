package pgstore

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"gorm.io/gorm"

	"ota-platform/internal/db"
	redispkg "ota-platform/internal/redis"
)

// CardStateReader provides read access to card execution state.
// The executor's CoordinationStore implements this interface, allowing
// the gateway to share the same process-local cache instead of querying Postgres.
type CardStateReader interface {
	GetCardState(ctx context.Context, cardID string) (*redispkg.CardState, error)
}

// GatewayCoordinationStore implements transport.CoordinationStore backed by
// process-local state (single-instance constraint) and Postgres.
type GatewayCoordinationStore struct {
	db *gorm.DB

	// Shared card state cache from the executor's CoordinationStore.
	// When non-nil, GetCardState reads from this cache instead of Postgres.
	cardStateCache CardStateReader

	// SMPP correlation: process-local one-shot map
	correlations sync.Map // smppMsgID -> *redispkg.SMPPMapping

	// Multipart DLR tracking: process-local
	multipartMu sync.Mutex
	multiparts  map[string]*multipartState // msgID -> state
}

type multipartState struct {
	mu        sync.Mutex
	total     int
	delivered int
	failed    int
}

// NewGatewayCoordinationStore creates a new transport CoordinationStore.
// The optional cardStateCache lets the gateway share the executor's process-local
// card state cache, avoiding redundant Postgres queries for MO/DLR handling.
func NewGatewayCoordinationStore(database *gorm.DB, cardStateCache CardStateReader) *GatewayCoordinationStore {
	return &GatewayCoordinationStore{
		db:             database,
		cardStateCache: cardStateCache,
		multiparts:     make(map[string]*multipartState),
	}
}

// StoreSMPPCorrelation stores an SMPP message ID to internal mapping in process memory.
func (s *GatewayCoordinationStore) StoreSMPPCorrelation(_ context.Context, smppMsgID string, mapping *redispkg.SMPPMapping) error {
	s.correlations.Store(smppMsgID, mapping)
	return nil
}

// LoadSMPPCorrelation retrieves and deletes the SMPP correlation mapping (one-shot).
// Returns (nil, nil) on cache miss.
func (s *GatewayCoordinationStore) LoadSMPPCorrelation(_ context.Context, smppMsgID string) (*redispkg.SMPPMapping, error) {
	v, ok := s.correlations.LoadAndDelete(smppMsgID)
	if !ok {
		return nil, nil
	}
	return v.(*redispkg.SMPPMapping), nil
}

// GetCardState returns card state from the shared executor cache (if available)
// or falls back to querying Postgres. Returns (nil, nil) if not found.
func (s *GatewayCoordinationStore) GetCardState(ctx context.Context, cardID string) (*redispkg.CardState, error) {
	if s.cardStateCache != nil {
		return s.cardStateCache.GetCardState(ctx, cardID)
	}

	var row db.CardExecutionState
	err := s.db.WithContext(ctx).Where("card_id = ?", cardID).Order("transition_seq DESC").First(&row).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, nil
		}
		return nil, fmt.Errorf("get card state: %w", err)
	}

	var lastMsgID string
	if row.LastMsgID != nil {
		lastMsgID = row.LastMsgID.String()
	}
	return &redispkg.CardState{
		CampaignID:    row.CampaignID.String(),
		CurrentStep:   row.CurrentStep,
		Status:        row.Status,
		RetryCount:    row.RetryCount,
		LastMsgID:     lastMsgID,
		TransitionSeq: row.TransitionSeq,
	}, nil
}

// InitMultipartTracking initializes tracking for a multipart SMS message.
func (s *GatewayCoordinationStore) InitMultipartTracking(_ context.Context, msgID string, totalParts int) error {
	s.multipartMu.Lock()
	s.multiparts[msgID] = &multipartState{total: totalParts}
	s.multipartMu.Unlock()
	return nil
}

// RecordPartDLR atomically records a DLR for one part of a multipart SMS.
// Returns ErrNoMultipartTracking if no tracking was initialized for this message.
func (s *GatewayCoordinationStore) RecordPartDLR(_ context.Context, msgID string, delivered bool) (allResolved bool, anyFailed bool, err error) {
	s.multipartMu.Lock()
	state, ok := s.multiparts[msgID]
	s.multipartMu.Unlock()

	if !ok {
		return false, false, redispkg.ErrNoMultipartTracking
	}

	state.mu.Lock()
	defer state.mu.Unlock()

	if delivered {
		state.delivered++
	} else {
		state.failed++
	}

	resolved := (state.delivered + state.failed) >= state.total
	hasFailed := state.failed > 0

	if resolved {
		// Clean up tracking state once fully resolved.
		s.multipartMu.Lock()
		delete(s.multiparts, msgID)
		s.multipartMu.Unlock()
	}

	return resolved, hasFailed, nil
}
