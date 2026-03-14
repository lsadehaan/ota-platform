package pgstore

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	goredis "github.com/redis/go-redis/v9"
	"golang.org/x/time/rate"
	"gorm.io/gorm"

	"ota-platform/internal/db"
	redispkg "ota-platform/internal/redis"
)

// ErrCacheMiss is returned when a process-local cache lookup finds no entry.
var ErrCacheMiss = errors.New("cache miss")

// CoordinationStore implements executor.CoordinationStore backed by
// process-local caches (reconstructable on restart) and Postgres (durable).
type CoordinationStore struct {
	db *gorm.DB

	// Process-local caches
	profiles sync.Map // profileID -> json.RawMessage
	commands sync.Map // campaignID -> []redispkg.CampaignCommandCache
	params   sync.Map // campaignID -> *redispkg.CampaignParams

	// Campaign status: Postgres-backed with short TTL local cache
	statusCache sync.Map // campaignID -> *statusEntry

	// Card state: write-through to Postgres with local cache
	cardStates sync.Map // cardID -> *redispkg.CardState

	// Throttle: in-process rate limiters per campaign
	throttleMu sync.Mutex
	throttles  map[string]*rate.Limiter
}

type statusEntry struct {
	status    string
	cachedAt  time.Time
}

const statusCacheTTL = 30 * time.Second

// NewCoordinationStore creates a new executor CoordinationStore.
func NewCoordinationStore(database *gorm.DB) *CoordinationStore {
	return &CoordinationStore{
		db:        database,
		throttles: make(map[string]*rate.Limiter),
	}
}

// CheckAndSetDedupe always returns (true, nil). Kafka partition-based
// delivery provides exactly-once semantics within the process; cross-process
// deduplication is unnecessary for a single-instance deployment.
func (s *CoordinationStore) CheckAndSetDedupe(_ context.Context, _ string) (bool, error) {
	return true, nil
}

// AcquireThrottle checks the in-process token bucket for the given campaign.
// A new limiter is created on first access.
func (s *CoordinationStore) AcquireThrottle(_ context.Context, campaignID string, ratePerSec int) (bool, error) {
	s.throttleMu.Lock()
	lim, ok := s.throttles[campaignID]
	if !ok || int(lim.Limit()) != ratePerSec {
		lim = rate.NewLimiter(rate.Limit(ratePerSec), ratePerSec)
		s.throttles[campaignID] = lim
	}
	s.throttleMu.Unlock()
	return lim.Allow(), nil
}

// GetCachedProfile retrieves a profile from the process-local cache.
// Returns goredis.Nil on cache miss (compatible with executor's error check).
func (s *CoordinationStore) GetCachedProfile(_ context.Context, profileID string, out interface{}) error {
	v, ok := s.profiles.Load(profileID)
	if !ok {
		return goredis.Nil
	}
	raw := v.(json.RawMessage)
	return json.Unmarshal(raw, out)
}

// CacheProfile stores a profile in the process-local cache as JSON.
func (s *CoordinationStore) CacheProfile(_ context.Context, profileID string, profile interface{}) error {
	data, err := json.Marshal(profile)
	if err != nil {
		return fmt.Errorf("marshal profile: %w", err)
	}
	s.profiles.Store(profileID, json.RawMessage(data))
	return nil
}

// GetCachedCampaignParams retrieves cached campaign parameters.
// Returns (nil, nil) on cache miss.
func (s *CoordinationStore) GetCachedCampaignParams(_ context.Context, campaignID string) (*redispkg.CampaignParams, error) {
	v, ok := s.params.Load(campaignID)
	if !ok {
		return nil, nil
	}
	return v.(*redispkg.CampaignParams), nil
}

// CacheCampaignParams stores campaign parameters in the process-local cache.
func (s *CoordinationStore) CacheCampaignParams(_ context.Context, campaignID string, params *redispkg.CampaignParams) error {
	s.params.Store(campaignID, params)
	return nil
}

// GetCampaignCommands retrieves cached campaign commands.
// Returns (nil, nil) on cache miss.
func (s *CoordinationStore) GetCampaignCommands(_ context.Context, campaignID string) ([]redispkg.CampaignCommandCache, error) {
	v, ok := s.commands.Load(campaignID)
	if !ok {
		return nil, nil
	}
	return v.([]redispkg.CampaignCommandCache), nil
}

// CacheCampaignCommands stores campaign commands in the process-local cache.
func (s *CoordinationStore) CacheCampaignCommands(_ context.Context, campaignID string, cmds []redispkg.CampaignCommandCache) error {
	s.commands.Store(campaignID, cmds)
	return nil
}

// GetCampaignStatus reads the campaign status, serving from a 30-second local
// cache and falling back to Postgres.
func (s *CoordinationStore) GetCampaignStatus(ctx context.Context, campaignID string) (string, error) {
	if v, ok := s.statusCache.Load(campaignID); ok {
		entry := v.(*statusEntry)
		if time.Since(entry.cachedAt) < statusCacheTTL {
			return entry.status, nil
		}
	}

	var campaign db.Campaign
	err := s.db.WithContext(ctx).Select("status").Where("id = ?", campaignID).First(&campaign).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return "", nil
		}
		return "", fmt.Errorf("get campaign status: %w", err)
	}

	s.statusCache.Store(campaignID, &statusEntry{
		status:   campaign.Status,
		cachedAt: time.Now(),
	})
	return campaign.Status, nil
}

// SetCampaignStatus writes the campaign status to Postgres and updates the local cache.
func (s *CoordinationStore) SetCampaignStatus(ctx context.Context, campaignID, status string) error {
	err := s.db.WithContext(ctx).Model(&db.Campaign{}).Where("id = ?", campaignID).Update("status", status).Error
	if err != nil {
		return fmt.Errorf("set campaign status: %w", err)
	}
	s.statusCache.Store(campaignID, &statusEntry{
		status:   status,
		cachedAt: time.Now(),
	})
	return nil
}

// SetCardState updates the process-local cache only. Postgres persistence is
// handled asynchronously by the batched card state writer (RunCardStateWriter).
// This avoids a synchronous 200-400ms Postgres write per card transition.
func (s *CoordinationStore) SetCardState(_ context.Context, cardID string, state *redispkg.CardState) error {
	s.cardStates.Store(cardID, state)
	return nil
}

// EvictCardState removes a card from the process-local cache.
// Called when a card reaches terminal state to bound memory growth.
func (s *CoordinationStore) EvictCardState(cardID string) {
	s.cardStates.Delete(cardID)
}

// GetCardState returns card state from the local cache or Postgres.
// Returns (nil, nil) if not found.
func (s *CoordinationStore) GetCardState(ctx context.Context, cardID string) (*redispkg.CardState, error) {
	if v, ok := s.cardStates.Load(cardID); ok {
		return v.(*redispkg.CardState), nil
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
	state := &redispkg.CardState{
		CampaignID:    row.CampaignID.String(),
		CurrentStep:   row.CurrentStep,
		Status:        row.Status,
		RetryCount:    row.RetryCount,
		LastMsgID:     lastMsgID,
		TransitionSeq: row.TransitionSeq,
	}
	s.cardStates.Store(cardID, state)
	return state, nil
}
