package transport

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"go.uber.org/zap"

	redispkg "ota-platform/internal/redis"
)

type delayedCorrelationStore struct {
	mapping      *redispkg.SMPPMapping
	visibleAfter int32
	lookups      atomic.Int32
}

func (s *delayedCorrelationStore) StoreSMPPCorrelation(context.Context, string, *redispkg.SMPPMapping) error {
	return nil
}

func (s *delayedCorrelationStore) LoadSMPPCorrelation(context.Context, string) (*redispkg.SMPPMapping, error) {
	if s.lookups.Add(1) >= s.visibleAfter {
		return s.mapping, nil
	}
	return nil, nil
}

func (s *delayedCorrelationStore) LookupCardByMSISDN(context.Context, string) (string, error) {
	return "", nil
}

func (s *delayedCorrelationStore) GetCardState(context.Context, string) (*redispkg.CardState, error) {
	return nil, nil
}

func TestLoadCorrelationRetriesSharedStore(t *testing.T) {
	store := &delayedCorrelationStore{
		mapping: &redispkg.SMPPMapping{
			MsgID:      "msg-1",
			CampaignID: "campaign-1",
			CardID:     "card-1",
			MSISDN:     "447700000001",
		},
		visibleAfter: 3,
	}

	gateway := &Gateway{
		redis:  store,
		logger: zap.NewNop(),
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	mapping, err := gateway.loadCorrelation(ctx, "smpp-1")
	if err != nil {
		t.Fatalf("loadCorrelation returned error: %v", err)
	}
	if mapping == nil {
		t.Fatal("expected mapping after retry")
	}
	if mapping.CardID != "card-1" {
		t.Fatalf("unexpected card id: %s", mapping.CardID)
	}
	if store.lookups.Load() < 3 {
		t.Fatalf("expected retries, got %d lookups", store.lookups.Load())
	}
}

func TestLoadCorrelationUsesLocalFastPath(t *testing.T) {
	store := &delayedCorrelationStore{}
	gateway := &Gateway{
		redis:  store,
		logger: zap.NewNop(),
	}

	expected := &redispkg.SMPPMapping{
		MsgID:      "msg-1",
		CampaignID: "campaign-1",
		CardID:     "card-1",
		MSISDN:     "447700000001",
	}
	gateway.storeLocalCorrelation("smpp-1", expected)

	mapping, err := gateway.loadCorrelation(context.Background(), "smpp-1")
	if err != nil {
		t.Fatalf("loadCorrelation returned error: %v", err)
	}
	if mapping == nil {
		t.Fatal("expected mapping from local cache")
	}
	if mapping.CardID != expected.CardID {
		t.Fatalf("unexpected card id: %s", mapping.CardID)
	}
	if store.lookups.Load() != 0 {
		t.Fatalf("expected no shared-store lookup, got %d", store.lookups.Load())
	}
}
