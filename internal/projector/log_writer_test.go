package projector

import (
	"testing"
	"time"

	kafkapkg "ota-platform/internal/kafka"
)

func TestToMessageLogRejectsInvalidUUIDs(t *testing.T) {
	lw := &LogWriter{}
	_, err := lw.toMessageLog(&kafkapkg.MessageLogEntry{
		ID:         "not-a-uuid",
		CardID:     "also-bad",
		Direction:  "MT",
		RawPayload: "YWJj",
		Status:     "created",
	})
	if err == nil {
		t.Fatal("expected invalid UUID error")
	}
}

func TestToMessageLogParsesTimestamps(t *testing.T) {
	lw := &LogWriter{}
	now := time.Now().UTC().Truncate(time.Second)
	ml, err := lw.toMessageLog(&kafkapkg.MessageLogEntry{
		ID:         "11111111-1111-1111-1111-111111111111",
		CardID:     "22222222-2222-2222-2222-222222222222",
		CampaignID: "33333333-3333-3333-3333-333333333333",
		Direction:  "MT",
		Status:     "sending",
		RawPayload: "YWJj",
		CreatedAt:  now.Format(time.RFC3339Nano),
		UpdatedAt:  now.Add(time.Second).Format(time.RFC3339Nano),
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !ml.CreatedAt.Equal(now) {
		t.Fatalf("created_at mismatch: got %s want %s", ml.CreatedAt, now)
	}
	if !ml.UpdatedAt.Equal(now.Add(time.Second)) {
		t.Fatalf("updated_at mismatch: got %s want %s", ml.UpdatedAt, now.Add(time.Second))
	}
}
