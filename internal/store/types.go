package store

import (
	"time"

	"github.com/google/uuid"
)

// CampaignStats holds aggregate progress counters for a campaign.
type CampaignStats struct {
	Total      int64
	Pending    int64
	InProgress int64
	Completed  int64
	Failed     int64
	Skipped    int64
}

// CampaignCardView is a denormalized card view within a campaign.
type CampaignCardView struct {
	CardID      string
	Status      string
	CurrentStep int
	RetryCount  int
	LastMsgID   string
	LastError   string
	UpdatedAt   time.Time
}

// CardStateRecord represents the full execution state of a card.
type CardStateRecord struct {
	CardID            uuid.UUID  `json:"card_id"`
	CampaignID        *uuid.UUID `json:"campaign_id,omitempty"`
	Status            string     `json:"status"`
	CurrentStep       int        `json:"current_step"`
	RetryCount        int        `json:"retry_count"`
	LastMsgID         *uuid.UUID `json:"last_msg_id,omitempty"`
	LastSMPPMessageID string     `json:"last_smpp_message_id,omitempty"`
	LastErrorCode     string     `json:"last_error_code,omitempty"`
	LastErrorText     string     `json:"last_error_text,omitempty"`
	UpdatedAt         time.Time  `json:"updated_at"`
}

// MessageRecord represents a complete message record.
type MessageRecord struct {
	ID             uuid.UUID
	CampaignID     *uuid.UUID
	CardID         uuid.UUID
	Direction      string
	Status         string
	SMPPMessageID  *string
	DLRStatus      *string
	CounterHex     *string
	PORStatusCode  *int16
	RawPayload     []byte
	SecuredPayload []byte
	PORData        []byte
	CreatedAt      time.Time
	UpdatedAt      time.Time
}

// MessageFilter defines query parameters for message listing.
type MessageFilter struct {
	CampaignID *uuid.UUID
	CardID     *uuid.UUID
	Direction  string
	Status     string
	From       time.Time
	To         time.Time
	Page       int
	PageSize   int
}

// ErrorCount is an aggregated error summary.
type ErrorCount struct {
	Key   string
	Count int64
}

// ThroughputPoint is a time-series throughput metric.
type ThroughputPoint struct {
	Timestamp time.Time `json:"timestamp"`
	Sent      float64   `json:"sent"`
	Delivered float64   `json:"delivered"`
	Failed    float64   `json:"failed"`
}

// MessageMetrics holds aggregate message counts.
type MessageMetrics struct {
	Total       int64
	MT          int64
	MO          int64
	Delivered   int64
	Undelivered int64
}

// CardKeyRecord holds cryptographic key material for a SIM card.
type CardKeyRecord struct {
	CardID    string
	EncKey    []byte
	AuthKey   []byte
	KEK       []byte
	ProfileID string
	MSISDN    string
}

// CardCounterRecord holds a counter value for a (card, application) pair.
type CardCounterRecord struct {
	CardID        string
	ApplicationID string
	CounterValue  int64
}
