package pipeline

import (
	"context"
	"time"
)

// PublishFuture represents a pending async publish. Call Await to block until
// the broker acknowledges (or rejects) the message.
type PublishFuture struct {
	ch chan error
}

// Await blocks until the publish completes or the context is cancelled.
func (f *PublishFuture) Await(ctx context.Context) error {
	if f == nil {
		return nil
	}
	select {
	case err := <-f.ch:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// ResolvedFuture creates a pre-resolved future (for test mocks and sync paths).
func ResolvedFuture(err error) *PublishFuture {
	f := &PublishFuture{ch: make(chan error, 1)}
	f.ch <- err
	return f
}

// BatchItem represents a single message in a batch publish operation.
type BatchItem struct {
	Key   string
	Value interface{}
}

// SendSMSMessage is produced to request sending an OTA SMS.
type SendSMSMessage struct {
	MsgID      string    `json:"msg_id"`
	CampaignID string    `json:"campaign_id"`
	CardID     string    `json:"card_id"`
	MSISDN     string    `json:"msisdn"`
	TON        byte      `json:"ton"`
	NPI        byte      `json:"npi"`
	DataCoding byte      `json:"data_coding"`
	ProtocolID byte      `json:"protocol_id"`
	ESMClass   byte      `json:"esm_class"`
	Parts      []SMSPart `json:"parts"`
}

// SMSPart represents one segment of a concatenated SMS.
type SMSPart struct {
	Sequence int    `json:"sequence"`
	Total    int    `json:"total"`
	RefNum   int    `json:"ref_num"`
	Payload  []byte `json:"payload"` // raw binary payload (no hex encoding)
}

// CardEvent is the unified event type for card state machine transitions.
type CardEvent struct {
	Type       string `json:"type"`     // card.activate, card.dlr_received, card.mo_received
	EventID    string `json:"event_id"` // UUID for idempotency
	CardID     string `json:"card_id"`
	CampaignID string `json:"campaign_id"`

	// For card.activate:
	Step       int `json:"step,omitempty"`
	RetryCount int `json:"retry_count,omitempty"`

	// For card.dlr_received:
	MsgID         string `json:"msg_id,omitempty"`
	SMPPMessageID string `json:"smpp_message_id,omitempty"`
	DLRStatus     string `json:"dlr_status,omitempty"`
	ErrorCode     string `json:"error_code,omitempty"`

	// For card.mo_received:
	SourceMSISDN string `json:"source_msisdn,omitempty"`
	PayloadHex   string `json:"payload_hex,omitempty"`

	Timestamp time.Time `json:"timestamp"`
}

// CardStateChange represents a card state machine transition.
type CardStateChange struct {
	CampaignID    string    `json:"campaign_id"`
	CardID        string    `json:"card_id"`
	Status        string    `json:"status"`
	CurrentStep   int       `json:"current_step,omitempty"`
	RetryCount    int       `json:"retry_count,omitempty"`
	LastMsgID     string    `json:"last_msg_id,omitempty"`
	LastSMPPID    string    `json:"last_smpp_message_id,omitempty"`
	LastError     string    `json:"last_error,omitempty"`
	LastErrCode   string    `json:"last_error_code,omitempty"`
	Timestamp     time.Time `json:"timestamp"`
	TransitionSeq int64     `json:"transition_seq,omitempty"`
	StatsFrom     string    `json:"stats_from,omitempty"`
	StatsTo       string    `json:"stats_to,omitempty"`
}

// MessageLogAction represents the type of message log operation.
type MessageLogAction struct {
	Action string           `json:"action"` // "create" or "update"
	Log    *MessageLogEntry `json:"log,omitempty"`
	// For updates:
	ID      string                 `json:"id,omitempty"`
	Updates map[string]interface{} `json:"updates,omitempty"`
}

// MessageLogEntry is the representation of a message log row.
type MessageLogEntry struct {
	ID             string    `json:"id"`
	CampaignID     string    `json:"campaign_id,omitempty"`
	CardID         string    `json:"card_id"`
	Direction      string    `json:"direction"`
	CreatedAt      time.Time `json:"created_at,omitempty"`
	UpdatedAt      time.Time `json:"updated_at,omitempty"`
	RawPayload     []byte    `json:"raw_payload"`               // raw binary (no base64)
	SecuredPayload []byte    `json:"secured_payload,omitempty"` // raw binary (no base64)
	CounterHex     string    `json:"counter_hex,omitempty"`
	Status         string    `json:"status"`
	SMPPMessageID  string    `json:"smpp_message_id,omitempty"`
}
