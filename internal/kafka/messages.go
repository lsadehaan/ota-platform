package kafka

import "time"

// SendSMSMessage is produced to the send-sms topic to request sending an OTA SMS.
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
	Payload  string `json:"payload"` // hex-encoded binary payload
}

// CardEvent is the unified event type for the card-events topic.
// All card state machine transitions flow through this type.
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

// MessageLogAction represents the type of message log operation.
type MessageLogAction struct {
	Action string           `json:"action"` // "create" or "update"
	Log    *MessageLogEntry `json:"log,omitempty"`
	// For updates:
	ID      string                 `json:"id,omitempty"`
	Updates map[string]interface{} `json:"updates,omitempty"`
}

// MessageLogEntry is the Kafka representation of a message log row.
type MessageLogEntry struct {
	ID             string `json:"id"`
	CampaignID     string `json:"campaign_id,omitempty"`
	CardID         string `json:"card_id"`
	Direction      string `json:"direction"`
	CreatedAt      string `json:"created_at,omitempty"`
	UpdatedAt      string `json:"updated_at,omitempty"`
	RawPayload     string `json:"raw_payload"`               // base64
	SecuredPayload string `json:"secured_payload,omitempty"` // base64
	CounterHex     string `json:"counter_hex,omitempty"`
	Status         string `json:"status"`
	SMPPMessageID  string `json:"smpp_message_id,omitempty"`
}
