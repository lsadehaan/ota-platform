package events

import "time"

type Envelope struct {
	EventID              string    `json:"event_id"`
	EventType            string    `json:"event_type"`
	SchemaVersion        uint32    `json:"schema_version"`
	OccurredAt           time.Time `json:"occurred_at"`
	TraceID              string    `json:"trace_id,omitempty"`
	Producer             string    `json:"producer"`
	TenantID             string    `json:"tenant_id,omitempty"`
	CampaignRunID        string    `json:"campaign_run_id,omitempty"`
	CampaignDefinitionID string    `json:"campaign_definition_id,omitempty"`
	ShardID              string    `json:"shard_id,omitempty"`
	CardID               string    `json:"card_id,omitempty"`
}

type Event[T any] struct {
	Meta    Envelope `json:"meta"`
	Payload T        `json:"payload"`
}

type CardActivatePayload struct {
	Step             int    `json:"step"`
	RetryCount       int    `json:"retry_count"`
	ActivationReason string `json:"activation_reason"`
}

type CardDLRReceivedPayload struct {
	MsgID         string `json:"msg_id"`
	SMPPMessageID string `json:"smpp_message_id"`
	DLRStatus     string `json:"dlr_status"`
	ErrorCode     string `json:"error_code"`
	IsFinal       bool   `json:"is_final"`
}

type CardMOReceivedPayload struct {
	SourceMSISDN      string `json:"source_msisdn"`
	PayloadHex        string `json:"payload_hex"`
	CorrelationSource string `json:"correlation_source"`
}

type CardFailedPayload struct {
	Step          int    `json:"step"`
	RetryCount    int    `json:"retry_count"`
	FailureCode   string `json:"failure_code"`
	FailureReason string `json:"failure_reason"`
}

type CardRetryRequestedPayload struct {
	Step       int    `json:"step"`
	RetryCount int    `json:"retry_count"`
	Reason     string `json:"reason"`
}

type CardCompletedPayload struct {
	FinalStep   int       `json:"final_step"`
	CompletedAt time.Time `json:"completed_at"`
}

type SMSPart struct {
	Sequence   int    `json:"sequence"`
	Total      int    `json:"total"`
	RefNum     int    `json:"ref_num"`
	PayloadHex string `json:"payload_hex"`
}

type SMSSubmitRequestedPayload struct {
	MsgID      string    `json:"msg_id"`
	MSISDN     string    `json:"msisdn"`
	TON        byte      `json:"ton"`
	NPI        byte      `json:"npi"`
	DataCoding byte      `json:"data_coding"`
	ProtocolID byte      `json:"protocol_id"`
	ESMClass   byte      `json:"esm_class"`
	Parts      []SMSPart `json:"parts"`
}

type TransportDLRFinalizedPayload struct {
	MsgID         string `json:"msg_id"`
	SMPPMessageID string `json:"smpp_message_id"`
	DLRStatus     string `json:"dlr_status"`
	ErrorCode     string `json:"error_code"`
	LogicalStatus string `json:"logical_status"`
}

type TransportSubmitAcceptedPayload struct {
	MsgID         string `json:"msg_id"`
	SMPPMessageID string `json:"smpp_message_id"`
	PartSequence  int    `json:"part_sequence"`
	PartTotal     int    `json:"part_total"`
}

type TransportSubmitFailedPayload struct {
	MsgID         string `json:"msg_id"`
	FailureCode   string `json:"failure_code"`
	FailureReason string `json:"failure_reason"`
	PartSequence  int    `json:"part_sequence"`
	PartTotal     int    `json:"part_total"`
}

type TransportMOReceivedPayload struct {
	SourceMSISDN          string `json:"source_msisdn"`
	DestMSISDN            string `json:"dest_msisdn"`
	PayloadHex            string `json:"payload_hex"`
	ResolvedCardID        string `json:"resolved_card_id"`
	CorrelationConfidence string `json:"correlation_confidence"`
}

type ExecutionCardStateChangedPayload struct {
	PreviousStatus  string `json:"previous_status"`
	NewStatus       string `json:"new_status"`
	Step            int    `json:"step"`
	RetryCount      int    `json:"retry_count"`
	SnapshotVersion uint64 `json:"snapshot_version"`
}

type ExecutionCounterAllocatedPayload struct {
	ApplicationID string `json:"application_id"`
	CounterValue  uint64 `json:"counter_value"`
	CounterHex    string `json:"counter_hex"`
}

type MessageMTCreatedPayload struct {
	MsgID             string `json:"msg_id"`
	Direction         string `json:"direction"`
	Status            string `json:"status"`
	RawPayloadB64     string `json:"raw_payload_b64"`
	SecuredPayloadB64 string `json:"secured_payload_b64"`
	CounterHex        string `json:"counter_hex,omitempty"`
}
