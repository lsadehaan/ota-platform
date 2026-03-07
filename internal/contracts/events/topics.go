package events

const (
	TopicPlannerEvents   = "planner-events"
	TopicCardEvents      = "card-events"
	TopicSendSMS         = "send-sms"
	TopicTransportEvents = "transport-events"
	TopicExecutionEvents = "execution-events"
	TopicMessageLog      = "message-log"
)

const (
	EventCampaignRunCreated         = "campaign.run.created"
	EventCampaignRunResumeRequested = "campaign.run.resume_requested"
	EventCampaignRunAbortRequested  = "campaign.run.abort_requested"

	EventCardActivate       = "card.activate"
	EventCardDLRReceived    = "card.dlr_received"
	EventCardMOReceived     = "card.mo_received"
	EventCardRetryRequested = "card.retry_requested"
	EventCardCompleted      = "card.completed"
	EventCardFailed         = "card.failed"

	EventSMSSubmitRequested      = "sms.submit_requested"
	EventTransportDLRFinalized   = "transport.dlr_finalized"
	EventTransportMOReceived     = "transport.mo_received"
	EventTransportSubmitAccepted = "transport.submit_accepted"
	EventTransportSubmitFailed   = "transport.submit_failed"

	EventExecutionCardStateChanged = "execution.card_state_changed"
	EventExecutionCounterAllocated = "execution.counter_allocated"
	EventExecutionCommandBuilt     = "execution.command_built"
	EventExecutionCardCompleted    = "execution.card_completed"
	EventExecutionCardFailed       = "execution.card_failed"

	EventMessageMTCreated = "message.mt_created"
	EventMessageMTUpdated = "message.mt_updated"
	EventMessageMOCreated = "message.mo_created"
)
