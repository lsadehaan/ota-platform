package transport

import (
	"context"

	"ota-platform/internal/pipeline"
	redispkg "ota-platform/internal/redis"
	"github.com/idnteq/go-smsc/smpp"
)

// CoordinationStore captures the transport coordination operations.
type CoordinationStore interface {
	StoreSMPPCorrelation(ctx context.Context, smppMsgID string, mapping *redispkg.SMPPMapping) error
	LoadSMPPCorrelation(ctx context.Context, smppMsgID string) (*redispkg.SMPPMapping, error)
	GetCardState(ctx context.Context, cardID string) (*redispkg.CardState, error)
	InitMultipartTracking(ctx context.Context, msgID string, totalParts int) error
	RecordPartDLR(ctx context.Context, msgID string, delivered bool) (allResolved bool, anyFailed bool, err error)
}

// MSISDNResolver resolves an MSISDN to a card ID for MO correlation.
type MSISDNResolver interface {
	LookupCardByMSISDN(ctx context.Context, msisdn string) (string, error)
}

type EventProducer interface {
	Publish(ctx context.Context, key string, message interface{}) (*pipeline.PublishFuture, error)
	Close() error
}

type SMPPClient interface {
	Connect(ctx context.Context) error
	ActiveConnections() int
	Submit(req *smpp.SubmitRequest) (*smpp.SubmitResponse, error)
	Close() error
}
