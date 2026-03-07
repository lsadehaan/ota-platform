package transport

import (
	"context"
	"go.uber.org/zap"

	redispkg "ota-platform/internal/redis"
	"ota-platform/internal/smpp"
)

// CoordinationStore captures the transport coordination operations.
type CoordinationStore interface {
	StoreSMPPCorrelation(ctx context.Context, smppMsgID string, mapping *redispkg.SMPPMapping) error
	LoadSMPPCorrelation(ctx context.Context, smppMsgID string) (*redispkg.SMPPMapping, error)
	LookupCardByMSISDN(ctx context.Context, msisdn string) (string, error)
	GetCardState(ctx context.Context, cardID string) (*redispkg.CardState, error)
}

type EventProducer interface {
	Publish(ctx context.Context, key string, message interface{}) error
	Close() error
}

type SMPPClient interface {
	Connect(ctx context.Context) error
	ActiveConnections() int
	Submit(req *smpp.SubmitRequest) (*smpp.SubmitResponse, error)
	Close() error
}

// Service is the SMS gateway service type.
type Service = Gateway

// NewService constructs the current SMS gateway implementation.
func NewService(smppConfig smpp.Config, poolConfig smpp.PoolConfig, kafkaBrokers []string, store CoordinationStore, logger *zap.Logger) *Service {
	return NewGateway(smppConfig, poolConfig, kafkaBrokers, store, logger)
}

// NewServiceWithDeps constructs a gateway with injected transport dependencies.
// It is intended for benchmarks and integration-style tests.
func NewServiceWithDeps(smppClient SMPPClient, eventProducer EventProducer, store CoordinationStore, logger *zap.Logger) *Service {
	if logger == nil {
		logger = zap.NewNop()
	}
	return &Gateway{
		smppPool:      smppClient,
		eventProducer: eventProducer,
		redis:         store,
		logger:        logger,
		telemetry:     newGatewayTelemetry(logger),
	}
}

// HandleSendSMSMessage exposes the send handler for benchmark/integration use.
func (s *Service) HandleSendSMSMessage(ctx context.Context, key []byte, value []byte) error {
	return s.handleSendSMS(ctx, key, value)
}

// HandleDLRReceipt exposes DLR processing for benchmark/integration use.
func (s *Service) HandleDLRReceipt(ctx context.Context, sourceAddr string, payload []byte) {
	s.handleDLR(ctx, sourceAddr, payload)
}

// HandleMOPayload exposes MO processing for benchmark/integration use.
func (s *Service) HandleMOPayload(ctx context.Context, sourceAddr, destAddr string, payload []byte) {
	s.handleMO(ctx, sourceAddr, destAddr, payload)
}
