package transport

import (
	"context"
	"encoding/json"
	"testing"

	"go.uber.org/zap"

	kafkapkg "ota-platform/internal/kafka"
	redispkg "ota-platform/internal/redis"
	"ota-platform/internal/smpp"
)

type benchmarkSMPPClient struct {
	next int
}

func (c *benchmarkSMPPClient) Connect(context.Context) error { return nil }
func (c *benchmarkSMPPClient) ActiveConnections() int        { return 1 }
func (c *benchmarkSMPPClient) Close() error                  { return nil }
func (c *benchmarkSMPPClient) Submit(*smpp.SubmitRequest) (*smpp.SubmitResponse, error) {
	c.next++
	return &smpp.SubmitResponse{MessageID: "smpp-bench-" + string(rune(c.next))}, nil
}

type benchmarkTransportStore struct{}

func (benchmarkTransportStore) StoreSMPPCorrelation(context.Context, string, *redispkg.SMPPMapping) error {
	return nil
}
func (benchmarkTransportStore) LoadSMPPCorrelation(context.Context, string) (*redispkg.SMPPMapping, error) {
	return nil, nil
}
func (benchmarkTransportStore) LookupCardByMSISDN(context.Context, string) (string, error) {
	return "", nil
}
func (benchmarkTransportStore) GetCardState(context.Context, string) (*redispkg.CardState, error) {
	return nil, nil
}

type benchmarkEventProducer struct{}

func (benchmarkEventProducer) Publish(context.Context, string, interface{}) error { return nil }
func (benchmarkEventProducer) Close() error                                       { return nil }

func BenchmarkHandleSendSMS(b *testing.B) {
	msg := kafkapkg.SendSMSMessage{
		MsgID:      "msg-1",
		CampaignID: "campaign-1",
		CardID:     "card-1",
		MSISDN:     "1234567890",
		TON:        1,
		NPI:        1,
		DataCoding: 0,
		ProtocolID: 0,
		ESMClass:   0x40,
		Parts: []kafkapkg.SMSPart{
			{Sequence: 1, Total: 3, Payload: "AABBCC"},
			{Sequence: 2, Total: 3, Payload: "DDEEFF"},
			{Sequence: 3, Total: 3, Payload: "112233"},
		},
	}
	payload, err := json.Marshal(msg)
	if err != nil {
		b.Fatalf("marshal send-sms: %v", err)
	}

	gateway := &Gateway{
		smppPool:      &benchmarkSMPPClient{},
		eventProducer: benchmarkEventProducer{},
		redis:         benchmarkTransportStore{},
		logger:        zap.NewNop(),
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := gateway.handleSendSMS(context.Background(), []byte(msg.CardID), payload); err != nil {
			b.Fatalf("handle send sms: %v", err)
		}
	}
}
