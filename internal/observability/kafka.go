package observability

import (
	"context"

	"github.com/segmentio/kafka-go"
	"go.opentelemetry.io/otel"
)

type HeaderCarrier struct {
	headers *[]kafka.Header
}

func NewHeaderCarrier(headers *[]kafka.Header) HeaderCarrier {
	return HeaderCarrier{headers: headers}
}

func (c HeaderCarrier) Get(key string) string {
	for _, header := range *c.headers {
		if header.Key == key {
			return string(header.Value)
		}
	}
	return ""
}

func (c HeaderCarrier) Set(key, value string) {
	headers := *c.headers
	for i := range headers {
		if headers[i].Key == key {
			headers[i].Value = []byte(value)
			*c.headers = headers
			return
		}
	}
	*c.headers = append(headers, kafka.Header{Key: key, Value: []byte(value)})
}

func (c HeaderCarrier) Keys() []string {
	keys := make([]string, 0, len(*c.headers))
	for _, header := range *c.headers {
		keys = append(keys, header.Key)
	}
	return keys
}

func InjectKafkaHeaders(ctx context.Context, headers *[]kafka.Header) {
	otel.GetTextMapPropagator().Inject(ctx, NewHeaderCarrier(headers))
}

func ExtractKafkaContext(ctx context.Context, headers []kafka.Header) context.Context {
	copied := make([]kafka.Header, len(headers))
	copy(copied, headers)
	return otel.GetTextMapPropagator().Extract(ctx, NewHeaderCarrier(&copied))
}
