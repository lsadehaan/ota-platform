FROM golang:1.26-alpine AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /smpp-probe-smsc ./cmd/smpp-probe-smsc

FROM alpine:3.21
RUN apk add --no-cache ca-certificates
COPY --from=builder /smpp-probe-smsc /smpp-probe-smsc
ENTRYPOINT ["/smpp-probe-smsc"]
