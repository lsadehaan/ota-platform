FROM golang:1.26-alpine AS builder
RUN apk add --no-cache git
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o /sms-gateway ./cmd/sms-gateway

FROM alpine:3.19
RUN apk add --no-cache ca-certificates
COPY --from=builder /sms-gateway /usr/local/bin/sms-gateway
CMD ["sms-gateway"]
