FROM golang:1.26-alpine AS builder
RUN apk add --no-cache git
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o /kafka-init ./cmd/kafka-init

FROM alpine:3.19
RUN apk add --no-cache ca-certificates
COPY --from=builder /kafka-init /usr/local/bin/kafka-init
CMD ["kafka-init"]
