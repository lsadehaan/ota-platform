FROM golang:1.26-alpine AS builder
RUN apk add --no-cache git
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o /mock-smsc ./cmd/mock-smsc

FROM alpine:3.19
RUN apk add --no-cache ca-certificates
COPY --from=builder /mock-smsc /usr/local/bin/mock-smsc
EXPOSE 2775
CMD ["mock-smsc"]
