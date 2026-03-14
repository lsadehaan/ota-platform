FROM golang:1.26-alpine AS builder
RUN apk add --no-cache git
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o /smpp-loadgen ./cmd/smpp-loadgen

FROM alpine:3.21
RUN apk add --no-cache ca-certificates
COPY --from=builder /smpp-loadgen /usr/local/bin/smpp-loadgen
VOLUME ["/artifacts"]
ENTRYPOINT ["/usr/local/bin/smpp-loadgen"]
