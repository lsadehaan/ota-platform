FROM golang:1.26-bookworm AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN GOPROXY=https://proxy.golang.org,direct go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /ota-engine ./cmd/ota-engine

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates && rm -rf /var/lib/apt/lists/*
COPY --from=builder /ota-engine /ota-engine
ENTRYPOINT ["/ota-engine"]
