FROM golang:1.26-alpine AS builder
RUN apk add --no-cache git
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o /card-executor ./cmd/card-executor

FROM alpine:3.19
RUN apk add --no-cache ca-certificates
COPY --from=builder /card-executor /usr/local/bin/card-executor
CMD ["card-executor"]
