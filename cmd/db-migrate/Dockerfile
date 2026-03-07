FROM golang:1.26-alpine AS builder
RUN apk add --no-cache git
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o /db-migrate ./cmd/db-migrate

FROM alpine:3.19
RUN apk add --no-cache ca-certificates
COPY --from=builder /db-migrate /usr/local/bin/db-migrate
CMD ["db-migrate"]
