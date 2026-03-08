.PHONY: build test lint benchmarks e2e-test e2e-load run-api run-planner run-executor run-projector run-gateway run-reconciler run-smsc run-db-migrate run-kafka-init migrate-up migrate-down docker-up docker-down docker-up-load web-dev web-build

build:
	go build -o bin/ota-api ./cmd/ota-api
	go build -o bin/campaign-planner ./cmd/campaign-planner
	go build -o bin/card-executor ./cmd/card-executor
	go build -o bin/read-model-projector ./cmd/read-model-projector
	go build -o bin/sms-gateway ./cmd/sms-gateway
	go build -o bin/reconciler ./cmd/reconciler
	go build -o bin/db-migrate ./cmd/db-migrate
	go build -o bin/kafka-init ./cmd/kafka-init
	go build -o bin/benchmark-runner ./cmd/benchmark-runner
	go build -o bin/component-load-runner ./cmd/component-load-runner
	go build -o bin/e2e-load-runner ./cmd/e2e-load-runner
	go build -o bin/mock-smsc ./cmd/mock-smsc

test:
	go test ./... -v -count=1

lint:
	golangci-lint run ./...

benchmarks:
	go run ./cmd/benchmark-runner -output benchmark-report.md

e2e-test:
	go test -tags=e2e ./e2e -v -count=1

e2e-load:
	go run ./cmd/e2e-load-runner

run-api:
	go run ./cmd/ota-api

run-planner:
	go run ./cmd/campaign-planner

run-executor:
	go run ./cmd/card-executor

run-projector:
	go run ./cmd/read-model-projector

run-gateway:
	go run ./cmd/sms-gateway

run-reconciler:
	go run ./cmd/reconciler

run-db-migrate:
	go run ./cmd/db-migrate

run-kafka-init:
	go run ./cmd/kafka-init

run-smsc:
	go run ./cmd/mock-smsc

migrate-up:
	migrate -path db/migrations -database "$$DATABASE_URL" up

migrate-down:
	migrate -path db/migrations -database "$$DATABASE_URL" down

docker-up:
	docker compose -f deployments/docker-compose.yml up -d

docker-down:
	docker compose -f deployments/docker-compose.yml down

docker-up-load:
	docker compose -f deployments/docker-compose.yml -f deployments/docker-compose.load.yml up -d

web-dev:
	cd web && npm run dev

web-build:
	cd web && npm run build

run-component-load:
	go run ./cmd/component-load-runner
