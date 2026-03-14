.PHONY: build test lint benchmarks e2e-test e2e-load run-engine run-smsc run-db-migrate migrate-up migrate-down engine-up engine-down engine-up-load web-dev web-build smscgw-test

build:
	go build -o bin/ota-engine ./cmd/ota-engine
	go build -o bin/db-migrate ./cmd/db-migrate
	go build -o bin/mock-smsc ./cmd/mock-smsc

test:
	go test ./... -v -count=1

lint:
	golangci-lint run ./...

benchmarks:
	go run ./cmd/benchmark-runner -output benchmark-report.md

E2E_TEST_TIMEOUT ?= 30m
e2e-test:
	go test -tags=e2e ./e2e -v -count=1 -timeout=$(E2E_TEST_TIMEOUT)

e2e-load:
	go run ./cmd/e2e-load-runner

run-engine:
	go run ./cmd/ota-engine

run-db-migrate:
	go run ./cmd/db-migrate

run-smsc:
	go run ./cmd/mock-smsc

migrate-up:
	migrate -path db/migrations -database "$$DATABASE_URL" up

migrate-down:
	migrate -path db/migrations -database "$$DATABASE_URL" down

engine-up:
	docker compose -f deployments/docker-compose.engine.yml up -d

engine-down:
	docker compose -f deployments/docker-compose.engine.yml down

engine-up-load:
	docker compose -f deployments/docker-compose.engine.yml -f deployments/docker-compose.engine-load.yml up -d

web-dev:
	cd web && npm run dev

web-build:
	cd web && npm run build

smscgw-test:
	./tests/smsc-gateway/run_tests.sh
