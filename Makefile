.PHONY: build test test-race test-integration lint cover run docker-up docker-down docker clean

build:
	go build -o bin/treasury ./cmd/treasury

test:
	go test ./cmd/... ./internal/... -coverprofile=coverage.out -coverpkg=./cmd/...,./internal/...

test-race:
	go test -race ./cmd/... ./internal/... -coverprofile=coverage.out -coverpkg=./cmd/...,./internal/...

test-integration:
	docker compose up -d postgres redis
	@echo "Waiting for Postgres + Redis to be healthy..."
	@sleep 5
	DB_URL=postgres://treasury:treasury@localhost:5432/treasury?sslmode=disable \
	REDIS_URL=redis://localhost:6379/0 \
	go test -tags=integration -race ./cmd/... ./internal/... -coverprofile=coverage.out -coverpkg=./cmd/...,./internal/...
	docker compose down

lint:
	golangci-lint run

cover: test
	go tool cover -func=coverage.out | tail -1

run:
	go run ./cmd/treasury

docker-up:
	docker compose up -d --build

docker-down:
	docker compose down

docker:
	docker build -t ai-crypto-onramp/treasury-orchestration .

clean:
	rm -rf bin/ coverage.out