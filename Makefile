.PHONY: build test test-integration lint coverage run docker-build docker-run docker-up docker-down clean migrate-up migrate-down

build:
	go build -o bin/treasury ./cmd/treasury

test:
	go test ./cmd/... ./internal/... -race -coverprofile=coverage.out -coverpkg=./cmd/...,./internal/...

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

coverage: test
	go tool cover -func=coverage.out | tail -1

run:
	go run ./cmd/treasury

docker-build:
	docker build -t ai-crypto-onramp/treasury-orchestration .

docker-run:
	docker run --rm -p 8080:8080 ai-crypto-onramp/treasury-orchestration

docker-up:
	docker compose up -d --build

docker-down:
	docker compose down

clean:
	rm -rf bin/ coverage.out

migrate-up:
	go run ./cmd/migrate --up

migrate-down:
	go run ./cmd/migrate --down