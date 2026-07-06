.PHONY: build test run lint docker-build docker-run clean

build:
	go build -o bin/treasury ./cmd/treasury

test:
	go test ./... -race -coverprofile=coverage.out -coverpkg=./...

run:
	go run ./cmd/treasury

lint:
	go vet ./...

docker-build:
	docker build -t ai-crypto-onramp/treasury-orchestration .

docker-run:
	docker run --rm -p 8080:8080 ai-crypto-onramp/treasury-orchestration

clean:
	rm -rf bin/ coverage.out
