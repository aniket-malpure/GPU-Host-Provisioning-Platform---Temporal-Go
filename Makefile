.PHONY: tidy build test test-unit vet fmt up down demo clean

# Resolve and lock third-party deps (Temporal SDK, kafka-go). Needs network access.
tidy:
	go mod tidy
	go mod tidy -tags kafka

build:
	go build -o bin/gpuctl ./cmd/gpuctl
	go build -o bin/worker ./cmd/worker
	go build -o bin/api ./cmd/api

# Production build with the Kafka event transport.
build-kafka:
	go build -tags kafka -o bin/worker ./cmd/worker
	go build -tags kafka -o bin/api ./cmd/api

# Everything, including Temporal workflow tests (requires `make tidy` first).
test:
	go test -race -count=1 ./...

# Packages with no third-party dependencies; runs without `make tidy`.
test-unit:
	go test -race -count=1 ./internal/domain/ ./internal/hardware/ ./internal/drift/ \
		./internal/events/ ./internal/model/ ./internal/activities/ ./internal/remediation/ \
		./internal/api/ ./cmd/gpuctl/

vet:
	go vet ./...

fmt:
	gofmt -l -w .

up:
	docker compose -f deploy/docker-compose.yml up --build -d

down:
	docker compose -f deploy/docker-compose.yml down -v

# End-to-end demo against `make up`: create a cluster, break a node, watch it heal.
demo: build
	./scripts/demo.sh

clean:
	rm -rf bin
