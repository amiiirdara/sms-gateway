SHELL := /bin/sh
DATABASE_URL ?= postgres://sms:sms@localhost:5432/sms_gateway?sslmode=disable
SERVICES := api-gateway outbox-relay campaign-expander dispatcher report-sink billing-consumer reconciler reporting-api operator-mock

.PHONY: build test lint tidy \
	docker-up docker-down docker-logs \
	migrate-up migrate-down sqlc \
	$(addprefix run-,$(SERVICES))

## Build every cmd/ binary into ./bin
build:
	@mkdir -p bin
	@for s in $(SERVICES); do \
		echo "building $$s"; \
		go build -o bin/$$s ./cmd/$$s; \
	done

test:
	go test ./...

lint:
	go vet ./...

tidy:
	go mod tidy

## Bring up the full local stack (Postgres, Redis, Kafka, ClickHouse, all services)
docker-up:
	docker compose up -d --build

docker-down:
	docker compose down

docker-logs:
	docker compose logs -f

## Apply/rollback Postgres migrations (requires golang-migrate: https://github.com/golang-migrate/migrate)
migrate-up:
	migrate -path db/migrations -database "$(DATABASE_URL)" up

migrate-down:
	migrate -path db/migrations -database "$(DATABASE_URL)" down 1

## Regenerate sqlc code from db/queries + db/migrations (requires sqlc: https://sqlc.dev)
sqlc:
	sqlc generate

## Run a single service locally against `docker-compose up` infra, e.g. `make run-api-gateway`
run-%:
	go run ./cmd/$*
