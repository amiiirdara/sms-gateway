# SMS Gateway — common local commands (GNU Make / Git Bash on Windows).

.PHONY: up down build test test-integration smoke scenarios load-test vet sqlc migrate-up

up:
	docker compose up -d --build

down:
	docker compose down

build:
	go build ./cmd/...

vet:
	go vet ./...

# Regenerate sqlc → internal/db/sqlc (requires sqlc on PATH).
sqlc:
	sqlc generate

# Re-apply golang-migrate against Compose Postgres (stack must be up).
migrate-up:
	docker compose run --rm migrate

# Fast unit tests (same as CI).
test:
	go test ./... -short -count=1

# Integration tests that need Docker (testcontainers).
test-integration:
	go test ./internal/platform/redis/ ./internal/platform/inbox/ ./internal/domain/messaging/ -count=1 -timeout 5m

smoke:
	powershell -NoProfile -File scripts/smoke-edge.ps1

# E2E scenario suite → docs/scenario-report/results.json (Compose stack must be up).
scenarios:
	powershell -NoProfile -File scripts/run-scenario-suite.ps1

# Override BASE_URL if Adobe Connect owns 127.0.0.1:8080 (e.g. http://[::1]:8080).
load-test:
	k6 run scripts/load-accept.js
