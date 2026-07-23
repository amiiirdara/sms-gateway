# SMS Gateway

[![CI](https://github.com/amiiirdara/sms-gateway/actions/workflows/ci.yml/badge.svg)](https://github.com/amiiirdara/sms-gateway/actions/workflows/ci.yml)

A multi-tenant SMS Gateway for the [ArvanCloud software developer challenge](https://github.com/amiiirdara/sms-gateway): send SMS via REST against a prepaid credit balance, with a guaranteed-SLA **Express** lane and batch **campaign** sending.

Designed for ~100M messages/day with highly skewed per-tenant traffic. Built in Go with Kafka, Redis, PostgreSQL, and ClickHouse.

**Repo:** https://github.com/amiiirdara/sms-gateway

**Verified locally:** create → topup → normal/Express/`campaign` → `sent`; edge 402/AoN/exact-zero; k6 accept-path 20/s × 30s (see [load-test report](docs/load-test-report.md)); CI runs `go vet` + `go test -short` on every push.

## Documentation

| Doc | Brief |
|---|---|
| [docs/reviewer-guide.md](docs/reviewer-guide.md) | **Start here** — submission blurb, Compose up, smoke, edge script, k6 (~5 min) |
| [ARCHITECTURE.md](ARCHITECTURE.md) | Full system design: Outbox/Inbox, Express SLA, campaigns, data model, API surface |
| [docs/architecture.svg](docs/architecture.svg) · [docs/architecture.png](docs/architecture.png) | One-page visual of the accept → Kafka → dispatch / billing / reports flow |
| [openapi/openapi.yaml](openapi/openapi.yaml) | REST API contract (paths, auth, request/response schemas) |
| [docs/metrics.md](docs/metrics.md) | Prometheus catalog — business + technical metrics ([code](internal/platform/metrics/metrics.go)) |
| [docs/grafana-sms-gateway.json](docs/grafana-sms-gateway.json) | Optional Grafana dashboard for `sms_*` metrics |
| [docs/security-ops-checklist.md](docs/security-ops-checklist.md) | Tenant isolation, API-key hashing, rate limits, Inbox, billing controls |
| [docs/trade-offs.md](docs/trade-offs.md) | Deliberate non-goals; why 100M/day isn’t proven on Compose and what a real proof needs |
| [docs/load-test-report.md](docs/load-test-report.md) | k6 accept-path scenario, thresholds, execution notes, and recorded run results |
| [AGENTS.md](AGENTS.md) | Repo orientation for contributors / AI agents (layout, non-negotiables) |
| [LICENSE](LICENSE) | MIT |
| [.github/workflows/ci.yml](.github/workflows/ci.yml) | CI: `go vet` + `go test -short` on push/PR |
| [Makefile](Makefile) | `make up` / `make test` / `make smoke` / `make load-test` |

## What is implemented

- Account create (open, rate-limited) + API-key tenant isolation
- Prepaid balance with atomic Redis Lua check-and-decrement (spend to exact zero, never negative)
- Redis Streams outbox → Kafka relay (no dual-write loss between debit and dispatch)
- Normal + Express dispatch lanes; Express hard deadline (2 min) drops late messages and refunds
- Campaigns (normal priority only, all-or-nothing balance reservation, up to 10k recipients)
- Ledger + Inbox idempotency; reconciler safety net (auto-heal only Redis > ledger)
- Reporting API: message status, paginated reports, campaign aggregates (ClickHouse)
- Operator mock + pluggable multi-operator routing; Docker Compose local stack
- Prometheus metrics at `GET /metrics` (api-gateway `:8080`; workers `METRICS_ADDR`, default `:9090`) — catalog in [docs/metrics.md](docs/metrics.md)

## Quickstart

**Prerequisites:** Docker Desktop, Go 1.25+ (for local builds/tests).

```bash
make up
# or: docker compose up -d --build
# migrate runs automatically via the migrate service
# api-gateway  → http://localhost:8080
# reporting-api → http://localhost:8081
```

Pinned infra tags: `postgres:16.6-alpine`, `redis:7.4-alpine`, `apache/kafka:3.7.0`, `clickhouse/clickhouse-server:24.8-alpine`, `migrate/migrate:v4.18.1`.

### End-to-end example (PowerShell)

```powershell
# 1. Create account
$acc = Invoke-RestMethod -Method Post -Uri http://localhost:8080/v1/accounts `
  -ContentType application/json -Body '{"name":"demo"}'
$H = @{ Authorization = "Bearer $($acc.apiKey)"; "Content-Type" = "application/json" }

# 2. Top up
Invoke-RestMethod -Method Post -Uri http://localhost:8080/v1/topups -Headers $H -Body '{"amount":100}'

# 3. Send a normal SMS (E.164 or local Iranian mobile)
$msg = Invoke-RestMethod -Method Post -Uri http://localhost:8080/v1/messages -Headers $H `
  -Body '{"to":"09121234567","text":"hello","priority":"normal"}'

# 4. Poll status on reporting-api
Invoke-RestMethod -Method Get -Uri "http://localhost:8081/v1/messages/$($msg.messageId)" `
  -Headers @{ Authorization = "Bearer $($acc.apiKey)" }
```

### Express & campaigns

```powershell
# Express (OTP-style) — dropped + refunded if not dispatched within 2 minutes
Invoke-RestMethod -Method Post -Uri http://localhost:8080/v1/messages -Headers $H `
  -Body '{"to":"+989121234567","text":"otp-1234","priority":"express"}'

# Campaign — always normal priority; all-or-nothing on balance
Invoke-RestMethod -Method Post -Uri http://localhost:8080/v1/campaigns -Headers $H `
  -Body '{"text":"promo","recipients":["09121111111","09122222222"]}'
```

## Architecture (short)

```
Client → API Gateway → Redis (atomic debit + outbox stream)
                      → Outbox Relay → Kafka
                      → Dispatcher (normal | express) → Operator
                      → Billing / Report Sink → Postgres + ClickHouse
Client → Reporting API → Postgres (point lookup) / ClickHouse (reports)
```

Key reliability choices (details in [ARCHITECTURE.md](ARCHITECTURE.md)):

- **Outbox (Redis Streams) + Inbox:** debit and “why” are one atomic Lua op; consumers dedupe before side effects
- **Express SLA:** Tier 1 target 95% ≤ 1m; Tier 2 hard ceiling 2m → drop + refund
- **Tenant isolation:** `account_id` always from API key, never from client path/query/body
- **Fairness:** Kafka partition key = `account_id`; Express is a separate topic + worker pool

## Services (`cmd/`)

| Binary | Role |
|---|---|
| `api-gateway` | REST ingestion |
| `outbox-relay` | Redis outbox → Kafka |
| `campaign-expander` | Campaign → per-recipient messages |
| `dispatcher` | `--mode=normal\|express` → operator |
| `billing-consumer` | Ledger debit / refund |
| `report-sink` | Postgres status + ClickHouse events |
| `reporting-api` | Status & reports |
| `reconciler` | Redis ↔ ledger safety net |
| `operator-mock` | Simulated telecom API |

## Tests

```bash
# Unit tests (fast; what CI runs with -short)
make test
# or: go test ./... -short -count=1 && go vet ./...

# Redis Lua / Postgres Inbox integration (needs Docker; skipped under -short)
make test-integration

# Edge-case smoke (Compose stack must be up)
make smoke

# Small accept-path load test (requires k6; Compose stack must be up)
make load-test
# See docs/load-test-report.md for scenario, thresholds, and recorded results
```

Verified manually against Compose: create → topup → normal send → `sent`; Express → `sent`; campaign 3/3 `sent`; balance arithmetic correct.

## Trade-offs / out of scope

Deliberately **not** built (see [docs/trade-offs.md](docs/trade-offs.md)): real MNOs, multi-region, login/OAuth UI, full Prometheus/Grafana stack in Compose, and a **100M/day load proof**. The architecture targets that scale; this repo proves correctness on a lean demo stack.

## Local ports

| Service | Address |
|---|---|
| api-gateway | http://localhost:8080 (`/metrics` for Prometheus) |
| reporting-api | http://localhost:8081 |
| Postgres | localhost:5432 (`sms`/`sms`, db `sms_gateway`) |
| Redis | localhost:6379 |
| Kafka (host) | localhost:9094 |
| ClickHouse HTTP | http://localhost:8123 |

## Repo layout

```
cmd/                 One deployable binary per service
internal/
  config/            Env-based configuration
  platform/          postgres, redis, kafka, clickhouse, inbox, httpx, lifecycle, metrics
  domain/            billing, messaging, campaigns
  db/sqlc/           Generated by sqlc — do not hand-edit
db/migrations/       golang-migrate (Postgres source of truth)
db/queries/          Hand-written SQL for sqlc
clickhouse/init/     ClickHouse DDL
openapi/             REST contract
docs/                Reviewer guide, metrics, security checklist, diagrams, load report
.github/workflows/   CI (go vet + go test -short)
.cursor/rules|skills Agent conventions and recipes
```

## License

[MIT](LICENSE)

