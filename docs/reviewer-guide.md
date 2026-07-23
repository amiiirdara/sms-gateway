# Reviewer Guide (first 5 minutes)

Quick path for challenge reviewers. Full design: [ARCHITECTURE.md](../ARCHITECTURE.md). API contract: [openapi/openapi.yaml](../openapi/openapi.yaml).

## Submission blurb (paste into the challenge form)

> Multi-tenant SMS Gateway in Go (Kafka, Redis, Postgres, ClickHouse). Prepaid credit with atomic Redis Lua debit + Redis Streams outbox into Kafka; Inbox-deduped consumers for dispatch, billing, and reporting. Express lane with a 2-minute hard deadline (drop + refund). Campaigns are normal-priority, all-or-nothing. Tenant identity always from API key. Lean Docker Compose demo designed toward ~100M SMS/day (not load-proven at that scale). Docs: architecture, OpenAPI, Prometheus metrics catalog, security checklist, E2E scenario report, k6 accept-path report. Repo: https://github.com/amiiirdara/sms-gateway — start at `docs/reviewer-guide.md`.

## 1. Start the stack (~2–3 min)

**Need:** Docker Desktop, optionally Go 1.25+ and [k6](https://k6.io/docs/get-started/installation/).

```bash
make up
# or: docker compose up -d --build
```

Infra images are pinned in `docker-compose.yml` (Postgres 16.6, Redis 7.4, Kafka 3.7.0, ClickHouse 24.8, migrate v4.18.1). Compose sets ClickHouse password `sms` (`CLICKHOUSE_PASSWORD`) for the `default` user. Prefer a recent `master` tip for app images rebuilt from this repo.

Wait until services are up. Then:

| URL | Role |
|---|---|
| http://localhost:8080 | API gateway (+ `GET /metrics`) |
| http://localhost:8081 | Reporting API |
| http://localhost:8080/healthz | Liveness |

> **Windows note:** If Adobe Connect (or another app) owns `127.0.0.1:8080`, use `http://[::1]:8080` (IPv6 loopback). PowerShell `http://localhost:8080` often works too (prefers IPv6).

## 2. Happy path smoke (~1 min)

PowerShell:

```powershell
$base = 'http://localhost:8080'   # or http://[::1]:8080
$acc = Invoke-RestMethod -Method Post -Uri "$base/v1/accounts" `
  -ContentType application/json -Body '{"name":"reviewer"}'
$H = @{ Authorization = "Bearer $($acc.apiKey)"; "Content-Type" = "application/json" }
Invoke-RestMethod -Method Post -Uri "$base/v1/topups" -Headers $H -Body '{"amount":10}'
$msg = Invoke-RestMethod -Method Post -Uri "$base/v1/messages" -Headers $H `
  -Body '{"to":"09121234567","text":"hello","priority":"normal"}'
# Poll until status is sent
Invoke-RestMethod -Method Get -Uri "http://localhost:8081/v1/messages/$($msg.messageId)" -Headers $H
```

Expect: create → topup → `accepted` → after a few seconds `sent`.

## 3. Edge-case smoke (~30 s)

```powershell
powershell -File scripts/smoke-edge.ps1
# or: make smoke
```

Covers insufficient funds (402), campaign all-or-nothing, spend-to-exact-zero.

## 4. Optional: E2E scenario suite (~15 s)

```powershell
make scenarios
# or: powershell -File scripts/run-scenario-suite.ps1
```

Seven flows (normal / Express / campaign / 402 / AoN / validation / burst) with Prometheus deltas and charts. Report: [scenario-report.md](scenario-report.md).

## 5. Optional: load test (~30 s)

```powershell
$env:BASE_URL = 'http://[::1]:8080'   # or http://localhost:8080
k6 run scripts/load-accept.js
# or: make load-test
```

Report: [load-test-report.md](load-test-report.md) (20 req/s × 30s, ~600 accepts, p95 &lt; 10 ms in recorded runs).

## 6. What was verified

| Check | Result |
|---|---|
| Create account + API-key auth | OK |
| Top-up + balance | OK |
| Normal send → `sent` | OK |
| Express send → `sent` | OK |
| Campaign fan-out | OK |
| Insufficient funds / AoN reject | OK (edge script + scenario suite) |
| E2E scenario suite (7/7) | PASS — see scenario report |
| Accept-path load (k6) | PASS — see load-test report |
| Unit tests | `go test ./internal/domain/... ./internal/platform/httpx/...` |
| CI | GitHub Actions: `go vet` + `go test -short` |

## Key docs

| Doc | Why open it |
|---|---|
| [ARCHITECTURE.md](../ARCHITECTURE.md) | Outbox/Inbox, Express SLA, data model |
| [openapi/openapi.yaml](../openapi/openapi.yaml) | REST contract |
| [metrics.md](metrics.md) | Prometheus catalog ([metrics.go](../internal/platform/metrics/metrics.go)) |
| [security-ops-checklist.md](security-ops-checklist.md) | Tenant isolation, keys, rate limits, Inbox ([auth](../internal/platform/httpx/auth/auth.go)) |
| [trade-offs.md](trade-offs.md) | Deliberate non-goals |
| [architecture.svg](architecture.svg) | One-page system diagram |
| [scenario-report.md](scenario-report.md) | E2E flows + metric charts |
| [load-test-report.md](load-test-report.md) | k6 scenario + results |
| [grafana-sms-gateway.json](grafana-sms-gateway.json) | Optional Grafana dashboard for `sms_*` |

## Mental model (30 seconds)

```
Client → api-gateway → Redis (atomic debit + outbox)
                    → outbox-relay → Kafka
                    → dispatcher → operator-mock
                    → billing-consumer / report-sink → Postgres + ClickHouse
Client → reporting-api → status & reports
```

Tenant identity always comes from the API key — never from a client-supplied account ID.
