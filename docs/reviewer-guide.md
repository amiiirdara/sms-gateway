# Reviewer Guide (first 5 minutes)

Quick path for challenge reviewers. Full design: [ARCHITECTURE.md](../ARCHITECTURE.md). API contract: [openapi/openapi.yaml](../openapi/openapi.yaml).

## 1. Start the stack (~2–3 min)

**Need:** Docker Desktop, optionally Go 1.25+ and [k6](https://k6.io/docs/get-started/installation/).

```bash
docker compose up -d --build
```

Wait until services are up. Then:

| URL | Role |
|---|---|
| http://localhost:8080 | API gateway (+ `GET /metrics`) |
| http://localhost:8081 | Reporting API |
| http://localhost:8080/healthz | Liveness |

> **Windows note:** If Adobe Connect (or another app) owns `127.0.0.1:8080`, use `http://localhost:8080` (IPv6) or `http://[::1]:8080` for k6.

## 2. Happy path smoke (~1 min)

PowerShell:

```powershell
$acc = Invoke-RestMethod -Method Post -Uri http://localhost:8080/v1/accounts `
  -ContentType application/json -Body '{"name":"reviewer"}'
$H = @{ Authorization = "Bearer $($acc.apiKey)"; "Content-Type" = "application/json" }
Invoke-RestMethod -Method Post -Uri http://localhost:8080/v1/topups -Headers $H -Body '{"amount":10}'
$msg = Invoke-RestMethod -Method Post -Uri http://localhost:8080/v1/messages -Headers $H `
  -Body '{"to":"09121234567","text":"hello","priority":"normal"}'
# Poll until status is sent
Invoke-RestMethod -Method Get -Uri "http://localhost:8081/v1/messages/$($msg.messageId)" -Headers $H
```

Expect: create → topup → `accepted` → after a few seconds `sent`.

## 3. Edge-case smoke (~30 s)

```powershell
powershell -File scripts/smoke-edge.ps1
```

Covers insufficient funds (402), campaign all-or-nothing, spend-to-exact-zero.

## 4. Optional: load test (~30 s)

```powershell
$env:BASE_URL = 'http://[::1]:8080'   # or http://localhost:8080
k6 run scripts/load-accept.js
```

Report: [load-test-report.md](load-test-report.md) (20 req/s × 30s, ~600 accepts, p95 &lt; 10 ms in recorded runs).

## 5. What was verified

| Check | Result |
|---|---|
| Create account + API-key auth | OK |
| Top-up + balance | OK |
| Normal send → `sent` | OK |
| Express send → `sent` | OK |
| Campaign fan-out | OK |
| Insufficient funds / AoN reject | OK (edge script) |
| Accept-path load (k6) | PASS — see load-test report |
| Unit tests | `go test ./internal/domain/... ./internal/platform/httpx/...` |
| CI | GitHub Actions: `go vet` + `go test -short` |

## Key docs

| Doc | Why open it |
|---|---|
| [ARCHITECTURE.md](../ARCHITECTURE.md) | Outbox/Inbox, Express SLA, data model |
| [openapi/openapi.yaml](../openapi/openapi.yaml) | REST contract |
| [metrics.md](metrics.md) | Prometheus business + technical metrics |
| [security-ops-checklist.md](security-ops-checklist.md) | Tenant isolation, keys, Inbox |
| [trade-offs.md](trade-offs.md) | Deliberate non-goals |
| [architecture.svg](architecture.svg) | One-page system diagram |
| [load-test-report.md](load-test-report.md) | k6 scenario + results |

## Mental model (30 seconds)

```
Client → api-gateway → Redis (atomic debit + outbox)
                    → outbox-relay → Kafka
                    → dispatcher → operator-mock
                    → billing-consumer / report-sink → Postgres + ClickHouse
Client → reporting-api → status & reports
```

Tenant identity always comes from the API key — never from a client-supplied account ID.
