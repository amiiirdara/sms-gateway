# Security & Ops Checklist

Verification aid for reviewers. These behaviors are implemented in code; this doc maps them to where to look.

## Tenant isolation

| Control | Status | Where |
|---|---|---|
| `account_id` resolved from API key only (never path/query/body) | Yes | [`internal/platform/httpx/auth`](../internal/platform/httpx/auth/auth.go), api-gateway / reporting-api handlers |
| By-ID lookups filter by authenticated `account_id` | Yes | reporting queries / handlers |
| Cross-tenant resource → **404** (not 403) | Yes | Same lookups (no existence leak) |
| Kafka partition key = `account_id` (fairness under skew) | Yes | outbox-relay / campaign-expander publish |

## API keys

| Control | Status | Where |
|---|---|---|
| Keys stored as hash (`accounts.api_key_hash`) | Yes | `billing.CreateAccount`, auth middleware |
| Raw key returned **once** at create; never logged | Yes | create-account response only |
| `Authorization: Bearer <apiKey>` | Yes | OpenAPI + auth middleware |

## Open account endpoint

| Control | Status | Where |
|---|---|---|
| `POST /v1/accounts` unauthenticated (intentional) | Yes | api-gateway |
| Abuse control on open signup | Yes | Redis token bucket by **RemoteAddr** IP — [`ratelimit.ByIP`](../internal/platform/httpx/ratelimit/ratelimit.go); set `TRUST_PROXY=1` only behind a real reverse proxy that sets XFF |
| Per-tenant ingest rate limit | Yes | Same bucket on `POST /v1/messages` and `/v1/campaigns` after auth (`INGEST_RATE_*`) |

## Balance & billing

| Control | Status | Where |
|---|---|---|
| Atomic check-and-decrement (Lua), never plain GET then SET | Yes | Redis Lua accept scripts |
| Spend to exact zero allowed; never negative | Yes | Lua + unit/integration tests |
| Campaign all-or-nothing reserve | Yes | campaigns accept |
| Ledger debits/refunds durable + Inbox-deduped | Yes | billing-consumer |
| Refund on `failed` / `expired_sla_missed` | Yes | billing-consumer |
| Reconciler auto-heals **only** Redis > ledger | Yes | reconciler (never invents free credit the other way) |

## Reliability (Outbox / Inbox)

| Control | Status | Where |
|---|---|---|
| Debit + outbox entry atomic in Redis | Yes | accept Lua in [`internal/platform/redis/redis.go`](../internal/platform/redis/redis.go) |
| Consumers Inbox-check before side effects | Yes | [`internal/platform/inbox`](../internal/platform/inbox), dispatcher, billing, report-sink |
| Duplicate deliveries skipped | Yes | `processed_events` / inbox package |
| Express deadline checked at dispatch time | Yes | [`cmd/dispatcher`](../cmd/dispatcher/main.go) express mode |

## Observability & ops

| Control | Status | Where |
|---|---|---|
| Prometheus `/metrics` on api-gateway | Yes | `:8080/metrics` |
| Worker metrics via `METRICS_ADDR` | Yes | default `:9090` |
| Metric catalog | Yes | [metrics.md](metrics.md) |
| Health endpoint | Yes | `GET /healthz` on api-gateway |

## Secrets & local demo

| Control | Status | Guidance |
|---|---|---|
| Compose Postgres (`sms`/`sms`) | Demo only | Change for any shared environment |
| Compose ClickHouse (`default` / `sms` via `CLICKHOUSE_PASSWORD`) | Demo only | Required by ClickHouse 24.8 image; app uses `NewWithPassword` |
| No secrets in git | Yes | Use env / Compose env files locally |

## Quick review commands

```bash
# Unit tests (no Docker)
go test ./internal/domain/... ./internal/platform/httpx/... -count=1

# Edge smokes (stack up)
powershell -File scripts/smoke-edge.ps1

# Full E2E scenario suite (stack up) → docs/scenario-report/
make scenarios

# Sample metrics
curl -s http://localhost:8080/metrics | findstr sms_
```
