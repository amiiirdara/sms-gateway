# SMS Gateway - Architecture

Status: **v1 — implemented and verified locally via Docker Compose** (happy-path + edge smokes, [E2E scenario suite](docs/scenario-report.md), [k6 accept-path](docs/load-test-report.md)).

Brief whole-project tour (with links back here): [docs/project-report.md](docs/project-report.md).

## 1. Overview

A multi-tenant SMS Gateway: tenants (businesses) send SMS to any phone number via a REST API, must maintain a prepaid credit balance to do so, and can retrieve delivery reports for single messages and bulk campaigns. The system is designed for ~100M messages/day with highly skewed per-tenant traffic, and offers a premium "Express" lane with a hard delivery-time SLA (e.g. for OTP codes).

This document is the source of truth for the system design. It intentionally favors a **lean, horizontally-scalable Docker Compose build** over a fully productionized deployment - every component is designed so it *can* scale to the target numbers, without us building out the full production operational surface (multi-broker Kafka, replicated Postgres, etc.) up front.

## 2. Requirements

- Send SMS to any number via REST API; view reports of sent messages.
- Every tenant has a limited credit/balance; must top up before sending; must be able to spend it down to **exactly zero**, never negative.
- Scale target: tens of thousands of businesses, ~100M messages/day (~1,160/sec average, highly bursty/skewed across tenants).
- **Express** lane for time-sensitive messages (e.g. OTP) with a guaranteed delivery-time SLA to the operator.
- No auth/user-management system required (see section 10 for how tenant identity is still handled safely).
- English/Persian priced the same; all messages are single-segment.
- REST API only, no UI.
- Go preferred.

## 3. Key architectural decisions

| Decision | Choice | Why |
|---|---|---|
| Message broker | **Kafka** | Durable buffering, independent consumer groups per concern (dispatch/billing/reporting), replay, partition-based tenant isolation at 100M/day scale. |
| Language | **Go** | Matches the brief's stated preference; strong concurrency primitives for high-throughput I/O-bound services. |
| Live balance | **Redis** (atomic Lua) | Sub-millisecond atomic check-and-decrement under massive concurrency; see section 5. Accept and `GET /v1/balance` use this number. |
| Durable balance | **PostgreSQL** (`accounts.balance`) | Mutated column — the prepaid amount that has been durably applied. Heal and cold-start trust this number. Never a SUM of a log. |
| Credit log | **ClickHouse** (`credit_events`) | Append-only topup/debit/refund history. Never summed to produce a balance. |
| System of record | **PostgreSQL** | Strong consistency for accounts, durable balance, message/campaign state and Inbox. |
| Reporting store | **ClickHouse** | Purpose-built for high-ingest, aggregation-heavy "give me my report" queries at 100M-rows/day scale (CQRS read side), plus the credit log. |
| Postgres tooling | `pgx` + `sqlc` + `golang-migrate` | Compile-time-checked SQL, zero ORM/reflection overhead, full control over query shape. No GORM. |
| Consistency mechanism | **Outbox (Redis Streams) + Inbox pattern** | See section 5 - solves the "hot-path store and durable store can drift" problem without relying on fragile reconciliation. |

### Why are the reporting APIs separated from the API gateway? 

They're separate **services**, not just different ports. The `8080` / `8081` split is how Compose maps two containers that both listen on `:8080` inside the network onto the host without colliding.

#### What each does

| | **API Gateway** (`:8080`) | **Reporting API** (`:8081`) |
|---|---|---|
| Role | Write / accept path | Read / query path |
| Endpoints | accounts, topups, send SMS, campaigns | message status, reports, campaign aggregates |
| Stores | Postgres + Redis (Lua debit + outbox); ClickHouse credit-log append on topup | Postgres + ClickHouse (+ Redis for auth) |

That matches the CQRS shape: gateway accepts and debits live balance; ClickHouse is the analytics / credit-log read side; `reporting-api` serves status/reports.

#### Why separate them

1. **Protect the hot path** — Accept is latency-critical (auth, rate limit, Redis Lua). Heavy report queries against ClickHouse must not share that process or connection pool.

2. **Different dependencies** — Report **queries** live only on `reporting-api`. The gateway may append a credit-log row on topup (after durable commit); accept (`POST /v1/messages`) still does not talk to ClickHouse. Keeping heavy analytics out of the accept binary protects the hot path.

3. **Independent scale / failure** — You can scale or restart reporting without taking down ingest (and vice versa). A ClickHouse blip must not take down `POST /v1/messages` (credit-log append is after money commits; a blip must not block durable catch-up).

4. **Bounded contexts** — Write logic lives in `billing` / `messaging` / `campaigns`; read shaping in `reporting`. Separate `cmd/` binaries match that.

#### About the ports

In Compose, both use `HTTP_ADDR: ":8080"` in-container; only the host publish differs:

```133:151:docker-compose.yml
  api-gateway:
    ...
    ports:
      - "8080:8080"

  reporting-api:
    ...
    ports:
      - "8081:8080"
```

`GET /metrics` is on the gateway because accept-path Prometheus metrics are emitted there; workers expose their own metrics via `METRICS_ADDR`, not via reporting-api.

**Bottom line:** separation is intentional CQRS / load isolation — command API vs read API — not an arbitrary port choice.

## 4. High-level architecture (single-message flow)

Static diagram: [docs/architecture.svg](docs/architecture.svg) · [docs/architecture.png](docs/architecture.png)

```mermaid
flowchart LR
    Client["API Client"] -->|"REST (API key)"| Gateway["API Gateway (ingestion)"]
    Gateway -->|"atomic: check, decrement, append outbox (Lua)"| Redis[("Redis: balance, outbox streams, idempotency, rate limits")]
    Redis -->|"XREADGROUP"| Relay["Outbox Relay"]
    Relay -->|"publish, then XACK"| Kafka{{"Kafka"}}
    Kafka -->|"sms.outbound.normal"| NormalWorkers["Normal Dispatcher Workers (Inbox dedup)"]
    Kafka -->|"sms.outbound.express"| ExpressWorkers["Express Dispatcher Workers (Inbox dedup, deadline check)"]
    NormalWorkers --> OperatorMock["Operator Adapter (mock telecom API)"]
    ExpressWorkers --> OperatorMock
    NormalWorkers -->|"dispatch result"| Results{{"sms.dispatch-results"}}
    ExpressWorkers -->|"dispatch result or expired_sla_missed"| Results
    Results --> ReportSink["Report Sink Consumer (Inbox dedup)"]
    Results --> Billing["Billing Consumer (Inbox dedup)"]
    ReportSink --> ClickHouse[("ClickHouse: message_events, credit_events")]
    ReportSink --> Postgres[("Postgres: accounts, messages, campaigns, history")]
    Billing --> Postgres
    Billing --> ClickHouse
    Billing -.->|"refund on failure or SLA-miss"| Redis
    Reconciler["Reconciliation Job (safety net)"] -.->|"heal live down to durable"| Redis
    Reconciler -.-> Postgres
    ReportingAPI["Reporting API"] --> ClickHouse
    ReportingAPI --> Postgres
    Client -->|"GET status / reports (API key)"| ReportingAPI
```

Binaries (`cmd/`): `api-gateway`, `outbox-relay`, `campaign-expander`, `dispatcher` (`--mode=normal|express`), `report-sink`, `billing-consumer`, `reconciler`, `reporting-api`, `operator-mock`.

## 5. Reliability backbone: Outbox + Inbox

**The problem:** the API Gateway's hot-path balance decrement lives in Redis (for atomic, sub-millisecond concurrency control), while the durable system of record is Postgres, and the dispatch pipeline runs through Kafka. If these were three independent writes, a crash between them could create a permanent gap - a debit with no record of what it paid for, or a message accepted but never dispatched. A periodic "compare Redis to Postgres and fix" reconciliation job cannot safely close this gap on its own: it can hand back credit that was already legitimately spent (a free-money bug) just as easily as it can catch a real drift.

**The fix:** make the decrement and "the event to propagate" a single atomic operation, then guarantee retried, eventual delivery of that event. Reconciliation becomes a safety net, never the primary sync mechanism.

### 5.1 Outbox (write side)

The API Gateway's Lua script does two things atomically, in one indivisible Redis operation:

1. `if balance >= cost: balance -= cost else return INSUFFICIENT_FUNDS`
2. On success: `XADD outbox:messages * account_id ... message_id ... to ... priority ... cost ... deadline ...` (a Redis Stream entry, in the **same** script execution).

Because both happen atomically, it is impossible to decrement the balance without durably recording why. The debit can be *delayed* downstream, never silently lost.

**Why a Redis Stream *alongside* Kafka** (not instead of it): these solve different problems. Kafka is the durable backbone for everything downstream of acceptance (dispatch, billing, reporting fan-out) and is deliberately kept out of the synchronous request path. Redis Streams' only job is to be the tiny atomic staging log between the synchronous balance decrement and Kafka.

- It has to live **in Redis**, co-located with `balance`, to get atomicity with the decrement via one Lua script. The classic transactional-outbox pattern writes to the same Postgres transaction as the business row - but `balance` intentionally isn't in Postgres on the hot path, so the outbox medium must be wherever the atomic decrement happens.
- Publishing to Kafka synchronously from the request would reintroduce a dual-write across two different systems, and make Kafka a hard dependency on the hot path (worse p99, and a Kafka blip fails all sends even though balance logic is fine).
- Among Redis data structures, a **Stream** (not a List/Set) gives an outbox relay exactly the semantics it needs natively: consumer groups, per-entry `XACK`, and `XAUTOCLAIM`/`XCLAIM` to reclaim entries from a crashed consumer - a List would require hand-rolling all of that.
- It's a deliberately short-lived staging buffer, not a Kafka replacement - Kafka remains the backbone for throughput/retention/fan-out at 100M/day scale.

`cmd/outbox-relay` consumes `outbox:messages` (and `outbox:campaigns`) via a Redis Streams consumer group, publishes to the right Kafka topic with an idempotent producer, and only `XACK`s after Kafka confirms. A crash-and-retry can cause a duplicate publish - safe, because every downstream consumer is idempotent (Inbox, below). The billing consumer subscribes to this same accepted-event stream, so the durable debit is *guaranteed* to eventually exist.

### 5.2 Inbox (read side)

Kafka is at-least-once, so every consumer (`dispatcher`, `report-sink`, `billing-consumer`, `campaign-expander`) can see the same message more than once. Each keeps an Inbox/dedup check before any side effect: a Postgres table `processed_events(consumer_name, event_id, processed_at)` with a unique constraint on `(consumer_name, event_id)`, checked-or-inserted in the same transaction as the business write. Duplicate delivery becomes safe by construction (never double-charge, never double-send).

**Ordering rules (crash-safe):**

- **Dispatcher:** call the operator **outside** any Postgres TX; short TX for Inbox + message rows; publish `sms.dispatch-results` after commit. If Inbox already shows processed (commit succeeded, publish failed), **republish** the result from Postgres instead of no-opping.
- **Billing refunds:** Inbox + durable-balance Δ, **commit**, then credit live balance. Never increment live before Commit. On Inbox duplicate, do not apply a second money Δ; ensure the credit-log row, and align live to durable (this can raise live after a crash between durable refund and Redis `INCR`). Inbox is the only debit/refund idempotency.
- **Report-sink:** Postgres status writes → ClickHouse ensure-insert → Inbox mark (Inbox only after both stores succeed). ClickHouse uses `ReplacingMergeTree` + an exists-check for idempotent retries.
- **Accept idempotency:** `Idempotency-Key` is reserved inside the Redis Lua debit script (not a separate GET/SET race).
- **DLQ:** consumers use bounded retries (`ConsumeLoopWithStore`, Redis-backed attempt counters) then publish to `sms.dlq`.

### 5.3 What reconciliation is for

`cmd/reconciler` runs periodically and calls `Heal` (seed + down-heal). It is a safety net, never the primary sync mechanism:

- Live balance **higher** than durable → set live down immediately (dangerous "free credit" direction). Heal never grants credit.
- Live balance **lower** than durable on a **present** key → leave live alone (expected lag from async durable catch-up). Blind auto-heal up could invent spendable credit. Inbox-hit refund is a separate path: it may align live **to** durable after a crash between durable refund and Redis `INCR`.
- **Seed:** copy durable → live only if the Redis key is **absent**. Never overwrite a present live key with a higher durable value.
- Redis runs with AOF persistence (`appendfsync everysec` minimum) so a normal restart doesn't even trigger this path.

## 6. Event history & auditability

Every important entity has a current-state table **and** a companion append-only history (a lightweight audit-trail style, not strict event sourcing):

- **Balance/credit:** durable balance is the mutated `accounts.balance` column (never a cache of a SUM). Credit history is ClickHouse `credit_events` (append-only topup/debit/refund); nothing heals, seeds, or spends by summing it.
- **Message lifecycle:** `messages` holds current status; `message_status_events` (Postgres, append-only) records every transition. The same lifecycle events flow into ClickHouse `message_events` for large-scale analytics.
- **Campaigns:** `campaigns` holds current aggregate status; per-recipient `messages` rows carry their own history; campaign aggregates are computed on read.
- **Outbox entries** are themselves an event log of every accepted send/debit.

## 7. Express SLA policy - drop if useless

Delivering an Express message after its usefulness window (e.g. an OTP) is worse than not sending it, since the cost was spent for nothing.

- Every Express message carries a hard deadline = `accepted_at + 2 minutes` (see targets below), set when the outbox entry is created.
- The Express Dispatcher checks the deadline immediately before calling the Operator Adapter (not just at enqueue time): past deadline -> do not call the operator, mark `expired_sla_missed`.
- The Billing Consumer treats `expired_sla_missed` exactly like a dispatch failure: automatic refund.
- **Tier 1 (target/advertised SLA, used for alerting and sizing the dedicated Express pool): 95% dispatched within 1 minute.**
- **Tier 2 (hard ceiling, used for the auto-drop rule): 99.9% dispatched within 2 minutes.**
- Normal-priority messages have no such deadline.
- **Campaigns never use the Express lane** - see section 9.

## 8. Component breakdown

- **API Gateway** (`cmd/api-gateway`): REST entry point. Resolves `account_id` from the API key only (never client input). Runs the Lua accept script for `/v1/messages` and `/v1/campaigns`. Topup requires `Idempotency-Key`, mutates durable balance, credits live, then appends a credit-log row. Per-tenant token-bucket rate limiting at ingestion (independent of balance).
- **Outbox Relay** (`cmd/outbox-relay`): drains Redis outbox streams into Kafka reliably.
- **Dispatcher** (`cmd/dispatcher --mode=normal|express`): Inbox-dedup, calls the Operator Adapter, retries with backoff, enforces the Express deadline.
- **Report Sink** (`cmd/report-sink`): Inbox-dedup, updates `messages`/`message_status_events` (Postgres) and `message_events` (ClickHouse).
- **Billing Consumer** (`cmd/billing-consumer`): calls `ApplyDebit` / `ApplyRefund` (Inbox + durable Δ inside billing; then credit-log append). Refunds credit live only after durable commit.
- **Reconciler** (`cmd/reconciler`): safety-net `Heal` — seed absent live keys; set live down when live > durable (section 5.3).
- **Campaign Expander** (`cmd/campaign-expander`): fans an accepted campaign out into individual messages (section 9).
- **Reporting API** (`cmd/reporting-api`): serves `/v1/messages/{id}`, `/v1/reports`, `/v1/campaigns/*`.
- **Operator Mock** (`cmd/operator-mock`): simulates a telecom operator (latency + configurable failure rate) - there is no real operator integration in this build.

### Kafka topics

- `sms.outbound.normal` - partitioned (e.g. 32-64 partitions) by `hash(account_id)`; a bursty tenant only saturates *their* partition(s) - the core noisy-neighbor mitigation.
- `sms.outbound.express` - separate topic + dedicated, over-provisioned consumer pool so lag stays ~0.
- `sms.dispatch-results` - consumed independently by Report Sink and Billing Consumer.
- `sms.dlq` - messages that exhaust retries.

## 9. Batch sending: Campaigns

A campaign = one message body sent to many recipients in a single request.

**Campaigns are always normal priority - Express is not available for campaigns.** Express capacity is deliberately small and dedicated to guarantee a tight per-message SLA for time-sensitive single sends (OTPs); letting a large campaign into that lane would force massive over-provisioning or break the SLA for real Express traffic.

**Accept flow** (`POST /v1/campaigns { text, recipients: [...] }`, capped at 10,000 recipients/request to keep the Redis Lua critical section short):

1. Validate every recipient number.
2. `total_cost = cost_per_message * len(recipients)`.
3. One Lua call: check `balance >= total_cost`, decrement once, `XADD outbox:campaigns` with the recipient list.
4. **Insufficient balance -> all-or-nothing**: reject the entire campaign with `402` and the exact shortfall; no partial sends.
5. Return `202 Accepted` immediately.

**Campaign Expander** (`cmd/campaign-expander`) consumes `outbox:campaigns`:

1. Generates a deterministic per-recipient `message_id = hash(campaign_id, recipient_index)` - this is what makes expansion safely retryable.
2. Bulk-inserts the `campaigns` row and all `messages` rows in one transaction via `INSERT ... ON CONFLICT (id) DO NOTHING`.
3. Publishes one event per recipient to `sms.outbound.normal` **only**, using the same `hash(account_id)` partitioning.
4. `XACK`s the campaign outbox entry only once all of the above succeeds.

From there, dispatch/billing/reporting work exactly as for a single message, carrying a `campaign_id` field. Refunds on failure/expiry are per-recipient-message.

**Campaign reporting:** `GET /v1/reports?campaignId=...` for per-message listing; `GET /v1/campaigns/{id}/report` for an aggregate view (`totalRecipients, sent, failed, expiredSlaMissed, pending, totalCost, refundedAmount`).

## 10. REST API surface (v1)

Tenant identity is **always** resolved server-side from the API key (`Authorization: Bearer <apiKey>`); never accepted as a path/query/body parameter. Cross-tenant lookups return `404` (not `403`) to avoid leaking existence of another tenant's resources.

Recipient phone numbers (`to`) accept both E.164 (`+989121234567`) and local Iranian mobile format (`09121234567`); local numbers are normalized to E.164 internally.

| Method & path | Auth | Notes |
|---|---|---|
| `POST /v1/accounts` | none (open, rate-limited) | `{ name }` -> `{ accountId, apiKey }` |
| `POST /v1/topups` | API key + `Idempotency-Key` | `{ amount }` -> `{ balance }` (durable + live; replay returns stored durable balance) |
| `GET /v1/balance` | API key | -> `{ balance }` (live balance) |
| `POST /v1/messages` | API key + `Idempotency-Key` | `{ to, text, priority }` -> `{ messageId, status, cost }`; `priority`: `normal｜express` |
| `GET /v1/messages/{id}` | API key | 404 if not found/not owned |
| `POST /v1/campaigns` | API key + `Idempotency-Key` | `{ text, recipients: [...] }` -> `{ campaignId, totalRecipients, cost }` (always normal priority) |
| `GET /v1/campaigns` | API key | paginated list |
| `GET /v1/campaigns/{id}` | API key | summary/status |
| `GET /v1/campaigns/{id}/report` | API key | aggregate report |
| `GET /v1/reports?campaignId=&from=&to=&status=&page=` | API key | paginated, scoped to caller |

See [`openapi/openapi.yaml`](openapi/openapi.yaml) for the full machine-readable contract.

## 11. Data model

**Postgres** (mutable business tables include `created_at`, `updated_at`; Inbox is append-only):

- `accounts(id, api_key_hash, name, balance, created_at, updated_at)` — `balance` is durable balance, mutated in place
- `topup_idempotency(account_id, idempotency_key, amount, durable_balance, created_at, updated_at)` — one successful topup per account+key; replay returns stored durable_balance
- `campaigns(id, account_id, text, total_recipients, cost_per_message, total_cost, status, created_at, updated_at)`
- `messages(id, account_id, campaign_id nullable, recipient, priority, cost, status, operator, deadline_at nullable, created_at, updated_at, dispatched_at)`
- `message_status_events(id, message_id, status, occurred_at, created_at, updated_at)`
- `processed_events(consumer_name, event_id, processed_at, created_at)` — append-only Inbox; no `updated_at`

**Redis:** `balance:{account_id}`, `outbox:messages`, `outbox:campaigns`, `idem:{account_id}:{idempotency_key}`, `ratelimit:{account_id}`.

**ClickHouse** (database `sms_gateway`):
- `message_events(event_time, message_id, account_id, campaign_id nullable, recipient, priority, status, cost, operator)` — delivery reporting; not money
- `credit_events(event_time, account_id, type[topup|debit|refund], amount, message_id nullable, idempotency_key nullable)` — credit log; never summed for a balance

See [`db/migrations/`](db/migrations) for the executable schema and [`clickhouse/init/`](clickhouse/init) for the ClickHouse table definition.

## 12. Tech stack

- **HTTP:** Go stdlib `net/http` (`ServeMux` + middleware in `internal/platform/httpx`)
- **Postgres:** `jackc/pgx` (driver/pool) + `sqlc` (compile-time typed queries) + `golang-migrate` (migrations). No GORM.
- **Kafka:** `segmentio/kafka-go`
- **Redis:** `redis/go-redis`
- **ClickHouse:** `ClickHouse/clickhouse-go` (native protocol; Compose demo password via `CLICKHOUSE_PASSWORD`, default `sms`)
- **Logging:** stdlib `log`; **metrics:** Prometheus (`sms_*` catalog in [docs/metrics.md](docs/metrics.md))

## 13. Cross-cutting concerns

- **Idempotency:** `Idempotency-Key` header on `/v1/messages` and `/v1/campaigns` (Redis, inside the Lua debit); on `/v1/topups` (Postgres `topup_idempotency` in the same TX as the durable increase). Inbox (`processed_events`) for consumer-side debit/refund/dispatch dedup.
- **Retries/DLQ:** exponential backoff in dispatchers; `sms.dlq` for exhausted retries.
- **Observability:** Prometheus business + pipeline metrics on api-gateway and workers (`METRICS_ADDR`); Express SLA / dispatch latency histograms; logs include service-level context (correlation by `message_id`/`campaign_id` where handlers emit it).
- **Multi-operator routing:** pluggable `OperatorAdapter` interface + simple `Router`; one mock operator for the core build.
- **Local/dev deployment:** single `docker-compose.yml` (see below).

## 14. Local development

```bash
make up                        # docker compose up -d --build (migrate service applies Postgres schema)
# ClickHouse: default/sms (CLICKHOUSE_PASSWORD=sms); HTTP :8123, native :9000
make sqlc                      # regenerate typed queries (requires sqlc CLI)
make migrate-up                # re-run migrations against Compose Postgres
go run ./cmd/api-gateway       # optional: run a binary on the host against Compose infra
```

Host ports: api-gateway `8080`, reporting-api `8081`, Postgres `5432`, Redis `6379`, Kafka advertised host listener `9094`, ClickHouse HTTP `8123`.

If another process owns `127.0.0.1:8080` (common on Windows with Adobe Connect), use `http://[::1]:8080`.

Verification helpers: `make smoke`, `make scenarios` ([scenario report](docs/scenario-report.md)), `make load-test` ([load-test report](docs/load-test-report.md)).

See the [README](README.md) for details, and [AGENTS.md](AGENTS.md) for repo conventions when working on this codebase with an AI agent.

## 15. Explicitly out of scope (for this build)

- Real telecom operator integration (mocked instead).
- Multi-broker Kafka / replicated Postgres / ClickHouse cluster / Redis replicas (documented as production hardening, not built).
- Per-recipient message personalization/templating in campaigns.
- Any GUI.
