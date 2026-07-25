# SMS Gateway — Project Report

One-page map of the whole system. Each section is intentionally **brief**; follow the links when you want depth.


| Want…                     | Go to                                           |
| ------------------------- | ----------------------------------------------- |
| Run it in 5 minutes       | [reviewer-guide.md](reviewer-guide.md)          |
| Deep design rationale     | [ARCHITECTURE.md](../ARCHITECTURE.md)           |
| REST contract             | [openapi/openapi.yaml](../openapi/openapi.yaml) |
| Repo layout / conventions | [AGENTS.md](../AGENTS.md)                       |
| Entry point / quickstart  | [README.md](../README.md)                       |


---

## 1. What this is

A **multi-tenant SMS Gateway** in Go: create an account, top up prepaid credit, send single SMS (normal or Express) or batch campaigns, then read delivery status and reports. Built for an ArvanCloud developer challenge as a **lean Docker Compose demo** whose pieces are designed to scale toward ~100M SMS/day — that throughput is a **design target**, not a measured proof.

**Repo:** [https://github.com/amiiirdara/sms-gateway](https://github.com/amiiirdara/sms-gateway)

More: [ARCHITECTURE.md §1–2](../ARCHITECTURE.md) · [trade-offs.md](trade-offs.md)

---



## 2. Stack at a glance


| Layer               | Choice                      | Role                                      |
| ------------------- | --------------------------- | ----------------------------------------- |
| Language            | Go                          | Services under `cmd/`                     |
| Hot-path balance    | Redis + Lua                 | Atomic debit + outbox in one script       |
| Staging outbox      | Redis Streams               | Bridge accept → Kafka without dual-write  |
| Event backbone      | Kafka                       | Dispatch, billing, reporting fan-out, DLQ |
| System of record    | PostgreSQL (`pgx` + `sqlc`) | Accounts, ledger, messages, Inbox         |
| Analytics / reports | ClickHouse                  | High-ingest `message_events`              |
| Operator            | HTTP mock                   | Pluggable adapters; no real carrier       |


Diagram: [architecture.svg](architecture.svg) · narrative: [ARCHITECTURE.md §4](../ARCHITECTURE.md)

---



## 3. End-to-end message flow

```
Client
  → api-gateway          (auth, rate limit, Lua debit + outbox)
  → outbox-relay         (Redis Stream → Kafka)
  → dispatcher           (operator call; Express deadline)
  → sms.dispatch-results
       ├→ billing-consumer   (ledger debit/refund; Redis credit on fail)
       └→ report-sink        (Postgres status + ClickHouse event)
Client
  → reporting-api        (status / reports by API key)
```

Campaigns add `campaign-expander` between accept and per-recipient outbound publish. `reconciler` periodically compares Redis vs ledger (safety net only).

More: [ARCHITECTURE.md §4–5, §8–9](../ARCHITECTURE.md)

---



## 4. Services (`cmd/`)


| Binary              | Job                                                    |
| ------------------- | ------------------------------------------------------ |
| `api-gateway`       | REST ingest: accounts, topups, messages, campaigns     |
| `outbox-relay`      | Drain Redis outbox → Kafka; ACK only after publish     |
| `campaign-expander` | Fan campaign into per-recipient messages (cursor-safe) |
| `dispatcher`        | `--mode=normal|express` → operator; Inbox + results    |
| `billing-consumer`  | Durable ledger debit/refund; refund → Redis            |
| `report-sink`       | Message status in PG + events in ClickHouse            |
| `reporting-api`     | `GET` status, paginated reports, campaign aggregates   |
| `reconciler`        | Drift detect; auto-heal only Redis > ledger            |
| `operator-mock`     | Fake carrier (latency + admin failure-rate)            |


Layout rules: [AGENTS.md](../AGENTS.md) · `cmd` → `domain` → `platform`.

---



## 5. Reliability: Outbox, Inbox, DLQ

**Problem:** Redis balance, Postgres, and Kafka must not diverge after a crash.

**Outbox:** Accept Lua does `balance -= cost` and `XADD` outbox in one atomic Redis op — you cannot debit without recording why.

**Inbox:** Every Kafka consumer checks `processed_events` before side effects (at-least-once safe).

**Ordering highlights:**

- Dispatcher: operator **outside** TX; commit then publish results; Inbox hit → **republish** from DB.
- Refunds: ledger+Inbox commit **then** Redis credit; duplicates → `AlignRedisToLedger`.
- Report-sink: PG → ClickHouse ensure → Inbox mark.
- Retries: Redis-backed attempt counters → `sms.dlq` after max attempts.

More: [ARCHITECTURE.md §5](../ARCHITECTURE.md)

---



## 6. Billing & prepaid credit

- Flat **1 credit / message**; spend to **exact zero** allowed; never negative.
- Hot path: Redis Lua only (never GET-then-SET).
- Durable truth: Postgres `ledger_entries` (topup / debit / refund).
- Failures and Express SLA misses → automatic refund.
- Campaigns: **all-or-nothing** reserve for the whole recipient list.

More: [ARCHITECTURE.md §5](../ARCHITECTURE.md) · [security-ops-checklist.md](security-ops-checklist.md) (Balance & billing)

---



## 7. Express SLA & campaigns

**Express (single sends only):** hard deadline = accept + **2 minutes**. Past deadline → do not call operator → `expired_sla_missed` → refund. Separate Kafka topic + worker pool from normal.

**Campaigns:** always **normal** priority; up to 10k recipients/request; expansion uses a cursor (`expanded_through_index`) so restarts don’t double-send.

More: [ARCHITECTURE.md §7, §9](../ARCHITECTURE.md)

---



## 8. API surface

Unauthenticated: `POST /v1/accounts` (rate-limited).

Authenticated (`Authorization: Bearer <apiKey>`): topups, balance, send message/campaign, message status, reports, campaign status.

Contract (paths, schemas, errors): **[openapi/openapi.yaml](../openapi/openapi.yaml)**.

Quick curls / PowerShell: [reviewer-guide.md](reviewer-guide.md) · [README.md](../README.md)

---



## 9. Tenant isolation & security

- No login system; **API key →** `account_id` resolved server-side only.
- Never trust client-supplied account IDs in path/query/body.
- Cross-tenant lookup → **404** (not 403).
- Keys stored hashed; raw key returned once at create.
- Signup + ingest token-bucket rate limits (Redis).
- Kafka partition key ≈ `account_id` (noisy-neighbor mitigation).

Checklist with code pointers: [security-ops-checklist.md](security-ops-checklist.md)

---



## 10. Data stores


| Store      | Holds                                                                                        |
| ---------- | -------------------------------------------------------------------------------------------- |
| Redis      | `balance:{id}`, outbox streams, idempotency keys, rate-limit buckets, Kafka attempt counters |
| Postgres   | accounts, ledger, messages, status events, campaigns, `processed_events` (Inbox)             |
| ClickHouse | `message_events` (`ReplacingMergeTree`) for reports                                          |
| Kafka      | `sms.outbound.{normal,express}`, `sms.dispatch-results`, `sms.dlq`                           |


Schema: `db/migrations/` · queries: `db/queries/` → `internal/db/sqlc/` · CH DDL: `clickhouse/init/`.

More: [ARCHITECTURE.md §6, §11](../ARCHITECTURE.md)

---



## 11. Repo map

```
cmd/                 Deployable binaries
internal/domain/     billing, messaging, campaigns, reporting
internal/platform/   postgres, redis, kafka, clickhouse, inbox, httpx, metrics, logx
db/                  migrations + sqlc queries
deploy/              Dockerfile, Prometheus, Grafana
openapi/             REST contract
scripts/             smoke, scenarios, k6
docs/                This report + specialized docs
```

Conventions / non-negotiables: [AGENTS.md](../AGENTS.md) · `.cursor/rules/`

---



## 12. Observability

- Prometheus metrics (`sms_*`) on api-gateway `:8080/metrics` and workers `:9090`.
- Compose: Prometheus `:9091`, Grafana `:3000` (admin/`sms`).
- Dashboard: [deploy/grafana/dashboards/sms-gateway.json](../deploy/grafana/dashboards/sms-gateway.json)
- Alerts: [deploy/prometheus/alerts.yml](../deploy/prometheus/alerts.yml) (DLQ, handle errors, outbox errors, pipeline backlog, reader queue).

Catalog: [metrics.md](metrics.md)

---



## 13. Local run & verification


| Command                         | What                                                 |
| ------------------------------- | ---------------------------------------------------- |
| `make up`                       | Full Compose stack (migrate included)                |
| `make smoke`                    | Edge cases: 402, campaign AoN, exact-zero            |
| `make smoke-failure`            | Operator always-5xx → failed + refund                |
| `make scenarios`                | E2E suite → [scenario-report.md](scenario-report.md) |
| `make load-test` / `load-mixed` | k6 accept / Express+campaign                         |
| `make test`                     | `go test ./... -short`                               |


Windows: if `127.0.0.1:8080` is taken, use `http://[::1]:8080`.

How-to: [reviewer-guide.md](reviewer-guide.md) · evidence: [scenario-report.md](scenario-report.md) · [load-test-report.md](load-test-report.md)

---



## 14. CI

GitHub Actions (`[.github/workflows/ci.yml](../.github/workflows/ci.yml)`):

1. `go vet` + `go test -short`
2. Compose up + edge smoke + operator failure-injection smoke

---



## 15. Deliberately out of scope

Real carriers / SMPP, login/OAuth/admin UI, inbound SMS, multi-region, Kubernetes, proving 100M/day on a laptop, DLQ replay UI.

Honest list + capacity notes: [trade-offs.md](trade-offs.md)

---



## 16. Reading order (recommended)

1. **This report** — orientation
2. [reviewer-guide.md](reviewer-guide.md) — run the demo
3. [ARCHITECTURE.md](../ARCHITECTURE.md) — design depth
4. [openapi/openapi.yaml](../openapi/openapi.yaml) — API details
5. [security-ops-checklist.md](security-ops-checklist.md) · [metrics.md](metrics.md) · [trade-offs.md](trade-offs.md)
6. [scenario-report.md](scenario-report.md) · [load-test-report.md](load-test-report.md) — proof artifacts
7. [AGENTS.md](../AGENTS.md) — if you will change code

Submission blurb: see top of [reviewer-guide.md](reviewer-guide.md).