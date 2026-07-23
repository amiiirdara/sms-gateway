# Trade-offs / Out of scope

This project is a **lean, demo-scale Compose build** designed so each piece *can* scale toward ~100M SMS/day. The following were **deliberately not built** so the submission stays reviewable and correct on the hot path.

## Product / domain

| Not built | Why |
|---|---|
| Real mobile-network operators | Mock + pluggable HTTP adapters; no carrier contracts or SMPP |
| Multi-segment / priced-by-encoding SMS | Brief allows flat 1 credit / message |
| User login / OAuth / admin console | Open self-serve accounts + API keys only (per brief) |
| Inbound SMS / MO webhooks | Outbound gateway only |
| Multi-region active-active | Single Compose region; design notes in ARCHITECTURE.md |
| Per-tenant custom SLAs beyond Express | One Express hard deadline (2 min) for all tenants |

## Scale / proof

| Not built | Why |
|---|---|
| **100M/day load proof** | Capacity campaign, not another feature — details below |
| Full Prometheus + Grafana stack in Compose | Metrics are exported; scraping/alerting UI is out of scope |
| Chaos / partition testing harness | Inbox/Outbox designed for retries; no automated chaos suite |

### Can you prove 100M/day?

**Not with this Compose demo alone.** ~100M/day ≈ **~1,160 msg/s average**, with much higher peaks and skewed tenants. Proving it means a capacity campaign, not another feature.

| What you’d need | Trade-off |
|---|---|
| Multi-broker Kafka, many partitions, multiple dispatcher/billing/report replicas | Infra cost + ops complexity vs one-box Compose |
| Redis Cluster (or sharded balance keys), tuned Lua/pipeline | Harder local repro; more failure modes |
| Postgres primary + replicas / connection pooling; ClickHouse sized for ingest | Money + tuning; not “laptop Docker” |
| Realistic operator latency/error model (or stub at target RPS) | Mock that always succeeds overstates capacity |
| Soak (hours/days) + backlog/lag SLOs, not a 30s k6 | Time; find GC, FD, disk, consumer lag issues |
| Separate load gen (k6/vegeta fleet) aimed at accept **and** end-to-end drain | Accept-only proof ≠ dispatch/billing capacity |
| Observability (Prometheus/Grafana + dashboards from [docs/metrics.md](metrics.md)) | Extra stack; needed to trust the run |

**Sensible middle ground for this challenge:** keep architecture claims, show the [E2E scenario suite](scenario-report.md) + accept-path k6 + [metrics catalog](metrics.md), and treat 100M/day as a **design target**, not a measured demo.

## Platform polish

| Not built | Why |
|---|---|
| Kubernetes manifests / Helm | Compose is enough for challenge review |
| mTLS between services | Local Docker network trust |
| Secrets manager / key rotation UI | API keys hashed at rest; rotation ops not productized |
| DLQ replay UI | DLQ topic exists for poison messages; ops tooling minimal |

## What we prioritized instead

- Correct prepaid debit (atomic Redis Lua, spend to exact zero)
- Outbox + Inbox so debit and “why” cannot diverge silently
- Express hard deadline + refund
- Tenant isolation from API key
- Clear architecture docs, OpenAPI, metrics catalog, E2E scenario suite, and a small k6 accept-path test

If something is missing from this list and you need it for production, treat this repo as the **reference architecture + working vertical slice**, not a turnkey carrier gateway.
