# Prometheus Metrics Catalog

All custom metrics use the `sms_` prefix. Standard Go runtime (`go_*`) and process (`process_*`) collectors are also registered.

## Scrape endpoints

| Service | Endpoint | Notes |
|---|---|---|
| `api-gateway` | `GET http://<host>:8080/metrics` | Same HTTP server as the REST API |
| `dispatcher` (normal/express) | `GET http://<host>:9090/metrics` | `METRICS_ADDR` (default `:9090`) |
| `outbox-relay` | `GET http://<host>:9090/metrics` | `METRICS_ADDR` |
| `billing-consumer` | `GET http://<host>:9090/metrics` | `METRICS_ADDR` |
| `report-sink` | `GET http://<host>:9090/metrics` | `METRICS_ADDR` |
| `campaign-expander` | `GET http://<host>:9090/metrics` | `METRICS_ADDR` |
| `reconciler` | `GET http://<host>:9090/metrics` | `METRICS_ADDR` |

In Docker Compose, worker metrics ports are **not** published to the host by default (in-network scrape only). For local inspection, publish `METRICS_ADDR` or `docker compose exec <service> wget -qO- localhost:9090/metrics`.

Source of truth for definitions: [`internal/platform/metrics/metrics.go`](../internal/platform/metrics/metrics.go).

---

## Business / domain metrics

These answer product questions: volume, revenue/credit flow, SLA misses, campaign size.

### Accounts & prepaid credit

| Metric | Type | Labels | Meaning | Emitted by |
|---|---|---|---|---|
| `sms_accounts_created_total` | counter | — | Successful `POST /v1/accounts` | api-gateway |
| `sms_topups_total` | counter | — | Successful top-ups | api-gateway |
| `sms_topup_credits_total` | counter | — | Sum of credits added | api-gateway |
| `sms_credits_spent_total` | counter | `priority`, `source` (`single`\|`campaign`) | Credits reserved at accept | api-gateway |
| `sms_credits_refunded_total` | counter | `reason` (`failed`\|`expired_sla_missed`) | Credits returned after failure/SLA miss | billing-consumer |
| `sms_ledger_debits_total` | counter | `priority` | Durable ledger debit rows written | billing-consumer |
| `sms_ledger_refunds_total` | counter | `reason` | Durable ledger refund rows written | billing-consumer |

**How to use:** `rate(sms_credits_spent_total[5m])` ≈ accept TPS in credits. Compare spent vs refunded for net credit burn. Ledger counters lag Redis hot-path slightly (async consumer).

### Message accept (ingestion)

| Metric | Type | Labels | Meaning | Emitted by |
|---|---|---|---|---|
| `sms_messages_accepted_total` | counter | `priority` (`normal`\|`express`) | Single SMS accepted (202) | api-gateway |
| `sms_messages_rejected_total` | counter | `reason` (`insufficient_funds`\|`validation`) | Single SMS rejected | api-gateway |
| `sms_accept_duration_seconds` | histogram | `priority`, `result` (`accepted`\|`rejected`\|`error`) | Redis Lua accept latency | api-gateway |

**How to use:** Acceptance rate = accepted / (accepted+rejected). p95 of `sms_accept_duration_seconds` is the hot-path SLA for debit+outbox.

### Campaigns

| Metric | Type | Labels | Meaning | Emitted by |
|---|---|---|---|---|
| `sms_campaigns_accepted_total` | counter | — | Campaigns accepted (all-or-nothing reserve) | api-gateway |
| `sms_campaigns_rejected_total` | counter | `reason` (`insufficient_funds`\|`validation`) | Campaigns rejected | api-gateway |
| `sms_campaign_recipients_accepted_total` | counter | — | Recipients on accepted campaigns | api-gateway |
| `sms_campaigns_expanded_total` | counter | — | Campaigns expanded to Kafka | campaign-expander |
| `sms_campaign_messages_expanded_total` | counter | — | Per-recipient messages published | campaign-expander |

**How to use:** Lag between `sms_campaign_recipients_accepted_total` and `sms_campaign_messages_expanded_total` signals expander backlog.

### Dispatch & Express SLA

| Metric | Type | Labels | Meaning | Emitted by |
|---|---|---|---|---|
| `sms_dispatch_total` | counter | `mode`, `status`, `operator` | Final dispatch outcome | dispatcher |
| `sms_dispatch_latency_seconds` | histogram | `mode`, `priority` | Accept → dispatch wall time | dispatcher |
| `sms_express_sla_missed_total` | counter | — | Express hard deadline drops | dispatcher |
| `sms_operator_send_duration_seconds` | histogram | `operator`, `result` (`ok`\|`error`\|`skipped_sla`) | Operator HTTP send latency | dispatcher |

`status` values: `sent`, `failed`, `expired_sla_missed`.  
`mode`: `normal` \| `express`.

**How to use:** Express Tier-2 miss rate = `rate(sms_express_sla_missed_total[5m]) / rate(sms_dispatch_total{mode="express"}[5m])`. Histogram buckets go up to 120s to cover the 2-minute hard deadline.

### Reporting sink

| Metric | Type | Labels | Meaning | Emitted by |
|---|---|---|---|---|
| `sms_report_events_total` | counter | `status` | Events written to Postgres + ClickHouse | report-sink |

### Reconciler (safety net)

| Metric | Type | Labels | Meaning | Emitted by |
|---|---|---|---|---|
| `sms_reconciler_drift_total` | counter | `direction` (`redis_gt_ledger`\|`redis_lt_ledger`) | Detected Redis↔ledger divergence | reconciler |
| `sms_reconciler_heals_total` | counter | — | Auto-heals (Redis > ledger only) | reconciler |

**How to use:** Any sustained `redis_gt_ledger` is a pager-worthy free-credit risk (healed automatically). `redis_lt_ledger` is usually expected lag from async ledger writes—alert on growth, don't auto-heal.

---

## Technical / pipeline metrics

These answer ops questions: latency, duplicates, retries, HTTP health.

### HTTP API

| Metric | Type | Labels | Meaning | Emitted by |
|---|---|---|---|---|
| `sms_http_request_duration_seconds` | histogram | `method`, `path`, `status` | Handler wall time by route template | api-gateway |

`path` is a **low-cardinality template** (`/v1/messages`, `/v1/campaigns`, …), never raw URLs with IDs.

### Outbox → Kafka

| Metric | Type | Labels | Meaning | Emitted by |
|---|---|---|---|---|
| `sms_outbox_relayed_total` | counter | `priority` | Redis Stream entries published to Kafka | outbox-relay |
| `sms_outbox_relay_errors_total` | counter | `stage` (`publish`\|`ack`\|`claim_publish`) | Relay failures | outbox-relay |

**How to use:** Compare `rate(sms_messages_accepted_total)` (+ campaign expands) vs `rate(sms_outbox_relayed_total)` for outbox lag. Rising `claim_publish` suggests stale pending entries.

### Inbox idempotency

| Metric | Type | Labels | Meaning | Emitted by |
|---|---|---|---|---|
| `sms_inbox_processed_total` | counter | `consumer` | First-time successful Inbox commits | dispatcher, billing-*, report-sink |
| `sms_inbox_duplicates_total` | counter | `consumer` | Skipped already-processed events | same |
| `sms_consumer_handle_errors_total` | counter | `consumer` | Handle failures (no Kafka commit → retry) | same |

**How to use:** Duplicate ratio = duplicates / (processed+duplicates). Spikes after rebalance/redeploy are normal; sustained high error rate is not.

### Runtime (built-in)

| Metric family | Meaning |
|---|---|
| `go_*` | Goroutines, GC, memstats |
| `process_*` | RSS, CPU, FDs (where supported) |

---

## Example PromQL

```promql
# Accept TPS (single messages)
sum(rate(sms_messages_accepted_total[1m]))

# Insufficient-funds reject ratio
sum(rate(sms_messages_rejected_total{reason="insufficient_funds"}[5m]))
  /
sum(rate(sms_messages_accepted_total[5m]) + rate(sms_messages_rejected_total[5m]))

# Express p95 accept→dispatch latency
histogram_quantile(0.95, sum by (le) (rate(sms_dispatch_latency_seconds_bucket{mode="express"}[5m])))

# Outbox publish error rate
sum(rate(sms_outbox_relay_errors_total[5m]))
```

## Label cardinality rules

- Never label by `account_id`, `message_id`, phone number, or API key.
- Prefer fixed enums: priority, status, mode, operator name, reason, consumer name, HTTP path template.
