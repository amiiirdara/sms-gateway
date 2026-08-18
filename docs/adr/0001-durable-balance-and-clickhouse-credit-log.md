# Durable balance in Postgres, credit log in ClickHouse

Status: **accepted**

Prepaid credit today lives in three places with conflicting roles: Redis (live spend), Postgres `ledger_entries` (event log whose SUM is treated as ground truth), and `accounts.balance` (a cached SUM that nothing reads). We split the model into three named concepts — **live balance**, **durable balance**, and **credit log** — and assign each to the store that fits its job. **Durable balance** is the single mutated number in Postgres (`accounts.balance`). **Live balance** stays in Redis with the existing Lua accept path unchanged. **Credit log** is append-only topup/debit/refund history in ClickHouse, written asynchronously and never summed to produce a balance.

## Context

The accept path must stay sub-millisecond and atomic (Redis Lua + outbox). Durable catch-up for debits and refunds already runs asynchronously via Kafka and the billing consumer. The Postgres ledger was therefore always lagging live balance; reconciler and cold-start logic papered over that lag by treating `SUM(ledger_entries)` as truth and rewriting `accounts.balance` from it on every insert — a second derived copy that is never read by the balance API.

ClickHouse is already the analytics read side (`message_events`). Keeping the credit log in Postgres forces every durable debit/refund through a transactional SUM on a table that will grow without bound at 100M messages/day, while the number operators actually need for heal and cold-start is a single column per account.

## Decision

1. **Mutate durable balance directly.** Topup, debit catch-up, and refund increment or decrement `accounts.balance` in the same Postgres transaction as the Inbox check. No `SUM(ledger_entries)`. No Postgres credit log table.

2. **Append the credit log to ClickHouse after money commits.** Order: Inbox + durable Δ in one Postgres commit → ensure credit-log row (ClickHouse adapter). A ClickHouse outage must not block durable catch-up. On Inbox duplicate (Kafka retry), do not apply money again; ensure the log row, and on refund replay heal live balance from durable balance.

3. **Live balance rules unchanged for spend.** Accept and `GET /v1/balance` read live balance only. Refunds credit live balance only after the durable refund has committed. Lua remains the sole live debit mutator.

4. **Reconciler compares live vs durable balance — not a log SUM.**
   - Live > durable → **heal** (set live down).
   - Live key absent → **seed** from durable balance.
   - Live key present and live < durable → warn only (expected async lag; never grant credit upward).

5. **Inbox is the only money idempotency for debit and refund.** Partial unique indexes on `ledger_entries` are removed with the table. ClickHouse `ReplacingMergeTree` may collapse duplicate log rows after merge; it cannot enforce prepaid invariants.

6. **Topup requires `Idempotency-Key`.** The key is stored in Postgres in the same transaction as the durable increase so client retries cannot double-credit.

7. **Dedicated ClickHouse table for the credit log.** Do not fold topup/debit/refund into `message_events` — that table tracks delivery lifecycle, has no home for topup, and already computes a separate "refunded" notion from dispatch status.

8. **Deepen the billing module.** Durable apply, refund-then-live-credit, heal, seed, and cost live behind one billing interface. `cmd/billing-consumer` unmarshals Kafka and calls billing; callers do not pass a Postgres TX handle or reference ClickHouse. Cost is owned by billing and passed into messaging/campaigns accept (no cross-domain import).

9. **Campaigns unchanged at the money boundary.** All-or-nothing live reservation; per-message durable debit and refund; no campaign-level credit-log row.

## Considered options

| Option | Why rejected |
|---|---|
| Keep Postgres `ledger_entries`, stop SUM-ing, use `accounts.balance` as truth | Leaves two credit logs once ClickHouse is added; the Postgres table still grows on the write path for no benefit. |
| Derive durable balance from `SUM(credit_log)` in ClickHouse | ClickHouse is eventually consistent and not transactional with Postgres Inbox. Healing or seeding from a SUM reintroduces the lag and free-credit risks we are removing. |
| Write credit log and durable Δ in one step before Inbox | ClickHouse blip blocks billing catch-up and widens live–durable lag. |
| Fold credit events into `message_events` | No topup rows; mixes prepaid with delivery reporting; campaign aggregates already invent refunds from status. |
| Strangle: dual-write Postgres ledger and ClickHouse log during migration | Two histories, fake seam, SUM smell persists. One cut with backfill/verify of durable balance. |
| Reconciler auto-heals live up toward durable | Can invent free credit; masks real bugs. |

## Consequences

**Positive**

- One durable number per account; heal and seed read a column, not an aggregate query.
- Postgres write path on billing catch-up shrinks (no ledger insert + SUM + cache update per event).
- Credit log ingest scales on ClickHouse, aligned with the existing CQRS reporting side.
- Billing module becomes the single seam for durable money; tests hit one interface with an in-memory credit-log adapter.

**Negative / constraints**

- **Supersedes** the previous ARCHITECTURE.md story that treated a Postgres credit ledger SUM as ground truth (§3, §5.3, §6). Those sections now describe live balance, durable balance, and the ClickHouse credit log.
- Credit log is **eventually** present in ClickHouse; audit queries must tolerate brief lag after a committed durable change.
- Live balance can legitimately sit below durable balance while debits catch up; reconciler warnings on that direction remain expected, not bugs.
- Topup API contract gains a required idempotency header (OpenAPI: `POST /v1/topups` requires `Idempotency-Key`).
- Migration must backfill `accounts.balance` from the existing ledger SUM (or verify it already matches) before dropping `ledger_entries`.

**Out of scope for this ADR**

- HTTP auth still resolving accounts via sqlc (`billing.Queries()`); a separate auth seam if needed later.
- Changes to `message_events` schema or reporting-api read paths beyond any new credit-log query surface.

## Vocabulary

Domain terms are defined in [CONTEXT.md](../../CONTEXT.md): live balance, durable balance, credit log, heal, seed.
