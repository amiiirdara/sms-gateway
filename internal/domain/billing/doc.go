// Package billing owns the credit ledger: topups, debits, refunds, and the
// reconciliation safety net. See ARCHITECTURE.md sections 5 and 6.
//
// Planned contents:
//   - Wallet service: wraps the Redis CheckAndDebit Lua call (platform/redis)
//     used by cmd/api-gateway.
//   - Ledger repository: wraps db/queries/ledger_entries.sql.
//   - Billing consumer handler: consumed by cmd/billing-consumer, writes debit
//     entries on accepted events and refund entries on failure/expired_sla_missed.
//   - Reconciler logic: consumed by cmd/reconciler.
package billing
