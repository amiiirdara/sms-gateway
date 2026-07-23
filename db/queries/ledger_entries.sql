-- name: InsertLedgerEntry :one
INSERT INTO ledger_entries (account_id, type, amount, message_id)
VALUES ($1, $2, $3, $4)
RETURNING id, account_id, type, amount, message_id, created_at, updated_at;

-- name: SumLedgerEntriesByAccount :one
-- The ground truth for an account's balance - used by the reconciler
-- and by cold-start Redis bootstrap (ARCHITECTURE.md section 5.3).
SELECT COALESCE(SUM(amount), 0)::bigint AS balance
FROM ledger_entries
WHERE account_id = $1;

-- name: ListLedgerEntriesByAccount :many
SELECT id, account_id, type, amount, message_id, created_at, updated_at
FROM ledger_entries
WHERE account_id = $1
ORDER BY created_at DESC
LIMIT $2 OFFSET $3;
