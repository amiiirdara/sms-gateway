-- name: CreateAccount :one
INSERT INTO accounts (api_key_hash, name, balance)
VALUES ($1, $2, 0)
RETURNING id, api_key_hash, name, balance, created_at, updated_at;

-- name: GetAccountByAPIKeyHash :one
SELECT id, api_key_hash, name, balance, created_at, updated_at
FROM accounts
WHERE api_key_hash = $1;

-- name: GetAccountByID :one
SELECT id, api_key_hash, name, balance, created_at, updated_at
FROM accounts
WHERE id = $1;

-- name: UpdateAccountBalance :one
-- Used only by the reconciler / cold-start bootstrap to align the Postgres-cached
-- balance with SUM(ledger_entries). The hot-path debit/credit itself happens in
-- Redis (see ARCHITECTURE.md section 5) and is durably recorded via ledger_entries.
UPDATE accounts
SET balance = $2, updated_at = now()
WHERE id = $1
RETURNING id, api_key_hash, name, balance, created_at, updated_at;
