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
-- Aligns Postgres-cached balance with SUM(ledger_entries). Hot-path debit/credit
-- is Redis (ARCHITECTURE.md section 5); ledger_entries is durable ground truth.
UPDATE accounts
SET balance = $2, updated_at = now()
WHERE id = $1
RETURNING id, api_key_hash, name, balance, created_at, updated_at;

-- name: ListAccountIDs :many
-- Keyset pagination for reconciler sweeps (pass uuid.Nil for the first page).
SELECT id FROM accounts
WHERE id > $1
ORDER BY id
LIMIT $2;
