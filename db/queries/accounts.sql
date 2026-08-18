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
UPDATE accounts
SET balance = $2, updated_at = now()
WHERE id = $1
RETURNING id, api_key_hash, name, balance, created_at, updated_at;

-- name: AddAccountBalance :one
UPDATE accounts
SET balance = balance + $2, updated_at = now()
WHERE id = $1
RETURNING id, api_key_hash, name, balance, created_at, updated_at;

-- name: GetAccountBalance :one
SELECT balance
FROM accounts
WHERE id = $1;

-- name: ListAccountIDs :many
-- Keyset pagination for reconciler sweeps (pass uuid.Nil for the first page).
SELECT id FROM accounts
WHERE id > $1
ORDER BY id
LIMIT $2;
