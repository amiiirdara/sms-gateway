-- name: TryInsertTopupIdempotency :one
INSERT INTO topup_idempotency (account_id, idempotency_key, amount, durable_balance)
VALUES ($1, $2, $3, 0)
ON CONFLICT (account_id, idempotency_key) DO NOTHING
RETURNING account_id, idempotency_key, amount, durable_balance, created_at, updated_at;

-- name: GetTopupIdempotency :one
SELECT account_id, idempotency_key, amount, durable_balance, created_at, updated_at
FROM topup_idempotency
WHERE account_id = $1 AND idempotency_key = $2;

-- name: SetTopupIdempotencyBalance :exec
UPDATE topup_idempotency
SET durable_balance = $3, updated_at = now()
WHERE account_id = $1 AND idempotency_key = $2;
