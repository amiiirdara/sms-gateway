-- name: CreateMessage :one
-- `created_at` is passed explicitly (not defaulted) so that retried Campaign
-- Expander runs are idempotent: the composite primary key is (id, created_at),
-- and every retry must reuse the same accepted_at timestamp for the same
-- deterministic message id, or ON CONFLICT would never match. See
-- ARCHITECTURE.md section 9.2 and .cursor/rules/kafka-consumer-conventions.mdc.
INSERT INTO messages (id, account_id, campaign_id, recipient, priority, cost, status, deadline_at, created_at)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
ON CONFLICT (id, created_at) DO NOTHING
RETURNING id, account_id, campaign_id, recipient, priority, cost, status, operator, deadline_at, dispatched_at, created_at, updated_at;

-- name: GetMessageByIDForAccount :one
SELECT id, account_id, campaign_id, recipient, priority, cost, status, operator, deadline_at, dispatched_at, created_at, updated_at
FROM messages
WHERE id = $1 AND account_id = $2;

-- name: UpdateMessageStatus :exec
UPDATE messages
SET status = $3, operator = $4, dispatched_at = $5, updated_at = now()
WHERE id = $1 AND created_at = $2;

-- name: InsertMessageStatusEvent :exec
INSERT INTO message_status_events (message_id, status, occurred_at)
VALUES ($1, $2, $3);

-- name: ListMessagesByCampaign :many
SELECT id, account_id, campaign_id, recipient, priority, cost, status, operator, deadline_at, dispatched_at, created_at, updated_at
FROM messages
WHERE campaign_id = $1 AND account_id = $2
ORDER BY created_at DESC
LIMIT $3 OFFSET $4;
