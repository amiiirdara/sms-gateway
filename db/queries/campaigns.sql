-- name: CreateCampaign :one
INSERT INTO campaigns (id, account_id, text, total_recipients, cost_per_message, total_cost, status)
VALUES ($1, $2, $3, $4, $5, $6, $7)
ON CONFLICT (id) DO NOTHING
RETURNING id, account_id, text, total_recipients, cost_per_message, total_cost, status, created_at, updated_at;

-- name: GetCampaignByID :one
SELECT id, account_id, text, total_recipients, cost_per_message, total_cost, status, created_at, updated_at
FROM campaigns
WHERE id = $1 AND account_id = $2;

-- name: ListCampaignsByAccount :many
SELECT id, account_id, text, total_recipients, cost_per_message, total_cost, status, created_at, updated_at
FROM campaigns
WHERE account_id = $1
ORDER BY created_at DESC
LIMIT $2 OFFSET $3;

-- name: UpdateCampaignStatus :exec
UPDATE campaigns
SET status = $2, updated_at = now()
WHERE id = $1;
