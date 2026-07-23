-- name: MarkEventProcessed :one
-- Returns false (no row) if the event was already processed - caller should
-- skip the side effect but still commit the Kafka offset. Must run in the same
-- transaction as the business write it guards. See ARCHITECTURE.md section 5.2.
INSERT INTO processed_events (consumer_name, event_id)
VALUES ($1, $2)
ON CONFLICT (consumer_name, event_id) DO NOTHING
RETURNING consumer_name, event_id, processed_at;
