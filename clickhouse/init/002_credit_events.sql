-- ClickHouse append-only credit history (topup, debit, refund).
-- Written asynchronously from billing; never SUMmed to produce a live balance.
--
-- ReplacingMergeTree(event_time) collapses duplicate keys after merges:
-- debit/refund by (account_id, type, message_id); topup by (account_id, type, idempotency_key).
-- The client also EnsureCreditEvent-checks before insert for immediate idempotency.

CREATE DATABASE IF NOT EXISTS sms_gateway;

CREATE TABLE IF NOT EXISTS sms_gateway.credit_events
(
    event_time      DateTime64(3),
    account_id      UUID,
    type            LowCardinality(String), -- topup | debit | refund
    amount          Int64,
    message_id      Nullable(UUID),
    idempotency_key Nullable(String)
)
ENGINE = ReplacingMergeTree(event_time)
PARTITION BY toYYYYMM(event_time)
ORDER BY (
    account_id,
    type,
    ifNull(message_id, toUUID('00000000-0000-0000-0000-000000000000')),
    ifNull(idempotency_key, '')
);
