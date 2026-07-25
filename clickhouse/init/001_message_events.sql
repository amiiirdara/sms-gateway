-- ClickHouse reporting store. See ARCHITECTURE.md sections 5, 9, 11.
-- Populated by the Report Sink consumer from `sms.dispatch-results`;
-- never written to synchronously from the API.
--
-- ReplacingMergeTree(event_time) collapses duplicate (message_id, status)
-- rows from consumer retries after background merges. report-sink also
-- EnsureMessageEvent-checks before insert for immediate idempotency.

CREATE DATABASE IF NOT EXISTS sms_gateway;

CREATE TABLE IF NOT EXISTS sms_gateway.message_events
(
    event_time   DateTime64(3),
    message_id   UUID,
    account_id   UUID,
    campaign_id  Nullable(UUID),
    recipient    String,
    priority     LowCardinality(String), -- 'normal' | 'express'
    status       LowCardinality(String), -- accepted | queued | dispatched | sent | failed | expired_sla_missed
    cost         Int64,
    operator     LowCardinality(String)
)
ENGINE = ReplacingMergeTree(event_time)
PARTITION BY toYYYYMM(event_time)
ORDER BY (account_id, message_id, status)
TTL toDateTime(event_time) + INTERVAL 2 YEAR;
