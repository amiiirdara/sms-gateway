-- Initial schema. See ARCHITECTURE.md section 11 for the full data model rationale.

CREATE EXTENSION IF NOT EXISTS pgcrypto; -- for gen_random_uuid()

CREATE TABLE accounts (
    id             uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    api_key_hash   text NOT NULL UNIQUE,
    name           text NOT NULL,
    balance        bigint NOT NULL DEFAULT 0, -- cached projection of SUM(ledger_entries.amount); ledger is ground truth
    created_at     timestamptz NOT NULL DEFAULT now(),
    updated_at     timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE ledger_entries (
    id             uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    account_id     uuid NOT NULL REFERENCES accounts (id),
    type           text NOT NULL CHECK (type IN ('topup', 'debit', 'refund')),
    amount         bigint NOT NULL, -- positive for topup/refund, negative for debit
    message_id     uuid, -- nullable: topups have no associated message
    created_at     timestamptz NOT NULL DEFAULT now(),
    updated_at     timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX idx_ledger_entries_account_id ON ledger_entries (account_id, created_at);

CREATE TABLE campaigns (
    id                 uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    account_id         uuid NOT NULL REFERENCES accounts (id),
    text               text NOT NULL,
    total_recipients   integer NOT NULL,
    cost_per_message   bigint NOT NULL,
    total_cost         bigint NOT NULL,
    status             text NOT NULL, -- accepted | expanded | failed
    created_at         timestamptz NOT NULL DEFAULT now(),
    updated_at         timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX idx_campaigns_account_id ON campaigns (account_id, created_at);

-- Partitioned by created_at (day/month partitions created ahead of time by an ops job,
-- e.g. pg_partman or a scheduled migration - a DEFAULT partition catches anything
-- outside pre-created ranges so writes never fail).
CREATE TABLE messages (
    id             uuid NOT NULL DEFAULT gen_random_uuid(),
    account_id     uuid NOT NULL REFERENCES accounts (id),
    campaign_id    uuid REFERENCES campaigns (id),
    recipient      text NOT NULL,
    priority       text NOT NULL CHECK (priority IN ('normal', 'express')),
    cost           bigint NOT NULL,
    status         text NOT NULL, -- accepted | queued | dispatched | sent | failed | expired_sla_missed
    operator       text,
    deadline_at    timestamptz, -- set only for express messages
    dispatched_at  timestamptz,
    created_at     timestamptz NOT NULL DEFAULT now(),
    updated_at     timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (id, created_at)
) PARTITION BY RANGE (created_at);

CREATE TABLE messages_default PARTITION OF messages DEFAULT;

CREATE INDEX idx_messages_account_id ON messages (account_id, created_at);
CREATE INDEX idx_messages_campaign_id ON messages (campaign_id, created_at) WHERE campaign_id IS NOT NULL;

CREATE TABLE message_status_events (
    id             uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    message_id     uuid NOT NULL,
    status         text NOT NULL,
    occurred_at    timestamptz NOT NULL,
    created_at     timestamptz NOT NULL DEFAULT now(),
    updated_at     timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX idx_message_status_events_message_id ON message_status_events (message_id, occurred_at);

-- Inbox pattern dedup table - see ARCHITECTURE.md section 5.2.
CREATE TABLE processed_events (
    consumer_name  text NOT NULL,
    event_id       text NOT NULL,
    processed_at   timestamptz NOT NULL DEFAULT now(),
    created_at     timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (consumer_name, event_id)
);
