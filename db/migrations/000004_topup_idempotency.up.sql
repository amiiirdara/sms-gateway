CREATE TABLE topup_idempotency (
    account_id        uuid NOT NULL REFERENCES accounts(id),
    idempotency_key   text NOT NULL,
    amount            bigint NOT NULL,
    durable_balance   bigint NOT NULL DEFAULT 0,
    created_at        timestamptz NOT NULL DEFAULT now(),
    updated_at        timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (account_id, idempotency_key)
);
